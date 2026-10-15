# frozen_string_literal: true

module Pgbus
  module MCP
    # A turnkey, gated Rack app that serves the read-only pgbus diagnostic MCP
    # server over HTTP. Mount it inside your existing Rails app — no second
    # process, same DB credentials, same connection pool:
    #
    #   # config/routes.rb
    #   mount Pgbus::MCP.rack_app(token: ENV["PGBUS_MCP_TOKEN"]) => "/pgbus/mcp"
    #
    # The underlying transport runs in stateless + JSON-response mode, which:
    #   * makes every request a self-contained POST returning a single JSON
    #     object (no in-memory session, no SSE stream, no reaper thread), so it
    #     is safe behind multiple Puma/Falcon workers — any worker can answer
    #     any request;
    #   * is exactly what a read-only diagnostic server needs (it never pushes
    #     server-initiated messages to the client).
    #
    # Security: requests are rejected with 401 unless they carry the configured
    # token (or pass the supplied auth callable). Run it on an internal network
    # / behind your VPN, never internet-exposed.
    #
    # DNS-rebinding protection: since mcp 0.23 the transport validates the Host
    # header (loopback only, by default) and the Origin header (same-origin
    # only). That defends a server bound to localhost against a browser page
    # whose DNS name was re-pointed at 127.0.0.1 — a page that can carry no
    # bearer token, because the secret never reaches the attacker's origin. So
    # when this app is gated (+token+ or +auth+) the check is redundant, and
    # left on it rejects every request to a real hostname
    # (https://app.example.com/pgbus/mcp → 403 "Invalid Host header"). The
    # default therefore follows the gate: off when gated, on when unauthenticated.
    # Override with +dns_rebinding_protection:+, and widen the accepted hosts /
    # origins with +allowed_hosts:+ / +allowed_origins:+ when the check is on.
    class RackApp
      BEARER_PREFIX = "Bearer "
      # First mcp release with the transport's allowed_hosts / allowed_origins /
      # dns_rebinding_protection options. Older gems would raise ArgumentError
      # on the pass-through; fail with the fix spelled out instead.
      MIN_MCP_VERSION = Gem::Version.new("0.23.0").freeze
      # Only the JSON body string is frozen and reused. The outer response triple
      # and its headers hash MUST be built fresh per call (#unauthorized) so
      # downstream Rack middleware can mutate them — Rack::TempfileReaper assigns
      # response[2] and Rack::ETag adds headers. Returning a frozen array/hash
      # raised FrozenError → 500 instead of 401 (issue #304).
      UNAUTHORIZED_BODY = { jsonrpc: "2.0", id: nil, error: { code: -32_001, message: "Unauthorized" } }.to_json.freeze

      # @param data_source [Pgbus::Web::DataSource] read layer the tools query.
      #   Built once and shared: DataSource acquires Pgbus::BusRecord.connection
      #   from the ActiveRecord pool on each call, so a single instance is
      #   request-safe across threads/workers.
      # @param allow_payloads [Boolean] when true, tools honor a per-call
      #   include_payloads flag; otherwise message bodies are always redacted.
      # @param token [String, nil] shared secret. When present, requests must
      #   send `Authorization: Bearer <token>`. nil disables the built-in gate.
      # @param auth [#call, nil] custom authenticator taking a Rack::Request and
      #   returning truthy to allow. Mirrors Pgbus.configuration.web_auth. Takes
      #   precedence over +token+ when both are given.
      # @param allowed_hosts [Array<String>, nil] extra Host values the
      #   transport accepts beyond loopback when DNS-rebinding protection is on;
      #   a bare name matches any port, "host:port" matches exactly.
      # @param allowed_origins [Array<String>, nil] extra Origin values accepted
      #   beyond same-origin when DNS-rebinding protection is on.
      # @param dns_rebinding_protection [Boolean, nil] nil (default) = on only
      #   when the app is unauthenticated; true/false forces it. See the class
      #   docs for why the gate makes the check redundant.
      def initialize(data_source: Pgbus::Web::DataSource.new, allow_payloads: false, token: nil, auth: nil,
                     allowed_hosts: nil, allowed_origins: nil, dns_rebinding_protection: nil)
        check_mcp_version!
        @token = token
        @auth = auth
        @server = Server.build(data_source: data_source, allow_payloads: allow_payloads)
        @transport = ::MCP::Server::Transports::StreamableHTTPTransport.new(
          @server,
          stateless: true, enable_json_response: true,
          allowed_hosts: allowed_hosts, allowed_origins: allowed_origins,
          dns_rebinding_protection: dns_rebinding_protection.nil? ? unauthenticated? : dns_rebinding_protection
        )
        warn_unauthenticated! if unauthenticated?
      end

      # Mount THIS object, never the bare transport. The auth gate lives here
      # and runs before handle_request for *every* HTTP method (POST/GET/
      # DELETE/...). Mounting @transport directly would skip authorization —
      # the gem's transport has no auth of its own.
      def call(env)
        request = Rack::Request.new(env)
        return unauthorized unless authorized?(request)

        @transport.handle_request(request)
      end

      private

      # A fresh, mutable 401 response triple each call. See UNAUTHORIZED_BODY.
      # Header keys are lowercase per the Rack 3 SPEC (matches stream_app.rb and
      # prometheus_exporter.rb); Rack::Headers looks them up case-insensitively.
      def unauthorized
        [
          401,
          { "content-type" => "application/json", "www-authenticate" => "Bearer" },
          [UNAUTHORIZED_BODY]
        ]
      end

      # An explicit auth callable wins; otherwise fall back to the bearer token;
      # otherwise (neither configured) allow — the constructor already warned.
      def authorized?(request)
        return !!@auth.call(request) if @auth
        return bearer_matches?(request) if @token

        true
      end

      def bearer_matches?(request)
        header = request.get_header("HTTP_AUTHORIZATION").to_s
        return false unless header.start_with?(BEARER_PREFIX)

        Runner.secure_compare?(@token, header.delete_prefix(BEARER_PREFIX))
      end

      def unauthenticated?
        @token.nil? && @auth.nil?
      end

      def warn_unauthenticated!
        Pgbus.logger.warn do
          "[Pgbus::MCP] HTTP diagnostic server mounted without authentication. " \
            "Pass token: or auth: to Pgbus::MCP.rack_app, and keep it on an internal network."
        end
      end

      def check_mcp_version!
        installed = Gem::Version.new(::MCP::VERSION)
        return if installed >= MIN_MCP_VERSION

        raise Pgbus::Error,
              "Pgbus::MCP.rack_app requires mcp >= #{MIN_MCP_VERSION} (the transport's DNS-rebinding " \
              "options); mcp #{installed} is installed. Run `bundle update mcp`."
      end
    end

    module_function

    # Build a gated Rack app serving the read-only diagnostic tools over HTTP.
    # See {RackApp} for the parameters and deployment guidance.
    def rack_app(data_source: Pgbus::Web::DataSource.new, allow_payloads: false, token: nil, auth: nil,
                 allowed_hosts: nil, allowed_origins: nil, dns_rebinding_protection: nil)
      RackApp.new(data_source: data_source, allow_payloads: allow_payloads, token: token, auth: auth,
                  allowed_hosts: allowed_hosts, allowed_origins: allowed_origins,
                  dns_rebinding_protection: dns_rebinding_protection)
    end
  end
end
