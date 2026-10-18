# frozen_string_literal: true

require "active_support/security_utils"

module Pgbus
  module MCP
    # Boots the pgbus diagnostic MCP server over stdio. This is what
    # `pgbus mcp` invokes. Separated from Server so the server assembly stays
    # transport-agnostic and unit-testable without touching $stdin/$stdout.
    #
    # Security posture (issue #180):
    #   * Never starts unless explicitly invoked (`pgbus mcp`).
    #   * Reuses the app's existing DB connection/config — no new privileged
    #     path is opened.
    #   * Payloads are redacted unless PGBUS_MCP_ALLOW_PAYLOADS is truthy.
    #   * Optional token gate: when PGBUS_MCP_TOKEN is set, the same value must
    #     be present in PGBUS_MCP_AUTH_TOKEN at boot or the server refuses to
    #     start. stdio is a local, parent-spawned channel, so the gate is a
    #     boot-time precondition rather than a per-message header.
    module Runner
      TRUTHY = %w[1 true yes on].freeze

      module_function

      # Build the server, enforce the optional token gate, and drive the stdio
      # transport until the client disconnects.
      #
      # @param env [Hash] environment to read flags from (injectable for tests).
      def run(env: ENV)
        authorize!(env)

        allow_payloads = truthy?(env["PGBUS_MCP_ALLOW_PAYLOADS"])
        server = Server.build(allow_payloads: allow_payloads)
        transport = ::MCP::Server::Transports::StdioTransport.new(server)
        log_start(allow_payloads)
        transport.open
      end

      # Raise unless the configured token matches. No-op when PGBUS_MCP_TOKEN
      # is unset (token gating is opt-in).
      def authorize!(env)
        expected = env["PGBUS_MCP_TOKEN"]
        return if expected.nil? || expected.empty?

        provided = env["PGBUS_MCP_AUTH_TOKEN"]
        return if secure_compare?(expected, provided)

        raise Pgbus::Error,
              "pgbus MCP: authentication failed. PGBUS_MCP_TOKEN is set; provide a matching PGBUS_MCP_AUTH_TOKEN."
      end

      def truthy?(value)
        TRUTHY.include?(value.to_s.strip.downcase)
      end

      # Constant-time comparison to avoid leaking the token via timing.
      # Delegates to ActiveSupport's vetted implementation (railties already
      # pulls in active_support). secure_compare handles unequal lengths
      # safely — it compares fixed-length SHA256 digests, then verifies the
      # raw values match — so it never raises on a length mismatch.
      def secure_compare?(expected, provided)
        return false if provided.nil?

        ActiveSupport::SecurityUtils.secure_compare(expected, provided)
      end

      def log_start(allow_payloads)
        mode = allow_payloads ? "payloads ALLOWED" : "payloads redacted"
        kind = Server.write_tools? ? "diagnostic server with the workflow write tools ON (mcp_write_tools)" : "read-only diagnostic server"
        Pgbus.logger.info { "[Pgbus::MCP] starting #{kind} over stdio (#{mode})" }
      end
    end
  end
end
