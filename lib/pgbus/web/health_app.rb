# frozen_string_literal: true

require "json"

module Pgbus
  module Web
    # Plain Rack app exposing HTTP liveness and readiness probes for
    # orchestrators (Kubernetes kubelet, load balancers). Mount it in the host
    # Rails app — no auth, no DB access on the liveness path:
    #
    #   # config/routes.rb
    #   mount Pgbus::Web::HealthApp.new => "/pgbus/health"
    #
    #   # kubelet
    #   livenessProbe:  { httpGet: { path: /pgbus/health/livez, port: 3000 } }
    #   readinessProbe: { httpGet: { path: /pgbus/health/readyz, port: 3000 } }
    #
    # The supervisor process can also serve these two paths standalone via
    # {Pgbus::Web::HealthServer} when `config.health_port` is set — that path
    # synthesizes a minimal Rack env and calls this same #call, so the routing
    # and verdict logic live in exactly one place.
    #
    # Routing (env-only, no Rack::Request so it works under the bare-socket
    # HealthServer too):
    #   GET  /livez  → 200 text/plain "ok", unconditionally, NO database access
    #                  (the serving process is up — that is all liveness means).
    #   GET  /readyz → build a DataSource, run MCP::HealthAnalyzer#verdict:
    #                    OK / DEGRADED → 200, STALLED → 503, body = verdict JSON.
    #                  Any StandardError (DB unreachable) → 503 {"status":"ERROR"},
    #                  logged via Pgbus.logger (never swallowed).
    #   unknown path → 404; non-GET on a known path → 405.
    class HealthApp
      LIVEZ = "/livez"
      READYZ = "/readyz"
      KNOWN_PATHS = [LIVEZ, READYZ].freeze
      private_constant :KNOWN_PATHS

      TEXT = { "Content-Type" => "text/plain" }.freeze
      JSON_HEADERS = { "Content-Type" => "application/json" }.freeze
      private_constant :TEXT, :JSON_HEADERS

      # DEGRADED is deliberately "ready": the serving process can still answer,
      # and flapping a pod out of rotation on a transient DEGRADED (a stale
      # sibling, a paused queue) would remove capacity for no gain. Only STALLED
      # — the silent-wedge signal — fails readiness.
      READY_STATUSES = %w[OK DEGRADED].freeze
      private_constant :READY_STATUSES

      # @param data_source [Pgbus::Web::DataSource, nil] read layer for /readyz.
      #   nil (the default) builds a fresh DataSource per readiness check, which
      #   avoids serving stale metrics from a long-lived app's memoized instance.
      # @param local_readiness [#call, nil] when set, /readyz answers from this
      #   callable's {Process::ReadinessSnapshot} instead of the cluster-wide
      #   analyzer — the supervisor's standalone HealthServer passes its own
      #   snapshot so a rolling deploy's health gate measures THIS container,
      #   not the fleet (issue #386). The Rails-mounted app leaves it nil and
      #   keeps the cluster verdict.
      def initialize(data_source: nil, local_readiness: nil)
        @data_source = data_source
        @local_readiness = local_readiness
      end

      def call(env)
        path = env["PATH_INFO"].to_s
        return not_found unless KNOWN_PATHS.include?(path)
        return method_not_allowed unless env["REQUEST_METHOD"] == "GET"

        path == LIVEZ ? livez : readyz
      end

      private

      # Liveness: the process is running. No DB, no analyzer, no config lookups.
      def livez
        [200, TEXT.dup, ["ok"]]
      end

      def readyz
        return local_readyz if @local_readiness

        # HealthAnalyzer lives in the MCP namespace, which is excluded from
        # Zeitwerk (its *tools* subclass the optional `mcp` gem). The analyzer
        # itself has no gem dependency, so require just that one file — the
        # standalone health server must work without the `mcp` gem installed.
        require "pgbus/mcp/health_analyzer" unless defined?(Pgbus::MCP::HealthAnalyzer)

        verdict = Pgbus::MCP::HealthAnalyzer.new(data_source_for_check).verdict
        status = READY_STATUSES.include?(verdict[:status].to_s) ? 200 : 503
        [status, JSON_HEADERS.dup, [verdict.to_json]]
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web::HealthApp] readiness check failed: #{e.class}: #{e.message}" }
        [503, JSON_HEADERS.dup, [{ status: "ERROR", error: e.message }.to_json]]
      end

      # Container-local readiness: no database, no analyzer — just the
      # supervisor's published snapshot. The error path mirrors the cluster
      # readyz: 503 ERROR, logged, never swallowed.
      def local_readyz
        snapshot = @local_readiness.call
        status = snapshot.ready? ? 200 : 503
        body = { status: snapshot.status, expected: snapshot.expected, live: snapshot.live }
        [status, JSON_HEADERS.dup, [body.to_json]]
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web::HealthApp] local readiness check failed: #{e.class}: #{e.message}" }
        [503, JSON_HEADERS.dup, [{ status: "ERROR", error: e.message }.to_json]]
      end

      # Reuse an injected DataSource (tests, an app that wants one shared
      # instance), dropping its per-instance memos first; otherwise build a
      # fresh one each check. Either way no check sees the last one's queue,
      # process, stream or table metrics.
      def data_source_for_check
        return DataSource.new unless @data_source

        @data_source.reset_cache! if @data_source.respond_to?(:reset_cache!)
        @data_source
      end

      def not_found
        [404, TEXT.dup, ["pgbus: not found"]]
      end

      def method_not_allowed
        [405, TEXT.merge("Allow" => "GET"), ["pgbus: method not allowed"]]
      end
    end
  end
end
