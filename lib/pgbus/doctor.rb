# frozen_string_literal: true

require "uri"
require "pgbus/mcp/health_analyzer"

module Pgbus
  # Preflight diagnostics for a pgbus deployment — the single command that
  # answers "is this environment healthy enough to run?". Runs twelve checks and
  # returns a machine-readable result plus a human report, so `pgbus doctor`
  # and `rake pgbus:doctor` can gate a deploy or CI run (exit 0 on success,
  # 1 on any failure).
  #
  # Doctor probes through Pgbus::Client (DB, PGMQ schema, queues, NOTIFY),
  # Pgbus::Web::DataSource (process liveness, via Pgbus::MCP::HealthAnalyzer),
  # or Pgbus::DedicatedConnection (the streamer/notify-listener check — a
  # deliberate exception to "everything via Client": those runtime paths
  # bypass the Client's pools, so probing via the Client cannot catch a
  # broken dedicated path; see issue #352). It never raises — a broken
  # environment turns into :fail results, never a crash — so it is safe to
  # run against a database that is down or a half-installed schema.
  class Doctor
    # A single check result. status is one of :ok, :warn, :fail.
    Check = Struct.new(:name, :status, :detail) do
      def to_h
        { name: name, status: status, detail: detail }
      end
    end

    STATUS_ICON = { ok: "✓", warn: "!", fail: "✗" }.freeze

    # The ordered check suite, name → method. The name strings are the public,
    # stable identity of each check (used in the report and to select subsets).
    CHECKS = {
      "Configuration" => :check_configuration,
      "Ruby JIT" => :check_ruby_jit,
      "Database" => :check_database,
      "PGMQ schema" => :check_pgmq_schema,
      "Queues" => :check_queues,
      "LISTEN/NOTIFY" => :check_notify,
      "Process liveness" => :check_processes,
      "GlobalID allowlist" => :check_allowed_global_id_models,
      "Broadcast queue" => :check_broadcast_queue,
      "Primary affinity" => :check_primary,
      "Dedicated connections" => :check_dedicated_connections,
      "Connection budget" => :check_connection_budget
    }.freeze

    # Process liveness reads the pgbus_processes table (via HealthAnalyzer), so
    # it is only meaningful once workers have registered. The supervisor boot
    # preflight (issue #347) runs BEFORE forking any worker, so it excludes this
    # check — otherwise stale prior-generation worker rows plus a visible backlog
    # would produce a false STALLED verdict on a redeploy.
    BOOT_SKIP = ["Process liveness"].freeze

    # The subset of checks whose :fail is genuinely deploy-fatal AND not a
    # transient the supervisor is designed to ride out. Only these abort a
    # `:strict` boot. Configuration#validate! failing is a real config bug;
    # an absent PGMQ schema means the migrations never ran. Deliberately NOT
    # Queues/Database: the lenient queue bootstrap swallows a boot-time DB blip
    # so children crash-and-backoff and recover, and verify_connection! already
    # gated a hard-down DB before the preflight — making either strict-fatal
    # would turn a tolerated transient into a fleet-wide lockstep boot abort.
    STRICT_FATAL = ["Configuration", "PGMQ schema"].freeze

    def initialize(config: Pgbus.configuration, client: Pgbus.client, data_source: nil)
      @config = config
      @client = client
      @data_source = data_source
    end

    # Run all checks and return an array of result hashes:
    #   { name:, status: :ok|:warn|:fail, detail: }
    def run
      @run ||= run_checks(CHECKS.keys)
    end

    # True when no check failed. Warnings do not fail the run — they surface a
    # concern (e.g. an outdated PGMQ schema) without blocking a deploy.
    def success?
      run.none? { |c| c[:status] == :fail }
    end

    # --- Supervisor boot preflight (issue #347) ---

    # The checks safe to run inside the booting supervisor before any worker is
    # forked: everything except the worker-dependent process-liveness check.
    # Runs a genuine SUBSET — check_processes is never invoked — so there is no
    # pre-fork HealthAnalyzer/DataSource round-trip.
    def boot_checks
      @boot_checks ||= run_checks(CHECKS.keys - BOOT_SKIP)
    end

    # True unless a strict-fatal check (see STRICT_FATAL) failed. Warnings and
    # transient-shaped failures (Queues, Database) never block a `:strict` boot.
    def boot_ok?
      boot_checks.none? { |c| c[:status] == :fail && STRICT_FATAL.include?(c[:name]) }
    end

    # Human-readable report for the boot preflight — the boot_checks subset.
    def boot_report
      report(boot_checks)
    end

    # Human-readable report: one line per check, then a resolved-config summary
    # with passwords redacted. Suitable for stdout in the CLI and rake task.
    # Defaults to the full run; pass a filtered result array (e.g. boot_checks)
    # to render a subset.
    def report(checks = run)
      lines = ["Pgbus Doctor", "=" * 40]
      checks.each do |check|
        icon = STATUS_ICON.fetch(check[:status], "?")
        lines << format("%<icon>s %<name>-22s %<detail>s", icon: icon, name: check[:name], detail: check[:detail])
      end
      lines << ""
      lines << "Configuration"
      lines << ("-" * 40)
      config_summary.each { |key, value| lines << format("  %<key>-20s %<value>s", key: key, value: value) }
      lines.join("\n")
    end

    # Resolved-config summary for the report. Password-bearing fields
    # (database_url, connection_params) are redacted.
    def config_summary
      {
        queue_prefix: @config.queue_prefix,
        default_queue: @config.default_queue,
        pgmq_schema_mode: @config.pgmq_schema_mode,
        resolved_pool_size: safe { @config.resolved_pool_size },
        listen_notify: @config.listen_notify,
        roles: @config.roles || "all",
        capsules: capsule_summary,
        database_url: redacted_database_url,
        connection_params: redacted_connection_params
      }.compact
    end

    private

    # Run the named checks in CHECKS order and return an array of result hashes.
    # Only the requested checks are INVOKED — a check omitted from `names` does
    # no work (e.g. boot_checks never calls check_processes, so no HealthAnalyzer
    # round-trip pre-fork).
    def run_checks(names)
      CHECKS.filter_map { |name, method| send(method).to_h if names.include?(name) }
    end

    # The dashboard data source backing the process-liveness check. Built lazily
    # so the boot preflight (which excludes that check) never constructs a
    # DataSource or touches the dashboard layer at the fork boundary.
    def data_source
      @data_source ||= Pgbus::Web::DataSource.new(client: @client)
    end

    # True in a Rails production environment. Guarded so the doctor still runs
    # outside Rails (plain Ruby, tests) without assuming Rails is loaded.
    def production?
      defined?(Rails) && Rails.respond_to?(:env) && Rails.env.production?
    end

    # 1. Configuration validity.
    def check_configuration
      @config.validate!
      Check.new(name: "Configuration", status: :ok, detail: "valid")
    rescue Pgbus::ConfigurationError, ArgumentError => e
      Check.new(name: "Configuration", status: :fail, detail: e.message)
    rescue StandardError => e
      Check.new(name: "Configuration", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 1b. Ruby JIT (issue #484). With Postgres close by, a worker is GVL-bound
    # and YJIT is worth ~15-20% jobs/s (docs/performance.md). Rails enables it via
    # config.yjit (load_defaults 7.2+, or 8.0+ outside local envs). Reports
    # only — enabling a JIT is the application's decision, never pgbus's.
    def check_ruby_jit
      label = RubyJit.label
      return Check.new(name: "Ruby JIT", status: :ok, detail: "#{label.upcase} enabled") unless label == "none"
      return Check.new(name: "Ruby JIT", status: :ok, detail: "no YJIT available on this Ruby") unless RubyJit.yjit_available?
      return Check.new(name: "Ruby JIT", status: :ok, detail: "YJIT off in a local environment (expected)") if local_env?

      Check.new(name: "Ruby JIT", status: :warn,
                detail: "YJIT is available but off — a CPU-bound worker (Postgres nearby) runs ~15% fewer jobs/s " \
                        "without it; no measurable effect when network-bound. " \
                        "Enable it with config.yjit = true (or load_defaults 7.2+) or RUBY_YJIT_ENABLE=1")
    rescue StandardError => e
      Check.new(name: "Ruby JIT", status: :warn, detail: "could not determine (#{e.class}: #{e.message})")
    end

    # A Rails app outside development/test. Without Rails the environment is
    # unknown, so assume local and stay quiet rather than warn on a guess.
    def local_env?
      return true unless defined?(Rails) && Rails.respond_to?(:env) && Rails.env

      env = Rails.env
      env.respond_to?(:local?) ? env.local? : %w[development test].include?(env.to_s)
    end

    # 2. Database connectivity — SELECT 1 via Client#ping.
    def check_database
      @client.ping
      Check.new(name: "Database", status: :ok, detail: "reachable")
    rescue StandardError => e
      Check.new(name: "Database", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 3. PGMQ schema presence + installed-vs-vendored version.
    #
    # A nil version does NOT automatically mean "not installed": PGMQ installed
    # via the extension, or before pgbus added version tracking, leaves the
    # pgmq schema fully working with no row in pgbus_pgmq_schema_versions. So we
    # distinguish, mirroring `pgbus:pgmq:status`: schema absent → fail; schema
    # present but untracked → warn; tracked but behind → warn; current → ok.
    def check_pgmq_schema
      installed = @client.pgmq_schema_version
      latest = Pgbus::PgmqSchema.latest_version

      if installed.nil?
        unless @client.pgmq_installed?
          return Check.new(name: "PGMQ schema", status: :fail,
                           detail: "not installed — run `rails generate pgbus:install`")
        end

        return Check.new(name: "PGMQ schema", status: :warn,
                         detail: "installed but no version tracking — run `rails generate pgbus:upgrade_pgmq`")
      end

      if Gem::Version.new(installed) < Gem::Version.new(latest)
        Check.new(name: "PGMQ schema", status: :warn,
                  detail: "installed #{installed}, vendored #{latest} — run `rails generate pgbus:upgrade_pgmq`")
      else
        Check.new(name: "PGMQ schema", status: :ok, detail: "up to date (#{installed})")
      end
    rescue StandardError => e
      Check.new(name: "PGMQ schema", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 4. Queue existence — every configured queue must have its PGMQ table(s).
    # Resolve each logical queue to the PHYSICAL names bootstrap creates (via the
    # client's queue strategy) so a priority queue's _p0.._pN sub-tables are what
    # we diff against list_queues — not the bare prefixed name priority mode
    # never creates.
    def check_queues
      configured = @client.configured_queues.flat_map { |q| @client.physical_queue_names(q) }
      existing = existing_queue_names
      missing = configured - existing

      if missing.empty?
        Check.new(name: "Queues", status: :ok, detail: "#{configured.size} configured queue(s) present")
      else
        Check.new(name: "Queues", status: :fail, detail: "missing queue(s): #{missing.join(", ")}")
      end
    rescue StandardError => e
      Check.new(name: "Queues", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 5. LISTEN/NOTIFY — when configured on, every configured queue should carry
    # the insert-NOTIFY trigger. A missing trigger is a warning, not a failure:
    # pgbus still works (it falls back to polling), it just loses instant wakeup.
    def check_notify
      return Check.new(name: "LISTEN/NOTIFY", status: :ok, detail: "disabled in config") unless @config.listen_notify

      without_trigger = @client.configured_queues.reject { |q| @client.notify_enabled?(q) }
      if without_trigger.empty?
        Check.new(name: "LISTEN/NOTIFY", status: :ok, detail: "triggers live on all configured queues")
      else
        Check.new(name: "LISTEN/NOTIFY", status: :warn,
                  detail: "no insert trigger on: #{without_trigger.join(", ")} (falling back to polling)")
      end
    rescue StandardError => e
      Check.new(name: "LISTEN/NOTIFY", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 6. Process liveness — the top-level health verdict (OK/DEGRADED/STALLED)
    # from the shared HealthAnalyzer. STALLED (the silent-worker-wedge) is a
    # failure; DEGRADED is a warning; OK passes.
    def check_processes
      verdict = Pgbus::MCP::HealthAnalyzer.new(data_source).verdict
      status = verdict[:status]
      detail = Array(verdict[:reasons]).first || status

      case status
      when "STALLED" then Check.new(name: "Process liveness", status: :fail, detail: detail)
      when "DEGRADED" then Check.new(name: "Process liveness", status: :warn, detail: detail)
      else Check.new(name: "Process liveness", status: :ok, detail: "OK")
      end
    rescue StandardError => e
      Check.new(name: "Process liveness", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 7. GlobalID allowlist — security. allowed_global_id_models = nil means
    # "allow ANY model as a GlobalID EventBus payload or ActiveJob argument",
    # which lets a crafted queue message deserialize arbitrary AR models
    # (issue #368 closed the job-path hole; both paths share the gate). It's
    # the default for upgrade continuity, so this is a warning (never a
    # failure), and only in production where the blast radius is real.
    def check_allowed_global_id_models
      if @config.allowed_global_id_models.nil? && production?
        return Check.new(name: "GlobalID allowlist", status: :warn,
                         detail: "allowed_global_id_models is nil (allow-all) in production — " \
                                 "set an explicit allowlist of models permitted as GlobalID " \
                                 "job arguments and EventBus payloads")
      end

      Check.new(name: "GlobalID allowlist", status: :ok,
                detail: @config.allowed_global_id_models.nil? ? "allow-all (non-production)" : "allowlist configured")
    rescue StandardError => e
      Check.new(name: "GlobalID allowlist", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 8. Broadcast-queue isolation — latency + correctness. With turbo-rails
    # loaded, the default broadcasts_to/broadcasts_refreshes path enqueues
    # render+broadcast ActiveJobs on the DEFAULT queue, so a browser SSE update
    # can wait behind long-running jobs (#311). Two failure modes:
    #
    #   (a) streams_broadcast_queue is nil — broadcasts share the default queue
    #       and can wait behind long jobs. A latency concern, so warn only in
    #       production (where the blast radius is real).
    #   (b) streams_broadcast_queue is set but NO worker capsule drains it —
    #       broadcasts route there and pile up unread, so the browser never
    #       updates. A correctness bug, so warn everywhere, not just production.
    #
    # Both are warnings, never failures. Only relevant when Turbo is loaded.
    def check_broadcast_queue
      broadcast_queue = @config.streams_broadcast_queue
      turbo = @config.streams_enabled && defined?(::Turbo::Broadcastable)

      if turbo && broadcast_queue && !worker_drains?(broadcast_queue)
        return Check.new(name: "Broadcast queue", status: :warn,
                         detail: "streams_broadcast_queue is \"#{broadcast_queue}\" but no worker capsule " \
                                 "drains it — broadcasts will pile up unread and never reach the browser; " \
                                 "add a capsule for the \"#{broadcast_queue}\" queue (or a wildcard worker)")
      end

      if turbo && production? && broadcast_queue.nil?
        return Check.new(name: "Broadcast queue", status: :warn,
                         detail: "streams_broadcast_queue is nil — turbo-rails broadcast jobs share the " \
                                 "default queue and can wait behind long-running jobs; set a dedicated queue " \
                                 "(e.g. \"realtime\") and back it with a worker capsule")
      end

      Check.new(name: "Broadcast queue", status: :ok,
                detail: broadcast_queue ? "dedicated: #{broadcast_queue}" : "n/a")
    rescue StandardError => e
      Check.new(name: "Broadcast queue", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # 9. Primary affinity — pooler safety (issue #332). A read/write-splitting
    # pooler (pgdog/pgcat) can route pgmq's VOLATILE read/archive to a read
    # replica, where workers read nothing and jobs stop with a healthy
    # heartbeat. If the job connection currently lands on a replica
    # (pg_is_in_recovery() => t), warn with the direct-port remediation. A
    # warning, never a failure: a deliberate replica-read setup is rare but
    # possible, and require_primary is the enforcement knob for those who want a
    # hard stop.
    def check_primary
      if @client.in_recovery?
        return Check.new(name: "Primary affinity", status: :warn,
                         detail: "job connection is on a read-only replica (pg_is_in_recovery() => t) — " \
                                 "a read/write-splitting pooler may be routing pgmq reads to a standby, " \
                                 "silently stalling jobs. Point the connection at the DIRECT primary port " \
                                 "(worker_notify_* / streams_* overrides) and set require_primary to reject " \
                                 "a replica at boot")
      end

      Check.new(name: "Primary affinity", status: :ok, detail: "on primary")
    rescue StandardError => e
      Check.new(name: "Primary affinity", status: :warn, detail: "could not determine (#{e.class}: #{e.message})")
    end

    # 10. Dedicated connections — the streamer's LISTEN connection and the
    # worker NotifyListener bypass the Client's pgmq pools and connect via
    # DedicatedConnection, so a broken dedicated path is invisible to every
    # other check (issue #352: :session-mode `:variables` broke ONLY these
    # connections while every Client path worked). Open each configured
    # dedicated connection exactly the way the runtime does, probe it, close
    # it. NOT strict-fatal, same reasoning as the Database check: a transient
    # DB blip at boot must not lockstep-abort a fleet.
    def check_dedicated_connections
      targets = []
      targets << ["streams", @config.streams_connection_options] if @config.streams_enabled
      targets << ["worker notify", @config.worker_notify_connection_options] if @config.worker_notify_wakeup?

      if targets.empty?
        return Check.new(name: "Dedicated connections", status: :ok,
                         detail: "disabled in config (streams + notify wakeup off)")
      end

      failures = targets.filter_map { |label, opts| probe_dedicated_connection(label, opts) }
      if failures.empty?
        Check.new(name: "Dedicated connections", status: :ok,
                  detail: "#{targets.map(&:first).join(" + ")} connect OK")
      else
        Check.new(name: "Dedicated connections", status: :fail, detail: failures.join("; "))
      end
    rescue StandardError => e
      Check.new(name: "Dedicated connections", status: :fail, detail: "#{e.class}: #{e.message}")
    end

    # How many direct LISTEN connections this config pins at steady state
    # (issue #381) — informational (always :ok) so operators can do capacity
    # math on the pooler's direct-connection budget from the doctor output.
    # Under :supervisor scope the whole host shares 1; under :fork it is one
    # per worker/consumer fork. Streams add one per web-server process, which
    # the doctor cannot count from here, so it is reported as a clause.
    def check_connection_budget
      worker_configs = @config.role_enabled?(:workers) ? Array(@config.workers) : []
      capsules = worker_configs.size
      # A processes: N capsule forks N workers, each with its own LISTEN under :fork scope.
      worker_forks = worker_configs.sum { |w| @config.processes_for(w) }
      consumers = @config.role_enabled?(:consumers) ? Array(@config.event_consumers).size : 0

      count =
        if !@config.worker_notify_wakeup?
          0
        elsif @config.worker_notify_scope == :supervisor
          (worker_forks + consumers).positive? ? 1 : 0
        else
          worker_forks + consumers
        end

      detail = format(
        "%<count>d direct LISTEN connection%<plural>s pinned (scope=%<scope>s; " \
        "%<capsules>d capsule%<cap_plural>s%<forks>s + %<consumers>d consumer%<con_plural>s%<share>s)",
        count: count, plural: count == 1 ? "" : "s", scope: @config.worker_notify_scope,
        capsules: capsules, cap_plural: capsules == 1 ? "" : "s",
        forks: worker_forks == capsules ? "" : " (#{worker_forks} worker processes)",
        consumers: consumers, con_plural: consumers == 1 ? "" : "s",
        share: count == 1 && @config.worker_notify_scope == :supervisor ? " share it" : ""
      )
      detail += streams_budget_clause if @config.streams_enabled
      Check.new(name: "Connection budget", status: :ok, detail: detail)
    rescue StandardError => e
      Check.new(name: "Connection budget", status: :warn, detail: "#{e.class}: #{e.message}")
    end

    # Streams add their own LISTEN footprint on web hosts: one per host with
    # the master hub (#382, the default — workers fall back per-worker only
    # during a hub outage), one per web process under :process scope.
    def streams_budget_clause
      if @config.streams_listen_scope == :master
        " + 1 per web host (streams master hub; per-worker fallback during a hub outage costs 1 per web process)"
      else
        " + 1 per web-server process (streams)"
      end
    end

    # Open one dedicated connection the way the runtime does, verify it
    # answers, close it. Returns nil on success, "label: error" on failure.
    def probe_dedicated_connection(label, opts)
      conn = Pgbus::DedicatedConnection.connect(opts)
      conn.exec("SELECT 1")
      nil
    rescue StandardError => e
      "#{label}: #{e.class}: #{e.message}"
    ensure
      begin
        conn&.close if conn.respond_to?(:close)
      rescue StandardError
        nil
      end
    end

    # True when some configured worker capsule drains the given queue — either
    # by naming it explicitly or via a "*" wildcard (which drains every queue).
    def worker_drains?(queue)
      Array(@config.workers).any? do |w|
        queues = w[:queues] || w["queues"] || []
        queues.include?("*") || queues.include?(queue)
      end
    end

    # Physical queue names known to PGMQ. list_queues rows may be Hashes (with a
    # :queue_name key) or value objects responding to #queue_name.
    def existing_queue_names
      Array(@client.list_queues).map do |row|
        if row.respond_to?(:queue_name)
          row.queue_name
        elsif row.is_a?(Hash)
          row[:queue_name] || row["queue_name"]
        end
      end.compact
    end

    def capsule_summary
      Array(@config.workers).map { |w| w[:name] || (w[:queues] || []).join("+") }.join(", ")
    end

    # Redact the password from a libpq URL/URI while keeping host/db visible so
    # the operator can still confirm which database they pointed at.
    def redacted_database_url
      return nil unless @config.database_url

      redact_url(@config.database_url)
    end

    # connection_params is a free-form libpq keyword hash, so redact every key
    # whose name looks secret (password, sslpassword, ...) — not just :password —
    # rather than assuming a fixed shape. Preserves symbol/string key form.
    SECRET_KEY_PATTERN = /pass|secret|token/i
    private_constant :SECRET_KEY_PATTERN

    def redacted_connection_params
      params = @config.connection_params
      return nil unless params.is_a?(Hash)

      params.each_with_object({}) do |(key, value), out|
        out[key] = key.to_s.match?(SECRET_KEY_PATTERN) ? "[REDACTED]" : value
      end
    end

    # Replace the password in a userinfo authority (scheme://user:pass@host) or a
    # key=value conninfo string (password=secret). Falls back to a blanket
    # redaction label if parsing fails, never leaking the original.
    #
    # The userinfo password can itself contain '@' and ':' (common in generated
    # secrets), so match it greedily up to the LAST '@' before the authority's
    # host — a lazy `[^@]+` would stop at the first '@' and leak the remainder.
    def redact_url(url)
      redacted = url.sub(%r{(://[^:/@]+:).+(@[^@/]*(?:[/?]|\z))}, '\1[REDACTED]\2')
      redacted.gsub(/(\bpassword=)('[^']*'|[^\s'"]+)/i, '\1[REDACTED]')
    rescue StandardError
      "[REDACTED]"
    end

    # Wrap a value-producing block so a broken resolver (e.g. resolved_pool_size
    # raising on a malformed worker config) degrades to nil instead of raising
    # out of config_summary.
    def safe
      yield
    rescue StandardError
      nil
    end
  end
end
