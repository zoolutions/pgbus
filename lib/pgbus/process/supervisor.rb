# frozen_string_literal: true

require "uri"

module Pgbus
  module Process
    class Supervisor
      include SignalHandler

      FORK_WAIT = 1 # seconds between fork checks
      WATCHDOG_INTERVAL = 10 # seconds between stall checks

      # A child that crashes within this many seconds of forking is treated
      # as crash-looping and restarted with exponential backoff (base
      # RESTART_BACKOFF_BASE, doubling per consecutive crash, capped at
      # RESTART_BACKOFF_MAX). A child that ran at least this long — or that
      # exited cleanly, like a recycling worker — restarts immediately and
      # resets its crash streak.
      RESTART_STABLE_UPTIME = 30
      RESTART_BACKOFF_BASE = 1
      RESTART_BACKOFF_MAX = 60

      attr_reader :config
      # The host-level shared LISTEN hub (issue #381). nil under :fork scope,
      # when notify wakeups are off entirely, or when no worker/consumer role
      # is enabled. Readable as a test seam.
      attr_reader :notify_hub
      # forks is readable everywhere; the writer exists only so tests can seed a
      # known set of children before exercising the reap/watchdog paths (in
      # production forks is populated by fork_* as children spawn).
      attr_accessor :forks

      # The child-fork bookkeeping (`forks`, `pending_restarts`), the
      # `shutting_down` flag, and `notify_hub` accept injected seeds so tests
      # can drive the monitor/reap loops from a known state without poking
      # private ivars. All default to the empty/false values production always
      # starts from.
      def initialize(config: Pgbus.configuration, forks: {}, shutting_down: false,
                     pending_restarts: [], last_watchdog_at: nil, notify_hub: nil)
        @config = config
        @forks = forks
        @shutting_down = shutting_down
        @last_watchdog_at = last_watchdog_at || monotonic_now
        @pending_restarts = pending_restarts
        @crash_counts = Hash.new(0)
        @notify_hub = notify_hub
        @intended_children = 0
        @readiness = Concurrent::AtomicReference.new(
          ReadinessSnapshot.new(booted: false, shutting_down: shutting_down, expected: 0, live: forks.size)
        )
      end

      # The current container-local readiness state. Safe to call from any
      # thread (the health server's accept thread reads it per probe).
      def readiness_snapshot
        @readiness.get
      end

      def shutting_down?
        @shutting_down
      end

      # Test seam: reset the watchdog clock so check_stalled_workers runs on the
      # next call instead of waiting out WATCHDOG_INTERVAL. Production advances
      # this internally after each watchdog pass.
      attr_writer :last_watchdog_at

      def run
        setup_signals
        start_heartbeat
        start_health_server

        Pgbus.logger.info { "[Pgbus] Supervisor starting pid=#{::Process.pid}" }

        # Emit a one-block boot diagnostics banner so operators can read the
        # resolved deployment config (connection target, pool, notify flags,
        # roles, capsules) straight from the log instead of attaching a console.
        log_boot_banner

        # Fail fast on a bad database_url / connection_params. PGMQ's pool is
        # lazy, so an unreachable DB would otherwise only surface once forked
        # children crash-loop against it. verify_connection! raises
        # Pgbus::ConfigurationError with an actionable message; we let it
        # propagate so the supervisor exits instead of forking anything.
        Pgbus.client.verify_connection!

        # Bootstrap queues once in the parent process before forking children.
        # This avoids the deadlock that occurs when multiple forked children
        # race to call enable_notify_insert (DROP TRIGGER + CREATE TRIGGER)
        # concurrently on the same queue tables. Children still call
        # bootstrap_queues! post-fork but the idempotent check in
        # notify_trigger_current? makes those calls cheap no-ops.
        bootstrap_queues

        # Optional in-process doctor preflight (issue #347): run the diagnostic
        # checks here — after config is loaded, the DB is verified reachable, and
        # queues are bootstrapped, but BEFORE any worker is forked — so an
        # entrypoint gets a single Rails boot instead of `pgbus doctor` + `pgbus
        # start`. :strict aborts the boot (raising, so nothing forks) on a
        # genuinely-fatal finding; :report only logs. Off by default.
        run_doctor_preflight unless config.doctor_on_boot.nil?

        # Host-level shared LISTEN (issue #381): under :supervisor scope, ONE
        # NotifyListener lives here and forks are woken over pipes — started
        # before any child forks so every fork_worker/fork_consumer can hand
        # its child a wake pipe.
        start_notify_hub

        boot_processes
        mark_booted
        monitor_loop
      ensure
        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Supervisor: graceful shutdown requested" }
        @shutting_down = true
        refresh_readiness
        signal_children("TERM")
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Supervisor: immediate shutdown requested" }
        @shutting_down = true
        refresh_readiness
        signal_children("QUIT")
      end

      private

      # Boot is complete: connection verified, queues bootstrapped, every
      # configured child fork ATTEMPTED. The baseline is the larger of the
      # intended-attempt count and the fork-table size: a boot-time fork
      # failure (EAGAIN/ENOMEM, logged-and-swallowed in fork_*) leaves
      # intended > live, so the readiness gate reports DEGRADED instead of
      # blessing a container that is missing workers. Roles that legitimately
      # declined to boot (scheduler with no recurring tasks) never reach a
      # fork_* method and are counted by neither side.
      def mark_booted
        @booted = true
        @expected_children = [@intended_children, @forks.size].max
        refresh_readiness
      end

      # Count a child the configuration intends this boot to run. Called at
      # the top of every fork_* method — before the fork can fail — and only
      # pre-boot, so restart_child's re-forks never inflate the baseline.
      def note_intended_child
        @intended_children += 1 unless @booted
      end

      # Publish a fresh snapshot; the swapped-in Data is immutable, so the
      # health server's accept thread always reads a consistent state.
      def refresh_readiness
        @readiness.set(
          ReadinessSnapshot.new(
            booted: !!@booted, shutting_down: @shutting_down,
            expected: @expected_children || 0, live: @forks.size
          )
        )
      end

      # Log a single boot diagnostics banner: the settings that actually
      # determine whether this deployment works. One consecutive block of
      # "[Pgbus] boot:"-prefixed info lines so it reads cleanly under both the
      # text and JSON log formatters. Every DB-dependent field is wrapped so a
      # transient failure degrades that field to "unknown" — the banner must
      # never abort boot.
      ROLE_FLAGS = %i[workers dispatcher scheduler consumers outbox].freeze
      private_constant :ROLE_FLAGS

      def log_boot_banner
        Pgbus.logger.info { "[Pgbus] boot: pgbus #{Pgbus::VERSION} pid=#{::Process.pid} jit=#{RubyJit.label}" }
        Pgbus.logger.info do
          "[Pgbus] boot: connection=#{redacted_connection_target} pool=#{banner_field { config.resolved_pool_size }}"
        end
        Pgbus.logger.info do
          "[Pgbus] boot: pgmq_schema_mode=#{config.pgmq_schema_mode} pgmq_version=#{installed_pgmq_version}"
        end
        Pgbus.logger.info do
          "[Pgbus] boot: listen_notify=#{config.listen_notify} " \
            "worker_notify_wakeup=#{config.worker_notify_wakeup?} " \
            "worker_notify_scope=#{config.worker_notify_scope}"
        end
        Pgbus.logger.info { "[Pgbus] boot: roles=#{enabled_roles.join(",")}" }
        log_capsule_banner
        log_consumer_banner
      end

      def log_capsule_banner
        return unless config.role_enabled?(:workers)

        Array(config.workers).each do |worker_config|
          name = worker_config[:name] || worker_config["name"] || "anonymous"
          queues = worker_config[:queues] || worker_config["queues"] || [config.default_queue]
          threads = worker_config[:threads] || worker_config["threads"] || 5
          mode = banner_field { config.execution_mode_for(worker_config) }
          Pgbus.logger.info do
            "[Pgbus] boot: capsule=#{name} queues=#{Array(queues).join(",")} threads=#{threads} mode=#{mode}"
          end
        end
      end

      def log_consumer_banner
        return unless config.role_enabled?(:consumers)

        Array(config.event_consumers).each do |consumer_config|
          topics = consumer_config[:topics] || consumer_config["topics"] || []
          threads = consumer_config[:threads] || consumer_config["threads"] || 3
          Pgbus.logger.info do
            "[Pgbus] boot: consumer topics=#{Array(topics).join(",")} threads=#{threads}"
          end
        end
      end

      # The set of roles that will actually boot, honoring config.roles.
      def enabled_roles
        ROLE_FLAGS.select { |role| config.role_enabled?(role) }
      end

      # Best-effort PGMQ version from the tracking table. "unknown" on any error
      # (missing table, DB down) so a boot log never fails on diagnostics.
      def installed_pgmq_version
        Pgbus.client.pgmq_schema_version || "unknown"
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] boot: pgmq_schema_version lookup failed: #{e.class}: #{e.message}" }
        "unknown"
      end

      # Reduce the configured connection to "host/dbname" with the password
      # never printed, across all three connection_options forms: a URL string,
      # a libpq keyword hash, or the AR-derived hash. Falls back to "unknown"
      # rather than leaking anything if the shape is unexpected.
      def redacted_connection_target
        target = config.database_url ? parse_url_target(config.database_url) : parse_hash_target(banner_connection_hash)
        target || "unknown"
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] boot: connection target redaction failed: #{e.class}: #{e.message}" }
        "unknown"
      end

      # connection_options returns the URL string when database_url is set, a
      # Hash for connection_params or AR-derived config, or a Proc fallback. We
      # only reach here for the non-URL cases, so a non-Hash (Proc) yields nil.
      def banner_connection_hash
        opts = config.connection_options
        opts.is_a?(Hash) ? opts : nil
      end

      def parse_url_target(url)
        uri = URI.parse(url)
        host = uri.host
        dbname = uri.path.to_s.sub(%r{\A/}, "")
        return nil if host.nil? && dbname.empty?

        [host, dbname].reject { |s| s.nil? || s.empty? }.join("/")
      rescue URI::InvalidURIError
        nil
      end

      def parse_hash_target(hash)
        return nil unless hash.is_a?(Hash)

        host = hash[:host] || hash["host"]
        dbname = hash[:dbname] || hash["dbname"] || hash[:database] || hash["database"]
        return nil if host.nil? && dbname.nil?

        [host, dbname].compact.join("/")
      end

      # Evaluate a banner field that touches config/DB; any failure degrades the
      # single field to "unknown" so one broken resolver can't blank the banner.
      def banner_field
        yield
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] boot: banner field failed: #{e.class}: #{e.message}" }
        "unknown"
      end

      def boot_processes
        # Boot workers (workers may be nil for scheduler-only or
        # dispatcher-only deployments via --workers-only / --scheduler-only /
        # --dispatcher-only CLI flags). Each role is gated by
        # config.role_enabled?, which returns true unless +config.roles+ has
        # been narrowed.
        if config.role_enabled?(:workers)
          # slot is the child's position in the config array — it keys the
          # crash-streak tracking so identically-configured siblings don't
          # share (and reset) each other's restart backoff.
          Array(config.workers).each_with_index { |worker_config, slot| fork_worker(worker_config, slot: slot) }
        end

        fork_dispatcher if config.role_enabled?(:dispatcher)
        boot_scheduler if config.role_enabled?(:scheduler)
        boot_consumers if config.role_enabled?(:consumers)
        boot_outbox_poller if config.role_enabled?(:outbox)
      end

      def fork_worker(worker_config, slot: nil)
        note_intended_child
        queues = worker_config[:queues] || [config.default_queue]
        threads = worker_config[:threads] || 5
        single_active = worker_config[:single_active_consumer] || false
        priority = worker_config[:consumer_priority] || 0
        exec_mode = config.execution_mode_for(worker_config)
        grp_mode = worker_config[:group_mode] || config.group_mode

        # OS-level liveness channel: the child writes a byte each loop
        # iteration, the parent drains the reader in monitor_loop. This lets
        # the watchdog detect a wedged worker without the database.
        liveness_reader, liveness_writer = IO.pipe
        # Wake channel, opposite direction (issue #381): the NotifyHub writes
        # W/H/P bytes, the child's WakePipe watcher reads them. Only under
        # :supervisor scope (hub present).
        wake_reader, wake_writer = IO.pipe if @notify_hub

        pid = fork do
          # Child owns the liveness writer + wake reader; close this fork's
          # own copies of the parent-side ends. Sibling pipe ends and the
          # hub's LISTEN socket are released in setup_child_process, which
          # every child type runs.
          liveness_reader.close
          wake_writer&.close
          restore_signals
          setup_child_process
          load_rails_app
          bootstrap_queues!
          worker = Worker.new(
            queues: queues, threads: threads, config: config,
            single_active_consumer: single_active, consumer_priority: priority,
            execution_mode: exec_mode, group_mode: grp_mode,
            read_ahead: config.read_ahead_for(worker_config),
            liveness_pipe: liveness_writer, wake_pipe: wake_reader
          )
          worker.run
        end

        unless pid
          close_pipe(liveness_reader)
          close_pipe(liveness_writer)
          close_pipe(wake_reader)
          close_pipe(wake_writer)
          Pgbus.logger.error { "[Pgbus] Failed to fork worker for queues=#{queues.join(",")}" }
          return
        end

        # Parent keeps the liveness reader + wake writer, discards its copies
        # of the child-side ends so each pipe reaches EOF once its sole owner
        # closes.
        close_pipe(liveness_writer)
        close_pipe(wake_reader)
        register_fork_with_hub(pid, wake_writer, queues)
        @forks[pid] = {
          type: :worker, config: worker_config, slot: slot, spawned_at: monotonic_now,
          liveness_reader: liveness_reader, last_pipe_tick_at: monotonic_now, pipe_seen: false,
          wake_writer: wake_writer
        }
        Pgbus.logger.info { "[Pgbus] Forked worker pid=#{pid} queues=#{queues.join(",")} mode=#{exec_mode}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        close_pipe(liveness_reader)
        close_pipe(liveness_writer)
        close_pipe(wake_reader)
        close_pipe(wake_writer)
        ErrorReporter.report(e, { action: "fork_worker", queues: queues })
      end

      # Hand the hub a worker fork's routing entry: explicit queues as
      # physical names, "*" as the unconditional wildcard flag (the hub wakes
      # wildcard forks for every channel, so the fork's own resolved set never
      # needs to be reported upstream). Registration must never abort the fork
      # bookkeeping that follows it — queue_name can raise on a malformed
      # name — so on error the fork registers with an empty set and rides its
      # poll ceiling (symmetric with register_consumer_with_hub).
      def register_fork_with_hub(pid, wake_writer, queues)
        return unless @notify_hub && wake_writer

        wildcard = queues.include?("*")
        physical = queues.reject { |q| q == "*" }.map { |q| config.queue_name(q) }
        @notify_hub.register_fork(pid: pid, queues: physical, wildcard: wildcard, pipe: wake_writer)
      rescue StandardError => e
        ErrorReporter.report(e, { action: "register_fork_with_hub", queues: queues })
        @notify_hub.register_fork(pid: pid, queues: [], wildcard: false, pipe: wake_writer)
      end

      def fork_dispatcher
        note_intended_child
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          dispatcher = Dispatcher.new(config: config)
          dispatcher.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork dispatcher" }
          return
        end

        @forks[pid] = { type: :dispatcher, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked dispatcher pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_dispatcher" })
      end

      def boot_scheduler
        return unless config.recurring_enabled
        return unless recurring_tasks_configured?

        fork_scheduler
      end

      def fork_scheduler
        note_intended_child
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          load_recurring_config
          bootstrap_queues!
          scheduler = Recurring::Scheduler.new(config: config)
          scheduler.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork scheduler" }
          return
        end

        @forks[pid] = { type: :scheduler, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked scheduler pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_scheduler" })
      end

      def recurring_tasks_configured?
        return true if config.recurring_tasks&.any?

        files = config.recurring_tasks_files
        return true if files&.any? { |f| File.exist?(f.to_s) }

        return true if config.recurring_tasks_file && File.exist?(config.recurring_tasks_file.to_s)

        if defined?(Rails) && Rails.respond_to?(:root) && Rails.root
          default_path = Rails.root.join("config", "recurring.yml")
          return File.exist?(default_path.to_s)
        end

        false
      end

      def load_recurring_config
        return if config.recurring_tasks&.any?

        files = config.recurring_tasks_files
        if files
          tasks = Recurring::ConfigLoader.load_all(files)
          config.recurring_tasks = tasks unless tasks.empty?
          return if tasks.any?
        end

        path = config.recurring_tasks_file
        path ||= defined?(Rails) && Rails.respond_to?(:root) && Rails.root ? Rails.root.join("config", "recurring.yml") : nil
        return unless path && File.exist?(path.to_s)

        config.recurring_tasks = Recurring::ConfigLoader.load(path)
      end

      def boot_consumers
        return unless config.event_consumers

        config.event_consumers.each_with_index do |consumer_config, slot|
          fork_consumer(consumer_config, slot: slot)
        end
      end

      def fork_consumer(consumer_config, slot: nil)
        note_intended_child
        # Array() so a consumer entry without :topics can't NoMethodError the
        # supervisor on the topics.join log lines below.
        topics = Array(consumer_config[:topics])
        threads = consumer_config[:threads] || 3

        # OS-level liveness channel: the consumer writes a byte each loop
        # iteration, the parent drains the reader in monitor_loop. This lets the
        # watchdog detect a wedged consumer without the database (issue #274),
        # exactly as fork_worker does for workers.
        liveness_reader, liveness_writer = IO.pipe
        # Wake channel from the NotifyHub (issue #381), as in fork_worker.
        wake_reader, wake_writer = IO.pipe if @notify_hub

        pid = fork do
          # Child owns the liveness writer + wake reader; close this fork's
          # own copies of the parent-side ends (see fork_worker).
          liveness_reader.close
          wake_writer&.close
          restore_signals
          setup_child_process
          load_rails_app
          consumer = Consumer.new(topics: topics, threads: threads, config: config,
                                  read_ahead: config.read_ahead_for(consumer_config),
                                  liveness_pipe: liveness_writer, wake_pipe: wake_reader)
          consumer.run
        end

        unless pid
          close_pipe(liveness_reader)
          close_pipe(liveness_writer)
          close_pipe(wake_reader)
          close_pipe(wake_writer)
          Pgbus.logger.error { "[Pgbus] Failed to fork consumer for topics=#{topics.join(",")}" }
          return
        end

        # Parent keeps the liveness reader + wake writer, discards its copies
        # of the child-side ends.
        close_pipe(liveness_writer)
        close_pipe(wake_reader)
        register_consumer_with_hub(pid, wake_writer, topics)
        @forks[pid] = {
          type: :consumer, config: consumer_config, slot: slot, spawned_at: monotonic_now,
          liveness_reader: liveness_reader, last_pipe_tick_at: monotonic_now, pipe_seen: false,
          wake_writer: wake_writer
        }
        Pgbus.logger.info { "[Pgbus] Forked consumer pid=#{pid} topics=#{topics.join(",")}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        close_pipe(liveness_reader)
        close_pipe(liveness_writer)
        close_pipe(wake_reader)
        close_pipe(wake_writer)
        ErrorReporter.report(e, { action: "fork_consumer", topics: topics })
      end

      # A consumer's routing entry mirrors Consumer#setup_subscriptions: the
      # registry derives the queue set from the topic list. Registry lookups
      # never abort a fork — on error the fork registers with an empty set and
      # rides its poll ceiling until the next supervisor restart.
      def register_consumer_with_hub(pid, wake_writer, topics)
        return unless @notify_hub && wake_writer

        physical = EventBus::Registry.instance
                                     .queue_names_for_topics(Array(topics))
                                     .map { |q| config.queue_name(q) }
        @notify_hub.register_fork(pid: pid, queues: physical, wildcard: false, pipe: wake_writer)
      rescue StandardError => e
        ErrorReporter.report(e, { action: "register_consumer_with_hub", topics: topics })
        @notify_hub.register_fork(pid: pid, queues: [], wildcard: false, pipe: wake_writer)
      end

      def boot_outbox_poller
        return unless config.outbox_enabled

        fork_outbox_poller
      end

      def fork_outbox_poller
        note_intended_child
        pid = fork do
          restore_signals
          setup_child_process
          load_rails_app
          poller = Outbox::Poller.new(config: config)
          poller.run
        end

        unless pid
          Pgbus.logger.error { "[Pgbus] Failed to fork outbox poller" }
          return
        end

        @forks[pid] = { type: :outbox_poller, spawned_at: monotonic_now }
        Pgbus.logger.info { "[Pgbus] Forked outbox poller pid=#{pid}" }
      rescue Errno::EAGAIN, Errno::ENOMEM => e
        ErrorReporter.report(e, { action: "fork_outbox_poller" })
      end

      def monitor_loop
        loop do
          break if @shutting_down && @forks.empty?

          process_signals
          reap_children
          drain_liveness_pipes
          unless @shutting_down
            process_pending_restarts
            check_stalled_workers
            # One hub beat per monitor pass: listener self-heal, LISTEN union
            # refresh, and fork status broadcast (issue #381).
            @notify_hub&.tick
          end
          # After reap + restarts so a clean recycle (reaped and re-forked in
          # the same pass) never dips the published live count (issue #386).
          refresh_readiness
          interruptible_sleep(FORK_WAIT)
        end
      end

      def reap_children
        loop do
          pid, status = ::Process.waitpid2(-1, ::Process::WNOHANG)
          break unless pid

          info = @forks.delete(pid)
          next unless info

          # Close the liveness reader as the fork leaves @forks so a crash-loop
          # (restart deferred up to RESTART_BACKOFF_MAX) can't leak an FD per
          # crash. Scrub the keys so a closed IO never rides into a restart.
          # The wake writer is closed by the hub's deregister (same IO object).
          close_pipe(info.delete(:liveness_reader))
          info.delete(:wake_writer)
          @notify_hub&.deregister_fork(pid)

          if @shutting_down
            Pgbus.logger.info { "[Pgbus] Child #{info[:type]} pid=#{pid} exited (status=#{status.exitstatus})" }
          else
            log_child_exit(info, pid, status)
            schedule_restart(info, status)
          end
        rescue Errno::ECHILD
          break
        end
      end

      # A clean exit outside shutdown is a worker/consumer recycle (max_jobs,
      # max_memory, max_lifetime) — expected, so INFO. Anything else is a
      # crash: WARN, naming the signal when there is one so an OOM SIGKILL
      # reads differently from an exit code (issue #438).
      def log_child_exit(info, pid, status)
        if status&.success?
          Pgbus.logger.info do
            "[Pgbus] Child #{info[:type]} pid=#{pid} exited cleanly (status=0) — restarting (worker recycle)"
          end
        else
          Pgbus.logger.warn do
            detail = if status && status.exitstatus.nil? && status.signaled?
                       "signal=#{status.termsig}"
                     else
                       "status=#{status&.exitstatus}"
                     end
            "[Pgbus] Child #{info[:type]} pid=#{pid} exited unexpectedly (#{detail})"
          end
        end
      end

      # Restart policy: a clean exit (worker recycling) or a crash after a
      # stable run restarts immediately with a fresh crash streak. A crash
      # within RESTART_STABLE_UPTIME of forking is a crash loop — the child
      # is dying on boot (bad config, unreachable DB, raising initializer) —
      # so back off exponentially instead of fork-crash-forking at full speed.
      # A child with no spawned_at (never set in practice) restarts
      # immediately, preserving the pre-backoff behavior.
      def schedule_restart(info, status)
        # Keyed on [type, slot], NOT the config hash — identically-configured
        # sibling workers would otherwise share one streak, letting one
        # sibling's clean recycle reset another sibling's crash backoff.
        key = [info[:type], info[:slot]]
        uptime = info[:spawned_at] ? monotonic_now - info[:spawned_at] : nil

        if status&.success? || uptime.nil? || uptime >= RESTART_STABLE_UPTIME
          @crash_counts.delete(key)
          return restart_child(info)
        end

        crashes = @crash_counts[key] += 1
        backoff = [RESTART_BACKOFF_BASE * (2**(crashes - 1)), RESTART_BACKOFF_MAX].min
        Pgbus.logger.warn do
          "[Pgbus] Child #{info[:type]} crashed after #{uptime.round(1)}s uptime " \
            "(crash ##{crashes}) — restarting in #{backoff}s"
        end
        @pending_restarts << { info: info, at: monotonic_now + backoff }
      end

      def process_pending_restarts
        now = monotonic_now
        due, pending = @pending_restarts.partition { |r| r[:at] <= now }
        @pending_restarts = pending
        due.each { |r| restart_child(r[:info]) }
      end

      def restart_child(info)
        case info[:type]
        when :worker
          fork_worker(info[:config], slot: info[:slot])
        when :dispatcher
          fork_dispatcher
        when :scheduler
          fork_scheduler
        when :consumer
          fork_consumer(info[:config], slot: info[:slot])
        when :outbox_poller
          fork_outbox_poller
        end
      end

      def check_stalled_workers
        now = monotonic_now
        return if (now - @last_watchdog_at) < WATCHDOG_INTERVAL

        @last_watchdog_at = now
        threshold = config.stall_threshold
        return unless threshold&.positive?

        # Workers AND consumers both stamp a loop beacon and carry a liveness
        # pipe (issue #274), so both are watched for a wedged claim/consume loop.
        watched_pids = @forks.select { |_, info| %i[worker consumer].include?(info[:type]) }.keys
        return if watched_pids.empty?

        db_ages = db_loop_tick_ages(watched_pids)

        watched_pids.each do |pid|
          info = @forks[pid]
          next unless info

          kill_stalled_worker(pid, threshold) if worker_stalled?(info, db_ages[pid], now, threshold)
        end
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Supervisor watchdog check failed: #{e.message}" }
      end

      # Read each watched fork's loop_tick_at from the process table and return
      # a {pid => wall-clock age in seconds} map. Isolated in its own rescue so a
      # database outage degrades to the OS-pipe fallback instead of skipping
      # the whole watchdog — the exact failure this pipe channel exists to fix.
      def db_loop_tick_ages(worker_pids)
        ages = {}
        ProcessEntry.where(kind: %w[worker consumer], pid: worker_pids).to_a.each do |entry|
          meta = entry.metadata
          next unless meta.is_a?(Hash)

          loop_tick = meta["loop_tick_at"]
          ages[entry.pid] = Time.current.to_f - loop_tick.to_f if loop_tick
        end
        ages
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Supervisor watchdog DB read failed, using pipe fallback: #{e.message}" }
        ages
      end

      # A worker is stalled only when EVERY liveness channel that has spoken
      # agrees it is stale (min-age / fresh-wins): a fresh DB row OR a fresh
      # pipe tick proves the loop is advancing. If no channel has any signal
      # (a slow-booting worker with no DB row and an unarmed pipe), we do not
      # kill — mirroring the pre-existing "no loop_tick_at → skip" tolerance.
      def worker_stalled?(info, db_age, now, threshold)
        pipe_age = (now - info[:last_pipe_tick_at] if info[:pipe_seen] && info[:last_pipe_tick_at])
        ages = [db_age, pipe_age].compact
        return false if ages.empty?

        ages.min > threshold
      end

      def kill_stalled_worker(pid, threshold)
        Pgbus.logger.error do
          "[Pgbus] Supervisor watchdog: worker pid=#{pid} claim loop stalled " \
            "(no liveness within threshold=#{threshold}s), sending SIGKILL"
        end
        ::Process.kill("KILL", pid)
      rescue Errno::ESRCH
        # already gone
      end

      # Drain each worker's liveness pipe. Any readable byte means the worker's
      # loop advanced since the last drain, so we stamp arrival on the parent's
      # own monotonic clock (never a worker timestamp — that would cross the
      # fork's incomparable CLOCK_MONOTONIC) and arm pipe_seen. Bounded to a
      # few reads per reader so a fast-writing worker can't wedge the 1s loop;
      # "any bytes ⇒ alive" needs no full drain. Per-reader rescue so one
      # closed reader (racing reap/shutdown) can't skip the rest.
      def drain_liveness_pipes
        now = monotonic_now
        @forks.each_value do |info|
          reader = info[:liveness_reader]
          next unless reader

          read_any = false
          begin
            2.times do
              reader.read_nonblock(4096)
              read_any = true
            end
          rescue IO::WaitReadable, EOFError
            # empty / drained, or writer closed (worker exiting — reap handles it)
          rescue IOError, Errno::EBADF
            # reader closed by a racing reap/shutdown this tick
            next
          end

          if read_any
            info[:last_pipe_tick_at] = now
            info[:pipe_seen] = true
          end
        end
      end

      def signal_children(sig)
        @forks.each_key do |pid|
          ::Process.kill(sig, pid)
        rescue Errno::ESRCH
          # Process already gone
        end
      end

      def setup_child_process
        # Every child type (worker, consumer, dispatcher, scheduler, outbox
        # poller) releases its copies of the parent's per-fork resources —
        # a dispatcher child holding a sibling worker's wake WRITER would
        # keep that sibling's pipe from ever reaching EOF after the
        # supervisor dies, pinning the sibling to the 15s NOTIFY ceiling
        # with no wake source (issue #381 review).
        close_inherited_parent_resources
        # Reset the PGMQ client so this forked process gets a fresh
        # PG::Connection instead of inheriting the parent's (which is
        # in undefined state post-fork and not thread-safe to share).
        Pgbus.reset_client!
        %w[INT TERM QUIT].each do |sig|
          trap(sig) { @shutting_down = true }
        end
      end

      # Lenient bootstrap for the parent (supervisor.rb#run). A transiently
      # unavailable DB at boot must not kill the supervisor — children
      # crash-and-backoff until it recovers — so every StandardError (including
      # SchemaNotReady) is reported and swallowed.
      def bootstrap_queues
        Pgbus.client.ensure_all_queues
      rescue StandardError => e
        ErrorReporter.report(e, { action: "bootstrap_queues" })
      end

      # Strict bootstrap for forked children (fork_worker, fork_scheduler). A
      # genuinely missing schema (database absent, migrations not run) surfaces
      # as Pgbus::SchemaNotReady — log its already-actionable message once and
      # re-raise so the child exits non-zero. schedule_restart then paces the
      # crash loop with exponential backoff (up to RESTART_BACKOFF_MAX) instead
      # of letting children boot and drown in downstream "relation does not
      # exist" errors. Other StandardErrors stay reported-and-swallowed, exactly
      # as the lenient variant handles them.
      def bootstrap_queues!
        Pgbus.client.ensure_all_queues
      rescue Pgbus::SchemaNotReady => e
        Pgbus.logger.error { "[Pgbus] #{e.message}" }
        raise
      rescue StandardError => e
        ErrorReporter.report(e, { action: "bootstrap_queues" })
      end

      # In-process doctor preflight (issue #347). Runs the boot-safe subset of
      # doctor checks (everything except the worker-dependent process-liveness
      # check, which has no workers to observe yet) against the supervisor's own
      # config and the already-verified shared client, and logs the report.
      #
      # In :strict mode, a genuinely-fatal check (Doctor::STRICT_FATAL —
      # Configuration or an absent PGMQ schema) aborts the boot by raising
      # Pgbus::ConfigurationError. Because this runs before boot_processes, no
      # child is forked, and `run`'s `ensure shutdown` tears down the heartbeat
      # and health server — the same fail-fast path verify_connection! uses.
      # A transient-shaped failure (Queues, Database) is reported but never
      # aborts: the lenient bootstrap above is built to let children ride out a
      # boot-time DB blip, and a strict abort there would take down a whole
      # fleet's cold boot in lockstep.
      def run_doctor_preflight
        doctor = Pgbus::Doctor.new(config: config, client: Pgbus.client)
        report = doctor.boot_report
        Pgbus.logger.info { "[Pgbus] doctor preflight (#{config.doctor_on_boot}):\n#{report}" }

        return unless config.doctor_on_boot == :strict
        return if doctor.boot_ok?

        raise Pgbus::ConfigurationError,
              "doctor preflight failed a fatal check (doctor_on_boot: :strict) — refusing to boot. " \
              "See the report above."
      end

      def load_rails_app
        return unless defined?(Rails) && Rails.respond_to?(:application) && Rails.application

        Rails.application.eager_load! if Rails.application.respond_to?(:eager_load!)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "supervisor",
          metadata: { pid: ::Process.pid, hostname: Socket.gethostname }
        )
        @heartbeat.start
      end

      # Serve /livez and /readyz over a plain TCP server when health_port is
      # configured. This gives orchestrators (Kubernetes) an HTTP probe surface
      # on the supervisor itself — the process that forks and watches workers —
      # without booting Rails or the dashboard. Disabled (nil) by default;
      # host apps that already run Puma can mount Pgbus::Web::HealthApp instead.
      def start_health_server
        return unless config.health_port

        # The standalone server answers /readyz from THIS supervisor's
        # container-local snapshot — a rolling deploy's health gate must
        # measure the new container, not the fleet-wide verdict a sibling
        # container's workers can satisfy (issue #386).
        app = Pgbus::Web::HealthApp.new(local_readiness: -> { readiness_snapshot })
        @health_server = Pgbus::Web::HealthServer.new(port: config.health_port, bind: config.health_bind, app: app)
        @health_server.start
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Close a pipe IO idempotently. nil (non-worker forks, already-scrubbed
      # entries) and already-closed IOs are no-ops; a rare EBADF/IOError from a
      # racing close is swallowed. FD management is single-threaded (main loop
      # only), so the closed? check-then-close needs no lock.
      def close_pipe(io)
        io.close if io && !io.closed?
      rescue IOError, Errno::EBADF
        nil
      end

      # Called only inside a just-forked child: close every sibling pipe end
      # inherited from the parent's FD table (liveness readers AND wake
      # writers), and release the child's copy of the NotifyHub's LISTEN
      # socket without a libpq Terminate (PQfinish would kill the PARENT's
      # session over the shared socket — see
      # NotifyListener#close_inherited_socket!).
      def close_inherited_parent_resources
        @forks.each_value do |info|
          close_pipe(info[:liveness_reader])
          close_pipe(info[:wake_writer])
        end
        @notify_hub&.close_inherited!
        @notify_hub = nil
      end

      # Build the host-level shared LISTEN hub (issue #381). Only under
      # :supervisor scope, with notify wakeups on, and with at least one role
      # that reads queues. A hub that fails to start degrades to no hub: forks
      # get no wake pipe and fall back to fast polling, exactly like a failed
      # per-fork listener under :fork scope.
      def start_notify_hub
        return unless config.worker_notify_wakeup?
        return unless config.worker_notify_scope == :supervisor
        return unless config.role_enabled?(:workers) || config.role_enabled?(:consumers)

        hub = NotifyHub.new(config: config)
        hub.start
        @notify_hub = hub
      rescue StandardError => e
        @notify_hub = nil
        ErrorReporter.report(e, { action: "start_notify_hub" })
        Pgbus.logger.error do
          "[Pgbus] NotifyHub failed to start — forks fall back to polling: #{e.class}: #{e.message}"
        end
      end

      def shutdown
        # Wait for children to drain and exit, bounded by config.shutdown_timeout
        # (default drain_timeout + 5) so raising the drain window can never
        # mean SIGKILLing workers mid-drain. An orchestrator's stop grace
        # period should exceed this value (issue #386).
        deadline = Time.now + config.shutdown_timeout

        until @forks.empty? || Time.now > deadline
          reap_children
          interruptible_sleep(0.5)
        end

        # Force kill any remaining
        signal_children("KILL") unless @forks.empty?

        # Close any liveness readers still open on un-reaped children so the
        # supervisor never leaks FDs across a restart of itself. (Wake writers
        # are closed by the hub's stop below.)
        @forks.each_value { |info| close_pipe(info[:liveness_reader]) }

        @notify_hub&.stop
        @health_server&.stop
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Supervisor stopped" }
      end
    end
  end
end
