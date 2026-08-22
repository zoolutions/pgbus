# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Dispatcher
      include SignalHandler

      # Maintenance runs on coarser intervals than the main loop
      CLEANUP_INTERVAL = 3600               # Run idempotency cleanup every hour
      REAP_INTERVAL = 300                   # Run stale process reaping every 5 minutes
      CONCURRENCY_INTERVAL = 300            # Run concurrency cleanup every 5 minutes
      BATCH_CLEANUP_INTERVAL = 3600         # Run batch cleanup every hour
      RECURRING_CLEANUP_INTERVAL = 3600     # Run recurring execution cleanup every hour
      ARCHIVE_COMPACTION_INTERVAL = 3600    # Run archive compaction every hour
      OUTBOX_CLEANUP_INTERVAL = 3600 # Run outbox cleanup every hour
      JOB_LOCK_CLEANUP_INTERVAL = 300 # Run job lock cleanup every 5 minutes
      STATS_CLEANUP_INTERVAL = 3600 # Run stats cleanup every hour
      ORPHAN_STREAM_SWEEP_INTERVAL = 3600 # Run orphan stream sweep every hour
      TABLE_MAINTENANCE_INTERVAL = Pgbus::TableMaintenance::MAINTENANCE_INTERVAL

      # Page size for archive compaction. Each cycle deletes up to this
      # many archived rows per queue. Tuned via constant rather than
      # configuration because the value rarely needs adjusting and a
      # too-small value just delays cleanup, never breaks anything.
      ARCHIVE_COMPACTION_BATCH_SIZE = 1000

      # Maintenance backoff during systemic outages. When every attempted
      # maintenance task fails for two consecutive cycles (e.g. the database
      # is down), skip maintenance entirely for an exponentially growing
      # window instead of retrying ~12 tasks every dispatch interval and
      # flooding logs and error trackers. The window doubles per consecutive
      # failed cycle and is capped. Constants (not config) per the precedent
      # above: the values rarely need tuning and only affect noise, never
      # correctness. Backoff exits the moment any task succeeds again.
      MAINTENANCE_BACKOFF_BASE = 30    # seconds; first backoff window
      MAINTENANCE_BACKOFF_MAX = 600    # seconds; window ceiling (10 minutes)

      # A pooled backend the server terminated while the dispatcher sat idle
      # between cycles surfaces with this exact PostgreSQL text on the next
      # query. It is expected and self-healing — release_maintenance_connections
      # returns the dead socket to the pool and the next cycle reconnects — so a
      # task that hits it logs a calm INFO rather than an alarming WARN. Matched
      # case-insensitively as a substring of the exception message. Common in
      # local development (Postgres restart, `rails db:migrate` terminating idle
      # backends, foreman/overmind restarting the DB), rare in production.
      TERMINATED_CONNECTION_SIGNAL = "terminating connection due to administrator command"

      # The per-task monotonic timestamps run_maintenance consults via run_if_due.
      # Enumerated so set_maintenance_timestamp can validate its argument.
      MAINTENANCE_TIMESTAMPS = %i[
        @last_cleanup_at @last_reap_at @last_concurrency_at @last_batch_cleanup_at
        @last_batch_sweep_at
        @last_recurring_cleanup_at @last_archive_compaction_at @last_stream_archive_compaction_at
        @last_outbox_cleanup_at @last_job_lock_cleanup_at @last_stats_cleanup_at
        @last_orphan_stream_sweep_at @last_table_maintenance_at
      ].freeze

      # Outcome of a single maintenance task in a cycle. Lets run_maintenance
      # distinguish success/failed/skipped and, on failure, carry the error
      # and task name for reporting or summarizing.
      MaintenanceResult = ::Struct.new(:status, :task, :error) do
        def success? = status == :success
        def skipped? = status == :skipped
      end

      # Accumulates the per-item outcomes of a task that loops over queues.
      # A task should only be considered failed when it attempted work and
      # *every* attempt failed — a partial failure (some items succeed) must
      # not trip maintenance backoff. #result returns the first error in the
      # total-failure case and nil otherwise, matching the "return e on
      # failure" contract run_if_due expects.
      class LoopOutcome
        def initialize
          @successes = 0
          @failures = 0
          @first_error = nil
        end

        def record_success
          @successes += 1
        end

        def record_failure(error)
          @failures += 1
          @first_error = error if @first_error.nil?
        end

        def result
          @first_error if @failures.positive? && @successes.zero?
        end
      end

      attr_reader :config, :maintenance_failure_streak, :maintenance_backoff_until

      def shutting_down?
        @shutting_down
      end

      # The last wall-clock timestamp (Time.now.to_f) stamped by the run loop
      # via the heartbeat's loop_tick_supplier. Wall-clock — NOT monotonic — so
      # it stays comparable across the parent/child process boundary the
      # supervisor watchdog reads it over. nil until the first stamp_loop_tick.
      def last_loop_tick
        @loop_tick_at.get
      end

      # Test seam: set one of the @last_*_at maintenance timestamps so a task
      # becomes (or stops being) due on the next run_maintenance. The timestamps
      # are per-task monotonic clocks with no constructor injection point (all
      # default to monotonic_now at construction), so a post-construction setter
      # is the minimal way to drive the due/not-due logic deterministically.
      # ivar must be one of the recognized @last_*_at names.
      def set_maintenance_timestamp(ivar, monotonic_value)
        raise ArgumentError, "unknown maintenance timestamp #{ivar}" unless MAINTENANCE_TIMESTAMPS.include?(ivar.to_sym)

        instance_variable_set(ivar, monotonic_value)
      end

      # Read one of the @last_*_at maintenance timestamps (companion to
      # set_maintenance_timestamp) so a test can assert a timestamp did or did
      # not advance without reaching into the ivar directly.
      def maintenance_timestamp(ivar)
        raise ArgumentError, "unknown maintenance timestamp #{ivar}" unless MAINTENANCE_TIMESTAMPS.include?(ivar.to_sym)

        instance_variable_get(ivar)
      end

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
        # Seed every per-task timestamp from the single source of truth so the
        # list can never drift from set_maintenance_timestamp's allow-list.
        now = monotonic_now
        MAINTENANCE_TIMESTAMPS.each { |ivar| instance_variable_set(ivar, now) }
        @maintenance_failure_streak = 0
        @maintenance_backoff_until = nil
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info do
          "[Pgbus] Dispatcher started: interval=#{config.dispatch_interval}s"
        end

        loop do
          stamp_loop_tick
          break if @shutting_down

          process_signals
          break if @shutting_down

          run_maintenance
          break if @shutting_down

          interruptible_sleep(config.dispatch_interval)
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
      end

      private

      def run_maintenance(now = monotonic_now)
        return if in_backoff?(now)

        results = run_maintenance_tasks(now)
        update_maintenance_backoff(now, results)
      ensure
        release_maintenance_connections
      end

      # Return every AR connection this cycle leased back to the pool. The
      # dispatcher is a long-lived loop that queries the pool on coarse
      # intervals (concurrency cleanup every 300s, most tasks hourly). If it
      # kept its pooled connection leased across the idle sleep, a backend the
      # server terminates while it sits idle — routine in local development
      # (a Postgres restart, `rails db:migrate` calling pg_terminate_backend,
      # foreman/overmind restarting the DB) — would be reused dead on the next
      # cycle, and every task would raise PG::ConnectionBad
      # ("terminating connection due to administrator command"). Releasing here
      # means the next cycle checks out afresh, and AR's verify!-on-checkout
      # transparently discards a dead socket and reconnects. Best-effort: a
      # failure to release must never crash the loop (mirrors the Streamer's
      # release_ar_connections).
      def release_maintenance_connections
        return unless defined?(::ActiveRecord::Base)

        Pgbus::BusRecord.connection_handler.clear_active_connections!
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Maintenance connection release failed: #{e.class}: #{e.message}" }
      end

      # True when the error is a pooled backend the server terminated while the
      # dispatcher was idle between cycles. Self-healing (see
      # TERMINATED_CONNECTION_SIGNAL / release_maintenance_connections), so
      # callers downgrade the log from WARN to INFO.
      def terminated_connection_error?(error)
        error.message.to_s.downcase.include?(TERMINATED_CONNECTION_SIGNAL)
      end

      # Log a maintenance task failure at the right volume: a self-healing
      # terminated-idle-connection is a calm INFO ("will reconnect next cycle"),
      # anything else stays a WARN operators should look at.
      def log_maintenance_failure(label, error)
        if terminated_connection_error?(error)
          Pgbus.logger.info do
            "[Pgbus] #{label} skipped: database connection was terminated while idle; " \
              "reconnecting on the next cycle (#{error.message.lines.first&.strip})"
          end
        else
          Pgbus.logger.warn { "[Pgbus] #{label} failed: #{error.message}" }
        end
      end

      # Runs every due maintenance task and returns their results. Each entry
      # is a MaintenanceResult carrying the outcome (:success/:failed/:skipped)
      # and, on failure, the exception and task name so run_maintenance can
      # decide whether to report per-task or summarize.
      def run_maintenance_tasks(now)
        results = []
        results << run_if_due(now, :@last_cleanup_at, CLEANUP_INTERVAL) { cleanup_processed_events }
        results << run_if_due(now, :@last_reap_at, REAP_INTERVAL) { reap_stale_processes }
        results << run_if_due(now, :@last_concurrency_at, CONCURRENCY_INTERVAL) { cleanup_concurrency }
        results << run_if_due(now, :@last_batch_cleanup_at, BATCH_CLEANUP_INTERVAL) { cleanup_batches }
        results << run_if_due(now, :@last_batch_sweep_at, config.batch_sweep_interval) { sweep_stalled_batches }
        results << run_if_due(now, :@last_recurring_cleanup_at, RECURRING_CLEANUP_INTERVAL) { cleanup_recurring_executions }
        results << run_if_due(now, :@last_archive_compaction_at, ARCHIVE_COMPACTION_INTERVAL) { compact_archives }
        results << run_if_due(now, :@last_stream_archive_compaction_at, ARCHIVE_COMPACTION_INTERVAL) { prune_stream_archives }
        results << run_if_due(now, :@last_outbox_cleanup_at, OUTBOX_CLEANUP_INTERVAL) { cleanup_outbox }
        results << run_if_due(now, :@last_job_lock_cleanup_at, JOB_LOCK_CLEANUP_INTERVAL) { cleanup_job_locks }
        results << run_if_due(now, :@last_stats_cleanup_at, STATS_CLEANUP_INTERVAL) { cleanup_stats }
        sweep_interval = config.streams_orphan_sweep_interval
        results << run_if_due(now, :@last_orphan_stream_sweep_at, sweep_interval) { sweep_orphan_streams } if sweep_interval
        results << run_if_due(now, :@last_table_maintenance_at, TABLE_MAINTENANCE_INTERVAL) { run_table_maintenance }
        results
      end

      # Runs the block when the interval has elapsed and reports the outcome.
      # Only updates the timestamp when the block succeeds — on failure the
      # next tick retries instead of waiting the full interval. Errors are NOT
      # reported here; run_maintenance decides whether to report per-task or
      # summarize into a single backoff warning.
      #
      # A task fails one of two ways: it raises (a few tasks let errors
      # propagate), or its own rescue returns the exception (most tasks catch
      # StandardError to log a descriptive warning, then hand the error back
      # here via `return e` so the cycle can still see the failure). Both are
      # normalized to a :failed result carrying the exception.
      def run_if_due(now, ivar, interval)
        return MaintenanceResult.new(:skipped, task_name(ivar), nil) unless now - instance_variable_get(ivar) >= interval

        outcome = yield
        return MaintenanceResult.new(:failed, task_name(ivar), outcome) if outcome.is_a?(StandardError)

        instance_variable_set(ivar, now)
        MaintenanceResult.new(:success, task_name(ivar), nil)
      rescue StandardError => e
        MaintenanceResult.new(:failed, task_name(ivar), e)
      end

      def task_name(ivar)
        ivar.to_s.delete_prefix("@last_").delete_suffix("_at")
      end

      def in_backoff?(now)
        @maintenance_backoff_until && now < @maintenance_backoff_until
      end

      # Given this cycle's results, advance or reset the failure streak and
      # the backoff window. Any success restores normal cadence. An all-failed
      # cycle (at least one task attempted, none succeeded) advances the
      # streak; the first such cycle reports every error individually, and the
      # cycle that reaches the streak threshold enters backoff with a single
      # summary warning instead.
      def update_maintenance_backoff(now, results)
        attempted = results.reject(&:skipped?)
        return if attempted.empty?

        if attempted.any?(&:success?)
          reset_maintenance_backoff
          return
        end

        enter_or_advance_backoff(now, attempted)
      end

      def reset_maintenance_backoff
        @maintenance_failure_streak = 0
        @maintenance_backoff_until = nil
      end

      def enter_or_advance_backoff(now, failures)
        first_failing_cycle = @maintenance_failure_streak.zero?
        @maintenance_failure_streak += 1

        if first_failing_cycle
          report_maintenance_failures(failures)
        elsif @maintenance_failure_streak >= 2
          begin_backoff(now, failures)
        end
      end

      def report_maintenance_failures(failures)
        failures.each do |result|
          ErrorReporter.report(result.error, { action: "dispatcher_maintenance", task: result.task })
        end
      end

      def begin_backoff(now, failures)
        delay = backoff_delay(@maintenance_failure_streak)
        @maintenance_backoff_until = now + delay
        first = failures.first
        Pgbus.logger.warn do
          "[Pgbus] Dispatcher maintenance backoff: #{failures.size} task(s) failing " \
            "(#{first.error.class}: #{first.error.message}); skipping maintenance for #{delay}s"
        end
      end

      # Exponential window: BASE * 2**(streak - 2), capped at MAX. Streak 2 is
      # the first backoff (2**0 == 1 == BASE), streak 3 doubles it, and so on.
      def backoff_delay(streak)
        [MAINTENANCE_BACKOFF_BASE * (2**(streak - 2)), MAINTENANCE_BACKOFF_MAX].min
      end

      def cleanup_processed_events
        ttl = config.idempotency_ttl
        return unless ttl&.positive?

        deleted = ProcessedEvent.expired(Time.current - ttl).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} expired processed events" } if deleted.positive?
      rescue StandardError => e
        log_maintenance_failure("Idempotency cleanup", e)
        e
      end

      def reap_stale_processes
        threshold = Heartbeat::ALIVE_THRESHOLD
        deleted = ProcessEntry.stale(Time.current - threshold).delete_all
        Pgbus.logger.info { "[Pgbus] Reaped #{deleted} stale processes" } if deleted.positive?
      rescue StandardError => e
        log_maintenance_failure("Stale process reaping", e)
        e
      end

      def cleanup_concurrency
        expired_keys = Concurrency::Semaphore.expire_stale
        expired_keys.each do |row|
          release_blocked_for_key(row["key"])
        end

        orphaned = Concurrency::BlockedExecution.expire_stale
        Pgbus.logger.debug { "[Pgbus] Expired #{orphaned} orphaned blocked executions" } if orphaned.positive?
      rescue StandardError => e
        log_maintenance_failure("Concurrency cleanup", e)
        e
      end

      def release_blocked_for_key(key)
        promoted = Concurrency::BlockedExecution.promote_next(key, client: Pgbus.client)
        Pgbus.logger.debug { "[Pgbus] Released blocked execution for key: #{key}" } if promoted
      rescue StandardError => e
        log_maintenance_failure("Releasing blocked execution for #{key}", e)
      end

      def cleanup_batches
        retention = config.batch_retention
        return unless retention&.positive?

        deleted = Batch.cleanup(older_than: Time.current - retention)
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} finished batches" } if deleted.positive?
      rescue StandardError => e
        log_maintenance_failure("Batch cleanup", e)
        e
      end

      def sweep_stalled_batches
        Batch.sweep_stalled
      rescue StandardError => e
        log_maintenance_failure("Batch sweep", e)
        e
      end

      def cleanup_stats
        retention = config.stats_retention
        return unless retention&.positive?

        cutoff = Time.current - retention

        if config.stats_enabled
          deleted = JobStat.cleanup!(older_than: cutoff)
          Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old job stats" } if deleted.positive?
        end

        return unless config.streams_stats_enabled

        deleted = StreamStat.cleanup!(older_than: cutoff)
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old stream stats" } if deleted.positive?
      end

      def run_table_maintenance
        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        raw_conn = conn.raw_connection
        maintained = TableMaintenance.run_maintenance(
          raw_conn,
          threshold: TableMaintenance::BLOAT_THRESHOLD,
          reindex: true,
          stop_check: -> { @shutting_down }
        )
        Pgbus.logger.info { "[Pgbus] Table maintenance completed: #{maintained} table(s) vacuumed" } if maintained.positive?
      rescue StandardError => e
        log_maintenance_failure("Table maintenance", e)
        e
      end

      # Polled at the top of each per-queue maintenance loop. Returns true when
      # a shutdown has begun, logging one summary line so the interruption is
      # visible. `index` is how many queues were already processed, so the
      # message reads "after N of M". An interrupted (early-returned) task
      # counts as success in run_if_due and simply runs again next interval.
      def interrupted?(label, index, total)
        return false unless @shutting_down

        Pgbus.logger.info { "[Pgbus] #{label} interrupted by shutdown after #{index} of #{total}" }
        true
      end

      def cleanup_job_locks
        # Clean up truly orphaned uniqueness keys: rows whose referenced
        # message no longer exists in the PGMQ queue. This handles crashes
        # or queue truncation. It must NEVER delete a lock while the message
        # is still in the queue, even if the lock is "old" — recurring jobs
        # that fail and retry can hold locks for hours.
        reaped = reap_orphaned_uniqueness_keys
        Pgbus.logger.info { "[Pgbus] Reaped #{reaped} orphaned uniqueness keys" } if reaped.positive?
      rescue StandardError => e
        log_maintenance_failure("Job lock cleanup", e)
        e
      end

      def reap_orphaned_uniqueness_keys
        keys = UniquenessKey.all.to_a
        return 0 if keys.empty?

        # Only consider locks that are old enough that we wouldn't be racing
        # an in-flight enqueue. visibility_timeout * 2 is the floor — anything
        # younger could be a freshly-acquired lock whose send_message hasn't
        # committed yet.
        threshold = Time.current - (config.visibility_timeout * 2)

        orphaned = keys.select do |key|
          next false unless key.created_at && key.created_at < threshold
          next false unless key.queue_name

          message_gone?(key)
        end

        return 0 if orphaned.empty?

        UniquenessKey.where(lock_key: orphaned.map(&:lock_key)).delete_all
      end

      # Returns true if the message referenced by this lock is definitely gone
      # from the queue. Returns false otherwise (message present, or unknown).
      #
      # Routes through Pgbus::Client#message_exists? so all PGMQ access stays
      # behind the client interface. The client returns nil when it can't
      # determine the answer (queue table missing, etc.); we treat that as
      # "still here" — the reaper must NEVER delete a lock when in doubt.
      def message_gone?(key)
        msg_id = key.msg_id.to_i
        result = if msg_id.positive?
                   Pgbus.client.message_exists?(key.queue_name, msg_id: msg_id)
                 else
                   Pgbus.client.message_exists?(key.queue_name, uniqueness_key: key.lock_key)
                 end

        result == false
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Reap check failed for #{key.lock_key}: #{e.message}" }
        false
      end

      def cleanup_outbox
        return unless config.outbox_enabled

        retention = config.outbox_retention
        return unless retention&.positive?

        deleted = OutboxEntry.published_before(Time.current - retention).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} published outbox entries" } if deleted.positive?
      rescue StandardError => e
        log_maintenance_failure("Outbox cleanup", e)
        e
      end

      def compact_archives
        retention = config.archive_retention
        return unless retention&.positive?

        cutoff = Time.current - retention
        batch_size = ARCHIVE_COMPACTION_BATCH_SIZE
        prefix = config.queue_prefix
        # Stream queues share the job prefix but are pruned separately by
        # prune_stream_archives (per-stream retention). Skip them here so a
        # stream archive is compacted by exactly one path, not both.
        stream_names = current_stream_queue_names

        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

        outcome = LoopOutcome.new
        queue_names.each_with_index do |full_name, index|
          break if interrupted?("Archive compaction", index, queue_names.size)
          next unless full_name.start_with?("#{prefix}_")
          next if stream_names.include?(full_name)

          stripped = full_name.delete_prefix("#{prefix}_")
          deleted = Pgbus.client.purge_archive(stripped, older_than: cutoff, batch_size: batch_size)
          Pgbus.logger.debug { "[Pgbus] Compacted #{deleted} archive entries from #{full_name}" } if deleted.positive?
          outcome.record_success
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Archive compaction failed for #{full_name}: #{e.message}" }
          outcome.record_failure(e)
        end
        outcome.result
      rescue StandardError => e
        log_maintenance_failure("Archive compaction", e)
        e
      end

      # Prunes per-stream archive tables. Unlike compact_archives (which
      # uses the global archive_retention, typically 7 days), streams need
      # per-stream retention because a chat-history stream and a
      # presence-ping stream have wildly different storage requirements.
      # Lookup order for a given queue: exact string match, then regex
      # match, then streams_default_retention (5 minutes by default).
      def prune_stream_archives
        stream_names = current_stream_queue_names
        return if stream_names.empty?

        batch_size = ARCHIVE_COMPACTION_BATCH_SIZE
        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

        outcome = LoopOutcome.new
        queue_names.each_with_index do |full_name, index|
          break if interrupted?("Stream archive compaction", index, queue_names.size)
          next unless stream_names.include?(full_name)

          retention = retention_for_stream_queue(full_name)
          next unless retention.positive?

          cutoff = Time.current - retention
          stripped = full_name.delete_prefix("#{config.queue_prefix}_")
          deleted = Pgbus.client.purge_archive(stripped, older_than: cutoff, batch_size: batch_size)
          if deleted.positive?
            Pgbus.logger.debug do
              "[Pgbus] Compacted #{deleted} stream archive entries from #{full_name}"
            end
          end
          outcome.record_success
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Stream archive compaction failed for #{full_name}: #{e.message}" }
          outcome.record_failure(e)
        end
        outcome.result
      rescue StandardError => e
        log_maintenance_failure("Stream archive compaction", e)
        e
      end

      def retention_for_stream_queue(full_name)
        retention_map = config.streams_retention || {}
        # Exact-match first — cheapest and most common path
        exact = retention_map[full_name]
        return exact.to_f if exact

        # Regex-match second for pattern-based overrides (e.g. /^chat_/)
        retention_map.each do |key, value|
          return value.to_f if key.is_a?(Regexp) && key.match?(full_name)
        end

        config.streams_default_retention.to_f
      end

      def sweep_orphan_streams
        threshold = config.streams_orphan_threshold
        return unless threshold

        stream_names = current_stream_queue_names
        return if stream_names.empty?

        conn = config.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

        dropped = 0
        outcome = LoopOutcome.new
        queue_names.each_with_index do |full_name, index|
          break if interrupted?("Orphan stream sweep", index, queue_names.size)
          next unless stream_names.include?(full_name)

          safe_name = QueueNameValidator.sanitize!(full_name)
          row = conn.select_one(<<~SQL, "Pgbus Orphan Check")
            SELECT
              (SELECT count(*) FROM pgmq.q_#{safe_name}) AS queue_length,
              (SELECT EXTRACT(epoch FROM (NOW() - created_at))::int
                 FROM pgmq.meta WHERE queue_name = '#{safe_name}') AS age_sec
          SQL

          next unless row

          empty = !row["queue_length"].to_i.positive?
          # Durable stream q_ tables are never drained by delivery (non-consuming
          # peek), so a non-empty queue that has aged past the threshold is a
          # leak, not a live stream — drop it. A nil age_sec (unknown created_at)
          # is treated as young, never as infinitely old.
          age_sec = row["age_sec"]
          stale = !age_sec.nil? && age_sec.to_i >= threshold.to_i
          next unless empty || stale

          Pgbus.client.drop_queue(full_name, prefixed: false)
          dropped += 1
          reason = empty ? "empty" : "aged past #{threshold.to_i}s threshold"
          Pgbus.logger.info { "[Pgbus] Dropped orphan stream queue (#{reason}): #{full_name}" }
          outcome.record_success
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Orphan stream sweep failed for #{full_name}: #{e.message}" }
          outcome.record_failure(e)
        end

        Pgbus.logger.debug { "[Pgbus] Orphan stream sweep complete: dropped #{dropped} queue(s)" } if dropped.positive?
        outcome.result
      rescue StandardError => e
        log_maintenance_failure("Orphan stream sweep", e)
        e
      end

      # Fresh set of physical stream-queue names for this maintenance pass.
      # Reset first so a stream created since the last hourly pass is picked
      # up without a process restart. Includes fingerprint-matched dormant
      # streams that never re-registered after the registry migration
      # (issue #366) so orphan sweep / stream-archive prune can still see
      # them. Empty only when neither the registry nor any fingerprint
      # matches exist — callers then treat every queue as a job queue.
      def current_stream_queue_names
        Pgbus::StreamQueue.reset_cache!
        Pgbus::StreamQueue.known_names
      end

      def cleanup_recurring_executions
        retention = config.recurring_execution_retention
        return unless retention&.positive?

        deleted = RecurringExecution.older_than(Time.current - retention).delete_all
        Pgbus.logger.debug { "[Pgbus] Cleaned up #{deleted} old recurring executions" } if deleted.positive?
      rescue StandardError => e
        log_maintenance_failure("Recurring execution cleanup", e)
        e
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Wall clock (not monotonic) so the dashboard can compare the beacon
      # against Time.now across processes; the heartbeat timer thread reads it
      # via loop_tick_supplier. A dispatcher wedged inside run_maintenance stops
      # advancing this while heartbeats keep firing, so the beacon ages and the
      # processes page can surface the stall.
      def stamp_loop_tick
        @loop_tick_at.set(Time.now.to_f)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "dispatcher",
          metadata: { pid: ::Process.pid },
          loop_tick_supplier: -> { @loop_tick_at.get }
        )
        @heartbeat.start
      end

      def shutdown
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Dispatcher stopped" }
      end
    end
  end
end
