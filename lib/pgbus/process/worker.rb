# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Worker
      include SignalHandler

      attr_reader :queues, :threads, :config, :execution_mode, :read_ahead,
                  :rate_counter, :wake_signal, :restore_streak, :lifecycle
      # stat_buffer is writable so a test can swap in a buffer double after
      # construction to assert graceful_shutdown / check_recycle flush it. The
      # executor captured the original buffer at construction, but these paths
      # flush @stat_buffer directly, so the swap is observable.
      attr_accessor :stat_buffer
      # notify_listener / notify_retry_at / notify_retry_backoff are writable so
      # tests can seed the self-healing listener state, re-arm the backoff window
      # between calls, and simulate start_notify_listener assigning the listener
      # from inside a stub (production mutates all three in the run loop).
      attr_accessor :notify_listener, :notify_retry_at, :notify_retry_backoff
      # Supervisor-mediated wake source (issue #381, worker_notify_scope:
      # :supervisor). Non-nil iff the supervisor forked us with a wake pipe;
      # its presence switches the whole notify lifecycle from a fork-local
      # NotifyListener to the shared hub. Readable as a test seam.
      attr_reader :wake_pipe

      # The collaborators below (rate_counter, wake_signal, stat_buffer) and the
      # recycle clock (started_at_monotonic) accept injected seeds so tests can
      # observe or stub them without poking private ivars. All default to the
      # exact values production constructs, so behavior is unchanged.
      def initialize(queues:, threads: 5, config: Pgbus.configuration,
                     single_active_consumer: false, consumer_priority: 0,
                     execution_mode: :threads, group_mode: nil, liveness_pipe: nil,
                     rate_counter: nil, wake_signal: nil, stat_buffer: :default,
                     notify_listener: nil, notify_retry_at: 0.0,
                     notify_retry_backoff: NOTIFY_RETRY_BASE_SECONDS,
                     started_at_monotonic: nil, wake_pipe: nil, read_ahead: nil)
        @queues = Array(queues)
        @initial_queues = @queues.dup.freeze
        @wildcard = @queues.include?("*")
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @group_mode = case group_mode
                      when nil then nil
                      when Symbol then group_mode
                      when String then group_mode.to_sym
                      else
                        raise ArgumentError,
                              "Invalid group_mode type: #{group_mode.class}. Must be nil, String, or Symbol"
                      end
        unless Pgbus::Configuration::VALID_GROUP_MODES.include?(@group_mode)
          raise ArgumentError,
                "Invalid group_mode: #{@group_mode.inspect}. Must be nil, :fifo, or :round_robin"
        end
        if FairShare.enabled?(config) && (@group_mode || config.group_mode)
          raise Pgbus::ConfigurationError,
                "fair_share and group_mode are mutually exclusive — this worker has group_mode " \
                "#{(@group_mode || config.group_mode).inspect} while config.fair_share is set"
        end
        @single_active_consumer = single_active_consumer
        @consumer_priority = consumer_priority
        @lifecycle = Lifecycle.new
        @last_wildcard_resolve = nil
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @jobs_failed = Concurrent::AtomicFixnum.new(0)
        # in_flight counts every claimed message: buffered and running.
        @in_flight = Concurrent::AtomicFixnum.new(0)
        @read_ahead = read_ahead || config.read_ahead
        @claim_buffer = ClaimBuffer.new(config: config)
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
        @rate_counter = rate_counter || RateCounter.new(:processed, :failed, :dequeued)
        @started_at = Time.current
        @started_at_monotonic = started_at_monotonic || monotonic_now
        # stat_buffer: :default means "build one iff config.stats_enabled";
        # passing an explicit value (including nil) overrides that for tests.
        @stat_buffer =
          if stat_buffer == :default
            if config.stats_enabled
              Pgbus::StatBuffer.new(
                flush_size: config.stats_flush_size,
                flush_interval: config.stats_flush_interval
              )
            end
          else
            stat_buffer
          end
        @executor = Pgbus::ActiveJob::Executor.new(stat_buffer: @stat_buffer)
        @wake_signal = wake_signal || WakeSignal.new
        @pool = ExecutionPools.build(
          mode: @execution_mode,
          capacity: threads,
          on_state_change: -> { @wake_signal.notify! }
        )
        @circuit_breaker = Pgbus::CircuitBreaker.new(config: config)
        @drain_started_at = nil
        # Evict/restore cooldown state (issue #209). When a permanently-deleted
        # queue is evicted down to an empty list, restoring the initial queues
        # immediately re-triggers the same undefined-table error every loop tick.
        # These back off the restore attempt on an exponential schedule instead.
        @restore_streak = 0
        @last_evicted_at = nil
        @deferral_warned = false
        @queue_lock = QueueLock.new if @single_active_consumer
        @notify_listener = notify_listener
        @notify_retry_at = notify_retry_at
        @notify_retry_backoff = notify_retry_backoff
        # OS-level liveness channel to the supervisor watchdog. Optional: nil
        # unless the supervisor forked us with one. Written from stamp_loop_tick
        # so the watchdog can detect a wedged worker even when the database (and
        # thus the Heartbeat's loop_tick_at) is unavailable.
        @liveness_pipe = liveness_pipe
        # Supervisor wake pipe (read end, inherited across fork). When present
        # the fork owns NO LISTEN connection: wakes and listener-health status
        # arrive as bytes from the supervisor's NotifyHub.
        @wake_pipe = wake_pipe ? WakePipe.new(wake_pipe, wake_signal: @wake_signal) : nil
      end

      def stats
        {
          jobs_processed: @jobs_processed.value,
          jobs_failed: @jobs_failed.value,
          in_flight: @in_flight.value,
          buffered: @claim_buffer.size,
          state: @lifecycle.state,
          execution_mode: @execution_mode,
          consumer_priority: @consumer_priority,
          single_active_consumer: @single_active_consumer,
          locked_queues: @queue_lock&.held_queues || [],
          rates: @rate_counter.rates,
          started_at: @started_at
        }.merge(@pool.metadata)
      end

      # Test seams for the atomic counters recycle logic and prefetch
      # flow-control consult. Production only increments these during message
      # handling; seeding them lets a test cross a threshold without running
      # thousands of real jobs.
      def jobs_processed=(count)
        @jobs_processed.value = count
      end

      def in_flight=(count)
        @in_flight.value = count
      end

      # The last wall-clock loop-tick stamp (Time.now.to_f) fed to the
      # heartbeat's loop_tick_supplier. Wall-clock — NOT monotonic — so it stays
      # comparable across the process boundary the supervisor watchdog reads it
      # over. nil until the first stamp_loop_tick.
      def last_loop_tick
        @loop_tick_at.get
      end

      NOTIFY_FALLBACK_POLL_SECONDS = 15

      # NotifyListener startup can fail on a transient boot-time condition (DB
      # restarting, failover, DNS blip) or its thread can die mid-run. Rather
      # than downgrade to blind polling until process restart, the loop retries
      # from ensure_notify_listener on an exponential backoff: NOTIFY_RETRY_BASE
      # doubling up to NOTIFY_RETRY_MAX. Constant-tuned, matching the
      # CircuitBreaker/Dispatcher precedent.
      NOTIFY_RETRY_BASE_SECONDS = 5
      NOTIFY_RETRY_MAX_SECONDS = 300

      # Residual pool wait in #shutdown, AFTER the drain loop already spent up
      # to config.drain_timeout on in-flight jobs. Short by design: a job still
      # running has proven it won't finish, and this wait competes with the
      # supervisor's shutdown_timeout deadline (issue #386).
      POOL_TERMINATION_WAIT = 5

      def run
        setup_signals
        start_heartbeat
        resolve_wildcard_queues
        ensure_fair_indexes
        start_wake_source
        @lifecycle.transition_to!(:running)
        Pgbus.logger.info do
          "[Pgbus] Worker started: queues=#{queues.join(",")} threads=#{threads} " \
            "mode=#{@execution_mode} notify_wakeup=#{notify_wakeup?} pid=#{::Process.pid}"
        end

        loop do
          stamp_loop_tick
          process_signals
          check_recycle
          refresh_wildcard_queues
          ensure_notify_listener
          # Draining, paused or stopped: claims nobody will run here go back
          # to the queue now rather than when their timeout runs out.
          return_claims unless @lifecycle.can_process?

          break if @lifecycle.stopped?
          # quiesced? (all slots free), not idle? (any slot free) — exiting
          # with work still in flight abandons those jobs to shutdown's short
          # POOL_TERMINATION_WAIT residual. Bounded by config.drain_timeout so
          # a stuck job can't wedge the loop forever.
          break if @lifecycle.draining? && (@pool.quiesced? || drain_deadline_exceeded?)

          claim_and_execute if @lifecycle.can_process?
          @stat_buffer&.flush_if_due
          @wake_signal.wait(timeout: config.polling_interval) if @lifecycle.draining? || @lifecycle.paused?
        end

        shutdown
      end

      def graceful_shutdown
        Pgbus.logger.info { "[Pgbus] Worker shutting down gracefully..." }
        Pgbus.stopping = true
        @lifecycle.transition_to(:draining)
        # Flush buffered stats at drain entry so the ≤ stats_flush_interval /
        # stats_flush_size window isn't lost if the supervisor watchdog SIGKILLs
        # a stalled worker before the drain-loop shutdown flush runs. Runs on the
        # main loop thread (signals are dispatched via process_signals, not in
        # trap context), so the DB write is safe. flush is thread-safe and
        # no-ops when the buffer is empty.
        @stat_buffer&.flush
        @wake_signal.notify!
      end

      def immediate_shutdown
        Pgbus.logger.warn { "[Pgbus] Worker shutting down immediately!" }
        Pgbus.stopping = true
        @lifecycle.transition_to!(:stopped)
        @wake_signal.notify!
        @pool.kill
      end

      WILDCARD_REFRESH_INTERVAL = 30 # seconds

      # Matches the physical queue name inside a "relation \"pgmq.q_foo\" does
      # not exist" error. Frozen module constant to avoid recompiling the
      # regex on every queue-missing error in hot fetch/read paths.
      MISSING_QUEUE_REGEX = /pgmq\.q_(\w+)/

      # Exponential backoff bounds for restoring evicted queues (issue #209).
      # After the worker's queues are fully evicted (queue table permanently
      # deleted), the first restore waits RESTORE_COOLDOWN_BASE seconds; each
      # consecutive failed restore doubles the wait up to RESTORE_COOLDOWN_MAX.
      # A successful fetch resets the streak so a recreated queue restores
      # promptly. Constant-tuned, matching the NOTIFY_RETRY precedent.
      RESTORE_COOLDOWN_BASE = 30
      RESTORE_COOLDOWN_MAX = 300

      private

      # One loop step (issue #486): feed buffered claims to free slots, read
      # the deficit (free slots + read_ahead - buffered, capped by
      # prefetch_limit), feed again. With read_ahead 0 the buffer never
      # outlives the step and qty is the free slot count, as before. Waits
      # only when the step moved nothing, so a full pool with a topped-up
      # buffer sleeps until a slot frees (the pool's on_state_change wakes it).
      def claim_and_execute
        drained = drain_claim_buffer
        claimed = claim_deficit
        drained += drain_claim_buffer
        @wake_signal.wait(timeout: wake_timeout) if drained.zero? && claimed.zero?
      end

      # The hold is released before the executor's own heartbeat tracking
      # registers the same key, so the message is never untracked in between.
      def drain_claim_buffer
        @claim_buffer.drain_into(@pool) do |claim|
          @pool.post do
            VisibilityHeartbeat.release(claim.hold)
            process_message(claim.message, claim.queue_name, source_queue: claim.source_queue)
          end
        end
      end

      def claim_deficit
        prefetch_room = config.prefetch_limit && (config.prefetch_limit - @in_flight.value)
        want = @claim_buffer.deficit(free_slots: @pool.available_capacity, read_ahead: @read_ahead,
                                     prefetch_room: prefetch_room)
        return 0 if want.zero?

        tagged_messages = fetch_messages(want)
        return 0 if tagged_messages.empty?

        @rate_counter.increment(:dequeued, tagged_messages.size)
        client = Pgbus.client
        tagged_messages.each do |queue_name, message, source_queue|
          detect_zombie(queue_name, message)
          @in_flight.increment
          @claim_buffer.push(queue_name, message, source_queue, client: client)
        end
        tagged_messages.size
      end

      def return_claims
        return if @claim_buffer.empty?

        @in_flight.decrement(@claim_buffer.return_all!(client: Pgbus.client))
      end

      # Returns an array of [queue_name, message] pairs so we always know
      # which queue each message came from.
      def fetch_messages(qty)
        restore_evicted_queues if queues.empty? && !@wildcard

        active_queues = queues.reject { |q| @circuit_breaker.paused?(q) }
        active_queues = active_queues.select { |q| @queue_lock.try_lock(q) } if @single_active_consumer

        if active_queues.empty?
          Pgbus.logger.debug do
            paused = queues.select { |q| @circuit_breaker.paused?(q) }
            "[Pgbus] Worker fetch: all queues filtered — queues=#{queues.join(",")} " \
              "paused=#{paused.join(",")}"
          end
          return []
        end

        results =
          if priority_enabled?
            fetch_prioritized(active_queues, qty)
          elsif @group_mode
            fetch_grouped(active_queues, qty)
          elsif fair_share_enabled?
            fetch_fair(active_queues, qty)
          elsif active_queues.size == 1
            queue = active_queues.first
            messages = Pgbus.client.read_batch(queue, qty: qty) || []
            messages.map { |m| [queue, m] }
          else
            fetch_multi(active_queues, qty)
          end

        # A read that reached here without an undefined-queue error means the
        # queue tables exist again; drop the restore backoff so a recreated
        # queue is reinstated promptly after the next eviction (issue #209).
        @restore_streak = 0
        results
      rescue Pgbus::ConnectionCircuitOpenError
        # The client-level connection breaker is open: the database has failed
        # enough consecutive connection attempts that reads now fail fast. Idle
        # this poll without an ErrorReporter call — the whole point of the
        # breaker is to stop every worker flooding the error tracker for the
        # duration of a database outage. The open/close transitions are logged
        # once by the client (Client#log_circuit_open / #log_circuit_close),
        # not per poll here.
        []
      rescue StandardError => e
        if undefined_queue_table_error?(e)
          evict_missing_queues(e)
        else
          ErrorReporter.report(e, { action: "fetch_messages", queues: active_queues })
        end
        []
      end

      # Detect "queue table missing" via the underlying PG::UndefinedTable
      # cause when available. Falls back to a guarded message check that
      # requires BOTH "pgmq.q_" (so we know it's our queue table) and
      # "does not exist", which keeps the eviction logic working for
      # adapters/exception wrappers that don't preserve the original
      # PG::UndefinedTable as #cause (e.g. PGMQ::Errors::ConnectionError
      # raised by pgmq-ruby's auto-reconnect path). Locale-fragile, but
      # this is gated by the very specific "pgmq.q_" prefix so a false
      # positive can only come from another error mentioning that exact
      # string — which is itself a queue-table error worth handling.
      def undefined_queue_table_error?(error)
        cause = error.respond_to?(:cause) ? error.cause : nil
        return true if defined?(PG::UndefinedTable) && cause.is_a?(PG::UndefinedTable)
        return true if error.message.include?("pgmq.q_") && error.message.include?("does not exist")

        false
      end

      def fetch_prioritized(active_queues, qty)
        remaining = qty
        results = []

        active_queues.each do |q|
          break if remaining <= 0

          batch = Pgbus.client.read_batch_prioritized(q, qty: remaining)
          batch.each do |physical_queue, message|
            results << [q, message, physical_queue]
          end
          remaining -= batch.size
        end

        results
      end

      # Use pgmq-ruby's read_multi to read from all queues in a single
      # SQL query (UNION ALL). Each returned message carries a queue_name
      # field so we can map it back to the logical queue.
      #
      # `qty` is the total pool capacity. pgmq-ruby treats `qty:` as per-queue,
      # so we also pass `limit: qty` to cap the total across all queues —
      # otherwise we get `queue_count * qty` messages and overflow the
      # execution pool, crashing the worker fork (issue #123).
      def fetch_multi(active_queues, qty)
        messages = Pgbus.client.read_multi(active_queues, qty: qty, limit: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || active_queues.first
          [logical, m]
        end
      end

      # Use grouped reads for fair or throughput-optimized multi-tenant processing.
      # Each queue is read independently with the configured group strategy.
      def fetch_grouped(active_queues, qty)
        remaining = qty
        results = []

        active_queues.each do |queue|
          break if remaining <= 0

          messages = case @group_mode
                     when :round_robin
                       Pgbus.client.read_grouped_rr(queue, qty: remaining) || []
                     else # :fifo
                       Pgbus.client.read_grouped(queue, qty: remaining) || []
                     end

          messages.each { |m| results << [queue, m] }
          remaining -= messages.size
        end

        results
      end

      # Fair share (issue #426): one weighted-interleave read per queue, in
      # list order with the remaining capacity — strict priority ACROSS queues
      # (the capsule DSL contract), fair WITHIN each queue. Same loop shape as
      # fetch_prioritized; under priority_levels the client composes both.
      def fetch_fair(active_queues, qty)
        remaining = qty
        results = []

        active_queues.each do |queue|
          break if remaining <= 0

          messages = Pgbus.client.read_batch_fair(queue, qty: remaining) || []
          messages.each { |m| results << [queue, m] }
          remaining -= messages.size
        end

        results
      end

      def fair_share_enabled?
        FairShare.enabled?(config)
      end

      # Build the fair index on every queue this worker serves (idempotent,
      # memoized in the client, CONCURRENTLY so a populated queue is never
      # write-locked). Queues created later get theirs at creation time.
      def ensure_fair_indexes
        return unless fair_share_enabled?

        queues.each { |q| Pgbus.client.ensure_fair_index(q) }
      end

      def priority_enabled?
        config.priority_levels && config.priority_levels > 1
      end

      def process_message(message, queue_name, source_queue: nil)
        result = @executor.execute(message, queue_name, source_queue: source_queue)
        @jobs_processed.increment
        @rate_counter.increment(:processed)
        if result == :failed
          @jobs_failed.increment
          @rate_counter.increment(:failed)
          @circuit_breaker.record_failure(queue_name)
        else
          @circuit_breaker.record_success(queue_name)
        end
      rescue StandardError => e
        @jobs_failed.increment
        @rate_counter.increment(:failed)
        @circuit_breaker.record_failure(queue_name)
        ErrorReporter.report(e, { action: "process_message", queue: queue_name })
      ensure
        @in_flight.decrement
      end

      # Resolve "*" to all non-DLQ queues from pgmq.meta, stripping the prefix.
      def resolve_wildcard_queues
        return unless @wildcard

        # Exclusion rationale (streams, event queues, DLQs) lives with the
        # shared resolver — the NotifyHub uses the same one, so the LISTEN
        # union and the fork's adopted set can't drift (issue #381).
        resolved = WildcardQueueResolver.resolve(config: config)

        if resolved.empty?
          Pgbus.logger.warn { "[Pgbus] Wildcard queue '*' resolved to no queues — falling back to default" }
          @queues = [config.default_queue]
        else
          if @last_wildcard_resolve && resolved != @queues
            Pgbus.logger.info { "[Pgbus] Wildcard queues changed: #{@queues.join(", ")} → #{resolved.join(", ")}" }
          end
          @queues = resolved
          Pgbus.logger.info { "[Pgbus] Wildcard queue '*' resolved to: #{@queues.join(", ")}" } unless @last_wildcard_resolve
        end
        @last_wildcard_resolve = monotonic_now
        sync_notify_listener_queues
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to resolve wildcard queues: #{e.message} — falling back to default" }
        @queues = [config.default_queue] unless @last_wildcard_resolve
      end

      # Periodically re-resolve wildcard queues to pick up new queues and
      # drop deleted ones without requiring a worker restart.
      def refresh_wildcard_queues
        return unless @wildcard
        return if @last_wildcard_resolve && (monotonic_now - @last_wildcard_resolve) < WILDCARD_REFRESH_INTERVAL

        resolve_wildcard_queues
        ensure_fair_indexes
      end

      # When a "relation does not exist" error occurs, the queue was deleted.
      # Extract the queue name from the error and remove it from the active list.
      def evict_missing_queues(error)
        prefix = "#{config.queue_prefix}_"
        if (match = MISSING_QUEUE_REGEX.match(error.message))
          physical_name = match[1]
          logical_name = physical_name.delete_prefix(prefix)
          if @queues.delete(logical_name)
            Pgbus.logger.warn { "[Pgbus] Evicted deleted queue '#{logical_name}' (#{physical_name}) from worker" }
          end
        end
        Pgbus.logger.error { "[Pgbus] Queue table missing: #{error.message}" }
        return unless @queues.empty? && !@wildcard

        # Open (or re-arm) the restore cooldown window: stamp the eviction time so
        # restore_evicted_queues can measure the backoff, and clear the
        # per-window deferral-warned flag so the next deferral logs exactly once.
        @last_evicted_at = monotonic_now
        @deferral_warned = false
        restore_evicted_queues
      end

      # Restore the worker's initial queues after a full eviction — but only once
      # the exponential cooldown has elapsed (issue #209). While the cooldown is
      # pending, leaves @queues empty (the caller idles via the empty-active_queues
      # path) and logs a single deferral warn per window rather than one error pair
      # per loop tick. Each actual restore escalates the streak so a permanently
      # deleted queue backs off toward RESTORE_COOLDOWN_MAX instead of spinning.
      def restore_evicted_queues
        return unless restore_cooldown_elapsed?

        @queues = @initial_queues.dup
        @restore_streak += 1
        Pgbus.logger.warn do
          "[Pgbus] Worker queue list was empty after eviction — " \
            "restoring initial queues: #{@queues.join(", ")}"
        end
      end

      # True once RESTORE_COOLDOWN_BASE * 2**streak seconds (capped at
      # RESTORE_COOLDOWN_MAX) have passed since the window opened. When still
      # pending, emits at most one deferral warn per window naming the wait.
      def restore_cooldown_elapsed?
        return true if @last_evicted_at.nil?

        wait = restore_cooldown_seconds
        return true if monotonic_now - @last_evicted_at >= wait

        unless @deferral_warned
          @deferral_warned = true
          Pgbus.logger.warn do
            "[Pgbus] All queues evicted; deferring restore — next restore attempt in #{wait}s " \
              "(streak=#{@restore_streak})"
          end
        end
        false
      end

      def restore_cooldown_seconds
        [RESTORE_COOLDOWN_BASE * (2**@restore_streak), RESTORE_COOLDOWN_MAX].min
      end

      def detect_zombie(queue_name, message)
        return unless config.zombie_detection
        return unless message.read_ct.to_i > 1

        return if FailedEventRecorder.exists?(queue_name: queue_name, msg_id: message.msg_id.to_i)

        Pgbus.logger.warn do
          "[Pgbus] Zombie message redelivered: queue=#{queue_name} msg_id=#{message.msg_id} " \
            "read_ct=#{message.read_ct} — previous read did not record a failure. " \
            "The worker may have crashed mid-execute or the executor silently dropped the job."
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Zombie detection failed: #{e.class}: #{e.message}" }
      end

      # Lazily stamps the drain start on first call — the predicate is only
      # reached while draining, so this covers every path into the drain
      # state (graceful_shutdown, recycling) without hooking each one.
      def drain_deadline_exceeded?
        @drain_started_at ||= monotonic_now
        return false unless monotonic_now - @drain_started_at > config.drain_timeout

        Pgbus.logger.warn do
          "[Pgbus] Worker drain deadline (#{config.drain_timeout}s) reached with #{@in_flight.value} job(s) " \
            "still in flight — proceeding to shutdown"
        end
        true
      end

      def check_recycle
        return unless @lifecycle.running?

        reason = recycle_reason
        return unless reason

        Pgbus.stopping = true
        @lifecycle.transition_to(:draining)
        # Flush buffered stats on recycle-triggered drain for the same reason as
        # graceful_shutdown: shrink the SIGKILL loss window. Same-thread, safe.
        @stat_buffer&.flush
        Pgbus::Instrumentation.instrument(
          "pgbus.worker.recycle",
          reason: reason,
          jobs_processed: @jobs_processed.value,
          memory_mb: current_memory_mb,
          lifetime_seconds: monotonic_now - @started_at_monotonic
        )
        @wake_signal.notify!
      end

      def recycle_reason
        return :max_jobs if exceeded_max_jobs?
        return :max_memory if exceeded_max_memory?
        return :max_lifetime if exceeded_max_lifetime?

        nil
      end

      def recycle_needed?
        !recycle_reason.nil?
      end

      def exceeded_max_jobs?
        return false unless config.max_jobs_per_worker && @jobs_processed.value >= config.max_jobs_per_worker

        Pgbus.logger.info { "[Pgbus] Worker recycling: max_jobs reached (#{@jobs_processed.value})" }
        true
      end

      def exceeded_max_memory?
        return false unless config.max_memory_mb && current_memory_mb > config.max_memory_mb

        Pgbus.logger.info { "[Pgbus] Worker recycling: memory limit (#{current_memory_mb}MB > #{config.max_memory_mb}MB)" }
        true
      end

      def exceeded_max_lifetime?
        return false unless config.max_worker_lifetime && (monotonic_now - @started_at_monotonic) > config.max_worker_lifetime

        Pgbus.logger.info { "[Pgbus] Worker recycling: lifetime exceeded" }
        true
      end

      # Instrumentation payload may report a value up to MEMORY_CHECK_TTL seconds old.
      def current_memory_mb
        MemoryUsage.current_mb
      end

      def notify_wakeup?
        config.respond_to?(:worker_notify_wakeup?) && config.worker_notify_wakeup?
      end

      def wake_timeout
        # A dead listener (running? false) will never wake the loop, so treat
        # it as absent and keep polling at the short interval until
        # ensure_notify_listener restarts it. A live-but-deaf listener
        # (delivering? false — a transaction-mode pooler or replica that drops
        # LISTEN) is the same story: it will never wake us, so don't raise the
        # poll floor to the 15s ceiling behind it (issue #332). Only a live,
        # delivering listener earns the long NOTIFY-mode ceiling.
        return effective_polling_interval unless notify_wakeup? && listener_delivering?

        [effective_polling_interval, config.polling_interval, NOTIFY_FALLBACK_POLL_SECONDS].max
      end

      def listener_delivering?
        # Supervisor scope: the hub's H/P broadcasts (via WakePipe) stand in
        # for the local listener's health — same fast-poll fallback rules.
        return @wake_pipe.delivering? if @wake_pipe

        @notify_listener&.running? && @notify_listener.delivering?
      end

      def effective_polling_interval
        return config.polling_interval if @consumer_priority.zero?

        ConsumerPriority.effective_polling_interval(
          base_interval: config.polling_interval,
          my_priority: @consumer_priority,
          max_priority: ConsumerPriority.max_active_priority(queues, ::Process.pid)
        )
      rescue StandardError
        config.polling_interval
      end

      # :supervisor scope: the fork opens NO LISTEN connection; the WakePipe
      # watcher is the wake source. A missing pipe under that scope means the
      # hub failed to start — plain polling, NEVER a local listener, or a hub
      # outage would balloon the host back to one direct connection per fork
      # (the exact budget the scope exists to protect). :fork scope: the
      # fork-local NotifyListener, exactly as before.
      def start_wake_source
        return @wake_pipe.start if @wake_pipe

        start_notify_listener if local_listener_scope?
      end

      # Local NotifyListener lifecycle (start + self-heal) is allowed only
      # under :fork scope.
      def local_listener_scope?
        config.worker_notify_scope == :fork
      end

      def stop_wake_source
        @wake_pipe&.stop
        @notify_listener&.stop
      end

      def start_notify_listener
        return unless notify_wakeup?

        @notify_listener = NotifyListener.new(
          physical_queues: physical_queue_names,
          on_wake: ->(_channel) { @wake_signal.notify! },
          connection_options: config.worker_notify_connection_options,
          health_check_ms: (config.polling_interval * 1000).to_i.clamp(250, 5_000),
          logger: Pgbus.logger
        ).start
      rescue StandardError => e
        @notify_listener = nil
        Pgbus.logger.error do
          "[Pgbus] NotifyListener failed to start, falling back to polling: #{e.class}: #{e.message}"
        end
      end

      # Self-heal a NotifyListener that never started or whose thread died.
      # Called each loop iteration but gated by a monotonic backoff timestamp
      # so a persistent outage retries on 5s→…→300s intervals, not every tick
      # (mirrors refresh_wildcard_queues' throttle). A restarted listener has
      # its queue subscription reconciled (wildcard workers) and the backoff
      # reset; a still-failing restart doubles the backoff up to the cap.
      def ensure_notify_listener
        # Supervisor scope: listener self-healing is the hub's job (it runs
        # once per host in the supervisor's monitor tick), and a pipe-less
        # fork under that scope must stay on plain polling — self-healing a
        # listener it was never allowed to start would leak a connection.
        return if @wake_pipe
        return unless local_listener_scope?
        return unless notify_wakeup?
        return if @notify_listener&.running?
        return if monotonic_now < @notify_retry_at

        stop_dead_notify_listener
        start_notify_listener

        if @notify_listener&.running?
          sync_notify_listener_queues
          @notify_retry_backoff = NOTIFY_RETRY_BASE_SECONDS
        else
          @notify_retry_backoff = [@notify_retry_backoff * 2, NOTIFY_RETRY_MAX_SECONDS].min
        end
        @notify_retry_at = monotonic_now + @notify_retry_backoff
      end

      # Stop a listener whose thread died so its dedicated PG connection is
      # released before start_notify_listener allocates a fresh one. A nil or
      # still-running listener is left alone (the caller already gated on it).
      def stop_dead_notify_listener
        return if @notify_listener.nil?

        @notify_listener.stop
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Failed to stop dead NotifyListener: #{e.class}: #{e.message}" }
      ensure
        @notify_listener = nil
      end

      def sync_notify_listener_queues
        return unless @notify_listener

        desired = physical_queue_names.to_set
        current = @notify_listener.listening_to.to_set { |c| channel_to_physical(c) }
        (desired - current).each { |q| @notify_listener.add_queue(q) }
        (current - desired).each { |q| @notify_listener.remove_queue(q) }
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] NotifyListener queue sync failed: #{e.class}: #{e.message}" }
      end

      # Through config.queue_name (not raw concatenation) so a logical name
      # needing normalization (e.g. "orders-handler" → "orders_handler")
      # yields the SAME physical name the queue table was created with — the
      # NOTIFY channel derives from the table name, so a raw concat would
      # LISTEN on a channel that never fires. Keeps the fork-local (:fork
      # scope) channels identical to the NotifyHub's (issue #381 review).
      def physical_queue_names
        queues.map { |q| config.queue_name(q) }
      end

      def channel_to_physical(channel)
        NotifyListener.physical_for(channel)
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "worker",
          metadata: {
            queues: queues, threads: threads, pid: ::Process.pid,
            execution_mode: @execution_mode, consumer_priority: @consumer_priority
          },
          on_beat: -> { on_heartbeat },
          loop_tick_supplier: -> { @loop_tick_at.get },
          metadata_supplier: -> { throughput_metadata }
        )
        @heartbeat.start
      end

      # Runs once per heartbeat interval (not per job), so it's the right place
      # to snapshot the per-beat rate counters and emit connection-pool
      # observability without touching any per-job hot path. Pool utilization
      # goes out as a `pgbus.client.pool` event carrying {size:, available:,
      # pool_timeout:}. Reading the pool must never crash the beat — pool_stats
      # already rescues to {}, and this whole method is guarded so a listener or
      # unexpected error can't take down the heartbeat thread.
      def on_heartbeat
        @rate_counter.snapshot!
        emit_pool_stats
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Worker heartbeat hook error: #{e.class}: #{e.message}" }
      end

      def emit_pool_stats
        stats = Pgbus.client.pool_stats
        return if stats.empty?

        Pgbus::Instrumentation.instrument("pgbus.client.pool", stats)
      end

      # Per-worker throughput persisted into pgbus_processes.metadata on every
      # heartbeat so the dashboard can show cluster-wide live rates. Called by
      # the Heartbeat's metadata_supplier after on_beat (snapshot!) has
      # refreshed the rate counter, so these rates are current. Keys are
      # stringified because the value round-trips through JSON in the metadata
      # column.
      def throughput_metadata
        {
          "rates" => @rate_counter.rates.transform_keys(&:to_s),
          "jobs_processed" => @jobs_processed.value,
          "jobs_failed" => @jobs_failed.value,
          "in_flight" => @in_flight.value,
          "buffered" => @claim_buffer.size
        }
      end

      def shutdown
        Pgbus.logger.info { "[Pgbus] Worker draining thread pool..." }
        stop_wake_source
        @pool.shutdown
        # Residual wait only: the drain loop already waited up to
        # config.drain_timeout for in-flight jobs. A job still running here has
        # proven it won't finish; waiting another full window would push the
        # worker past the supervisor's shutdown_timeout deadline (issue #386).
        @pool.wait_for_termination(POOL_TERMINATION_WAIT)
        VisibilityHeartbeat.stop
        @stat_buffer&.stop
        @queue_lock&.unlock_all
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Worker stopped. Processed: #{@jobs_processed.value}, Failed: #{@jobs_failed.value}" }
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Stamp the loop-progress beacon with a wall-clock timestamp.
      # Wall clock is required because the supervisor watchdog reads
      # this value from a different process (cross-fork) and the
      # dashboard reads it from a different host.
      #
      # Also pokes the OS-level liveness pipe (when the supervisor forked us
      # with one) so the watchdog has a database-independent signal. The write
      # is non-blocking and never raises in the hot path: exception: false
      # returns :wait_writable on a full pipe (which itself proves recent,
      # undrained ticks — liveness — so a dropped write is fine), and the
      # rescue covers the reader-gone / fd-closed cases so a dead pipe can
      # never crash the worker loop. The payload is a content-free byte: the
      # supervisor treats "any bytes readable" as liveness and stamps arrival
      # time on its own monotonic clock, so no timestamp crosses the fork.
      def stamp_loop_tick
        @loop_tick_at.set(Time.now.to_f)
        return unless @liveness_pipe

        @liveness_pipe.write_nonblock("\0", exception: false)
      rescue Errno::EPIPE, IOError, Errno::EBADF
        nil
      end
    end
  end
end
