# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    class Consumer
      include SignalHandler

      attr_reader :topics, :threads, :config, :execution_mode,
                  :queue_names, :wake_signal, :notify_retry_backoff, :circuit_breaker
      # notify_listener is writable so tests can simulate a start_notify_listener
      # success from inside a stub (production sets it in start_notify_listener).
      # notify_retry_at is writable so a test can re-arm the backoff window
      # between successive ensure_notify_listener calls.
      attr_accessor :notify_listener, :notify_retry_at
      # Supervisor-mediated wake source (issue #381): non-nil iff the
      # supervisor forked us with a wake pipe. Readable as a test seam.
      attr_reader :wake_pipe
      # stat_buffer is writable so a test can swap in a buffer double after
      # construction and assert graceful_shutdown / check_recycle / shutdown flush
      # it (mirrors Worker#stat_buffer).
      attr_accessor :stat_buffer

      def shutting_down?
        @shutting_down
      end

      def jobs_processed
        @jobs_processed.value
      end

      # Seed the processed-job counter. Used by tests to drive the recycle
      # thresholds without running thousands of real jobs; production only ever
      # increments it via the AtomicFixnum during message handling.
      def jobs_processed=(count)
        @jobs_processed.value = count
      end

      # Mirrors Worker::NOTIFY_FALLBACK_POLL_SECONDS. When a live NotifyListener
      # drives wake-up latency, the empty-read wait is a safety net rather than
      # the steady-state cadence, so it rises to this ceiling: one missed NOTIFY
      # costs bounded latency, never a stuck queue.
      NOTIFY_FALLBACK_POLL_SECONDS = 15

      # NotifyListener startup can fail on a transient boot-time condition (DB
      # restarting, failover, DNS blip) or its thread can die mid-run. The loop
      # retries from ensure_notify_listener on an exponential backoff:
      # NOTIFY_RETRY_BASE doubling up to NOTIFY_RETRY_MAX. Matches Worker.
      NOTIFY_RETRY_BASE_SECONDS = 5
      NOTIFY_RETRY_MAX_SECONDS = 300

      # The notify-listener lifecycle state (`notify_listener`, `notify_retry_at`,
      # `notify_retry_backoff`), the recycle clock (`started_at_monotonic`), and
      # the resolved subscription set (`queue_names`) accept injected seeds so
      # tests can drive the run loop from a known state without poking private
      # ivars. All default to the values production initializes to; `queue_names`
      # defaults to nil, meaning "derive from the registry in setup_subscriptions".
      def initialize(topics:, threads: 3, config: Pgbus.configuration, execution_mode: :threads,
                     queue_names: nil, liveness_pipe: nil, stat_buffer: :default,
                     notify_listener: nil, notify_retry_at: 0.0,
                     notify_retry_backoff: NOTIFY_RETRY_BASE_SECONDS,
                     started_at_monotonic: nil, wake_pipe: nil)
        @topics = Array(topics)
        @threads = threads
        @config = config
        @execution_mode = ExecutionPools.normalize_mode(execution_mode)
        @shutting_down = false
        @recycling = false
        @jobs_processed = Concurrent::AtomicFixnum.new(0)
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
        @started_at_monotonic = started_at_monotonic || monotonic_now
        @wake_signal = WakeSignal.new
        @pool = ExecutionPools.build(
          mode: @execution_mode,
          capacity: threads,
          on_state_change: -> { @wake_signal.notify! }
        )
        @registry = EventBus::Registry.instance
        @circuit_breaker = Pgbus::CircuitBreaker.new(config: config)
        # Queues already warned about for an unroutable message (issue #469).
        # handle_message runs on the execution pool, so this is touched from
        # several threads — Concurrent::Set makes add? the atomic
        # test-and-set the once-per-queue guarantee needs.
        @unrouted_queues = Concurrent::Set.new
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
        @queue_names = queue_names
        @notify_listener = notify_listener
        @notify_retry_at = notify_retry_at
        @notify_retry_backoff = notify_retry_backoff
        # OS-level liveness channel to the supervisor watchdog. nil unless the
        # supervisor forked us with one. Written from stamp_loop_tick so the
        # watchdog can detect a wedged consumer even when the database is down.
        @liveness_pipe = liveness_pipe
        # Supervisor wake pipe (read end): when present the fork owns NO
        # LISTEN connection — wakes and hub health arrive as bytes (issue #381).
        @wake_pipe = wake_pipe ? WakePipe.new(wake_pipe, wake_signal: @wake_signal) : nil
      end

      # The last wall-clock loop-tick stamp (Time.now.to_f) fed to the
      # heartbeat's loop_tick_supplier. Wall-clock so it stays comparable across
      # the process boundary the supervisor watchdog reads it over. nil until the
      # first stamp_loop_tick.
      def last_loop_tick
        @loop_tick_at.get
      end

      def run
        setup_signals
        start_heartbeat
        setup_subscriptions
        ensure_fair_indexes
        start_wake_source
        Pgbus.logger.info do
          "[Pgbus] Consumer started: topics=#{topics.join(",")} threads=#{threads} " \
            "notify_wakeup=#{notify_wakeup?} pid=#{::Process.pid}"
        end

        loop do
          stamp_loop_tick
          process_signals
          check_recycle
          ensure_notify_listener

          break if @shutting_down

          consume
          @stat_buffer&.flush_if_due
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
        # Flush buffered stats at drain entry so the window since the last flush
        # isn't lost if the supervisor watchdog SIGKILLs a stalled consumer
        # before shutdown runs. Same-thread (signals dispatched via
        # process_signals, not trap context), so the DB write is safe.
        @stat_buffer&.flush
        @wake_signal.notify!
      end

      def immediate_shutdown
        @shutting_down = true
        @wake_signal.notify!
        @pool.kill
      end

      private

      def setup_subscriptions
        # An injected queue_names: seed (test seam — the ctor documents nil as
        # "derive from the registry") survives #run. Derivation is shared with
        # the supervisor-owned NotifyHub (issue #381) so the LISTEN union
        # covers exactly the queues this fork reads.
        return unless @queue_names.nil?

        @queue_names = @registry.queue_names_for_topics(topics)
      end

      def consume
        idle = @pool.available_capacity
        return @wake_signal.wait(timeout: wake_timeout) if idle <= 0

        tagged_messages = fetch_messages(idle)

        if tagged_messages.empty?
          @wake_signal.wait(timeout: wake_timeout)
          return
        end

        tagged_messages.each do |queue_name, message|
          @pool.post { handle_message(message, queue_name) }
        end
      end

      # Returns an array of [queue_name, message] pairs. Queues whose circuit
      # breaker has tripped are skipped so a poison queue is left to cool down
      # instead of being hammered every tick (mirrors Worker#fetch_messages).
      def fetch_messages(qty)
        active_queues = @queue_names.reject { |q| @circuit_breaker.paused?(q) }
        return [] if active_queues.empty?

        if fair_share_enabled?
          fetch_fair(active_queues, qty)
        elsif active_queues.size == 1
          queue = active_queues.first
          (Pgbus.client.read_batch(queue, qty: qty) || []).map { |m| [queue, m] }
        else
          fetch_multi_consumer(active_queues, qty)
        end
      rescue Pgbus::ConnectionCircuitOpenError
        # The client-level connection breaker is open: the database has failed
        # enough consecutive connection attempts that reads fail fast. Idle this
        # poll without an ErrorReporter call so the whole consumer pool doesn't
        # flood the error tracker for the duration of a database outage. The
        # open/close transitions are logged once by the client, not per poll.
        []
      rescue StandardError => e
        ErrorReporter.report(e, { action: "fetch_messages", queues: active_queues })
        []
      end

      def handle_message(message, queue_name)
        execution_start = monotonic_now

        if message.read_ct.to_i > config.max_retries
          Pgbus.logger.warn { "[Pgbus] Consumer moving message #{message.msg_id} to DLQ after #{message.read_ct} reads" }
          Pgbus.client.move_to_dead_letter(queue_name, message)
          record_stat(message, queue_name, "dead_lettered", execution_start)
          return
        end

        raw = JSON.parse(message.message)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        handlers = @registry.handlers_for(routing_key || "", queue_name: queue_name)

        if handlers.empty?
          report_unrouted(queue_name, routing_key)
        else
          handlers.each do |subscriber|
            handler = subscriber.handler_class.new
            handler.process(message)
          end
        end

        Pgbus.client.archive_message(queue_name, message.msg_id.to_i)
        @circuit_breaker.record_success(queue_name)
        record_stat(message, queue_name, "success", execution_start)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Consumer error: #{e.class}: #{e.message}" }
        # Message stays in queue; VT will expire and it becomes available again.
        # read_ct tracks delivery attempts — when it exceeds max_retries,
        # the next read will route to DLQ above.
        @circuit_breaker.record_failure(queue_name)
        record_stat(message, queue_name, "failed", execution_start)
      ensure
        # Count every message the consumer handles — success, DLQ-routed, AND
        # rescued failure — mirroring Worker#process_message, which increments
        # unconditionally. Counting only successes would let max_jobs recycling
        # never trip on a poison/all-failing queue: the exact unbounded-memory
        # scenario recycling exists to bound.
        @jobs_processed.increment
      end

      # No subscriber in this process owns +queue_name+ (a stale
      # pgmq.topic_bindings row left by a renamed or removed handler, another
      # app bound to the same bus), or the owner's pattern no longer matches the
      # routing key. The message is archived by the caller either way — looping
      # it through VT redelivery would only walk it into the DLQ — but that used
      # to happen in total silence. The warning is rate-limited to once per
      # queue per process so a permanently stale binding cannot flood the log;
      # the instrumentation fires on every message, so the real rate stays
      # visible in metrics (issue #469).
      def report_unrouted(queue_name, routing_key)
        first_for_queue = @unrouted_queues.add?(queue_name)

        if first_for_queue
          Pgbus.logger.warn do
            "[Pgbus] Consumer read an unroutable event from queue #{queue_name} " \
              "(routing_key=#{routing_key.inspect}): no subscriber in this process owns that queue. " \
              "Archiving. This usually means a stale topic binding — a handler was renamed or removed " \
              "without unbinding its queue. Further unrouted messages on this queue are not logged."
          end
        end

        Pgbus::Instrumentation.instrument(
          "pgbus.event_unrouted",
          queue_name: queue_name,
          routing_key: routing_key
        )
      end

      # Record a job stat for the handled message, mirroring the shape the
      # executor pushes (Executor#record_stat) so consumer and worker throughput
      # land in the same pgbus_job_stats table. No-op unless stats are enabled.
      def record_stat(message, queue_name, status, start_time)
        return unless config.stats_enabled

        attrs = {
          job_class: "EventConsumer",
          queue_name: queue_name,
          status: status,
          duration_ms: ((monotonic_now - start_time) * 1000).round,
          enqueue_latency_ms: nil,
          retry_count: [message.read_ct.to_i - 1, 0].max
        }

        if @stat_buffer
          @stat_buffer.push(attrs)
        else
          JobStat.record!(**attrs)
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Consumer stat recording failed: #{e.message}" }
      end

      # `qty` is the total pool capacity. pgmq-ruby treats `qty:` as per-queue,
      # so we also pass `limit: qty` to cap the total across all queues —
      # otherwise we get `queue_count * qty` messages and overflow the
      # execution pool, crashing the consumer fork (issue #123).
      def fetch_multi_consumer(active_queues, qty)
        messages = Pgbus.client.read_multi(active_queues, qty: qty, limit: qty) || []
        prefix = "#{config.queue_prefix}_"

        messages.map do |m|
          logical = m.queue_name&.delete_prefix(prefix) || active_queues.first
          [logical, m]
        end
      end

      # Fair share for consumers (issue #427): one weighted-interleave read per
      # subscriber queue, in list order with the remaining capacity — strict
      # order across queues, fair across tenants WITHIN each queue. Same loop
      # shape as Worker#fetch_fair; the consumer has no priority levels or
      # group mode, so this is the whole fair branch.
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
        FairShare.event_enabled?(config)
      end

      # Build the fair index on every subscriber queue this consumer reads
      # (idempotent, memoized in the client, CONCURRENTLY so a populated queue
      # is never write-locked). Subscriber queues created later get theirs from
      # Subscriber#setup!.
      def ensure_fair_indexes
        return unless fair_share_enabled?

        @queue_names.each { |q| Pgbus.client.ensure_fair_index(q) }
      end

      # Signal the loop to exit cleanly once a recycle limit is hit. The clean
      # exit gets an immediate supervisor restart (supervisor.rb:305-307), so a
      # fresh fork replaces this one before its memory grows unbounded — the
      # same fix Worker#check_recycle provides. Guarded by @recycling so the
      # per-iteration call instruments and logs exactly once.
      def check_recycle
        return if @recycling

        reason = recycle_reason
        return unless reason

        @recycling = true
        @shutting_down = true
        # Flush buffered stats on recycle-triggered drain for the same reason as
        # graceful_shutdown: shrink the SIGKILL loss window. Same-thread, safe.
        @stat_buffer&.flush
        Pgbus::Instrumentation.instrument(
          "pgbus.consumer.recycle",
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

        Pgbus.logger.info { "[Pgbus] Consumer recycling: max_jobs reached (#{@jobs_processed.value})" }
        true
      end

      def exceeded_max_memory?
        return false unless config.max_memory_mb && current_memory_mb > config.max_memory_mb

        Pgbus.logger.info do
          "[Pgbus] Consumer recycling: memory limit (#{current_memory_mb}MB > #{config.max_memory_mb}MB)"
        end
        true
      end

      def exceeded_max_lifetime?
        return false unless config.max_worker_lifetime && (monotonic_now - @started_at_monotonic) > config.max_worker_lifetime

        Pgbus.logger.info { "[Pgbus] Consumer recycling: lifetime exceeded" }
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
        # (delivering? false) is the same story (issue #332 — parity with
        # Worker#wake_timeout). Under :supervisor scope the hub's H/P
        # broadcasts (via WakePipe) carry the same signal. Only a live,
        # delivering wake source earns the long NOTIFY-mode ceiling.
        return config.polling_interval unless notify_wakeup? && wake_source_delivering?

        [config.polling_interval, NOTIFY_FALLBACK_POLL_SECONDS].max
      end

      def wake_source_delivering?
        return @wake_pipe.delivering? if @wake_pipe

        @notify_listener&.running? && @notify_listener.delivering?
      end

      # :supervisor scope: the fork opens NO LISTEN connection; the WakePipe
      # watcher is the wake source, and a missing pipe (hub failed to start)
      # means plain polling — never a local listener (see Worker's twin).
      # :fork scope: the fork-local NotifyListener.
      def start_wake_source
        return @wake_pipe.start if @wake_pipe

        start_notify_listener if local_listener_scope?
      end

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
          "[Pgbus] Consumer NotifyListener failed to start, falling back to polling: #{e.class}: #{e.message}"
        end
      end

      # Self-heal a NotifyListener that never started or whose thread died.
      # Called each loop iteration but gated by a monotonic backoff timestamp so
      # a persistent outage retries on 5s→…→300s intervals, not every tick
      # (mirrors Worker#ensure_notify_listener).
      def ensure_notify_listener
        # Supervisor scope: self-healing is the hub's job (once per host);
        # a pipe-less fork under that scope stays on plain polling.
        return if @wake_pipe
        return unless local_listener_scope?
        return unless notify_wakeup?
        return if @notify_listener&.running?
        return if monotonic_now < @notify_retry_at

        stop_dead_notify_listener
        start_notify_listener

        @notify_retry_backoff = if @notify_listener&.running?
                                  NOTIFY_RETRY_BASE_SECONDS
                                else
                                  [@notify_retry_backoff * 2, NOTIFY_RETRY_MAX_SECONDS].min
                                end
        @notify_retry_at = monotonic_now + @notify_retry_backoff
      end

      # Stop a listener whose thread died so its dedicated PG connection is
      # released before start_notify_listener allocates a fresh one.
      def stop_dead_notify_listener
        return if @notify_listener.nil?

        @notify_listener.stop
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Consumer failed to stop dead NotifyListener: #{e.class}: #{e.message}" }
      ensure
        @notify_listener = nil
      end

      # Through config.queue_name so normalized subscriber queue names LISTEN
      # on the channel their table actually notifies (see Worker's twin).
      def physical_queue_names
        @queue_names.map { |q| config.queue_name(q) }
      end

      def start_heartbeat
        @heartbeat = Heartbeat.new(
          kind: "consumer",
          metadata: { topics: topics, threads: threads, pid: ::Process.pid },
          on_beat: -> { on_heartbeat },
          loop_tick_supplier: -> { @loop_tick_at.get }
        )
        @heartbeat.start
      end

      # Runs once per heartbeat interval (not per message), so it's the right
      # place to emit connection-pool observability without touching any per-job
      # hot path. Reading the pool must never crash the beat — pool_stats already
      # rescues to {}, and this whole method is guarded so an unexpected error
      # can't take down the heartbeat thread (mirrors Worker#on_heartbeat).
      def on_heartbeat
        emit_pool_stats
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Consumer heartbeat hook error: #{e.class}: #{e.message}" }
      end

      def emit_pool_stats
        stats = Pgbus.client.pool_stats
        return if stats.empty?

        Pgbus::Instrumentation.instrument("pgbus.client.pool", stats)
      end

      def shutdown
        stop_wake_source
        @pool.shutdown
        # The consumer has no quiesce-gated drain loop like Worker's, so this
        # wait IS its drain window — bound it by the same knob workers use
        # instead of a hardcoded 30s (issue #386).
        @pool.wait_for_termination(config.drain_timeout)
        @stat_buffer&.stop
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Consumer stopped. Processed: #{@jobs_processed.value}" }
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Stamp the loop-progress beacon with a wall-clock timestamp (required
      # because the supervisor watchdog reads it cross-fork and the dashboard
      # reads it cross-host). Also pokes the OS-level liveness pipe when the
      # supervisor forked us with one, giving the watchdog a database-independent
      # signal. The write is non-blocking and never raises in the hot path
      # (mirrors Worker#stamp_loop_tick).
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
