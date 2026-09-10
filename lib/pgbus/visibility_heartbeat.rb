# frozen_string_literal: true

module Pgbus
  # Keeps a running job's PGMQ message invisible for as long as the job is
  # actually running.
  #
  # PGMQ hands a message to one reader for `visibility_timeout` seconds. A job
  # that runs longer is redelivered while it is still running: a second copy
  # starts, `read_ct` climbs on every redelivery, and after `max_retries` the
  # message is dead-lettered — all without the job ever raising. The heartbeat
  # extends the VT of every in-flight message on a fixed cadence, so the
  # timeout only ever fires for a job whose process is gone (crash, SIGKILL),
  # which is the case it exists for.
  #
  # One background thread per process, started lazily by the first tracked
  # job and stopped by Worker#shutdown. Entries are keyed by physical queue +
  # msg_id and hold the client that read the message, so async and threaded
  # execution modes both work: the extension runs on this thread, never
  # inside the job's fiber. A fork forgets the parent's entries.
  #
  #   Pgbus::VisibilityHeartbeat.track(client:, queue_name:, msg_id:) { job.perform_now }
  #
  # Disable globally with `config.visibility_heartbeat = false`, tune the
  # cadence with `config.visibility_heartbeat_interval`, or opt a job class
  # out with `pgbus_visibility_heartbeat false`.
  module VisibilityHeartbeat
    # `concurrency` is `[key, duration]` for a concurrency-limited job, else
    # nil — one member, so the struct stays inside an 80-byte slot.
    Entry = Struct.new(:client, :queue_name, :prefixed, :msg_id, :job_class, :extended_at, :extensions,
                       :concurrency, keyword_init: true)

    # Per-job opt-out, included on ActiveJob::Base by the engine:
    #
    #   class ShortJob < ApplicationJob
    #     pgbus_visibility_heartbeat false
    #   end
    module JobMixin
      extend ActiveSupport::Concern

      included do
        class_attribute :pgbus_visibility_heartbeat_enabled, instance_writer: false, default: nil
      end

      class_methods do
        def pgbus_visibility_heartbeat(enabled = true) # rubocop:disable Style/OptionalBooleanParameter
          self.pgbus_visibility_heartbeat_enabled = enabled ? true : false
        end
      end
    end

    class << self
      # Track the message for the duration of the block.
      #
      # @param client [Pgbus::Client] the client that read the message
      # @param queue_name [String] logical name, or physical when prefixed: false
      # @param msg_id [Integer]
      # @param prefixed [Boolean] whether queue_name still needs the prefix
      # @param job_class [String, nil] for logging and instrumentation
      # @param config [Pgbus::Configuration]
      # @param concurrency [Array(String, Numeric), nil] semaphore key to keep alive alongside
      #   the message and how far to push its expiry on each beat
      def track(client:, queue_name:, msg_id:, prefixed: true, job_class: nil, config: Pgbus.configuration,
                concurrency: nil)
        return yield unless config.visibility_heartbeat

        entry = Entry.new(client: client, queue_name: queue_name, prefixed: prefixed, msg_id: msg_id.to_i,
                          job_class: job_class, extended_at: monotonic_now, extensions: 0,
                          concurrency: concurrency)
        register(entry, config)
        begin
          yield
        ensure
          unregister(entry)
        end
      end

      # Extend every tracked message whose last extension is older than the
      # heartbeat interval. Public so tests and callers without the thread
      # can drive it.
      def tick!(now: monotonic_now, config: Pgbus.configuration)
        interval = config.effective_visibility_heartbeat_interval
        due = synchronize { entries.values.select { |entry| now - entry.extended_at >= interval } }
        due.each { |entry| extend!(entry, now: now, config: config) }
        due.size
      end

      def tracked_count
        synchronize { entries.size }
      end

      # Stop the background thread. Tracked entries are kept: a job still
      # running during shutdown can drive tick! itself, and Worker#shutdown
      # only calls this once the pool has drained.
      def stop
        thread = synchronize do
          @running = false
          current = @thread
          @thread = nil
          current
        end
        return unless thread

        thread.wakeup if thread.alive?
        thread.join(1)
      end

      # Forget every entry and stop the thread. Test helper.
      def reset!
        stop
        synchronize { @entries = {} }
      end

      private

      def register(entry, config)
        synchronize do
          forget_parent_entries!
          entries[key_for(entry)] = entry
          ensure_thread(config)
        end
      end

      def unregister(entry)
        synchronize { entries.delete(key_for(entry)) }
      end

      def extend!(entry, now:, config:)
        vt = config.visibility_timeout
        entry.client.set_visibility_timeout(entry.queue_name, entry.msg_id, vt: vt, prefixed: entry.prefixed)
        entry.extended_at = now
        entry.extensions += 1
        touch_semaphore(entry)
        Instrumentation.instrument(
          "pgbus.job_visibility_extended",
          queue: entry.queue_name, job_class: entry.job_class, msg_id: entry.msg_id, vt: vt,
          extensions: entry.extensions
        )
        Pgbus.logger.debug do
          "[Pgbus::VisibilityHeartbeat] extended msg_id=#{entry.msg_id} queue=#{entry.queue_name} " \
            "job_class=#{entry.job_class} vt=#{vt} extensions=#{entry.extensions}"
        end
      rescue StandardError => e
        # The next tick retries; the message simply keeps its current VT.
        Pgbus.logger.warn do
          "[Pgbus::VisibilityHeartbeat] could not extend msg_id=#{entry.msg_id} queue=#{entry.queue_name}: " \
            "#{e.class}: #{e.message}"
        end
      end

      # Caller holds the mutex. Start the ticker once per process.
      def ensure_thread(config)
        return if @running && @thread&.alive?

        @running = true
        @thread = Thread.new { run_loop(config) }
        @thread.name = "pgbus-visibility-heartbeat"
      end

      def run_loop(config)
        while @running
          # Half the interval keeps every extension inside [interval, 1.5 * interval]
          # of the previous one — at most half the visibility timeout.
          sleep([config.effective_visibility_heartbeat_interval / 2.0, 0.05].max)
          break unless @running

          tick!(config: config)
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::VisibilityHeartbeat] ticker died: #{e.class}: #{e.message}" }
        synchronize { @running = false }
      end

      # Entries registered before a fork belong to the parent's jobs; the
      # thread did not survive the fork either.
      # A live holder keeps its semaphore from being swept, so `duration`
      # bounds heartbeat silence rather than run time. Its own rescue: a
      # failed touch must not cost the message its visibility extension.
      def touch_semaphore(entry)
        key, duration = entry.concurrency
        return unless key

        Concurrency::Semaphore.touch(key, duration || Concurrency::DEFAULT_DURATION)
      rescue StandardError => e
        Pgbus.logger.warn do
          "[Pgbus::VisibilityHeartbeat] could not touch semaphore #{key}: #{e.class}: #{e.message}"
        end
      end

      def forget_parent_entries!
        return if @pid == ::Process.pid

        @pid = ::Process.pid
        @entries = {}
        @running = false
        @thread = nil
      end

      def entries
        @entries ||= {}
      end

      def key_for(entry)
        [entry.queue_name, entry.msg_id]
      end

      def synchronize(&)
        (@mutex ||= Mutex.new).synchronize(&)
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
