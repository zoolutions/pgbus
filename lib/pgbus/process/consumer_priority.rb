# frozen_string_literal: true

module Pgbus
  module Process
    # Implements consumer priority by checking whether higher-priority
    # workers are active for the same queues. When a higher-priority
    # worker is healthy and not at its prefetch limit, lower-priority
    # workers yield by using a longer polling interval.
    #
    # Inspired by LavinMQ's consumer priority where higher-priority
    # consumers are served first and lower-priority consumers wait
    # until all higher-priority consumers are at their prefetch limit.
    module ConsumerPriority
      # Time-to-live (seconds) for cached max_active_priority lookups.
      # The underlying data only changes on heartbeat cadence, so a short
      # TTL avoids ~10 queries/second per prioritized worker at the default
      # 0.1s polling interval.
      CACHE_TTL = 5

      @cache_mutex = Mutex.new
      @cache = {} # rubocop:disable ThreadSafety/MutableClassInstanceVariable -- only read or written under @cache_mutex

      # Check if this worker should yield to a higher-priority worker.
      # Returns true if a higher-priority healthy worker exists for
      # any of the given queues.
      def self.should_yield?(queues:, my_priority:, my_pid:)
        return false if my_priority >= max_active_priority(queues, my_pid)

        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Consumer priority check failed: #{e.message}" }
        false
      end

      # Returns the highest consumer_priority among healthy workers
      # that share at least one queue with the given queue list,
      # excluding the current worker (by PID).
      #
      # Results are cached per (sorted queues, pid) for CACHE_TTL seconds.
      # On a miss the query runs outside the lock (a duplicate query on a
      # race is harmless), then the value is stored under the mutex.
      def self.max_active_priority(queues, my_pid)
        key = [queues.sort, my_pid]
        now = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)

        cached = @cache_mutex.synchronize { @cache[key] }
        return cached[:value] if cached && (now - cached[:at]) < CACHE_TTL

        value = compute_max_active_priority(queues, my_pid)
        @cache_mutex.synchronize { @cache[key] = { value: value, at: now } }
        value
      end

      # Clears all cached max_active_priority entries. Intended for spec
      # isolation; also safe to call at runtime.
      def self.reset_cache!
        @cache_mutex.synchronize { @cache.clear }
      end

      # Runs the actual query and reduces worker rows to the highest
      # consumer_priority sharing a queue with the given list.
      def self.compute_max_active_priority(queues, my_pid)
        conn = Pgbus.configuration.connects_to ? Pgbus::BusRecord.connection : ActiveRecord::Base.connection
        rows = conn.select_all(
          "SELECT metadata FROM pgbus_processes WHERE kind = 'worker' AND pid != $1 AND last_heartbeat_at > $2",
          "Pgbus ConsumerPriority",
          [my_pid, Time.now.utc - Heartbeat::ALIVE_THRESHOLD]
        )

        max_priority = 0
        rows.each do |row|
          metadata = row["metadata"]
          metadata = JSON.parse(metadata) if metadata.is_a?(String)
          next unless metadata

          other_queues = metadata["queues"] || []
          next unless queues.intersect?(other_queues)

          other_priority = metadata["consumer_priority"] || 0
          max_priority = other_priority if other_priority > max_priority
        end

        max_priority
      end
      private_class_method :compute_max_active_priority

      # Calculate the effective polling interval for this worker.
      # Higher-priority workers use the base interval.
      # Lower-priority workers multiply by a backoff factor.
      def self.effective_polling_interval(base_interval:, my_priority:, max_priority:)
        return base_interval if my_priority >= max_priority

        # Lower-priority workers back off: 3x the base interval
        base_interval * 3
      end
    end
  end
end
