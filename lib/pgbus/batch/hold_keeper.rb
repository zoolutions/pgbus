# frozen_string_literal: true

require "concurrent"

module Pgbus
  class Batch
    # Keeps the holds of this process's open batch scopes alive (issue #683).
    # A hold row is what stops the stalled-batch sweep from finishing a batch
    # whose block still holds members, and the sweep treats a hold older
    # than batch_stall_threshold as left by a dead process. A block can go
    # quiet for longer than that after its last perform_later, so a timer
    # touches every registered hold every quarter of the threshold for as
    # long as its scope is open in a live process.
    #
    # The timer only refreshes, never takes a hold: on its own connection a
    # batch row the block has not committed yet is invisible, and so is its
    # hold, which the sweep cannot see either. Built lazily, on the first
    # hold; a forked child starts its own.
    module HoldKeeper
      @holds = Set.new
      @mutex = Mutex.new
      @timer = nil
      @pid = nil

      class << self
        def register(token)
          @mutex.synchronize do
            @holds << token
            start_timer
          end
        end

        def unregister(token)
          @mutex.synchronize { @holds.delete(token) }
        end

        # Touches every registered hold now; the timer's tick. Returns the
        # number of rows touched (0 when a refresh failed: it is logged, and
        # the next tick tries again).
        def refresh!
          tokens = @mutex.synchronize { @holds.to_a }
          return 0 if tokens.empty?

          BatchExecution.connection_pool.with_connection do
            BatchExecution.where(job_id: tokens).update_all(created_at: Time.current)
          end
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Refreshing #{tokens&.size} batch hold(s) failed: #{e.class}: #{e.message}" }
          0
        end

        def interval
          Pgbus.configuration.batch_stall_threshold.to_f / 4
        end

        # Stops the timer and forgets every hold (specs, Pgbus.reset!).
        def reset!
          @mutex.synchronize do
            @timer&.shutdown
            @timer = nil
            @pid = nil
            @holds.clear
          end
        end

        private

        # Called under @mutex: one timer per process, a new one after fork.
        def start_timer
          return if @timer && @pid == ::Process.pid && @timer.running? # rubocop:disable ThreadSafety/ClassInstanceVariable -- under @mutex (register)

          @timer = Concurrent::TimerTask.new(execution_interval: interval) { refresh! } # rubocop:disable ThreadSafety/ClassInstanceVariable -- under @mutex (register)
          @timer.execute # rubocop:disable ThreadSafety/ClassInstanceVariable -- under @mutex (register)
          @pid = ::Process.pid # rubocop:disable ThreadSafety/ClassInstanceVariable -- under @mutex (register)
        end
      end
    end
  end
end
