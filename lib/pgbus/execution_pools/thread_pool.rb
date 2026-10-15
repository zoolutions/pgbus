# frozen_string_literal: true

require "concurrent"

module Pgbus
  module ExecutionPools
    class ThreadPool
      # How long #kill waits for each killed job to unwind (run its ensure
      # blocks) before giving up on it.
      KILL_JOIN_TIMEOUT = 1

      attr_reader :capacity

      def initialize(capacity:, on_state_change: nil)
        @capacity = capacity
        @on_state_change = on_state_change
        @available_capacity = Concurrent::AtomicFixnum.new(capacity)
        @pool = Concurrent::FixedThreadPool.new(capacity)
        @running = Concurrent::Map.new
      end

      def post(&block)
        @available_capacity.decrement
        begin
          @pool.post do
            @running[Thread.current] = true
            block.call
          ensure
            @running.delete(Thread.current)
            @available_capacity.increment
            @on_state_change&.call
          end
        rescue StandardError
          @available_capacity.increment
          raise
        end
      end

      def available_capacity
        @available_capacity.value
      end

      def idle?
        available_capacity.positive?
      end

      # True only when every slot is free — all in-flight work has finished.
      # idle? answers "can this pool accept work?"; quiesced? answers
      # "has this pool drained?". The worker drain loop needs the latter.
      def quiesced?
        available_capacity >= @capacity
      end

      def shutdown
        @pool.shutdown
      end

      def wait_for_termination(timeout)
        @pool.wait_for_termination(timeout)
      end

      # Thread#kill only asks a thread to stop: wait for the killed jobs to
      # unwind, so the caller can reclaim what they held (ExecutionPools.terminate).
      def kill
        running = @running.keys
        @pool.kill
        running.each { |thread| thread.join(KILL_JOIN_TIMEOUT) }
      end

      def metadata
        {
          mode: :threads,
          capacity: @capacity,
          busy: @capacity - available_capacity
        }
      end
    end
  end
end
