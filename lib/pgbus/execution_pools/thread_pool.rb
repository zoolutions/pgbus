# frozen_string_literal: true

require "concurrent"

module Pgbus
  module ExecutionPools
    class ThreadPool
      # How long #kill waits, in all, for the killed jobs to unwind (run their
      # ensure blocks) before giving up on those still running.
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
      # The running jobs are listed after the kill: a job that started in
      # between is killed too, and a job leaves the list only once it has
      # unwound, so every one still unwinding is joined. One budget for all
      # of them: stuck jobs must not hold a shutdown for capacity x the timeout.
      def kill
        @pool.kill
        unwinding = @running.keys
        deadline = monotonic_now + KILL_JOIN_TIMEOUT
        unwinding.each { |thread| thread.join([deadline - monotonic_now, 0].max) }
      end

      def metadata
        {
          mode: :threads,
          capacity: @capacity,
          busy: @capacity - available_capacity
        }
      end

      private

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end
    end
  end
end
