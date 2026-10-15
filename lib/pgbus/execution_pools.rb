# frozen_string_literal: true

module Pgbus
  module ExecutionPools
    class << self
      def build(mode:, capacity:, on_state_change: nil)
        case normalize_mode(mode)
        when :threads
          ThreadPool.new(capacity: capacity, on_state_change: on_state_change)
        when :async
          AsyncPool.new(capacity: capacity, on_state_change: on_state_change)
        end
      end

      def normalize_mode(mode)
        case mode.to_s
        when "", "threads"
          :threads
        when "async", "fiber"
          :async
        else
          raise ArgumentError, "Unknown execution_mode: #{mode.inspect}. Expected one of: :threads, :async, :fiber"
        end
      end

      # Stop +pool+ taking work, give the jobs already running +grace+
      # seconds to finish, and kill whatever is still running after that.
      # Either way, reclaim the ActiveRecord connections the jobs left checked
      # out: a killed thread or fiber never checks its own back in, and the
      # pool would stay that many connections short until its reaper ran.
      # Also when the wait raises (an async pool re-raises a fatal reactor
      # error). Returns true when every job finished within the grace (issue #527).
      def terminate(pool, grace:)
        pool.shutdown
        finished = pool.wait_for_termination(grace) ? true : false
        pool.kill unless finished
        finished
      ensure
        reclaim_connections
      end

      # The caller's own connection, then every connection whose owning
      # thread or fiber is dead. A no-op without ActiveRecord.
      def reclaim_connections
        return unless defined?(::ActiveRecord::Base)

        handler = ::ActiveRecord::Base.connection_handler
        handler.clear_active_connections!(:all)
        handler.connection_pool_list(:all).each(&:reap)
      end
    end
  end
end
