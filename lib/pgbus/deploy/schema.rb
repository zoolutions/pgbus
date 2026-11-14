# frozen_string_literal: true

module Pgbus
  module Deploy
    # Whether pgbus:add_deploy_task_run_outcomes has been migrated (issue
    # #660): pgbus_deploy_task_runs.completed_at / failed_at and
    # pgbus_deploy_tasks.dependencies. Until then after: is rejected at load,
    # sync leaves dependencies alone and the executor writes no outcome.
    #
    # A "yes" is kept for the process; a "no" stands for RECHECK_SECONDS, so a
    # process started before the migration picks the columns up without a
    # restart. A probe that raises (a database blip) is not an answer: the
    # error reaches the caller and nothing is kept, so a scheduler booting
    # through a blip does not skip its after: tasks as unmigrated.
    module Schema
      RECHECK_SECONDS = 60

      @mutex = Mutex.new
      @answer = nil # true, nil (never probed) or the monotonic time a "no" runs out

      class << self
        def outcomes?
          # Double-checked: the unlocked read only skips the lock while the
          # answer stands; it is written under @mutex.
          known = answer(@answer) # rubocop:disable ThreadSafety/ClassInstanceVariable
          return known unless known.nil?

          @mutex.synchronize do
            known = answer(@answer)
            next known unless known.nil?

            supported = probe
            @answer = supported || (monotonic_now + RECHECK_SECONDS)
            supported
          end
        end

        # Clears the memoized probe: specs exercise both schema shapes.
        def reset!
          @mutex.synchronize { @answer = nil }
        end

        private

        def answer(state)
          return true if state == true

          false if state && monotonic_now < state
        end

        def probe
          BusRecord.connection_pool.with_connection do |conn|
            conn.column_exists?(:pgbus_deploy_task_runs, :completed_at) &&
              conn.column_exists?(:pgbus_deploy_task_runs, :failed_at) &&
              conn.column_exists?(:pgbus_deploy_tasks, :dependencies)
          end
        end

        def monotonic_now
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
