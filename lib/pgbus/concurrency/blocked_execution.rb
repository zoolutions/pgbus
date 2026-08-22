# frozen_string_literal: true

require "json"

module Pgbus
  module Concurrency
    module BlockedExecution
      class << self
        # Insert a blocked execution for a job that hit the concurrency limit.
        def insert(concurrency_key:, queue_name:, payload:, duration:, priority: 0)
          Pgbus::BlockedExecution.create!(
            concurrency_key: concurrency_key,
            queue_name: queue_name,
            payload: JSON.generate(payload),
            priority: priority,
            expires_at: Time.current + duration
          )
        end

        # Release the next blocked execution for a given concurrency key.
        # Returns the released row (queue_name, payload) or nil if none.
        def release_next(concurrency_key)
          Pgbus::BlockedExecution.release_next!(concurrency_key)
        end

        # Atomically promote the next blocked execution: delete the row and enqueue
        # the job in a single transaction. Returns true if a job was promoted, false
        # otherwise. This avoids losing a blocked row if enqueue fails.
        def promote_next(concurrency_key, client:, delay: 0)
          released = nil
          msg_id = nil
          Pgbus::BlockedExecution.transaction do
            released = release_next(concurrency_key)
            raise ActiveRecord::Rollback unless released

            actual_delay = resolve_delay(released[:payload], delay)
            msg_id = client.send_message(released[:queue_name], released[:payload], delay: actual_delay)
          end

          if released && msg_id
            begin
              Batch.backfill_execution(released[:payload], msg_id, client.target_queue(released[:queue_name]))
            rescue StandardError => e
              Pgbus.logger.warn { "[Pgbus] Batch execution backfill failed after promote: #{e.message}" }
            end
          end

          !!released
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Promote blocked execution failed for #{concurrency_key}: #{e.message}" }
          false
        end

        # Delete blocked executions that have expired.
        # Returns the count of deleted rows.
        def expire_stale
          Pgbus::BlockedExecution.expired(Time.current).delete_all
        end

        # Count blocked executions for a given key. Useful for testing/monitoring.
        def count_for(concurrency_key)
          Pgbus::BlockedExecution.where(concurrency_key: concurrency_key).count
        end

        private

        def resolve_delay(payload, default_delay)
          scheduled_at = payload["scheduled_at"]
          return default_delay unless scheduled_at

          [Time.parse(scheduled_at).to_f - Time.current.to_f, 0].max.ceil
        rescue StandardError
          default_delay
        end
      end
    end
  end
end
