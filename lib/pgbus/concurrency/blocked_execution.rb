# frozen_string_literal: true

require "json"

module Pgbus
  module Concurrency
    module BlockedExecution
      class << self
        # Insert a blocked execution for a job that hit the concurrency limit.
        #
        # `expires_at` is a re-check hint for the sweep's ordering only — a
        # parked job is never deleted for being old. The only way out of the
        # table is promotion.
        #
        # The payload goes in as a Hash: the jsonb attribute serializes it
        # once. Handing it a pre-serialized String stored a JSON *string*
        # (double-encoded), which every document reader of the column —
        # `payload->>'job_id'`, the job-class lookup, `scheduled_at` — misread.
        def insert(concurrency_key:, queue_name:, payload:, duration:, priority: 0)
          Pgbus::BlockedExecution.create!(
            concurrency_key: concurrency_key,
            queue_name: queue_name,
            payload: payload,
            priority: priority,
            expires_at: Time.current + duration
          )
        end

        # Release the next blocked execution for a given concurrency key.
        # Returns the released row (queue_name, payload) or nil if none.
        def release_next(concurrency_key)
          Pgbus::BlockedExecution.release_next!(concurrency_key)
        end

        # Atomically promote the next blocked execution: delete the row, take a
        # semaphore slot for it and enqueue the job in a single transaction.
        # Returns true if a job was promoted, false otherwise.
        #
        # The slot is taken through the same guarded upsert an enqueue uses,
        # so a promotion can never push the key past its limit; when no slot
        # is free the savepoint rolls back and the row stays parked. Runs as a
        # savepoint so `Semaphore.signal` can wrap it with the release.
        def promote_next(concurrency_key, client:, delay: 0)
          released = nil
          msg_id = nil
          Pgbus::BlockedExecution.transaction(requires_new: true) do
            released = release_next(concurrency_key)
            raise ActiveRecord::Rollback unless released
            raise ActiveRecord::Rollback unless slot_taken?(concurrency_key, released[:payload])

            actual_delay = resolve_delay(released[:payload], delay)
            # Carry the enqueuer's priority through: under priority routing it
            # picks the _pN sub-queue, not just the release order (issue #423).
            msg_id = client.send_message(released[:queue_name], released[:payload],
                                         delay: actual_delay, priority: released[:priority])
          end

          return false unless released && msg_id

          backfill(released, msg_id, client)
          true
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Promote blocked execution failed for #{concurrency_key}: #{e.message}" }
          false
        end

        # Sweep: promote every parked job that can take a slot right now.
        # Covers what the completion-time signal cannot — a holder that died
        # (its semaphore expired and was swept) or a promote that failed.
        # Returns the number of jobs promoted.
        def promote_pending(client:, per_key: 100)
          Pgbus::BlockedExecution.repair_double_encoded!
          Pgbus::BlockedExecution.promotable_keys.sum do |key|
            promoted = 0
            promoted += 1 while promoted < per_key && promote_next(key, client: client)
            promoted
          end
        end

        # Count blocked executions for a given key. Useful for testing/monitoring.
        def count_for(concurrency_key)
          Pgbus::BlockedExecution.where(concurrency_key: concurrency_key).count
        end

        private

        # The batch execution row is bookkeeping, not the promotion. Give it
        # its own savepoint: `Semaphore.signal` calls promote_next inside a
        # transaction, and a database error out here would poison that
        # transaction — the commit then fails, un-deleting the parked row and
        # un-taking the slot while the message is already live, so the job
        # runs a second time.
        def backfill(released, msg_id, client)
          Pgbus::BlockedExecution.transaction(requires_new: true) do
            Batch.backfill_execution(released[:payload], msg_id,
                                     client.target_queue(released[:queue_name], released[:priority]))
          end
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Batch execution backfill failed after promote: #{e.message}" }
        end

        def slot_taken?(concurrency_key, payload)
          config = Concurrency.config_for_payload(payload)
          expires_at = Time.current + Concurrency.effective_duration(config[:duration])
          Pgbus::Semaphore.acquire!(concurrency_key, config[:limit], expires_at) == :acquired
        end

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
