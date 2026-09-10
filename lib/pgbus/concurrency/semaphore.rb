# frozen_string_literal: true

module Pgbus
  module Concurrency
    module Semaphore
      class << self
        # Attempt to acquire a slot in the semaphore for the given key.
        # Returns :acquired if a slot was available, :blocked if the limit is reached.
        #
        # The upsert takes the semaphore row lock until the surrounding
        # transaction commits — even when it returns :blocked. The adapter
        # relies on that: parking the job inside the same transaction means a
        # holder that signals concurrently waits on `release` below and then
        # sees the parked row (rails/solid_queue#712).
        def acquire(key, max_value, duration)
          expires_at = Time.current + duration
          Pgbus::Semaphore.acquire!(key, max_value, expires_at)
        end

        # Release one slot in the semaphore. Called after a job completes.
        def release(key)
          Pgbus::Semaphore.where(key: key).update_all("value = GREATEST(value - 1, 0)")
        end

        # A job holding a slot is done: give the slot back and hand it to the
        # next parked job for this key, in one transaction. The decrement locks
        # the semaphore row first, so an enqueue that is parking a job right
        # now commits before the promote looks for parked rows.
        def signal(key, client:)
          Pgbus::Semaphore.transaction do
            release(key)
            BlockedExecution.promote_next(key, client: client)
          end
        end

        # Push the semaphore's expiry out while a holder is still running.
        # Driven by the visibility heartbeat, so `duration` is the longest a
        # holder may go silent before its slot is presumed dead — not a cap on
        # how long a job may run.
        def touch(key, duration)
          # Floored here as well as at acquire: renewing for a raw duration
          # shorter than the gap to the next beat would let the lease lapse
          # mid-run and the sweep promote beside a running job.
          expires_at = Time.current + Concurrency.effective_duration(duration)
          Pgbus::Semaphore.connection_pool.with_connection do
            Pgbus::Semaphore.where(key: key).update_all(["expires_at = GREATEST(expires_at, ?)", expires_at])
          end
        end

        # Delete semaphores that have expired (safety net for crashed workers).
        # Returns an array of hashes with expired keys.
        # Uses DELETE ... RETURNING for atomicity (no race between pluck and delete).
        def expire_stale
          result = Pgbus::Semaphore.connection.exec_query(
            "DELETE FROM pgbus_semaphores WHERE expires_at < $1 RETURNING key",
            "Pgbus Semaphore Expire",
            [Time.current]
          )
          result.rows.map { |row| { "key" => row[0] } }
        end

        # Check current value for a key. Useful for testing/monitoring.
        def current_value(key)
          Pgbus::Semaphore.where(key: key).pick(:value)
        end
      end
    end
  end
end
