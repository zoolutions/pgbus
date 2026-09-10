# frozen_string_literal: true

module Pgbus
  class BlockedExecution < BusRecord
    self.table_name = "pgbus_blocked_executions"

    scope :for_key, ->(key) { where(concurrency_key: key) }

    # Atomic dequeue: DELETE the highest-priority row with FOR UPDATE SKIP LOCKED.
    # A parked job never ages out — `expires_at` orders the sweep, nothing more.
    # Returns { queue_name:, payload:, priority: } or nil.
    def self.release_next!(concurrency_key)
      result = connection.exec_query(
        <<~SQL,
          DELETE FROM pgbus_blocked_executions
          WHERE id = (
            SELECT id FROM pgbus_blocked_executions
            WHERE concurrency_key = $1
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          )
          RETURNING queue_name, payload, priority
        SQL
        "Pgbus Blocked Release",
        [concurrency_key]
      )

      row = result.first
      return nil unless row

      payload = row["payload"]
      # exec_query returns jsonb as text; a row written before the
      # double-encoding fix parses to a String holding the real document.
      2.times { payload = JSON.parse(payload) if payload.is_a?(String) }

      { queue_name: row["queue_name"], payload: payload, priority: row["priority"] }
    end

    # Rewrite rows parked before the double-encoding fix (a jsonb string
    # holding the document) as the document itself, so SQL readers such as
    # the batch sweep's `payload->>'job_id'` see them. Idempotent; the sweep
    # runs it before every promotion pass.
    def self.repair_double_encoded!
      where("jsonb_typeof(payload) = 'string'").update_all("payload = (payload #>> '{}')::jsonb")
    end

    # Keys with parked jobs, longest-waiting key first.
    def self.pending_keys(limit: 1000)
      group(:concurrency_key).order(Arel.sql("MIN(created_at)")).limit(limit).pluck(:concurrency_key)
    end
  end
end
