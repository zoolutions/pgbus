# frozen_string_literal: true

module Pgbus
  class BatchExecution < BusRecord
    self.table_name = "pgbus_batch_executions"

    # One row per outstanding batched job. Inserted before send_message so a
    # crash cannot produce an untracked in-flight job. ON CONFLICT DO NOTHING
    # makes a retry re-enqueue of the same ActiveJob id a no-op.
    # Raw SQL rather than insert_all(unique_by:) — Rails resolves unique_by
    # through the schema cache (issue #401).
    # queue_name is the logical queue at insert (so the sweep can probe for a
    # live message before the msg_id backfill lands); backfill! overwrites it
    # with the physical target after send.
    def self.insert_for!(batch_id:, job_id:, queue_name: nil)
      connection_pool.with_connection do |conn|
        conn.exec_query(
          "INSERT INTO #{table_name} (batch_id, job_id, queue_name, created_at) " \
          "VALUES ($1, $2, $3, $4) ON CONFLICT (job_id) DO NOTHING",
          "BatchExecution Insert",
          [batch_id, job_id, queue_name, Time.current]
        )
      end
    end

    def self.backfill!(job_id, msg_id:, queue_name:)
      where(job_id: job_id).update_all(msg_id: msg_id, queue_name: queue_name)
    end
  end
end
