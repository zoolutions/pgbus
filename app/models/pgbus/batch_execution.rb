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

    # insert_for! for many members of one batch, in one statement (issue
    # #683): +rows+ are [job_id, queue_name] pairs, bound as two arrays so
    # the statement keeps three parameters whatever the count.
    def self.insert_all_for!(batch_id:, rows:)
      return if rows.empty?

      job_ids, queue_names = rows.transpose
      connection_pool.with_connection do |conn|
        conn.exec_query(
          "INSERT INTO #{table_name} (batch_id, job_id, queue_name, created_at) " \
          "SELECT $1, r.job_id, r.queue_name, $4 FROM unnest($2::varchar[], $3::varchar[]) AS r(job_id, queue_name) " \
          "ON CONFLICT (job_id) DO NOTHING",
          "BatchExecution Insert All",
          [batch_id, encode_array(job_ids), encode_array(queue_names), Time.current]
        )
      end
    end

    def self.backfill!(job_id, msg_id:, queue_name:)
      where(job_id: job_id).update_all(msg_id: msg_id, queue_name: queue_name)
    end

    # backfill! for a whole send group in one statement (issue #683): +rows+
    # are [job_id, msg_id, queue_name] triples.
    def self.backfill_all!(rows)
      return if rows.empty?

      job_ids, msg_ids, queue_names = rows.transpose
      connection_pool.with_connection do |conn|
        conn.exec_query(
          "UPDATE #{table_name} AS e SET msg_id = v.msg_id, queue_name = v.queue_name " \
          "FROM unnest($1::varchar[], $2::bigint[], $3::varchar[]) AS v(job_id, msg_id, queue_name) " \
          "WHERE e.job_id = v.job_id",
          "BatchExecution Backfill All",
          [encode_array(job_ids), encode_array(msg_ids), encode_array(queue_names)]
        )
      end
    end

    # Deletes the rows of +job_ids+ in one statement; returns how many went.
    def self.delete_all_for!(job_ids)
      return 0 if job_ids.empty?

      connection_pool.with_connection do |conn|
        conn.exec_delete(
          "DELETE FROM #{table_name} WHERE job_id = ANY($1::varchar[])",
          "BatchExecution Delete All",
          [encode_array(job_ids)]
        )
      end
    end

    # A Postgres array literal, so an array binds as one text parameter the
    # same way on every Rails version.
    def self.encode_array(values)
      PG::TextEncoder::Array.new.encode(values.map { |v| v&.to_s })
    end
    private_class_method :encode_array
  end
end
