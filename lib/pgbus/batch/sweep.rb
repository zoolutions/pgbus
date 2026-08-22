# frozen_string_literal: true

module Pgbus
  class Batch
    # Repairs batches the regular completion path cannot finish: worker crash
    # between archive and row-delete, enqueue crash between row-insert and
    # send, a pending batch whose enqueue block never returned, or a processing
    # batch whose finish UPDATE rolled back after callbacks failed to enqueue.
    module Sweep
      STALL_THRESHOLD = 300 # seconds; solid_queue default stalled_for: 5.minutes

      class << self
        def run(stalled_for: STALL_THRESHOLD, batch_size: 500, client: Pgbus.client)
          return unless Batch.executions_migrated?

          payload = { stale_executions: 0, orphan_rows: 0, started_batches: 0, finished_batches: 0,
                      stalled_for: stalled_for }
          Instrumentation.instrument("pgbus.batch_sweep", payload) do |p|
            p[:stale_executions] = sweep_stale_executions(batch_size: batch_size, client: client, stalled_for: stalled_for)
            p[:orphan_rows] = sweep_orphan_rows(stalled_for: stalled_for, batch_size: batch_size)
            p[:started_batches] = start_stalled_pending(stalled_for: stalled_for, batch_size: batch_size)
            p[:finished_batches] = finish_stalled_processing(batch_size: batch_size)
          end
        end

        private

        def sweep_stale_executions(batch_size:, client:, stalled_for:)
          swept = 0
          cutoff = Time.current - stalled_for
          BatchExecution.where.not(msg_id: nil).where("created_at < ?", cutoff).find_each(batch_size: batch_size) do |row|
            outcome = classify_stale(row, client)
            next if outcome == :still_present

            resolve_stale(row, outcome)
            swept += 1
          end
          swept
        end

        def classify_stale(row, client)
          in_queue = client.message_in_queue?(row.queue_name, msg_id: row.msg_id)
          return :still_present if in_queue != false

          dlq = client.dead_letter_physical_name(row.queue_name)
          return :failed if client.message_with_job_id?(dlq, job_id: row.job_id) == true

          return :completed if client.message_archived?(row.queue_name, msg_id: row.msg_id) == true

          Pgbus.logger.warn do
            "[Pgbus] Batch execution #{row.job_id} (batch #{row.batch_id}) has no queue, " \
              "archive, or DLQ message — resolving as completed"
          end
          :completed
        end

        def resolve_stale(row, outcome)
          column = outcome == :failed ? "failed_jobs" : "completed_jobs"
          Batch.job_completed(row.batch_id, job_id: row.job_id) if column == "completed_jobs"
          Batch.job_discarded(row.batch_id, job_id: row.job_id) if column == "failed_jobs"
        end

        def sweep_orphan_rows(stalled_for:, batch_size:)
          swept = 0
          cutoff = Time.current - stalled_for
          blocked = blocked_job_ids
          BatchExecution.where(msg_id: nil).where("created_at < ?", cutoff).find_each(batch_size: batch_size) do |row|
            next if blocked.include?(row.job_id)

            deleted = BatchExecution.where(id: row.id, msg_id: nil).delete_all
            next unless deleted.positive?

            BatchEntry.where(batch_id: row.batch_id, status: %w[pending processing])
                      .update_all(["total_jobs = GREATEST(total_jobs - 1, 0)"])
            Batch.send(:finish_if_needed, Batch.try_finish!(row.batch_id))
            swept += 1
          end
          swept
        end

        def blocked_job_ids
          return Set.new unless BlockedExecution.table_exists?

          BlockedExecution.pluck(Arel.sql("payload->>'job_id'")).compact.to_set
        rescue StandardError
          Set.new
        end

        def start_stalled_pending(stalled_for:, batch_size:)
          started = 0
          cutoff = Time.current - stalled_for
          BatchEntry.pending.where("created_at < ?", cutoff).find_each(batch_size: batch_size) do |record|
            total = BatchExecution.where(batch_id: record.batch_id).count
            BatchEntry.where(batch_id: record.batch_id, status: "pending")
                      .update_all(total_jobs: total, status: "processing")
            result = Batch.try_finish!(record.batch_id)
            Batch.send(:finish_if_needed, result)
            started += 1
          end
          started
        end

        def finish_stalled_processing(batch_size:)
          finished = 0
          BatchEntry.processing.without_executions.find_each(batch_size: batch_size) do |record|
            # Legacy in-flight batches (migrated with an empty executions table)
            # have zero rows but unfinished counters — leave them on the
            # counter path. The true stall is counters already terminal and
            # the finish UPDATE rolled back after callback enqueue.
            next unless counters_terminal?(record)

            result = Batch.try_finish!(record.batch_id)
            Batch.send(:finish_if_needed, result)
            finished += 1 if result&.fetch(:just_finished, false)
          end
          finished
        end

        def counters_terminal?(record)
          failures = record.discarded_jobs.to_i
          record.total_jobs.positive? &&
            (record.completed_jobs + failures) == record.total_jobs
        end
      end
    end
  end
end
