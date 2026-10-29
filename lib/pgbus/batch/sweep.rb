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
        def run(stalled_for: Pgbus.configuration.batch_stall_threshold, batch_size: 500, client: Pgbus.client)
          return unless Batch.executions_migrated?

          payload = { stale_executions: 0, orphan_rows: 0, started_batches: 0, finished_batches: 0,
                      stalled_for: stalled_for }
          Instrumentation.instrument("pgbus.batch_sweep", payload) do |p|
            p[:stale_executions] = sweep_stale_executions(batch_size: batch_size, client: client, stalled_for: stalled_for)
            p[:orphan_rows] = sweep_orphan_rows(stalled_for: stalled_for, batch_size: batch_size, client: client)
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

        # A row with a msg_id was backfilled with the physical table its message
        # was sent to (the `_pN` sub-queue under priority routing), so every
        # probe takes that name as is.
        def classify_stale(row, client)
          in_queue = client.message_in_queue?(row.queue_name, msg_id: row.msg_id, prefixed: false)
          return :still_present if in_queue != false

          dlq = client.dead_letter_physical_name(row.queue_name, prefixed: false)
          return :failed if client.message_with_job_id?(dlq, job_id: row.job_id, prefixed: false) == true
          # completed_jobs :delete (issue #607) leaves no archive row to find.
          return :completed if Pgbus.configuration.completed_jobs == :delete

          return :completed if client.message_archived?(row.queue_name, msg_id: row.msg_id, prefixed: false) == true

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

        # Rows with no msg_id older than the threshold. A row only stays
        # msg_id-less when the enqueue died between row insert and send — OR
        # when the send landed and only the backfill failed, in which case the
        # message is live and must not be un-counted (issue #423). Probe the
        # queue (and DLQ) by job_id before deciding; nil (unknown) keeps the row.
        def sweep_orphan_rows(stalled_for:, batch_size:, client:)
          swept = 0
          cutoff = Time.current - stalled_for
          blocked = blocked_job_ids
          BatchExecution.where(msg_id: nil).where("created_at < ?", cutoff).find_each(batch_size: batch_size) do |row|
            next if blocked.include?(row.job_id)

            case classify_orphan(row, client)
            when :live then next
            when :failed
              Batch.job_discarded(row.batch_id, job_id: row.job_id)
            else
              next unless uncount_orphan!(row)
            end
            swept += 1
          end
          swept
        end

        # A row without a msg_id still holds the logical queue name it was
        # inserted with (the job's queue_name); the backfill was never applied.
        def classify_orphan(row, client)
          return :live if row.queue_name.nil?

          in_queue = client.message_with_job_id?(row.queue_name, job_id: row.job_id)
          return :live if in_queue != false

          dlq = client.dead_letter_physical_name(row.queue_name)
          return :failed if client.message_with_job_id?(dlq, job_id: row.job_id, prefixed: false) == true

          :orphan
        end

        # Returns true when this sweep removed the row (CAS on msg_id NULL).
        def uncount_orphan!(row) # rubocop:disable Naming/PredicateMethod
          deleted = BatchExecution.where(id: row.id, msg_id: nil).delete_all
          return false unless deleted.positive?

          BatchEntry.decrement_total_jobs!(row.batch_id)
          Batch.send(:finish_if_needed, Batch.try_finish!(row.batch_id))
          true
        end

        def blocked_job_ids
          return Set.new unless BlockedExecution.table_exists?

          BlockedExecution.pluck(Arel.sql("payload->>'job_id'")).compact.to_set
        rescue StandardError
          Set.new
        end

        # A pending batch whose enqueue block never returned. Jobs counted
        # themselves in as they were enqueued (issue #423), so total_jobs is
        # already right — only the status moves. "Stalled" means no execution
        # row was inserted within the threshold either: a long-running block
        # that is still enqueuing is live, not stalled.
        def start_stalled_pending(stalled_for:, batch_size:)
          started = 0
          cutoff = Time.current - stalled_for
          BatchEntry.pending
                    .where("created_at < ?", cutoff)
                    .where(
                      "NOT EXISTS (SELECT 1 FROM pgbus_batch_executions e " \
                      "WHERE e.batch_id = pgbus_batches.batch_id AND e.created_at >= ?)", cutoff
                    )
                    .find_each(batch_size: batch_size) do |record|
            BatchEntry.where(batch_id: record.batch_id, status: "pending").update_all(status: "processing")
            Batch.send(:finish_if_needed, Batch.try_finish!(record.batch_id))
            started += 1
          end
          started
        end

        def finish_stalled_processing(batch_size:)
          finished = 0
          BatchEntry.processing.without_executions.find_each(batch_size: batch_size) do |record|
            # Legacy in-flight batches (migrated with an empty executions table)
            # have zero rows but counters short of total_jobs — leave them on
            # the counter path. The true stalls are counters already terminal
            # (finish UPDATE rolled back after callback enqueue) and
            # total_jobs = 0 (enqueue block crashed before enqueuing a job).
            next unless counters_terminal?(record)

            result = Batch.try_finish!(record.batch_id)
            Batch.send(:finish_if_needed, result)
            finished += 1 if result&.fetch(:just_finished, false)
          end
          finished
        end

        def counters_terminal?(record)
          failures = record.discarded_jobs.to_i
          (record.completed_jobs + failures) == record.total_jobs
        end
      end
    end
  end
end
