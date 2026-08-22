# frozen_string_literal: true

module Pgbus
  class BatchEntry < BusRecord
    self.table_name = "pgbus_batches"

    # discarded_jobs remains valid until the add_batch_executions migration
    # folds it into failed_jobs. Both names are accepted so gem-before-migrate
    # and gem-after-migrate stay incrementable.
    COUNTER_COLUMNS = %w[completed_jobs discarded_jobs failed_jobs].freeze

    scope :finished, -> { where(status: "finished") }
    scope :stale, ->(before:) { finished.where("finished_at < ?", before) }
    scope :pending, -> { where(status: "pending") }
    scope :processing, -> { where(status: "processing") }
    scope :without_executions, -> { where.not(batch_id: BatchExecution.select(:batch_id)) }

    # Deprecated alias until 1.0: after the column is dropped this reads failed_jobs.
    def discarded_jobs
      has_attribute?(:discarded_jobs) ? self[:discarded_jobs] : failed_jobs
    end

    # Atomically add n to total_jobs on an unfinished batch. Returns true.
    # Raises Batch::AlreadyFinished when the row is already finished (0 rows
    # updated) — the adder-before-insert contract open batches (#415) rely on.
    def self.increment_total_jobs!(batch_id, count) # rubocop:disable Naming/PredicateMethod
      updated = where(batch_id: batch_id, status: %w[pending processing])
                .update_all(["total_jobs = total_jobs + ?", count])
      raise Batch::AlreadyFinished, "Can't add jobs into an already finished batch" if updated.zero?

      true
    end

    # Single-winner finish: status is processing AND no execution rows remain.
    # Join-free NOT EXISTS so the subquery stays in this UPDATE's WHERE.
    # Returns the number of rows updated (0 or 1).
    def self.finish_if_empty!(batch_id)
      # Counters must already be terminal so a pre-migration in-flight batch
      # (zero execution rows, incomplete counters) is not closed empty.
      where(batch_id: batch_id, status: "processing")
        .without_executions
        .where("completed_jobs + failed_jobs = total_jobs AND total_jobs > 0")
        .update_all(status: "finished", finished_at: Time.current)
    end

    # Finish the batch if every job already reached a terminal state. Used
    # after total_jobs is published: completion signals that arrived while the
    # enqueue block was still open saw total_jobs == 0 and could not finish
    # the batch themselves (PR #417). Row lock + status guard keep it
    # idempotent against concurrent completion signals.
    # Returns { just_finished:, record: } or nil if batch not found.
    def self.check_finished!(batch_id)
      return Batch.try_finish!(batch_id) if Batch.executions_migrated?

      transaction do
        record = lock.find_by(batch_id: batch_id)
        return nil unless record
        return { record: record, just_finished: false } if record.status == "finished"
        return { record: record, just_finished: false } unless record.completed_jobs + record.discarded_jobs == record.total_jobs

        record.update!(status: "finished", finished_at: Time.current)
        { record: record, just_finished: true }
      end
    end

    # Atomically increment the counter and, on the pre-migration path, detect
    # if this update caused the batch to finish. Uses row-level locking to
    # prevent duplicate callbacks. Returns { just_finished:, record: } or nil
    # if batch not found.
    def self.increment_counter!(batch_id, column)
      raise ArgumentError, "Invalid column: #{column}" unless COUNTER_COLUMNS.include?(column)

      transaction do
        record = lock.find_by(batch_id: batch_id)
        return nil unless record

        record.increment!(column)

        return { record: record, just_finished: false } if Batch.executions_migrated?

        just_finished = record.completed_jobs + record.discarded_jobs == record.total_jobs
        record.update!(status: "finished", finished_at: Time.current) if just_finished && record.status != "finished"

        { record: record, just_finished: just_finished && record.status == "finished" }
      end
    end
  end
end
