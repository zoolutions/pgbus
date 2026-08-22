# frozen_string_literal: true

require "securerandom"
require "json"

module Pgbus
  class Batch
    class AlreadyFinished < Error; end

    METADATA_KEY = "pgbus_batch_id"

    attr_reader :batch_id, :properties, :description,
                :on_finish, :on_success, :on_failure

    def on_discard
      on_failure
    end

    def initialize(on_finish: nil, on_success: nil, on_discard: nil, on_failure: nil, description: nil, properties: {})
      raise ArgumentError, "pass on_failure: only — on_discard: is a deprecated alias" if on_discard && on_failure

      if on_discard
        Pgbus.logger.warn do
          "[Pgbus] Batch on_discard: is deprecated and will be removed in 1.0 — use on_failure: instead"
        end
      end

      @batch_id = SecureRandom.uuid
      @on_finish = on_finish
      @on_success = on_success
      @on_failure = on_failure || on_discard
      @description = description
      @properties = properties
      @job_count = 0
    end

    # Enqueue a group of jobs as a batch.
    # Jobs enqueued inside the block are tracked as part of this batch.
    def enqueue(&)
      create_record
      count_jobs(&)
      update_total
      self
    end

    # Record a completed job. Returns the batch row after update.
    def self.job_completed(batch_id, job_id: nil)
      if executions_migrated?
        job_id ? resolve_execution(batch_id, job_id, "completed_jobs") : signal_without_row(batch_id, "completed_jobs")
      else
        update_counter(batch_id, "completed_jobs")
      end
    end

    # Record a discarded/dead-lettered job. Returns the batch row after update.
    def self.job_discarded(batch_id, job_id: nil)
      if executions_migrated?
        job_id ? resolve_execution(batch_id, job_id, "failed_jobs") : signal_without_row(batch_id, "failed_jobs")
      else
        update_counter(batch_id, "discarded_jobs")
      end
    end

    # Find a batch record by ID. Returns a hash or nil.
    def self.find(batch_id)
      BatchEntry.find_by(batch_id: batch_id)&.attributes
    end

    # Delete finished batches older than the given threshold.
    def self.cleanup(older_than:)
      BatchEntry.stale(before: older_than).delete_all
    end

    def self.executions_migrated?
      return true if @executions_migrated

      result = begin
        BatchExecution.table_exists?
      rescue StandardError
        false
      end
      @executions_migrated = true if result
      result
    end

    def self.reset_executions_migrated_cache!
      @executions_migrated = nil
    end

    def self.track_enqueue(payload)
      return unless executions_migrated?

      batch_id = payload[METADATA_KEY]
      job_id = payload["job_id"]
      return unless batch_id && job_id

      BatchExecution.insert_for!(batch_id: batch_id, job_id: job_id)
    end

    def self.untrack_enqueue(payload)
      return unless executions_migrated?

      job_id = payload["job_id"]
      return unless job_id

      BatchExecution.where(job_id: job_id).delete_all
    end

    def self.backfill_execution(payload, msg_id, queue_name)
      return unless executions_migrated?
      return unless payload && msg_id

      job_id = payload["job_id"]
      return unless job_id

      BatchExecution.backfill!(job_id, msg_id: msg_id, queue_name: queue_name)
    end

    # Single-winner finish via execution-row absence. After a winning UPDATE,
    # re-check exists? in a fresh statement (Postgres READ COMMITTED can let a
    # blocked CAS win from a stale NOT EXISTS snapshot — solid_queue's finalize).
    def self.try_finish!(batch_id)
      result = BatchEntry.transaction do
        updated = BatchEntry.finish_if_empty!(batch_id)
        next { just_finished: false, record: BatchEntry.find_by(batch_id: batch_id) } unless updated.positive?

        raise ActiveRecord::Rollback if BatchExecution.where(batch_id: batch_id).exists?

        { just_finished: true, record: BatchEntry.find_by(batch_id: batch_id) }
      end

      return { just_finished: false, record: BatchEntry.find_by(batch_id: batch_id) } if result.nil?

      result
    end

    def self.sweep_stalled(stalled_for: Sweep::STALL_THRESHOLD, batch_size: 500, client: Pgbus.client)
      Sweep.run(stalled_for: stalled_for, batch_size: batch_size, client: client)
    end

    class << self
      private

      def resolve_execution(batch_id, job_id, column)
        BatchEntry.transaction do
          deleted = BatchExecution.where(job_id: job_id).delete_all
          BatchEntry.increment_counter!(batch_id, column) if deleted.positive? || legacy_untracked_batch?(batch_id)
        end
        finish_if_needed(try_finish!(batch_id))
      end

      # A migrated batch with no execution rows at all is a pre-migration
      # in-flight group. Increment counters (the executor no longer hits the
      # discarded_jobs column) and let finish_if_empty! wait until they match.
      def legacy_untracked_batch?(batch_id)
        return false if BatchExecution.where(batch_id: batch_id).exists?

        record = BatchEntry.find_by(batch_id: batch_id)
        record && !counters_match_total?(record)
      end

      def counters_match_total?(record)
        failures = record.respond_to?(:discarded_jobs) ? record.discarded_jobs.to_i : record.failed_jobs.to_i
        record.total_jobs.positive? && (record.completed_jobs + failures) == record.total_jobs
      end

      def signal_without_row(batch_id, column)
        update_counter(batch_id, column)
        finish_if_needed(try_finish!(batch_id))
      end

      def finish_if_needed(result)
        return result unless result&.fetch(:just_finished, false) && result[:record]

        fire_callbacks(result[:record])
        instrument_finished(result[:record])
        result
      end

      def instrument_finished(record)
        Instrumentation.instrument(
          "pgbus.batch_finished",
          batch_id: record.respond_to?(:batch_id) ? record.batch_id : nil,
          total_jobs: record.respond_to?(:total_jobs) ? record.total_jobs : nil,
          completed_jobs: record.respond_to?(:completed_jobs) ? record.completed_jobs : nil,
          failed_jobs: failure_count(record)
        )
      end

      def failure_count(record)
        use_failed = record.respond_to?(:has_attribute?) &&
                     record.has_attribute?(:failed_jobs) &&
                     !record.has_attribute?(:discarded_jobs)
        return record.failed_jobs.to_i if use_failed
        return record.discarded_jobs.to_i if record.respond_to?(:discarded_jobs)

        0
      end

      def update_counter(batch_id, column)
        result = BatchEntry.increment_counter!(batch_id, column)
        return nil unless result

        finish_if_needed(result)
      end

      def fire_callbacks(record)
        properties = begin
          JSON.parse(record.properties.presence || "{}")
        rescue JSON::ParserError => e
          Pgbus.logger.error { "[Pgbus] Invalid batch properties JSON: #{e.message}" }
          {}
        end
        all_succeeded = failure_count(record).to_i.zero?

        enqueue_callback(record.on_finish_class, properties) if record.on_finish_class
        enqueue_callback(record.on_success_class, properties) if record.on_success_class && all_succeeded
        failure_class = failure_callback_class(record)
        enqueue_callback(failure_class, properties) if failure_class && !all_succeeded
      end

      def failure_callback_class(record)
        if record.respond_to?(:on_failure_class) && record.on_failure_class.present?
          record.on_failure_class
        elsif record.respond_to?(:on_discard_class)
          record.on_discard_class
        end
      end

      def enqueue_callback(class_name, properties)
        job_class = class_name.safe_constantize
        unless job_class && job_class < ::ActiveJob::Base
          Pgbus.logger.error { "[Pgbus] Batch callback class invalid or not an ActiveJob: #{class_name}" }
          return
        end
        job_class.perform_later(properties)
      end
    end

    private

    def create_record
      attrs = {
        batch_id: batch_id,
        description: description,
        on_finish_class: on_finish&.name,
        on_success_class: on_success&.name,
        properties: JSON.generate(properties),
        status: "pending"
      }
      if self.class.executions_migrated?
        attrs[:on_failure_class] = on_failure&.name
      else
        attrs[:on_discard_class] = on_failure&.name
      end
      BatchEntry.create!(attrs)
    end

    def count_jobs(&)
      previous_batch_id = Thread.current[:pgbus_batch_id]
      previous_count = Thread.current[:pgbus_batch_job_count]

      Thread.current[:pgbus_batch_id] = batch_id
      Thread.current[:pgbus_batch_job_count] = 0

      yield

      @job_count = Thread.current[:pgbus_batch_job_count] || 0
    ensure
      Thread.current[:pgbus_batch_id] = previous_batch_id
      Thread.current[:pgbus_batch_job_count] = previous_count
    end

    def update_total
      if @job_count.zero?
        # Finish empty batches immediately — no jobs to signal completion
        BatchEntry.where(batch_id: batch_id).update_all(
          total_jobs: 0,
          status: "finished",
          finished_at: Time.current
        )
        fire_empty_batch_callbacks
      else
        BatchEntry.where(batch_id: batch_id).update_all(total_jobs: @job_count, status: "processing")
        # Jobs can reach their terminal state while the enqueue block is still
        # open — those completion signals saw total_jobs == 0 and could not
        # finish the batch. Re-check now that the real total is visible.
        result = BatchEntry.check_finished!(batch_id)
        self.class.send(:finish_if_needed, result)
      end
    end

    def fire_empty_batch_callbacks
      record = BatchEntry.find_by(batch_id: batch_id)
      return unless record

      properties = parse_properties(record.properties)
      self.class.send(:enqueue_callback, record.on_finish_class, properties) if record.on_finish_class
      self.class.send(:enqueue_callback, record.on_success_class, properties) if record.on_success_class
    end

    def parse_properties(props)
      JSON.parse(props.presence || "{}")
    rescue JSON::ParserError => e
      Pgbus.logger.error { "[Pgbus] Invalid batch properties JSON: #{e.message}" }
      {}
    end
  end
end
