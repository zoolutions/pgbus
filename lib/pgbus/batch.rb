# frozen_string_literal: true

require "securerandom"
require "json"

module Pgbus
  class Batch
    class AlreadyFinished < Error; end
    # Raised by #enqueue when a batch with the same uniqueness_key is still
    # running and on_conflict: is :reject.
    class AlreadyRunning < Error; end

    METADATA_KEY = "pgbus_batch_id"

    # A unique batch holds one row in pgbus_uniqueness_keys from #enqueue until
    # the batch finishes. The row's lock_key carries the caller's key under
    # this prefix (so a job's ensures_uniqueness key never collides with it)
    # and its queue_name names the owning batch, which is how the reaper
    # tells a live run from an orphan (see .lock_orphaned?).
    LOCK_KEY_PREFIX = "batch:"
    LOCK_QUEUE_PREFIX = "batch:"
    VALID_CONFLICTS = %i[reject discard log].freeze

    # The callback slots, each mapped to the prefix of its pgbus_batches
    # columns. on_complete keeps the on_finish_* columns until the 1.0 schema
    # cleanup, so rows written before the rename still fire.
    CALLBACK_COLUMNS = { on_complete: "on_finish", on_success: "on_success", on_failure: "on_failure" }.freeze

    attr_reader :batch_id, :properties, :description,
                :on_complete, :on_success, :on_failure,
                :uniqueness_key, :on_conflict

    # Deprecated alias of #on_complete until 1.0.
    def on_finish = on_complete

    def on_discard
      on_failure
    end

    # Open a batch, yield a Pgbus::Batch::Builder, and start the batch when
    # the block returns. Every job the block enqueues (through +b.add+ or a
    # plain +perform_later+) joins the batch, which stays pending, and so
    # cannot finish, until the block is done. Keywords go to Batch.new.
    # Membership is the block's own thread or fiber (the tag lives in
    # Thread.current): b.add raises anywhere else, and a plain perform_later
    # from a thread or fiber the block spawns does not join the batch.
    #
    #   Pgbus::Batch.start(description: "Import") do |b|
    #     b.on_complete ImportDoneJob
    #     b.add(*rows.map { |row| ImportRowJob.new(row.id) })
    #   end
    def self.start(**, &block)
      raise ArgumentError, "Pgbus.batch needs a block: Pgbus.batch { |b| b.add(...) }" unless block

      batch = new(**)
      builder = Builder.new(batch)
      begin
        batch.enqueue { yield builder }
      ensure
        builder.close
      end
      batch
    end

    # @param on_complete [Class, ActiveJob::Base, nil] fired when every job
    #   reached a terminal state (+on_finish:+ is a deprecated alias)
    # @param uniqueness_key [String, nil] at most one unfinished batch with
    #   this key may exist; see #enqueue for what happens to the next one
    # @param on_conflict [Symbol] :reject (raise AlreadyRunning), :discard
    #   (skip the block silently) or :log (skip the block, warn)
    def initialize(on_complete: nil, on_finish: nil, on_success: nil, on_discard: nil, on_failure: nil,
                   description: nil, properties: {}, uniqueness_key: nil, on_conflict: :reject)
      raise ArgumentError, "pass on_failure: only — on_discard: is a deprecated alias" if on_discard && on_failure
      raise ArgumentError, "pass on_complete: only — on_finish: is a deprecated alias" if on_finish && on_complete
      unless VALID_CONFLICTS.include?(on_conflict)
        raise ArgumentError, "on_conflict must be one of #{VALID_CONFLICTS.join(", ")}, got #{on_conflict.inspect}"
      end

      if on_discard
        Pgbus.logger.warn do
          "[Pgbus] Batch on_discard: is deprecated and will be removed in 1.0 — use on_failure: instead"
        end
      end
      self.class.warn_on_finish_deprecated if on_finish

      @batch_id = SecureRandom.uuid
      @on_complete = on_complete || on_finish
      @on_success = on_success
      @on_failure = on_failure || on_discard
      @description = description
      @properties = properties
      @uniqueness_key = uniqueness_key&.to_s
      @on_conflict = on_conflict
      @discarded = false
      @started = false
    end

    # True when #enqueue found another batch with the same uniqueness_key
    # still running and skipped this one (on_conflict: :discard or :log).
    def discarded? = @discarded

    def lock_key
      "#{LOCK_KEY_PREFIX}#{uniqueness_key}" if uniqueness_key
    end

    # Enqueue a group of jobs as a batch. Jobs enqueued inside the block join
    # this batch.
    #
    # Re-callable while the batch is unfinished (open batches): the second and
    # later calls add to the existing group instead of creating a new record.
    # A job running inside the batch reaches its own handle through
    # +ActiveJob::Base#batch+ and can add siblings the same way.
    #
    # Raises Pgbus::Batch::AlreadyFinished once the batch has finished.
    def enqueue(&)
      return reopen(&) if @started
      return self unless acquire_lock!

      begin
        create_record
      rescue StandardError
        self.class.release_lock(batch_id)
        raise
      end
      @started = true
      count_jobs(&)
      start_processing
      self
    end

    # Enqueue ActiveJob instances (and anything the block enqueues) into this
    # batch: +enqueue { ActiveJob.perform_all_later(jobs) }+. Instances keep
    # their own +.set(queue:, wait:, priority:)+ options. On a started batch
    # this is an open-batch add, so a job running inside the batch can call
    # +batch.add(NextJob.new(id))+. Returns the jobs; on a unique batch that
    # another run discarded (#discarded?) nothing was sent.
    def add(*jobs, &block)
      jobs = Builder.job_instances(jobs)
      return jobs if jobs.empty? && block.nil?

      enqueue do
        ::ActiveJob.perform_all_later(jobs) if jobs.any?
        yield if block
      end
      jobs
    end

    # --- readers on a live batch ---------------------------------------

    def status = record&.status

    def total_jobs = record&.total_jobs.to_i

    def completed_jobs = record&.completed_jobs.to_i

    def failed_jobs = record ? self.class.send(:failure_count, record) : 0

    # Jobs still outstanding. Execution rows are the authority; unmigrated
    # installs fall back to counter arithmetic.
    def pending_jobs
      return [total_jobs - completed_jobs - failed_jobs, 0].max unless self.class.executions_migrated?

      BatchExecution.where(batch_id: batch_id).count
    end

    def progress_percentage
      total = total_jobs
      return 100 unless total.positive?

      ((completed_jobs + failed_jobs) * 100) / total
    end

    def finished? = status == "finished"

    # Cached row behind the delegated readers. Re-read with #reload.
    def record
      @record = BatchEntry.find_by(batch_id: batch_id) unless defined?(@record)
      @record
    end

    def reload
      remove_instance_variable(:@record) if defined?(@record)
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

    # Find a batch by id. Returns a rehydrated Pgbus::Batch handle, or nil.
    #
    # BREAKING (pre-1.0): this used to return the raw attributes Hash. Read
    # the same values off the handle (#status, #total_jobs, #properties, …),
    # or query Pgbus::BatchEntry directly for a row.
    def self.find(batch_id)
      record = BatchEntry.find_by(batch_id: batch_id)
      return nil unless record

      rehydrate(record)
    end

    # Build a handle around an existing row without creating a new batch.
    def self.rehydrate(record)
      batch = allocate
      batch.send(:initialize_from_record, record)
      batch
    end
    private_class_method :rehydrate

    # Delete finished batches older than the given threshold.
    def self.cleanup(older_than:)
      BatchEntry.stale(before: older_than).delete_all
    end

    # --- run-scoped uniqueness lock ------------------------------------

    # queue_name stored on the uniqueness row of a unique batch.
    def self.lock_queue_name(batch_id)
      "#{LOCK_QUEUE_PREFIX}#{batch_id}"
    end

    # True for a pgbus_uniqueness_keys row that belongs to a batch rather
    # than to a message.
    def self.lock_row?(queue_name)
      queue_name.to_s.start_with?(LOCK_QUEUE_PREFIX)
    end

    # For the dispatcher's reaper: a batch lock is an orphan once its batch
    # has finished (the release in finish_if_needed failed) or its row is
    # gone (cleanup). A batch that is still pending/processing keeps the lock
    # no matter how old it is — a nightly run legitimately holds it for
    # hours. Any lookup error keeps the lock; the reaper never deletes in
    # doubt.
    def self.lock_orphaned?(queue_name)
      batch_id = queue_name.to_s.delete_prefix(LOCK_QUEUE_PREFIX)
      record = BatchEntry.find_by(batch_id: batch_id)
      record.nil? || record.status == "finished"
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Batch lock lookup failed for #{queue_name}: #{e.message}" }
      false
    end

    # Drop the uniqueness row a batch holds, if any. Fail-soft: the reaper
    # releases the row once the batch is finished.
    def self.release_lock(batch_id)
      UniquenessKey.where(queue_name: lock_queue_name(batch_id)).delete_all
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus] Batch lock release failed for #{batch_id}: #{e.message}" }
      0
    end

    # Schema-probe memos (a true answer is kept for the life of the process)
    # and the warn-once flags, read and written under @schema_lock (issue
    # #527). The probes run outside it: a duplicate probe on a race is
    # harmless, a lock held across a query is not.
    @schema_lock = Mutex.new

    # on_finish: is in most batch calls written before the rename (issue
    # #538), so it warns once per process rather than on every batch.
    def self.warn_on_finish_deprecated
      first = @schema_lock.synchronize { @warned_on_finish ? false : (@warned_on_finish = true) }
      return unless first

      Pgbus.logger.warn do
        "[Pgbus] Batch on_finish is deprecated and will be removed in 1.0 — use on_complete instead"
      end
    end

    def self.executions_migrated?
      return true if @schema_lock.synchronize { @executions_migrated }

      result = begin
        BatchExecution.table_exists?
      rescue StandardError
        false
      end
      @schema_lock.synchronize { @executions_migrated = true } if result
      result
    end

    def self.warn_callback_jobs_unmigrated
      first = @schema_lock.synchronize { @warned_callback_jobs_unmigrated ? false : (@warned_callback_jobs_unmigrated = true) }
      return unless first

      Pgbus.logger.warn do
        "[Pgbus] Batch callback configured as an ActiveJob instance, but pgbus_batches has no " \
          "on_*_job columns yet — .set options (queue, wait, priority) are ignored until " \
          "`rails generate pgbus:add_batch_callback_jobs` runs"
      end
    end

    def self.reset_executions_migrated_cache!
      @schema_lock.synchronize do
        @executions_migrated = nil
        @callback_jobs_migrated = nil
        @warned_callback_jobs_unmigrated = nil
        @warned_on_finish = nil
      end
    end

    # True once the on_finish_job / on_success_job / on_failure_job jsonb
    # columns exist (issue #415). Until then, configured callback instances
    # have nowhere to live and only bare classes are stored.
    def self.callback_jobs_migrated?
      return true if @schema_lock.synchronize { @callback_jobs_migrated }

      result = begin
        BatchEntry.column_names.include?("on_finish_job")
      rescue StandardError
        false
      end
      @schema_lock.synchronize { @callback_jobs_migrated = true } if result
      result
    end

    # Count tagged payloads into their batch and insert their execution rows,
    # in ONE transaction, BEFORE any message is sent (issue #423). Every
    # commit point keeps the invariant
    #   total_jobs == outstanding rows + completed_jobs + failed_jobs
    # which is what lets a finish never race an add: the guarded increment
    # raises AlreadyFinished here — at perform_later, before send — when the
    # batch has already finished. Pass an Array to count a bulk send once.
    def self.track_enqueue(payloads)
      payloads = payloads.is_a?(Hash) ? [payloads] : Array(payloads)
      batch_id = payloads.first&.fetch(METADATA_KEY, nil)
      return if payloads.empty? || batch_id.nil?

      migrated = executions_migrated?
      BatchEntry.transaction do
        BatchEntry.increment_total_jobs!(batch_id, payloads.size)
        next unless migrated

        payloads.each do |payload|
          job_id = payload["job_id"]
          next unless job_id

          BatchExecution.insert_for!(batch_id: batch_id, job_id: job_id, queue_name: payload["queue_name"])
        end
      end
    end

    # A retry_on re-enqueue of a job that is already a batch member (issue
    # #424): same ActiveJob job_id, new PGMQ message. It keeps the ONE
    # execution row it already has (ON CONFLICT DO NOTHING) and is never
    # counted again — the batch waits for this job's terminal outcome, not
    # its first attempt. The backfill after send re-points the row at the new
    # message.
    def self.track_retry(payload)
      return unless executions_migrated?

      batch_id = payload[METADATA_KEY]
      job_id = payload["job_id"]
      return unless batch_id && job_id

      BatchExecution.insert_for!(batch_id: batch_id, job_id: job_id, queue_name: payload["queue_name"])
    end

    # --- "this job re-enqueued itself" bookkeeping ---------------------
    #
    # retry_on re-enqueues from INSIDE perform_now and returns normally, so the
    # executor cannot tell a retried attempt from a successful one. The
    # adapter records the job_id here after a successful retry send; the
    # executor consults it after perform and skips the completion signal, then
    # clears it per execute. Thread.current[] is fiber-local, which is the
    # right scope under execution_mode: :async — adapter and executor run in
    # the same fiber during perform.
    RETRY_REENQUEUED_KEY = :pgbus_batch_retry_reenqueued_job_ids

    def self.note_retry_reenqueued(job_id)
      (Thread.current[RETRY_REENQUEUED_KEY] ||= Set.new) << job_id
    end

    def self.forget_retry_reenqueued(job_id)
      Thread.current[RETRY_REENQUEUED_KEY]&.delete(job_id)
    end

    def self.retry_reenqueued?(job_id)
      Thread.current[RETRY_REENQUEUED_KEY]&.include?(job_id) || false
    end

    def self.clear_retry_reenqueued
      Thread.current[RETRY_REENQUEUED_KEY] = nil
    end

    # Reverse of track_enqueue for a job that will never run (discarded at
    # enqueue time, or its send raised with no msg_id).
    def self.untrack_enqueue(payload)
      batch_id = payload[METADATA_KEY]
      return unless batch_id

      job_id = payload["job_id"]
      migrated = executions_migrated?
      BatchEntry.transaction do
        BatchEntry.decrement_total_jobs!(batch_id)
        BatchExecution.where(job_id: job_id).delete_all if migrated && job_id
      end
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

    def self.sweep_stalled(stalled_for: Pgbus.configuration.batch_stall_threshold, batch_size: 500, client: Pgbus.client)
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
        # Every finish path (completion, sweep) funnels through here, so this
        # is the one place a unique batch gives its run lock back.
        release_lock(result[:record].batch_id) if result[:record].respond_to?(:batch_id)
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

        fire_callback(record, :on_finish, properties)
        fire_callback(record, :on_success, properties) if all_succeeded
        fire_failure_callback(record, properties) unless all_succeeded
      end

      # A configured instance (jsonb column) wins over the legacy class-name
      # column so an app that sets both gets the richer form.
      def fire_callback(record, slot, properties)
        job_data = callback_job_data(record, "#{slot}_job")
        return enqueue_callback_instance(job_data, record.batch_id) if job_data

        class_name = record.public_send("#{slot}_class")
        enqueue_callback(class_name, properties) if class_name
      end

      def fire_failure_callback(record, properties)
        job_data = callback_job_data(record, "on_failure_job")
        return enqueue_callback_instance(job_data, record.batch_id) if job_data

        failure_class = failure_callback_class(record)
        enqueue_callback(failure_class, properties) if failure_class
      end

      def callback_job_data(record, column)
        return nil unless record.respond_to?(column)

        data = record.public_send(column)
        data.presence
      end

      # Callbacks are never members of the batch they report on: batch_id is
      # cleared and callback_batch_id points at the finished batch, so
      # ActiveJob::Base#batch inside the callback reads that batch.
      def enqueue_callback_instance(job_data, batch_id)
        job = ::ActiveJob::Base.deserialize(job_data)
        job.batch_id = nil if job.respond_to?(:batch_id=)
        job.callback_batch_id = batch_id if job.respond_to?(:callback_batch_id=)
        job.enqueue
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Batch callback job could not be enqueued: #{e.class}: #{e.message}" }
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

    def initialize_from_record(record)
      @record = record
      @batch_id = record.batch_id
      @description = record.description
      @properties = parse_properties(record.properties)
      @on_complete = nil
      @on_success = nil
      @on_failure = nil
      @uniqueness_key = nil
      @on_conflict = :reject
      @discarded = false
      @started = true
    end

    # Take the run lock for a unique batch. Returns true when the batch may
    # proceed (no key, or the lock was won); false when another batch holds
    # the key and on_conflict is :discard or :log. :reject raises.
    def acquire_lock! # rubocop:disable Naming/PredicateMethod
      return true unless uniqueness_key

      acquired = UniquenessKey.acquire!(lock_key, queue_name: self.class.lock_queue_name(batch_id), msg_id: 0)
      UniquenessKey.clear_bind_stamp!(lock_key)
      return true if acquired

      case on_conflict
      when :reject
        raise AlreadyRunning, "Batch #{uniqueness_key.inspect} is already running"
      when :discard
        Pgbus.logger.info { "[Pgbus] Discarding batch #{uniqueness_key.inspect}: a batch with that key is still running" }
      else
        Pgbus.logger.warn { "[Pgbus] Batch #{uniqueness_key.inspect} skipped: a batch with that key is still running" }
      end
      @discarded = true
      false
    end

    # Add to an already-created batch. Each job counts itself in (guarded
    # increment + execution row, see .track_enqueue) as it is enqueued, so an
    # add into a finished batch raises at perform_later before anything is
    # sent; the fresh-read check here is only an early exit for the common
    # case. check_finished! afterwards covers a block whose jobs all reached a
    # terminal state while it was still open.
    def reopen(&)
      reload
      raise AlreadyFinished, "Can't add jobs into an already finished batch" if finished?

      count_jobs(&)
      reload
      self.class.send(:finish_if_needed, BatchEntry.check_finished!(batch_id))
      self
    end

    def create_record
      attrs = {
        batch_id: batch_id,
        description: description,
        properties: JSON.generate(properties),
        status: "pending"
      }
      CALLBACK_COLUMNS.each_key { |slot| attrs.merge!(callback_attributes(slot, public_send(slot))) }
      @record = BatchEntry.create!(attrs)
    end

    # Replace one callback on a batch that has not finished yet (the
    # builder's setters, inside the Pgbus.batch block, while the row is
    # still pending and so cannot fire its callbacks).
    def write_callback(slot, callback)
      updated = BatchEntry.where(batch_id: batch_id, status: %w[pending processing])
                          .update_all(callback_attributes(slot, callback))
      raise AlreadyFinished, "Can't change the callbacks of an already finished batch" if updated.zero?

      instance_variable_set(:"@#{slot}", callback)
      reload
    end

    # The pgbus_batches columns for one callback slot. Before the
    # add_batch_executions migration the failure class lives in
    # on_discard_class; before add_batch_callback_jobs there is no *_job
    # column, and a configured instance degrades to its class.
    def callback_attributes(slot, callback)
      prefix = CALLBACK_COLUMNS.fetch(slot)
      class_column = slot == :on_failure && !self.class.executions_migrated? ? "on_discard" : prefix
      attrs = { "#{class_column}_class": callback_class_name(callback) }
      attrs[:"#{prefix}_job"] = serialize_callback(callback) if self.class.callback_jobs_migrated?
      attrs
    end

    # A callback given as a bare class keeps the legacy *_class column; a
    # configured ActiveJob instance is serialized now (so .set options resolve
    # at creation, matching solid_queue) into the *_job jsonb column. Before
    # the add_batch_callback_jobs migration there is nowhere to keep the
    # instance, so it degrades to its class (the callback still fires, on its
    # default queue) with a warning rather than being dropped.
    def callback_class_name(callback)
      return callback.name if callback.is_a?(Class)
      return nil if callback.nil? || self.class.callback_jobs_migrated?

      self.class.warn_callback_jobs_unmigrated
      callback.class.name
    end

    def serialize_callback(callback)
      return nil if callback.nil? || callback.is_a?(Class)

      callback.serialize
    end

    def count_jobs(&)
      previous_batch_id = Thread.current[:pgbus_batch_id]
      Thread.current[:pgbus_batch_id] = batch_id
      yield
    ensure
      Thread.current[:pgbus_batch_id] = previous_batch_id
    end

    # End of the first block: the jobs already counted themselves in, so only
    # the status moves (guarded — the stalled-batch sweep may have flipped it
    # already). An empty block leaves total_jobs = 0, which try_finish!
    # closes through the same single-winner path as any other batch.
    def start_processing
      BatchEntry.where(batch_id: batch_id, status: "pending").update_all(status: "processing")
      reload
      self.class.send(:finish_if_needed, BatchEntry.check_finished!(batch_id))
    end

    def parse_properties(props)
      JSON.parse(props.presence || "{}")
    rescue JSON::ParserError => e
      Pgbus.logger.error { "[Pgbus] Invalid batch properties JSON: #{e.message}" }
      {}
    end
  end
end
