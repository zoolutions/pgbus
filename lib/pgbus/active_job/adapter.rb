# frozen_string_literal: true

require "active_job"

module Pgbus
  module ActiveJob
    class Adapter
      def enqueue(active_job)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = Serializer.serialize_job_hash(active_job)
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        payload_hash = Uniqueness.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash)

        if uniqueness_rejected?(active_job, payload_hash)
          uncount_batch_job(payload_hash)
          return active_job
        end

        enqueue_with_concurrency(active_job, queue, payload_hash)
      end

      def enqueue_at(active_job, timestamp)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = Serializer.serialize_job_hash(active_job)
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        payload_hash = Uniqueness.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash)
        delay = [(timestamp - Time.current.to_f).ceil, 0].max

        if uniqueness_rejected?(active_job, payload_hash)
          uncount_batch_job(payload_hash)
          return active_job
        end

        enqueue_with_concurrency(active_job, queue, payload_hash, delay: delay)
      end

      def enqueue_all(active_jobs)
        # Jobs with uniqueness or concurrency must go through individual enqueue
        # to acquire locks/semaphores — the bulk path cannot (issue #413)
        individual, bulk = active_jobs.partition { |j| Uniqueness.uniqueness_config(j) || concurrency_config(j) }
        individual.each do |j|
          if scheduled_in_future?(j)
            enqueue_at(j, j.scheduled_at.to_f)
          else
            enqueue(j)
          end
        end

        bulk.group_by { |j| j.queue_name || Pgbus.configuration.default_queue }.each do |queue, jobs|
          immediate, scheduled = jobs.partition { |j| !scheduled_in_future?(j) }
          enqueue_immediate(queue, immediate)
          scheduled.each { |j| enqueue_at(j, j.scheduled_at.to_f) }
        end

        active_jobs.count
      end

      private

      def enqueue_with_concurrency(active_job, queue, payload_hash, delay: 0)
        key = Concurrency.extract_key(payload_hash)
        concurrency = concurrency_config(active_job)
        priority = active_job.try(:priority)
        msg_id = nil

        if key && concurrency
          result = Concurrency::Semaphore.acquire(key, concurrency[:limit], concurrency[:duration])

          if result == :acquired
            msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
            active_job.provider_job_id = msg_id
            Batch.backfill_execution(payload_hash, msg_id, physical_queue(queue, priority))
          else
            handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: priority)
          end
        else
          msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
          active_job.provider_job_id = msg_id
          Batch.backfill_execution(payload_hash, msg_id, physical_queue(queue, priority))
        end

        Thread.current[:pgbus_acquired_uniqueness_key] = nil
        active_job
      rescue StandardError => e
        if msg_id.nil?
          rollback_acquired_uniqueness_lock
          uncount_batch_job(payload_hash)
        else
          # Message is live: drop the thread-local so a later discard on this
          # thread cannot release that job's uniqueness lock, but do not
          # DELETE the pgbus_uniqueness_keys row.
          Thread.current[:pgbus_acquired_uniqueness_key] = nil
        end
        raise e
      end

      def physical_queue(queue, priority)
        Pgbus.client.target_queue(queue, priority)
      end

      def concurrency_config(active_job)
        active_job.class.respond_to?(:pgbus_concurrency) && active_job.class.pgbus_concurrency
      end

      def handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: nil)
        case concurrency[:on_conflict]
        when :block
          Concurrency::BlockedExecution.insert(
            concurrency_key: key,
            queue_name: queue,
            payload: payload_hash,
            priority: priority || Pgbus.configuration.default_priority,
            duration: concurrency[:duration]
          )
        when :discard
          Pgbus.logger.info { "[Pgbus] Discarding job #{active_job.class.name}: concurrency limit for #{key}" }
          # The job will never run: roll back an :until_executed uniqueness lock
          # acquired earlier in this enqueue (no executor will release it), and
          # uncount it from its batch so completion is not waited on forever.
          rollback_acquired_uniqueness_lock
          uncount_batch_job(payload_hash)
        when :raise
          raise ConcurrencyLimitExceeded, "Concurrency limit reached for key: #{key}"
        end
      end

      # Releases an :until_executed lock this enqueue acquired, if any.
      # Used when the job is dropped before a message is sent (concurrency
      # :discard, or send_message raising) — otherwise the lock is orphaned
      # because no executor will ever release it.
      def rollback_acquired_uniqueness_lock
        rollback_key = Thread.current[:pgbus_acquired_uniqueness_key]
        return unless rollback_key

        begin
          Uniqueness.release_lock(rollback_key)
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Lock rollback failed: #{e.message}" }
        end
        Thread.current[:pgbus_acquired_uniqueness_key] = nil
      end

      def uniqueness_rejected?(active_job, payload_hash)
        uniqueness_key = Uniqueness.extract_key(payload_hash)
        return false unless uniqueness_key

        # A retry re-enqueue is the SAME logical job re-acquiring its OWN key.
        # ActiveJob increments `executions` at the start of perform_now (before
        # the body), then retry_on re-enqueues from inside perform_now — while
        # the executor still holds the key (it releases only on success/DLQ). So
        # a re-enqueue with executions > 0 would otherwise hit its own held key,
        # be rejected as a duplicate (JobNotUnique), and dead-letter the original
        # while losing the retry. Let the retry through; the existing key row
        # correctly stays held until the job finally succeeds or dead-letters.
        # See issue #333.
        return false if active_job.executions.to_i.positive?

        result = Uniqueness.acquire_enqueue_lock(uniqueness_key, active_job)

        # :no_lock means no enqueue-time lock needed (e.g. :while_executing strategy)
        return false if result == :no_lock

        # Store the acquired key so we can release it if enqueue fails
        Thread.current[:pgbus_acquired_uniqueness_key] = uniqueness_key if result == :acquired
        return false if result == :acquired

        config = Uniqueness.uniqueness_config(active_job)
        case config[:on_conflict]
        when :reject
          raise JobNotUnique, "Job #{active_job.class.name} is already locked"
        when :discard
          Pgbus.logger.info { "[Pgbus] Discarding duplicate job #{active_job.class.name}" }
          true
        when :log
          Pgbus.logger.warn { "[Pgbus] Duplicate job #{active_job.class.name} detected" }
          true
        else
          true
        end
      end

      # Reverses inject_batch_metadata for a job discarded at enqueue time
      # (uniqueness duplicate or concurrency :discard conflict): the message is
      # never sent, so it can never signal completion. Only applies while the
      # tagging batch's block is still active on this thread.
      def uncount_batch_job(payload_hash)
        return unless payload_hash[Batch::METADATA_KEY] &&
                      payload_hash[Batch::METADATA_KEY] == Thread.current[:pgbus_batch_id]

        count = Thread.current[:pgbus_batch_job_count]
        Thread.current[:pgbus_batch_job_count] = count - 1 if count&.positive?
        Batch.untrack_enqueue(payload_hash)
      end

      def inject_batch_metadata(payload_hash)
        batch_id = Thread.current[:pgbus_batch_id]
        return payload_hash unless batch_id

        Thread.current[:pgbus_batch_job_count] = (Thread.current[:pgbus_batch_job_count] || 0) + 1
        tagged = payload_hash.merge(Batch::METADATA_KEY => batch_id)
        Batch.track_enqueue(tagged)
        tagged
      end

      def enqueue_immediate(queue, jobs)
        return if jobs.empty?

        payloads = jobs.map { |j| inject_batch_metadata(Serializer.serialize_job_hash(j)) }
        physical = Pgbus.configuration.queue_name(queue)
        msg_ids = nil
        msg_ids = Pgbus.client.send_batch(queue, payloads)

        unless msg_ids.is_a?(Array) && msg_ids.size == jobs.size
          raise Pgbus::EnqueueError, "Pgbus batch enqueue failed: expected #{jobs.size} ids, got #{msg_ids&.size || 0}"
        end

        jobs.zip(msg_ids).each { |job, id| job.provider_job_id = id }
        payloads.zip(msg_ids).each { |payload, id| Batch.backfill_execution(payload, id, physical) }
      rescue Pgbus::EnqueueError
        Array(payloads).each_with_index do |payload, index|
          Batch.untrack_enqueue(payload) if msg_ids.nil? || msg_ids[index].nil?
        end
        raise
      rescue Pgbus::SchemaNotReady => e
        Pgbus.logger.error { "[Pgbus] #{e.message}" }
        raise
      end

      def scheduled_in_future?(job)
        job.scheduled_at && job.scheduled_at > Time.current
      end

      def enqueue_after_transaction_commit?
        true
      end
    end
  end
end
