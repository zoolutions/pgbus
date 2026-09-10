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
        payload_hash = FairShare.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash, active_job: active_job)

        if uniqueness_rejected?(active_job, payload_hash, queue: queue)
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
        payload_hash = FairShare.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash, active_job: active_job)
        delay = [(timestamp - Time.current.to_f).ceil, 0].max

        if uniqueness_rejected?(active_job, payload_hash, queue: queue)
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

        # Group by priority too: send_batch routes through the queue strategy,
        # so a mixed-priority bulk send needs one produce_batch per level.
        bulk.group_by { |j| [j.queue_name || Pgbus.configuration.default_queue, j.try(:priority)] }
            .each do |(queue, priority), jobs|
          immediate, scheduled = jobs.partition { |j| !scheduled_in_future?(j) }
          enqueue_immediate(queue, immediate, priority: priority)
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
        blocked = false

        if key && concurrency
          # The check and the park commit together, under the semaphore row
          # lock the upsert holds even when it reports :blocked — so a holder
          # signalling right now waits and then sees the parked row instead
          # of stranding it (rails/solid_queue#712).
          acquired = false
          Pgbus::Semaphore.transaction(requires_new: true) do
            acquired = Concurrency::Semaphore.acquire(key, concurrency[:limit], slot_lease(concurrency, delay)) ==
                       :acquired
            blocked = handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: priority) unless
              acquired
          end

          if acquired
            # Deliberately AFTER the commit. PGMQ has its own connection, so
            # the send can never join this transaction; sending first would
            # mean a failed commit leaves the message live with the slot
            # rolled back, and the next enqueue runs beside it. This way the
            # only crash window leaves a slot held with no message — the
            # sweep reclaims it, and until then the key is under-admitted,
            # never over-admitted.
            msg_id = send_holding_slot(key, queue, payload_hash, delay: delay, priority: priority)
            active_job.provider_job_id = msg_id
          end
        else
          msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
          active_job.provider_job_id = msg_id
        end

        # Bind before backfill so a live message is never left with an unbound
        # uniqueness row if execution-row bookkeeping raises.
        bind_acquired_uniqueness_lock(queue, msg_id) if msg_id
        Batch.backfill_execution(payload_hash, msg_id, physical_queue(queue, priority)) if msg_id
        # A retry re-enqueue that is now live (sent, or parked as a blocked
        # execution) must stop the original attempt from signalling completion.
        Batch.note_retry_reenqueued(payload_hash["job_id"]) if (msg_id || blocked) && retry_retagged?(payload_hash)
        uniqueness_key = Thread.current[:pgbus_acquired_uniqueness_key]
        UniquenessKey.clear_bind_stamp!(uniqueness_key) if uniqueness_key
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

      # A slot is leased for `duration` of silence, but the visibility
      # heartbeat that renews it only starts once a worker picks the message
      # up. A scheduled job waits in PGMQ until then, so its lease has to
      # cover the delay as well or the sweep expires it mid-wait and promotes
      # a second job for the same key.
      def slot_lease(concurrency, delay)
        Concurrency.effective_duration(concurrency[:duration]) + delay.to_i
      end

      # A send that raises is a deterministic failure, so hand the slot back
      # now rather than leaving the key short until the lease expires.
      def send_holding_slot(key, queue, payload_hash, delay:, priority:)
        Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
      rescue StandardError
        begin
          Concurrency::Semaphore.release(key)
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Could not release concurrency slot after failed send: #{e.message}" }
        end
        raise
      end

      def physical_queue(queue, priority)
        Pgbus.client.target_queue(queue, priority)
      end

      def concurrency_config(active_job)
        active_job.class.respond_to?(:pgbus_concurrency) && active_job.class.pgbus_concurrency
      end

      # Returns true when the job was parked as a blocked execution (it will
      # run later), false when it was dropped.
      def handle_conflict(concurrency, active_job, key, queue, payload_hash, priority: nil) # rubocop:disable Naming/PredicateMethod
        case concurrency[:on_conflict]
        when :block
          Concurrency::BlockedExecution.insert(
            concurrency_key: key,
            queue_name: queue,
            payload: payload_hash,
            priority: priority || Pgbus.configuration.default_priority,
            duration: concurrency[:duration]
          )
          return true
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
        false
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

      def uniqueness_rejected?(active_job, payload_hash, queue:)
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

        result = Uniqueness.acquire_enqueue_lock(uniqueness_key, active_job, queue_name: queue)

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

      def bind_acquired_uniqueness_lock(queue, msg_id)
        key = Thread.current[:pgbus_acquired_uniqueness_key]
        return unless key

        Uniqueness.bind_lock(key, queue_name: queue, msg_id: msg_id)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness bind failed: #{e.message}" }
      end

      # Reverses inject_batch_metadata for a job discarded at enqueue time
      # (uniqueness duplicate or concurrency :discard conflict): the message is
      # never sent, so it can never signal completion. Only applies while the
      # tagging batch's block is still active on this thread.
      def uncount_batch_job(payload_hash)
        batch_id = payload_hash[Batch::METADATA_KEY]
        return unless batch_id

        if batch_id == Thread.current[:pgbus_batch_id]
          Batch.untrack_enqueue(payload_hash)
        elsif retry_retagged?(payload_hash)
          # The retry never became live; the original attempt's row and its
          # normal completion signal stand.
          Batch.forget_retry_reenqueued(payload_hash["job_id"])
        end
      end

      # Tagged for a batch while no Batch#enqueue block is active on this
      # thread — only a retry_on re-enqueue gets there (issue #424).
      def retry_retagged?(payload_hash)
        payload_hash[Batch::METADATA_KEY] && Thread.current[:pgbus_batch_id].nil?
      end

      # A retry_on re-enqueue of a job that is already a batch member: same
      # job_id (executions > 0), batch_id carried by the BatchId mixin. It
      # rejoins its batch without being counted again. A first-attempt job
      # that merely has a batch_id outside a block is NOT tagged — membership
      # stays explicit; only the callback_batch_id never re-tags.
      def retry_batch_id_for(active_job)
        return nil unless active_job.respond_to?(:batch_id)
        return nil unless active_job.executions.to_i.positive?

        active_job.batch_id
      end

      # Tag the payload with the active batch and count it in (guarded
      # increment + execution row, before the send — Batch.track_enqueue). Pass
      # track: false to only tag, when the caller counts a bulk once.
      def inject_batch_metadata(payload_hash, active_job: nil, track: true)
        batch_id = Thread.current[:pgbus_batch_id]
        if batch_id
          tagged = payload_hash.merge(Batch::METADATA_KEY => batch_id)
          Batch.track_enqueue(tagged) if track
          return tagged
        end

        retry_batch_id = active_job && retry_batch_id_for(active_job)
        return payload_hash unless retry_batch_id

        tagged = payload_hash.merge(Batch::METADATA_KEY => retry_batch_id)
        Batch.track_retry(tagged)
        tagged
      end

      def enqueue_immediate(queue, jobs, priority: nil)
        return if jobs.empty?

        payloads = jobs.map do |j|
          inject_batch_metadata(FairShare.inject_metadata(j, Serializer.serialize_job_hash(j)), track: false)
        end
        # One guarded increment for the whole bulk, not one per job.
        Batch.track_enqueue(payloads) if Thread.current[:pgbus_batch_id]
        physical = physical_queue(queue, priority)
        msg_ids = nil
        msg_ids = Pgbus.client.send_batch(queue, payloads, priority: priority)

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
