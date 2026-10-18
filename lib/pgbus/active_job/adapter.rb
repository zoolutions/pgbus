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
        payload_hash = inject_batch_metadata(payload_hash, active_job)

        if uniqueness_rejected?(active_job, payload_hash, queue: queue)
          uncount_batch_job(payload_hash, active_job)
          return active_job
        end

        enqueue_with_concurrency(active_job, queue, payload_hash)
      ensure
        forget_enqueue_batch_id(active_job)
      end

      def enqueue_at(active_job, timestamp)
        queue = active_job.queue_name || Pgbus.configuration.default_queue
        payload_hash = Serializer.serialize_job_hash(active_job)
        payload_hash = Concurrency.inject_metadata(active_job, payload_hash)
        payload_hash = Uniqueness.inject_metadata(active_job, payload_hash)
        payload_hash = FairShare.inject_metadata(active_job, payload_hash)
        payload_hash = inject_batch_metadata(payload_hash, active_job)
        delay = [(timestamp - Time.current.to_f).ceil, 0].max

        if uniqueness_rejected?(active_job, payload_hash, queue: queue)
          uncount_batch_job(payload_hash, active_job)
          return active_job
        end

        enqueue_with_concurrency(active_job, queue, payload_hash, delay: delay)
      ensure
        forget_enqueue_batch_id(active_job)
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
        # Only a failure raised from inside the produce can be ambiguous.
        # Everything before it — taking the slot, parking the job — proves no
        # message exists, however database-shaped the error looks. Never
        # reset: once the send returns, msg_id is set and the rescue routes
        # on that instead.
        sending = false
        retry_lock = hand_lock_to_retry(active_job, payload_hash, queue)

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
            sending = true
            msg_id = send_holding_slot(key, queue, payload_hash, delay: delay, priority: priority)
            active_job.provider_job_id = msg_id
          end
        else
          sending = true
          msg_id = Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
          active_job.provider_job_id = msg_id
        end

        # Bind before backfill so a live message is never left with an unbound
        # uniqueness row if execution-row bookkeeping raises.
        bind_acquired_uniqueness_lock(queue, msg_id) if msg_id
        bind_retry_lock(retry_lock, queue, msg_id) if msg_id
        # Dropped by its concurrency limit: no retry will run, the attempt does.
        return_retry_lock(retry_lock) unless msg_id || blocked
        Batch.backfill_execution(payload_hash, msg_id, physical_queue(queue, priority)) if msg_id
        # A retry re-enqueue that is now live (sent, or parked as a blocked
        # execution) must stop the original attempt from signalling completion.
        Batch.note_retry_reenqueued(payload_hash["job_id"]) if (msg_id || blocked) && retry_retagged?(payload_hash, active_job)
        uniqueness_key = Thread.current[:pgbus_acquired_uniqueness_key]
        UniquenessKey.clear_bind_stamp!(uniqueness_key) if uniqueness_key
        Thread.current[:pgbus_acquired_uniqueness_key] = nil
        active_job
      rescue StandardError => e
        # An ambiguous send is treated exactly like a live message: the
        # produce may have committed with only its reply lost, and undoing
        # the bookkeeping for a message that is in fact live is the worse
        # error — it would leave that job running with no uniqueness lock and
        # uncounted by its batch. A batch left waiting for a job that never
        # existed is recovered by the stalled-batch sweep; a batch that
        # finishes early and fires its callback is not recoverable. The
        # `sending` guard keeps that reprieve to the produce itself: a
        # database error from the slot upsert or the park is not ambiguous.
        if msg_id.nil? && !(sending && ambiguous_delivery?(e))
          rollback_acquired_uniqueness_lock
          return_retry_lock(retry_lock)
          uncount_batch_job(payload_hash, active_job)
        else
          # Drop the thread-local so a later discard on this thread cannot
          # release that job's uniqueness lock, but do not DELETE the
          # pgbus_uniqueness_keys row.
          Thread.current[:pgbus_acquired_uniqueness_key] = nil
        end
        raise e
      end

      # True when a failure raised from inside the produce reached the
      # database, so the message may have been written even though the reply
      # did not come back. Deliberately an over-approximation: the client's
      # pre-produce queue setup is inside the same call, so its connection
      # errors are counted as ambiguous too. That errs toward keeping a lock
      # and a batch count the reapers can reclaim, rather than dropping
      # bookkeeping for a message that turns out to be live.
      def ambiguous_delivery?(error)
        (defined?(PGMQ::Errors::ConnectionError) && error.is_a?(PGMQ::Errors::ConnectionError)) ||
          (defined?(PG::Error) && error.is_a?(PG::Error))
      end

      # A slot is leased for `duration` of silence, but the visibility
      # heartbeat that renews it only starts once a worker picks the message
      # up. A scheduled job waits in PGMQ until then, so its lease has to
      # cover the delay as well or the sweep expires it mid-wait and promotes
      # a second job for the same key.
      def slot_lease(concurrency, delay)
        Concurrency.effective_duration(concurrency[:duration]) + delay.to_i
      end

      # Hand the slot back only when the failure proves nothing was
      # produced. On an ambiguous outcome the message may well be live, and
      # releasing would admit a second job beside it — so the hold stands
      # and the lease expiry plus the dispatcher's sweep reclaim it.
      def send_holding_slot(key, queue, payload_hash, delay:, priority:)
        Pgbus.client.send_message(queue, payload_hash, delay: delay, priority: priority)
      rescue StandardError => e
        if ambiguous_delivery?(e)
          Pgbus.logger.warn do
            "[Pgbus] Send outcome unknown for #{key}; holding its concurrency slot until the lease expires: #{e.message}"
          end
        else
          begin
            Concurrency::Semaphore.release(key)
          rescue StandardError => release_error
            Pgbus.logger.warn { "[Pgbus] Could not release concurrency slot after failed send: #{release_error.message}" }
          end
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
          uncount_batch_job(payload_hash, active_job)
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
        # while losing the retry. Let the retry through (issue #333); the key
        # row is handed to it (hand_lock_to_retry, then bind_retry_lock once
        # sent), so the attempt's release leaves it held until the job finally
        # succeeds or dead-letters (issue #564).
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

      # The job's :until_executed key and the delivery of the attempt running
      # it, when this enqueue is that attempt's retry: the retry is let
      # through the key the attempt holds and never acquires one, so the key
      # row has to follow it. nil for a first attempt (it acquired its own
      # row), a retry enqueued outside the executor (no attempt to take it
      # from), or a job run inline inside another job, whose delivery it sees.
      def retry_uniqueness_lock(active_job, payload_hash)
        key = Uniqueness.extract_key(payload_hash)
        return unless key && Uniqueness.extract_strategy(payload_hash) == :until_executed
        return unless active_job.executions.to_i.positive?

        delivery = Delivery.current
        [key, delivery] if delivery && delivery[:job_id] == active_job.job_id
      end

      # Take the attempt's row for its retry before the retry exists — sent or
      # parked — the way a first enqueue acquires before its send (see
      # UniquenessKey.unbind!). Returns [key, delivery, stamp] for
      # bind_retry_lock / return_retry_lock, or nil when there is nothing to
      # hand over. A failure is logged and the retry goes out without the row,
      # which then stays with the attempt.
      def hand_lock_to_retry(active_job, payload_hash, queue)
        key, delivery = retry_uniqueness_lock(active_job, payload_hash)
        return unless key

        stamp = UniquenessKey.unbind!(key, from_queue_name: Uniqueness.lock_queue_name(delivery[:queue_name]),
                                           from_msg_id: delivery[:msg_id], from_enqueued_at: delivery[:enqueued_at],
                                           queue_name: Uniqueness.lock_queue_name(queue))
        return [key, delivery, stamp] if stamp

        warn_retry_lock_not_held(key)
        nil
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness handoff to retry failed for #{key}: #{e.message}" }
        nil
      end

      # Point the handed-over row at the sent retry. The retry is live, so a
      # failure is logged, not raised: the row stays unbound and stamped
      # before the send, and the retry's own release takes it.
      def bind_retry_lock(retry_lock, queue, msg_id)
        return unless retry_lock

        key, _delivery, stamp = retry_lock
        UniquenessKey.bind!(key, queue_name: Uniqueness.lock_queue_name(queue), msg_id: msg_id, acquired_at: stamp)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness bind to retry failed for #{key}: #{e.message}" }
      end

      # No retry came of it (dropped by its concurrency limit, or the send
      # failed before any message existed): bind the row back to the attempt,
      # which runs on and releases it when it finishes.
      def return_retry_lock(retry_lock)
        return unless retry_lock

        key, delivery, stamp = retry_lock
        UniquenessKey.bind!(key, queue_name: Uniqueness.lock_queue_name(delivery[:queue_name]),
                                 msg_id: delivery[:msg_id], acquired_at: stamp)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness return to the attempt failed for #{key}: #{e.message}" }
      end

      # The row is gone (released by an operator, reaped) or a successor's:
      # the retry runs without the key, and a fresh enqueue is not deduped
      # against it.
      def warn_retry_lock_not_held(key)
        Pgbus.logger.warn do
          "[Pgbus] Uniqueness key #{key} is no longer held by the attempt that enqueued its retry; " \
            "the retry runs without it"
        end
      end

      # Reverses inject_batch_metadata for a job discarded at enqueue time
      # (uniqueness duplicate or concurrency :discard conflict): the message is
      # never sent, so it can never signal completion. Only applies to a job
      # counted in as a member (see #member_batch_id).
      def uncount_batch_job(payload_hash, active_job)
        batch_id = payload_hash[Batch::METADATA_KEY]
        return unless batch_id

        if batch_id == member_batch_id(active_job)
          Batch.untrack_enqueue(payload_hash)
        elsif retry_retagged?(payload_hash, active_job)
          # The retry never became live; the original attempt's row and its
          # normal completion signal stand.
          Batch.forget_retry_reenqueued(payload_hash["job_id"])
        end
      end

      # Tagged for a batch the job was neither added to nor enqueued inside
      # the block of — only a retry_on re-enqueue gets there (issue #424).
      def retry_retagged?(payload_hash, active_job)
        payload_hash[Batch::METADATA_KEY] && member_batch_id(active_job).nil?
      end

      # The batch a job joins as a new member: the one it was added to, kept
      # on the instance so it survives an enqueue deferred to after commit
      # (issue #553), else the Batch#enqueue block active on this thread. The
      # instance wins: a deferred enqueue can run while another batch's
      # block is open on the committing thread.
      def member_batch_id(active_job)
        (active_job.enqueue_batch_id if active_job.respond_to?(:enqueue_batch_id)) ||
          Thread.current[:pgbus_batch_id]
      end

      def forget_enqueue_batch_id(active_job)
        active_job.enqueue_batch_id = nil if active_job.respond_to?(:enqueue_batch_id=)
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

      # Tag the payload with the job's batch and count it in (guarded
      # increment + execution row, before the send — Batch.track_enqueue).
      def inject_batch_metadata(payload_hash, active_job)
        batch_id = member_batch_id(active_job)
        if batch_id
          tagged = payload_hash.merge(Batch::METADATA_KEY => batch_id)
          Batch.track_enqueue(tagged)
          return tagged
        end

        retry_batch_id = retry_batch_id_for(active_job)
        return payload_hash unless retry_batch_id

        tagged = payload_hash.merge(Batch::METADATA_KEY => retry_batch_id)
        Batch.track_retry(tagged)
        tagged
      end

      # Only tags: the bulk is counted once per batch below. A retry re-enqueue
      # never takes the bulk path (retry_on enqueues one job at a time).
      def tag_bulk_member(payload_hash, active_job)
        batch_id = member_batch_id(active_job)
        batch_id ? payload_hash.merge(Batch::METADATA_KEY => batch_id) : payload_hash
      end

      def enqueue_immediate(queue, jobs, priority: nil)
        return if jobs.empty?

        payloads = jobs.map do |j|
          tag_bulk_member(FairShare.inject_metadata(j, Serializer.serialize_job_hash(j)), j)
        end
        track_bulk(payloads)
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
      ensure
        jobs.each { |j| forget_enqueue_batch_id(j) }
      end

      # One guarded increment per batch for the whole bulk, not one per job.
      # When a later batch refuses (finished), the batches already counted are
      # uncounted before the error goes up: nothing has been sent yet.
      def track_bulk(payloads)
        tracked = []
        payloads.select { |p| p[Batch::METADATA_KEY] }
                .group_by { |p| p[Batch::METADATA_KEY] }
                .each_value do |members|
                  Batch.track_enqueue(members)
                  tracked.concat(members)
                end
      rescue StandardError
        tracked.each { |payload| Batch.untrack_enqueue(payload) }
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
