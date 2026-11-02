# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # Re-sending a job payload as a new message — DLQ Retry, and failed-job
      # Retry when the original message is gone (issue #631). The payload names
      # its uniqueness key and strategy, and the copy releases the row it
      # holds when it starts (:until_executing) or finishes (:until_executed).
      # Re-sent without a row of its own, that release could match a newer
      # copy's unbound row (one parked behind its concurrency limit) and free
      # the key while that copy waits. So the key is taken the way
      # Adapter#enqueue takes it: unbound before the send, bound to the new
      # message after it, given back if the send fails before producing.
      module Resend
        private

        # Yields to send the payload and returns true once it is sent, false
        # when another copy holds the key (nothing is sent, so the source — the
        # DLQ message or the failed row — stays for a later retry). The block
        # returns the new msg_id. +lock_queue+ is the queue as the executor
        # spells the lock (Uniqueness.lock_queue_name); +holder+ is the
        # [lock queue, msg_id, enqueued_at] of the message the payload was
        # read from, as the dashboard's discards name it (#lock_holder).
        def resend_holding_uniqueness(payload, lock_queue:, holder:)
          key = resend_lock_key(payload)
          unless key
            yield
            return true
          end

          stamp = take_resend_lock(key, lock_queue, holder)
          unless stamp
            Pgbus.logger.warn { "[Pgbus::Web] Not re-sending #{payload["job_class"]}: uniqueness key #{key} is held by another copy" }
            return false
          end

          begin
            msg_id = yield
          rescue StandardError => e
            give_back_resend_lock(key, stamp, e)
            raise
          end
          bind_resent_lock(key, lock_queue, msg_id, stamp)
          true
        ensure
          UniquenessKey.clear_bind_stamp!(key) if key
        end

        # A failed job's message back to work: its vt reset to 0 while it is
        # still queued (a second copy would run beside it), else its payload
        # re-sent. The failed row goes either way; false (row kept) while the
        # re-send's key is held.
        def retry_failed_job(event)
          physical = failed_message_queue(event)
          return resend_failed_event(event) unless physical

          @client.set_visibility_timeout(physical, event["msg_id"].to_i, vt: 0, prefixed: false)
          delete_failed_row(event)
          true
        end

        # A failed job's payload as a new message on its queue, its failed row
        # deleted once it is sent. Its message is gone, so it can hold only a
        # row still bound to it (nil send time: no unbound row is its own).
        # The delete comes after the key is bound: PGMQ sends on its own
        # connection, so a failed delete cannot undo the send, and must not
        # give back the key of a live copy.
        def resend_failed_event(event)
          payload = JSON.parse(event["payload"])
          headers = event["headers"]
          headers = JSON.parse(headers) if headers.is_a?(String)
          lock_queue = Uniqueness.lock_queue_name(event["queue_name"])

          sent = resend_holding_uniqueness(payload, lock_queue: lock_queue, holder: [lock_queue, event["msg_id"].to_i, nil]) do
            @client.send_message(event["queue_name"], payload, headers: headers)
          end
          delete_failed_row(event) if sent
          sent
        end

        def delete_failed_row(event)
          connection.exec_delete(
            "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [event["id"].to_i]
          )
        end

        # The key for the copy: the row the source message still holds is
        # taken over (it went without releasing it — a release that failed,
        # or a failed job whose message is gone), as a retry takes its
        # attempt's (Adapter#hand_lock_to_retry); else a free key is
        # acquired. Returns the row's created_at stamp (bind! and the
        # give-back match the row by it), nil when another copy holds the
        # key. A newer copy parked after the source released its row was
        # acquired after the source was sent, so the takeover never matches it.
        def take_resend_lock(key, lock_queue, holder)
          from_queue, from_msg_id, from_enqueued_at = holder
          stamp = UniquenessKey.unbind!(key, from_queue_name: from_queue, from_msg_id: from_msg_id,
                                             from_enqueued_at: from_enqueued_at, queue_name: lock_queue)
          return stamp if stamp
          return unless UniquenessKey.acquire!(key, queue_name: lock_queue, msg_id: 0)

          Thread.current[:pgbus_uniqueness_created_at]&.[](key)
        end

        # The send raised. Proven unsent, the row taken for it goes (matched
        # by its stamp, so never a successor's). An error that may hide a
        # committed send (a lost connection or reply, as Adapter#ambiguous_delivery?
        # counts it) keeps the row: the copy may be live, and the reaper
        # takes the row if it is not. A failed give-back is logged, so the
        # send's error is the one that surfaces.
        def give_back_resend_lock(key, stamp, error)
          if ambiguous_resend?(error)
            Pgbus.logger.warn { "[Pgbus::Web] Re-send of #{key} may have been sent (#{error.class}); its key stays held" }
            return
          end

          UniquenessKey.release_if_unbound!(key, acquired_before: stamp)
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus::Web] Giving back the uniqueness key #{key} failed: #{e.message}" }
        end

        def ambiguous_resend?(error)
          (defined?(PGMQ::Errors::ConnectionError) && error.is_a?(PGMQ::Errors::ConnectionError)) ||
            (defined?(PG::Error) && error.is_a?(PG::Error))
        end

        # The key a re-sent copy has to hold: only an enqueue-locked one. An
        # :until_executing payload with executions > 0 is a retry_on
        # re-enqueue, which went out with no row and whose copy releases none
        # (Executor#holds_uniqueness_row?): a row taken for it would never go.
        def resend_lock_key(payload)
          return unless payload.is_a?(Hash)

          key = Uniqueness.extract_key(payload)
          strategy = Uniqueness.extract_strategy(payload)
          return unless key && Uniqueness::ENQUEUE_LOCK_STRATEGIES.include?(strategy)
          return if strategy == :until_executing && payload["executions"].to_i.positive?

          key.to_s
        end

        # The copy is live: a failed bind is logged, not raised. The row stays
        # unbound and stamped before the send, so the copy's own release
        # still takes it.
        def bind_resent_lock(key, lock_queue, msg_id, stamp)
          UniquenessKey.bind!(key, queue_name: lock_queue, msg_id: msg_id.to_i, acquired_at: stamp)
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus::Web] Uniqueness bind of re-sent #{key} failed: #{e.message}" }
        end
      end
    end
  end
end
