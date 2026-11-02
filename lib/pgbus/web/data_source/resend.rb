# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # Re-sending a job payload as a new message — DLQ Retry, and failed-job
      # Retry when the original message is gone (issues #631, #636). The
      # payload names its uniqueness key and strategy, and its
      # limits_concurrency key. The copy releases the uniqueness row it holds
      # when it starts (:until_executing) or finishes (:until_executed), and
      # signals the semaphore when it finishes. Re-sent without a row or a
      # slot of its own, that release could free a newer parked copy's key,
      # and that signal gave back a slot another copy held and promoted a
      # parked job beside it. So the copy goes out the way Adapter#enqueue
      # sends a job: the uniqueness key taken unbound before the send and
      # bound to the new message after it; a slot taken before the send, or
      # the copy parked behind a full limit (on_conflict: :block), or the
      # retry refused (:discard, :raise) with the source left in place.
      module Resend
        private

        # Yields to send the payload; the block returns the new msg_id.
        # Returns :sent, :parked (a BlockedExecution the limit's next signal
        # promotes; nothing is sent and the block does not run), or false
        # when another copy holds the uniqueness key or the limit is full
        # under :discard / :raise (nothing is sent, so the source — the DLQ
        # message or the failed row — stays for a later retry).
        # +lock_queue+ is the queue as the executor spells the lock
        # (Uniqueness.lock_queue_name), which is also the logical queue a
        # parked copy is promoted onto; +holder+ is the [lock queue, msg_id,
        # enqueued_at] of the message the payload was read from, as the
        # dashboard's discards name it (#lock_holder). +drop_source+ removes
        # the source once the copy is parked (a sent copy's source is the
        # block's to remove).
        def resend_job(payload, lock_queue:, holder:, drop_source:, &)
          key = resend_lock_key(payload)
          stamp = key && take_resend_lock(key, lock_queue, holder)
          if key && !stamp
            Pgbus.logger.warn { "[Pgbus::Web] Not re-sending #{payload["job_class"]}: uniqueness key #{key} is held by another copy" }
            return false
          end

          # Only an error raised by the produce itself can hide a sent copy:
          # one from the slot upsert, the park or the source's removal proves
          # nothing went out, however database-shaped it is.
          sending = [false]
          begin
            outcome = resend_holding_slot(payload, lock_queue, drop_source, sending, &)
          rescue StandardError => e
            give_back_resend_lock(key, stamp, e, sending: sending.first) if key
            raise
          end

          case outcome
          when :refused
            give_back_resend_lock(key, stamp) if key
            false
          when :parked
            # Unbound, as a parked enqueue's row is: the promoted copy's
            # release takes it (UniquenessKey.release_held_by!).
            :parked
          else
            bind_resent_lock(key, lock_queue, outcome, stamp) if key
            :sent
          end
        ensure
          UniquenessKey.clear_bind_stamp!(key) if key
        end

        # A failed job's message back to work: its vt reset to 0 while it is
        # still queued (a second copy would run beside it; a failed attempt
        # keeps its concurrency slot, so it needs no other), else its payload
        # re-sent or parked. The failed row goes either way; false (row kept)
        # while the re-send's key is held or its limit refuses it.
        def retry_failed_job(event)
          physical = failed_message_queue(event)
          return resend_failed_event(event) unless physical

          @client.set_visibility_timeout(physical, event["msg_id"].to_i, vt: 0, prefixed: false)
          delete_failed_row(event)
          true
        end

        # A failed job's payload as a new message on its queue, its failed row
        # deleted once it is sent or parked. Its message is gone, so it can
        # hold only a row still bound to it (nil send time: no unbound row is
        # its own). The delete comes after the key is bound: PGMQ sends on its
        # own connection, so a failed delete cannot undo the send, and must
        # not give back the key of a live copy.
        def resend_failed_event(event) # rubocop:disable Naming/PredicateMethod
          payload = JSON.parse(event["payload"])
          headers = event["headers"]
          headers = JSON.parse(headers) if headers.is_a?(String)
          lock_queue = Uniqueness.lock_queue_name(event["queue_name"])

          outcome = resend_job(payload, lock_queue: lock_queue, holder: [lock_queue, event["msg_id"].to_i, nil],
                                        drop_source: -> { delete_failed_row(event) }) do
            @client.send_message(event["queue_name"], payload, headers: headers)
          end
          delete_failed_row(event) if outcome == :sent
          outcome != false
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

        # The copy did not go out (the limit refused it), or the send raised.
        # Proven unsent, the row taken for it goes (matched by its stamp, so
        # never a successor's). An error from the produce that may hide a
        # committed send (a lost connection or reply, as
        # Adapter#ambiguous_delivery? counts it) keeps the row: the copy may
        # be live, and the reaper takes the row if it is not. A failed
        # give-back is logged, so the send's error is the one that surfaces.
        def give_back_resend_lock(key, stamp, error = nil, sending: false)
          if sending && ambiguous_resend?(error)
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
