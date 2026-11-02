# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The limits_concurrency half of a re-send (issue #636; Resend#resend_job
      # is the whole). A re-sent copy signals its key's semaphore when it
      # finishes, so it has to hold a slot first, taken the way
      # Adapter#enqueue_with_concurrency takes one: the check and the park
      # commit together under the semaphore row lock (rails/solid_queue#712),
      # and the send follows the commit.
      module ResendSlot
        private

        # Returns the block's msg_id once the copy is sent holding a slot,
        # :parked when it waits behind a full limit (on_conflict: :block), and
        # :refused when the limit is full under :discard or :raise. A payload
        # with no concurrency key is sent as it is. +sending+ is :produce
        # around the produce and :park around the park's commit, so the
        # caller can tell an error that may hide a live copy from one that
        # proves nothing went out.
        def resend_holding_slot(payload, lock_queue, drop_source, sending, headers, &)
          key = payload.is_a?(Hash) && Concurrency.extract_key(payload)
          unless key
            sending[0] = :produce
            return yield
          end

          limits = resend_limits(payload)
          case (taken = take_resend_slot(key, limits, lock_queue, park_payload(payload, headers), sending))
          when :acquired then send_holding_resend_slot(key, sending, &)
          when :full
            Pgbus.logger.warn do
              "[Pgbus::Web] Not re-sending #{payload["job_class"]}: concurrency limit for #{key} is full " \
                "(on_conflict: :#{limits[:on_conflict]})"
            end
            :refused
          else drop_parked_source(taken, key, drop_source)
          end
        end

        # The limits live on the job class, not in the payload. A class that
        # no longer declares them, or no longer resolves, is judged the way a
        # promotion judges its parked job (Concurrency.config_for): against
        # the limit the semaphore row records, parked when that is full.
        def resend_limits(payload)
          klass = Object.const_get(payload["job_class"].to_s)
          config = klass.respond_to?(:pgbus_concurrency) && klass.pgbus_concurrency
          config || { limit: nil, duration: Concurrency::DEFAULT_DURATION, on_conflict: :block }
        rescue NameError
          { limit: nil, duration: Concurrency::DEFAULT_DURATION, on_conflict: :block }
        end

        # :acquired, :full, or the id of the BlockedExecution the copy was
        # parked as. The copy is sent right away, so its lease is the
        # duration alone (Adapter#slot_lease with no delay). Parked on the
        # logical queue at the job's own priority (the serialized "priority",
        # else the default, as Adapter#handle_conflict parks it): promote_next
        # sends it through Client#send_message, which prefixes the name and
        # picks the _pN sub-queue by that priority. Once the park is inserted
        # only its commit remains, and a COMMIT whose reply is lost may have
        # landed: +sending+ says so until the transaction returns.
        def take_resend_slot(key, limits, lock_queue, payload, sending)
          taken = Pgbus::Semaphore.transaction(requires_new: true) do
            lease = Concurrency.effective_duration(limits[:duration])
            next :acquired if Concurrency::Semaphore.acquire(key, limits[:limit], lease) == :acquired
            next :full unless limits[:on_conflict] == :block

            Concurrency::BlockedExecution.insert(
              concurrency_key: key, queue_name: lock_queue, payload: payload,
              priority: payload["priority"] || Pgbus.configuration.default_priority, duration: limits[:duration]
            ).rows.dig(0, 0).tap { sending[0] = :park }
          end
          sending[0] = nil
          taken
        end

        # pgbus_blocked_executions has no headers column: the headers the
        # copy would have been sent with (a DLQ retry's retries count) ride
        # in the parked payload, and promote_next sends them as headers again
        # (Concurrency::BlockedExecution::HEADERS_KEY).
        def park_payload(payload, headers)
          return payload if headers.nil? || headers.empty?

          payload.merge(Concurrency::BlockedExecution::HEADERS_KEY => headers)
        end

        # Hand the slot back only when the failure proves nothing was
        # produced (Adapter#send_holding_slot): on an ambiguous outcome the
        # copy may be live, so the hold stands and the lease expiry plus the
        # dispatcher's sweep reclaim it.
        def send_holding_resend_slot(key, sending)
          sending[0] = :produce
          yield
        rescue StandardError => e
          if ambiguous_resend?(e)
            Pgbus.logger.warn { "[Pgbus::Web] Re-send of #{key} may have been sent (#{e.class}); its concurrency slot stays held" }
          else
            begin
              Concurrency::Semaphore.release(key)
            rescue StandardError => release_error
              Pgbus.logger.warn { "[Pgbus::Web] Giving back the concurrency slot of #{key} failed: #{release_error.message}" }
            end
          end
          raise
        end

        # The park has committed; the source goes next (it lives on another
        # connection, or after the commit, so the two cannot share a
        # transaction). If that fails, the park is withdrawn while it still
        # waits, and the error raised: nothing changed, the source stays. A
        # removal whose outcome is unknown (a lost reply: its DELETE may have
        # committed) keeps the park, since withdrawing it could leave neither
        # copy; so does a park that cannot be withdrawn (already promoted: a
        # live copy). Either way the source may stay, for the operator to
        # discard: a duplicate is preferred over a lost job.
        def drop_parked_source(park_id, key, drop_source)
          drop_source.call
          :parked
        rescue StandardError => e
          raise if !ambiguous_resend?(e) && withdraw_park(park_id)

          Pgbus.logger.warn do
            "[Pgbus::Web] Re-sent copy of #{key} is parked, but removing its source failed (#{e.class}: #{e.message}); " \
              "the parked copy stands, and the source may stay to be discarded"
          end
          :parked
        end

        def withdraw_park(park_id)
          Pgbus::BlockedExecution.where(id: park_id).delete_all.positive?
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus::Web] Withdrawing the parked re-send #{park_id} failed: #{e.message}" }
          false
        end
      end
    end
  end
end
