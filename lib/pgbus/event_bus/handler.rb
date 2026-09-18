# frozen_string_literal: true

module Pgbus
  module EventBus
    class Handler
      class << self
        def idempotent!
          @idempotent = true
        end

        def idempotent?
          @idempotent == true
        end

        def dedup_cache
          @dedup_cache ||= DedupCache.new
        end
      end

      # Outcome of the two-phase claim. `age` is how long the losing delivery
      # found the existing claim to have been silent, in seconds (nil unless
      # the claim was pending).
      ClaimResult = Data.define(:status, :age) do
        def granted?
          status == :claimed
        end
      end

      # @param claim_beat [ClaimBeat, nil] the message's claim-liveness beat,
      #   supplied by Process::Consumer. Absent for a hand-rolled caller: the
      #   handler still runs, its claim simply ages from the claim instant.
      def process(message, claim_beat: nil)
        with_rails_executor { process!(message, claim_beat) }
      end

      def handle(event)
        raise NotImplementedError, "#{self.class.name} must implement #handle(event)"
      end

      private

      def process!(message, claim_beat = nil)
        raw = JSON.parse(message.message)
        event = build_event(raw)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        if self.class.idempotent?
          claim = claim_idempotency(event.event_id)
          unless claim.granted?
            instrument_skip(claim, event, message, routing_key)
            return :skipped
          end
        end

        instrument_payload = {
          event_id: event.event_id,
          handler: self.class.name,
          routing_key: routing_key,
          published_at: event.published_at,
          read_ct: message.read_ct.to_i,
          msg_id: message.msg_id.to_i
        }
        with_claim_beat(claim_beat, event.event_id) do
          Instrumentation.instrument("pgbus.event_processed", instrument_payload) do
            # Publisher's Current attributes (issue #431) are set for the handler
            # and reverted after (CurrentAttributes#set semantics); the Rails
            # executor wrap above additionally resets at completion.
            Pgbus::CurrentAttributes.restore(event.context) { handle(event) }
          end
        end
        complete_claim!(event.event_id) if self.class.idempotent?
        :handled
      rescue StandardError => e
        instrument(
          "pgbus.event_failed",
          event_id: event&.event_id,
          handler: self.class.name,
          routing_key: routing_key,
          error: e.class.name,
          exception_object: e
        )
        raise
      end

      # Mirrors Pgbus::ActiveJob::Executor#execute_job: wrap the handler
      # invocation in Rails.application.executor (or the reloader in dev)
      # so AR connections leased by `claim_idempotency` and `handle` are
      # released back to the pool when this method returns. Without the
      # wrap, every consumed event leaks one AR connection on the consumer
      # thread — in dev that wedges `clear_reloadable_connections!`,
      # producing a confusing Rack::Timeout in `MonitorMixin#wait_for_cond`.
      #
      # No-op when Rails isn't loaded (test harnesses, gem-only consumers).
      def with_rails_executor(&)
        return yield unless defined?(Rails) && Rails.respond_to?(:application) && Rails.application

        wrapper = reloading? ? Rails.application.reloader : Rails.application.executor
        wrapper.wrap(&)
      end

      def reloading?
        app_config = Rails.application.config
        if app_config.respond_to?(:enable_reloading)
          app_config.enable_reloading
        else
          !app_config.cache_classes
        end
      end

      def build_event(raw)
        payload = raw["payload"]
        payload = Serializer.locate_global_id(payload["_global_id"]) if payload.is_a?(Hash) && payload["_global_id"]

        # Same allowlist boundary as Serializer.deserialize_job_data: every
        # _aj_globalid in the persisted context is checked BEFORE anything is
        # located, so a crafted envelope cannot load an arbitrary model.
        context = raw[Pgbus::CurrentAttributes::METADATA_KEY]
        Serializer.assert_job_global_ids_allowed!(context) if context

        Event.new(
          event_id: raw["event_id"],
          payload: payload,
          published_at: raw["published_at"] ? Time.parse(raw["published_at"]) : nil,
          context: context
        )
      end

      def instrument(event_name, payload = {})
        return unless defined?(ActiveSupport::Notifications)

        ActiveSupport::Notifications.instrument(event_name, payload)
      end

      # Two-phase idempotency claim (issue #385). Phase 1: atomically claim
      # via INSERT ... ON CONFLICT DO NOTHING with completed_at NULL — a
      # *pending* claim. Returns a ClaimResult whose status is one of:
      #
      #   :claimed   — insert won (fresh claim), or the row was purged between
      #                the losing insert and the read, or an existing pending
      #                claim has gone silent for longer than the ownership
      #                window: the holder is dead by the heartbeat's own
      #                definition, so re-run rather than silently drop the
      #                execution a SIGKILL interrupted.
      #   :completed — the execution already finished. Skip.
      #   :owned     — pending, and its liveness stamp is fresh: the holder is
      #                still running (issue #470). Skip — running `handle`
      #                concurrently with the holder is exactly the
      #                double-execution `idempotent!` promises not to do. The
      #                holder either completes (nothing lost) or fails, leaving
      #                its own message for VT redelivery to recover.
      #   :cached    — a completed execution already in this process's memory.
      #
      # Phase 2 is complete_claim! after handle returns; only completed
      # executions enter the in-memory dedup cache.
      #
      # Legacy fallback: without the completed_at column (upgraded gem,
      # not-yet-migrated table) this degrades to the old single-phase claim,
      # which has no pending state and therefore no ownership question.
      def claim_idempotency(event_id)
        cache_key = dedup_key(event_id)
        return ClaimResult.new(status: :cached, age: nil) if self.class.dedup_cache.seen?(cache_key)

        result = ProcessedEvent.insert(
          { event_id: event_id, handler_class: self.class.name, processed_at: Time.now.utc },
          unique_by: %i[event_id handler_class]
        )

        unless ProcessedEvent.completion_column?
          self.class.dedup_cache.mark!(cache_key)
          return ClaimResult.new(status: result.rows.any? ? :claimed : :completed, age: nil)
        end

        return ClaimResult.new(status: :claimed, age: nil) if result.rows.any?

        inspect_existing_claim(event_id, cache_key)
      end

      # The insert lost, so a row exists (or existed). `pick` returns nil for
      # the whole row when it has since been purged — not a pending claim,
      # nothing is running, so claim it.
      def inspect_existing_claim(event_id, cache_key)
        completed_at, processed_at = ProcessedEvent
                                     .where(event_id: event_id, handler_class: self.class.name)
                                     .pick(:completed_at, :processed_at)

        if completed_at
          self.class.dedup_cache.mark!(cache_key)
          return ClaimResult.new(status: :completed, age: nil)
        end

        return ClaimResult.new(status: :claimed, age: nil) if processed_at.nil?

        age = Time.now.utc - processed_at.to_time.utc
        return ClaimResult.new(status: :owned, age: age) if age < claim_ownership_window

        ClaimResult.new(status: :claimed, age: age)
      end

      # How long a pending claim may stay silent before its holder counts as
      # dead. ClaimBeat refreshes a live claim from the visibility heartbeat,
      # which lands every extension inside [interval, 1.5 * interval] of the
      # previous one — two intervals leaves margin for a late beat without
      # stretching the window past the visibility timeout it rides on.
      #
      # With the heartbeat disabled a claim is never refreshed, so the window
      # degrades to "roughly the first two thirds of one visibility timeout
      # after the claim" — a redelivery, which cannot arrive before the VT has
      # lapsed, still re-runs exactly as it did before issue #470.
      def claim_ownership_window
        Pgbus.configuration.effective_visibility_heartbeat_interval * 2
      end

      # Register this claim with the message's beat for exactly the duration of
      # handle: before it, there is nothing to keep alive; after it,
      # complete_claim! owns the row and a beat touching processed_at would
      # race the completion stamp.
      def with_claim_beat(claim_beat, event_id)
        return yield unless claim_beat && self.class.idempotent? && ProcessedEvent.completion_column?

        claim_beat.register(event_id, self.class.name)
        begin
          yield
        ensure
          claim_beat.release(event_id, self.class.name)
        end
      end

      # A skip used to be silent, which made an over-eager re-run (issue #470)
      # invisible in production: nothing distinguished "deduplicated" from
      # "deferred to a live holder". The claim age and read_ct are what tell
      # an operator which one happened.
      def instrument_skip(claim, event, message, routing_key)
        Instrumentation.instrument(
          "pgbus.event_skipped",
          event_id: event.event_id,
          handler: self.class.name,
          routing_key: routing_key,
          reason: claim.status,
          claim_age: claim.age,
          read_ct: message.read_ct.to_i,
          msg_id: message.msg_id.to_i
        )
      end

      # Phase 2: stamp the claim completed and only then admit it to the
      # dedup cache. Skipped on legacy schemas (single-phase claims are
      # already cached at claim time). If this write fails, process!'s rescue
      # re-raises, the consumer leaves the message for VT redelivery, and the
      # still-pending claim re-runs — at-least-once, never a silent drop.
      #
      # Wrapped in StaleConnectionRetry because this is the one AR write that
      # happens AFTER handle() has already succeeded: a socket dropped here
      # costs the host app a paging exception for work that was in fact done.
      # The stamp is an idempotent `SET completed_at = <now>`, so repeating a
      # statement that may already have committed is safe.
      #
      # Phase 1 (claim_idempotency) is deliberately NOT wrapped. Its INSERT
      # may have committed before the socket died, and on a legacy schema the
      # retry's empty `result.rows` would read as "someone else owns this
      # claim" and report :completed — turning a recoverable drop into a
      # silently skipped event. VT redelivery is the correct recovery there.
      def complete_claim!(event_id)
        return unless ProcessedEvent.completion_column?

        StaleConnectionRetry.call(context: self.class.name) do
          ProcessedEvent
            .where(event_id: event_id, handler_class: self.class.name)
            .update_all(completed_at: Time.now.utc)
        end
        self.class.dedup_cache.mark!(dedup_key(event_id))
      end

      def dedup_key(event_id)
        "#{event_id}:#{self.class.name}"
      end
    end
  end
end
