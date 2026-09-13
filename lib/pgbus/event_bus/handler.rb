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

      def process(message)
        with_rails_executor { process!(message) }
      end

      def handle(event)
        raise NotImplementedError, "#{self.class.name} must implement #handle(event)"
      end

      private

      def process!(message)
        raw = JSON.parse(message.message)
        event = build_event(raw)
        routing_key = raw.dig("headers", "routing_key") || raw["routing_key"]

        return :skipped if self.class.idempotent? && !claim_idempotency?(event.event_id)

        instrument_payload = {
          event_id: event.event_id,
          handler: self.class.name,
          routing_key: routing_key,
          published_at: event.published_at,
          read_ct: message.read_ct.to_i,
          msg_id: message.msg_id.to_i
        }
        Instrumentation.instrument("pgbus.event_processed", instrument_payload) do
          # Publisher's Current attributes (issue #431) are set for the handler
          # and reverted after (CurrentAttributes#set semantics); the Rails
          # executor wrap above additionally resets at completion.
          Pgbus::CurrentAttributes.restore(event.context) { handle(event) }
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
      # so AR connections leased by `claim_idempotency?` and `handle` are
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
      # *pending* claim. Returns true when this delivery should run handle:
      #
      #   - insert won → fresh claim
      #   - insert lost, completed_at NULL → a prior attempt claimed but was
      #     killed before finishing (SIGKILL mid-handler); re-run so the crash
      #     doesn't silently drop the execution. Safe: PGMQ's VT means the
      #     prior holder is dead or wedged past its timeout — the same
      #     at-least-once window every non-idempotent handler has.
      #
      # Returns false (skip) only for a *completed* execution. Phase 2 is
      # complete_claim! after handle returns; only completed executions enter
      # the in-memory dedup cache.
      #
      # Legacy fallback: without the completed_at column (upgraded gem,
      # not-yet-migrated table) this degrades to the old single-phase claim.
      def claim_idempotency?(event_id)
        cache_key = dedup_key(event_id)
        return false if self.class.dedup_cache.seen?(cache_key)

        result = ProcessedEvent.insert(
          { event_id: event_id, handler_class: self.class.name, processed_at: Time.now.utc },
          unique_by: %i[event_id handler_class]
        )

        unless ProcessedEvent.completion_column?
          self.class.dedup_cache.mark!(cache_key)
          return result.rows.any?
        end

        return true if result.rows.any?

        completed_at = ProcessedEvent
                       .where(event_id: event_id, handler_class: self.class.name)
                       .pick(:completed_at)
        return true if completed_at.nil? # pending claim (or purged row) → re-run

        self.class.dedup_cache.mark!(cache_key)
        false
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
      # Phase 1 (claim_idempotency?) is deliberately NOT wrapped. Its INSERT
      # may have committed before the socket died, and on a legacy schema the
      # retry's empty `result.rows` would read as "someone else owns this
      # claim" and return false — turning a recoverable drop into a silently
      # skipped event. VT redelivery is the correct recovery there.
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
