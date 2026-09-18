# frozen_string_literal: true

module Pgbus
  module EventBus
    # Liveness for the idempotency claims in flight for one PGMQ message.
    #
    # A two-phase claim (issue #385) is a `pgbus_processed_events` row with
    # `completed_at IS NULL`. That state says "claimed, not finished" — it does
    # NOT say whether the claimer is dead or simply still running (issue #470).
    # Handler resolves the ambiguity by age: a claim whose `processed_at` has
    # gone quiet for longer than the ownership window is abandoned, anything
    # fresher is owned. For that age to mean "silence" rather than
    # "time since the claim was taken", something has to keep stamping it while
    # the handler runs. This is that something.
    #
    # One beat per message, created by Process::Consumer and handed to every
    # handler dispatched for it. A handler registers its claim for exactly the
    # duration of `handle` and the consumer's VisibilityHeartbeat `on_beat` hook
    # drives #touch! on the same cadence that re-arms the message's visibility
    # timeout — so the claim and the message go quiet together when the process
    # dies, and both stay fresh while it lives.
    #
    # #touch! runs on the heartbeat ticker thread while #register / #release run
    # on a pool thread, hence the mutex.
    class ClaimBeat
      def initialize
        @mutex = Mutex.new
        @claims = []
      end

      def register(event_id, handler_class)
        claim = [event_id, handler_class]
        @mutex.synchronize { @claims << claim unless @claims.include?(claim) }
        self
      end

      def release(event_id, handler_class)
        @mutex.synchronize { @claims.delete([event_id, handler_class]) }
        self
      end

      def size
        @mutex.synchronize { @claims.size }
      end

      def empty?
        size.zero?
      end

      # Refresh every registered claim's liveness stamp. Returns the number of
      # claims touched. No-op on a legacy schema: without `completed_at` there
      # are no pending claims to keep alive, and Handler's single-phase
      # fallback never consults the age.
      #
      # A claim that fails to update is logged and skipped rather than raised:
      # this runs inside the visibility heartbeat's beat, and one unwritable
      # row must not cost every other in-flight message its VT extension.
      def touch!
        return 0 unless ProcessedEvent.completion_column?

        now = Time.now.utc
        @mutex.synchronize { @claims.dup }.count { |event_id, handler_class| touch(event_id, handler_class, now) }
      end

      private

      def touch(event_id, handler_class, now)
        ProcessedEvent
          .where(event_id: event_id, handler_class: handler_class, completed_at: nil)
          .update_all(processed_at: now)
        true
      rescue StandardError => e
        Pgbus.logger.warn do
          "[Pgbus] Could not refresh idempotency claim #{handler_class}/#{event_id}: #{e.class}: #{e.message}"
        end
        false
      end
    end
  end
end
