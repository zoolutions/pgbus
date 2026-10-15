# frozen_string_literal: true

module Pgbus
  class ProcessedEvent < BusRecord
    self.table_name = "pgbus_processed_events"

    scope :expired, ->(before) { where("processed_at < ?", before) }

    @completion_column_mutex = Mutex.new

    class << self
      # Whether pgbus_processed_events has the completed_at column that backs
      # the two-phase idempotency claim (issue #385). Detected once per process
      # (memoized under a mutex) so the schema probe never lands on the
      # per-event hot path. An upgraded gem running against a not-yet-migrated
      # table gets `false` plus a one-time warning pointing at the upgrade
      # generator — Handler then falls back to the legacy single-phase claim.
      #
      # A detection error (e.g. the database is briefly unreachable) is NOT
      # memoized: it propagates to the caller — where the event's normal
      # failure path leaves the message for VT redelivery — and the next
      # delivery probes again.
      def completion_column?
        # Double-checked: the unlocked read only skips the lock once the
        # answer is set; it is written under @completion_column_mutex.
        detected = @completion_column # rubocop:disable ThreadSafety/ClassInstanceVariable
        return detected unless detected.nil?

        @completion_column_mutex.synchronize do
          @completion_column = detect_completion_column if @completion_column.nil?
          @completion_column
        end
      end

      # Test seam: clear the memoized detection so specs can exercise both
      # schema shapes in one process.
      def reset_completion_column_check!
        @completion_column_mutex.synchronize { @completion_column = nil }
      end

      private

      def detect_completion_column
        supported = column_names.include?("completed_at")
        unless supported
          Pgbus.logger.warn do
            "[Pgbus] pgbus_processed_events is missing the completed_at column; idempotent handlers " \
              "fall back to single-phase claims (a crash mid-handler can skip work on redelivery). " \
              "Run `rails generate pgbus:add_processed_event_completion` and migrate."
          end
        end
        supported
      end
    end
  end
end
