# frozen_string_literal: true

module Pgbus
  # Records job failures to pgbus_failed_events for dashboard visibility.
  # Uses upsert (INSERT ON CONFLICT UPDATE) keyed on (queue_name, msg_id)
  # so each message has at most one failed_event row tracking its latest error.
  class FailedEventRecorder
    class << self
      def record!(queue_name:, msg_id:, payload:, headers:, error:, retry_count:)
        connection.exec_query(
          <<~SQL.squish,
            INSERT INTO pgbus_failed_events
              (queue_name, msg_id, payload, headers, error_class, error_message, backtrace, retry_count, failed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
            ON CONFLICT (queue_name, msg_id) DO UPDATE SET
              error_class = EXCLUDED.error_class,
              error_message = EXCLUDED.error_message,
              backtrace = EXCLUDED.backtrace,
              retry_count = EXCLUDED.retry_count,
              failed_at = EXCLUDED.failed_at
          SQL
          "FailedEvent Record",
          [
            queue_name,
            msg_id.to_i,
            payload.is_a?(String) ? payload : JSON.generate(payload),
            headers.is_a?(String) ? headers : headers&.then { |h| JSON.generate(h) },
            error.class.name,
            error.message.to_s.truncate(10_000),
            error.backtrace&.first(30)&.join("\n"),
            retry_count
          ]
        )
      rescue StandardError => e
        ErrorReporter.report(e, { action: "record_failed_event", queue: queue_name, msg_id: msg_id })
      end

      def exists?(queue_name:, msg_id:)
        result = connection.select_value(
          "SELECT 1 FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2 LIMIT 1",
          "FailedEvent Exists",
          [queue_name, msg_id.to_i]
        )
        !result.nil?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] FailedEvent exists? check failed: #{e.class}: #{e.message}" }
        false
      end

      # Drops every row recorded under any of `queue_names`. Called when a
      # queue is dropped: PGMQ restarts msg_ids on recreate, so a surviving
      # row would aim the dashboard's retry/discard at an unrelated message.
      # Queue names are validated word characters, so the array literal is safe.
      def clear_queue!(queue_names)
        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE queue_name = ANY($1::text[])",
          "FailedEvent Clear Queue",
          ["{#{queue_names.join(",")}}"]
        )
      rescue StandardError => e
        Pgbus.logger.error do
          "[Pgbus] Failed to clear failed events for dropped queue(s) #{queue_names.join(", ")}: " \
            "#{e.class}: #{e.message}"
        end
      end

      def clear!(queue_name:, msg_id:)
        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE queue_name = $1 AND msg_id = $2",
          "FailedEvent Clear",
          [queue_name, msg_id.to_i]
        )
      rescue StandardError => e
        # ERROR-level: a failed clear leaves a stale row in the dashboard
        # AFTER the job actually succeeded — confusing and load-bearing
        # for users debugging recurring duplicates.
        Pgbus.logger.error do
          "[Pgbus] Failed to clear failed event for queue=#{queue_name} msg_id=#{msg_id}: " \
            "#{e.class}: #{e.message}"
        end
      end

      private

      def connection
        if defined?(BusRecord) && BusRecord.connected?
          BusRecord.connection
        else
          ActiveRecord::Base.connection
        end
      end
    end
  end
end
