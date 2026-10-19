# frozen_string_literal: true

require "time"

module Pgbus
  # Records job failures to pgbus_failed_events for dashboard visibility.
  # Uses upsert (INSERT ON CONFLICT UPDATE) keyed on the message, so each
  # message has at most one failed_event row tracking its latest error.
  #
  # A message is (queue_name, msg_id, source_queue): queue_name is the logical
  # queue, source_queue the physical table the message was read from. Under
  # priority routing each `_pN` sub-queue numbers its messages from its own
  # sequence, so the logical name and msg_id alone can name two jobs (issue
  # #583). A row written before the source_queue column existed has NULL there
  # and is read as belonging to any table of its queue. Until the column is
  # migrated, every query keeps its (queue_name, msg_id) shape.
  class FailedEventRecorder
    # How long a "column missing" answer stands before the schema is probed
    # again, so a process started before the migration picks the column up.
    SOURCE_QUEUE_RECHECK_SECONDS = 60

    @source_queue_column_mutex = Mutex.new

    class << self
      def record!(queue_name:, msg_id:, source_queue:, payload:, headers:, error:, retry_count:)
        details = [
          payload.is_a?(String) ? payload : JSON.generate(payload),
          headers.is_a?(String) ? headers : headers&.then { |h| JSON.generate(h) },
          error.class.name,
          error.message.to_s.truncate(10_000),
          error.backtrace&.first(30)&.join("\n"),
          retry_count
        ]
        legacy = !source_queue_column?
        if legacy
          connection.exec_query(LEGACY_RECORD_SQL, "FailedEvent Record", [queue_name, msg_id.to_i, *details])
        else
          connection.exec_query(RECORD_SQL, "FailedEvent Record", [queue_name, source_queue, msg_id.to_i, *details])
        end
      rescue StandardError => e
        # A process started before the migration would keep sending the old
        # ON CONFLICT target, whose index the migration drops: probe again.
        reset_source_queue_column_check! if legacy
        ErrorReporter.report(e, { action: "record_failed_event", queue: queue_name, msg_id: msg_id })
      end

      def exists?(queue_name:, msg_id:, source_queue:)
        sql, binds = message_scope(queue_name, msg_id, source_queue)
        result = connection.select_value("SELECT 1 FROM pgbus_failed_events WHERE #{sql} LIMIT 1", "FailedEvent Exists", binds)
        !result.nil?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] FailedEvent exists? check failed: #{e.class}: #{e.message}" }
        false
      end

      # The message's latest recorded failure, read by a mover just before it
      # dead-letters the message and clears the row (issue #495). failed_at
      # comes back as an ISO 8601 UTC string whatever the adapter decodes it
      # to. A failed read returns nil: it must never stop a dead-letter.
      # The message's own row wins over one written before source_queue.
      def last_error(queue_name:, msg_id:, source_queue:)
        sql, binds = message_scope(queue_name, msg_id, source_queue)
        order = binds.size > 2 ? " ORDER BY source_queue IS NULL, failed_at DESC" : ""
        row = connection.select_one(
          "SELECT error_class, error_message, backtrace, retry_count, failed_at " \
          "FROM pgbus_failed_events WHERE #{sql}#{order} LIMIT 1",
          "FailedEvent Last Error",
          binds
        )
        return unless row

        {
          error_class: row["error_class"],
          error_message: row["error_message"],
          backtrace: row["backtrace"],
          retry_count: row["retry_count"].to_i,
          failed_at: iso_utc(row["failed_at"])
        }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] FailedEvent last_error read failed: #{e.class}: #{e.message}" }
        nil
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

      # Deletes the message's own row, and a row written before source_queue
      # existed: a sibling sub-queue's row with the same msg_id stays.
      def clear!(queue_name:, msg_id:, source_queue:)
        sql, binds = message_scope(queue_name, msg_id, source_queue)
        connection.exec_delete("DELETE FROM pgbus_failed_events WHERE #{sql}", "FailedEvent Clear", binds)
      rescue StandardError => e
        # ERROR-level: a failed clear leaves a stale row in the dashboard
        # AFTER the job actually succeeded — confusing and load-bearing
        # for users debugging recurring duplicates.
        Pgbus.logger.error do
          "[Pgbus] Failed to clear failed event for queue=#{queue_name} msg_id=#{msg_id}: " \
            "#{e.class}: #{e.message}"
        end
      end

      # Whether pgbus_failed_events has the source_queue column (issue #583).
      # Probed like ProcessedEvent.completion_column?: a gem upgraded before
      # its migration gets false, a one-time warning, and the (queue_name,
      # msg_id) queries. A present column is remembered for good; a missing
      # one for SOURCE_QUEUE_RECHECK_SECONDS. A probe that raises is not
      # memoized.
      def source_queue_column?
        # Double-checked: the unlocked read only skips the lock while the
        # answer stands; it is written under @source_queue_column_mutex. The
        # state is true, nil (never probed) or the monotonic time a missing
        # column's answer runs out.
        known = source_queue_column_answer(@source_queue_column) # rubocop:disable ThreadSafety/ClassInstanceVariable
        return known unless known.nil?

        @source_queue_column_mutex.synchronize do
          known = source_queue_column_answer(@source_queue_column)
          next known unless known.nil?

          supported = detect_source_queue_column(warn: @source_queue_column.nil?)
          @source_queue_column = supported || (monotonic_now + SOURCE_QUEUE_RECHECK_SECONDS)
          supported
        end
      end

      # Clears the memoized probe: specs exercise both schema shapes, and a
      # failed legacy write re-probes after the migration lands.
      def reset_source_queue_column_check!
        @source_queue_column_mutex.synchronize { @source_queue_column = nil }
      end

      private

      # WHERE fragment and binds naming one message's rows.
      def message_scope(queue_name, msg_id, source_queue)
        return ["queue_name = $1 AND msg_id = $2", [queue_name, msg_id.to_i]] unless source_queue_column?

        ["queue_name = $1 AND msg_id = $2 AND (source_queue = $3 OR source_queue IS NULL)",
         [queue_name, msg_id.to_i, source_queue]]
      end

      # true, false while a missing column's answer stands, nil to probe.
      def source_queue_column_answer(state)
        return true if state == true

        false if state && monotonic_now < state
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      def detect_source_queue_column(warn:)
        supported = connection.column_exists?(:pgbus_failed_events, :source_queue)
        if warn && !supported
          Pgbus.logger.warn do
            "[Pgbus] pgbus_failed_events is missing the source_queue column; under priority routing two " \
              "sub-queues' failures with the same msg_id share one row. " \
              "Run `rails generate pgbus:add_failed_events_source_queue` (or pgbus:update) and migrate."
          end
        end
        supported
      end

      def iso_utc(value)
        time = value.is_a?(String) ? Time.parse(value) : value
        time&.utc&.iso8601(6)
      rescue ArgumentError
        value.to_s
      end

      def connection
        if defined?(BusRecord) && BusRecord.connected?
          BusRecord.connection
        else
          ActiveRecord::Base.connection
        end
      end
    end

    UPSERT_ERROR_COLUMNS = <<~SQL.gsub(/\s+/, " ").strip.freeze
      DO UPDATE SET
        error_class = EXCLUDED.error_class,
        error_message = EXCLUDED.error_message,
        backtrace = EXCLUDED.backtrace,
        retry_count = EXCLUDED.retry_count,
        failed_at = EXCLUDED.failed_at
    SQL
    private_constant :UPSERT_ERROR_COLUMNS

    RECORD_SQL = <<~SQL.gsub(/\s+/, " ").strip.freeze
      INSERT INTO pgbus_failed_events
        (queue_name, source_queue, msg_id, payload, headers, error_class, error_message, backtrace, retry_count, failed_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CURRENT_TIMESTAMP)
      ON CONFLICT (queue_name, msg_id, source_queue) #{UPSERT_ERROR_COLUMNS}
    SQL
    private_constant :RECORD_SQL

    LEGACY_RECORD_SQL = <<~SQL.gsub(/\s+/, " ").strip.freeze
      INSERT INTO pgbus_failed_events
        (queue_name, msg_id, payload, headers, error_class, error_message, backtrace, retry_count, failed_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
      ON CONFLICT (queue_name, msg_id) #{UPSERT_ERROR_COLUMNS}
    SQL
    private_constant :LEGACY_RECORD_SQL
  end
end
