# frozen_string_literal: true

module Pgbus
  class Client
    # Non-consuming peek across PGMQ live (`q_`) and archive (`a_`) tables. Used
    # exclusively by `Pgbus::Web::Streamer` for SSE replay — workers continue to
    # use `read_batch` (claim semantics). The two read paths are disjoint.
    #
    # The cursor is the highest msg_id the client has already seen. Replay returns
    # everything strictly greater, ordered by msg_id ASC, capped by `limit`.
    module ReadAfter
      # Ractor-shareable like the streamer's other dispatch values (issue
      # #527): the Strings are frozen in place, they are the read's own.
      Envelope = Data.define(:msg_id, :enqueued_at, :payload, :source) do
        def initialize(msg_id:, enqueued_at:, payload:, source:)
          enqueued_at&.freeze
          payload&.freeze
          source&.freeze
          super
        end
      end

      DEFAULT_LIMIT = 500

      def read_after(stream_name, after_id:, limit: DEFAULT_LIMIT)
        sanitized = sanitized_queue(stream_name)
        sql = build_read_after_sql(sanitized)

        rows = synchronized do
          with_streams_connection do |conn|
            conn.exec_params(sql, [after_id.to_i, limit.to_i]).to_a
          end
        end

        rows.map { |row| build_envelope(row) }
      rescue StandardError => e
        raise unless missing_stream_queue?(e, sanitized)

        []
      end

      def stream_current_msg_id(stream_name)
        sanitized = sanitized_queue(stream_name)
        sql = "SELECT COALESCE(MAX(msg_id), 0) AS max FROM pgmq.q_#{sanitized}"
        synchronized do
          with_streams_connection do |conn|
            conn.exec(sql).first.fetch("max").to_i
          end
        end
      rescue StandardError => e
        raise unless missing_stream_queue?(e, sanitized)

        0
      end

      def stream_oldest_msg_id(stream_name)
        sanitized = sanitized_queue(stream_name)
        sql = <<~SQL
          SELECT LEAST(
            (SELECT MIN(msg_id) FROM pgmq.q_#{sanitized}),
            (SELECT MIN(msg_id) FROM pgmq.a_#{sanitized})
          ) AS least
        SQL
        synchronized do
          with_streams_connection do |conn|
            value = conn.exec(sql).first.fetch("least")
            value&.to_i
          end
        end
      rescue StandardError => e
        raise unless missing_stream_queue?(e, sanitized)

        nil
      end

      private

      # True if +error+ is a PG::UndefinedTable (or an
      # ActiveRecord::StatementInvalid wrapping one) complaining about
      # the stream's own PGMQ queue table (pgmq.q_<sanitized> or
      # pgmq.a_<sanitized>).
      #
      # The stream-watermark and replay SQL above run on every page render
      # for streams like `pgbus_stream_from Current.user`, but the queue
      # table is only created on the FIRST broadcast via
      # `ensure_stream_queue`. On a fresh database the very first page
      # render therefore reads from a table that doesn't exist yet —
      # semantically, "no queue" means "no messages" and must translate
      # to a 0 watermark / empty replay rather than an exception. Any
      # OTHER UndefinedTable (wrong schema, typo, operator error) still
      # propagates so real bugs don't get swallowed.
      #
      # See issues #101 and #104. The comparison is case-insensitive because
      # Postgres downcases unquoted identifiers in its error output, while
      # `sanitized` can contain uppercase characters for GlobalID-keyed streams
      # (e.g. `pgbus_Z2lkOi8vY29zbW9zL1VzZXIvMQ` from
      # `pgbus_stream_from Current.user` — stream queues share the job prefix,
      # see Configuration#queue_name). A case-sensitive substring match would
      # miss the downcased relation name and let the exception escape.
      #
      # The regex uses `\b` word boundaries so `pgmq.q_<needle>` doesn't
      # accidentally match longer related identifiers like
      # `pgmq.q_<needle>_archive` — a PGMQ internal object we'd want to
      # propagate as a real error rather than silently swallow.
      def missing_stream_queue?(error, sanitized)
        pg_error = pg_undefined_table?(error) ? error : error.cause
        return false unless pg_undefined_table?(pg_error)

        message = pg_error.message.to_s.downcase
        needle = Regexp.escape(sanitized.downcase)
        message.match?(/\bpgmq\.(?:q|a)_#{needle}\b/)
      end

      def pg_undefined_table?(error)
        defined?(::PG::UndefinedTable) && error.is_a?(::PG::UndefinedTable)
      end

      # Builds the union of live and archive tables. The outer ORDER BY + LIMIT
      # ensures we never return more than `limit` rows total even if both
      # subqueries hit it. The 'live'/'archive' constants are how the streamer
      # tells whether a row was peeked from the queue or replayed from history;
      # the streamer doesn't currently distinguish them, but we keep the column
      # so debugging is straightforward when archive replay misbehaves.
      def build_read_after_sql(sanitized)
        <<~SQL
          (
            SELECT msg_id, enqueued_at, message, 'live'::text AS source
            FROM pgmq.q_#{sanitized}
            WHERE msg_id > $1
            ORDER BY msg_id ASC
            LIMIT $2
          )
          UNION ALL
          (
            SELECT msg_id, enqueued_at, message, 'archive'::text AS source
            FROM pgmq.a_#{sanitized}
            WHERE msg_id > $1
            ORDER BY msg_id ASC
            LIMIT $2
          )
          ORDER BY msg_id ASC
          LIMIT $2
        SQL
      end

      def sanitized_queue(stream_name)
        full = config.queue_name(stream_name)
        QueueNameValidator.sanitize!(full)
      end

      def build_envelope(row)
        Envelope.new(
          msg_id: row.fetch("msg_id").to_i,
          enqueued_at: row.fetch("enqueued_at"),
          payload: row.fetch("message"),
          source: row.fetch("source")
        )
      end
    end
  end
end
