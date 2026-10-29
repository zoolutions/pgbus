# frozen_string_literal: true

require "digest"

module Pgbus
  class Client
    # Native prepared statements (issue #605): pgbus-owned static SQL for the
    # seven hot-path queue operations, run as named prepared statements on the
    # pooled connection instead of through pgmq's plpgsql functions.
    #
    # pgmq.read / archive / delete / send / send_batch build their statement
    # with FORMAT(...) and run it with EXECUTE. plpgsql never caches the plan
    # of a dynamic EXECUTE, so Postgres parses and plans the statement on every
    # call. The SQL below is the same statement those functions build
    # (lib/pgbus/pgmq_schema/pgmq_v1.13.1.sql), written once per queue table,
    # so Postgres can keep its plan for the life of the connection.
    #
    # Each queue is checked once per process: its q_ and a_ tables must have
    # every column the statements touch (REQUIRED_COLUMNS). A queue whose table
    # lacks one (a pgmq extension older than `headers`), or
    # `config.native_statements = false`, keeps calling the pgmq functions.
    #
    # Statements are prepared on demand: exec_prepared first; an
    # InvalidSqlStatementName means this session has not prepared it yet, so
    # prepare and run it again. The server is the registry: nothing records
    # which connection holds which statement. Errors are not wrapped here: the
    # call runs inside @pgmq.with_connection, which retries a lost connection
    # once and raises PGMQ::Errors::ConnectionError, exactly as on the function
    # path.
    #
    # Inside an open transaction (the shared-AR path, an enqueue inside the
    # app's transaction) a failed exec_prepared would abort the caller's
    # transaction, so there the same SQL runs through exec_params.
    #
    # A transaction-mode pooler without prepared-statement support (PgBouncer
    # before 1.21, or 1.21+ without max_prepared_statements) drops prepared
    # statements between transactions. When that is seen (see
    # NativeStatements.note_missing) the process switches to exec_params of the
    # same static SQL for good and logs it once.
    module NativeStatements
      REQUIRED_COLUMNS = %w[msg_id read_ct enqueued_at last_read_at vt message headers].freeze

      COLUMNS_SQL = "SELECT table_name, column_name FROM information_schema.columns " \
                    "WHERE table_schema = 'pgmq' AND table_name IN ($1, $2)"

      # Statements found missing on a backend that already had them, in a row,
      # before the process stops preparing.
      LOST_STATEMENT_LIMIT = 3

      Statement = Data.define(:name, :sql)

      # Process-wide, built at load: the pooler is a property of the process's
      # database route, not of one Client instance.
      @lost_in_a_row = Concurrent::AtomicFixnum.new(0)
      @unprepared = Concurrent::AtomicBoolean.new(false)
      # "backend_pid:statement name" for every statement this process has
      # prepared. A statement missing on a backend listed here was dropped
      # under us (a pooler moved the session); one missing on a new backend
      # (a fresh connection, a reconnect) is only a first prepare.
      @prepared_on = Concurrent::Map.new

      # rubocop:disable ThreadSafety/ClassInstanceVariable -- the three are
      # Concurrent primitives built once at load and never reassigned; every
      # read and write below goes through their atomic operations.
      class << self
        # True once the process has given up on prepared statements.
        def unprepared?
          @unprepared.true?
        end

        def note_prepared(backend_pid, name)
          @prepared_on["#{backend_pid}:#{name}"] = true
        end

        # A statement this session needed to prepare. Counts toward the switch
        # only when this backend had prepared it before.
        def note_missing(backend_pid, name)
          return unless @prepared_on.key?("#{backend_pid}:#{name}")

          unprepare!("#{LOST_STATEMENT_LIMIT} prepared statements in a row disappeared from their connection") if
            @lost_in_a_row.increment >= LOST_STATEMENT_LIMIT
        end

        def note_executed
          @lost_in_a_row.value = 0 unless @lost_in_a_row.value.zero?
        end

        def unprepare!(reason)
          return unless @unprepared.make_true

          Pgbus.logger.warn do
            "[Pgbus::Client] Native statements: #{reason}, which is what a transaction-mode pooler " \
              "without prepared-statement support does (PgBouncer < 1.21, or no max_prepared_statements). " \
              "This process now sends the same SQL unprepared. Set max_prepared_statements on the pooler, " \
              "or config.native_statements = false to use the pgmq functions."
          end
        end

        # Test seam: forget the switch and every recorded statement.
        def reset!
          @lost_in_a_row.value = 0
          @unprepared.make_false
          @prepared_on.clear
        end
        # rubocop:enable ThreadSafety/ClassInstanceVariable

        # The seven statements for one queue, keyed by op. Names are
        # pgbus_<op>_<12 hex of SHA-256(sql)>: 63 bytes is the identifier limit,
        # and hashing the SQL (which names the table) means two pgbus versions
        # behind one pooler never run each other's text under one name.
        def statements_for(queue_table, archive_table)
          sql_for("pgmq.#{quote_ident(queue_table)}", "pgmq.#{quote_ident(archive_table)}").to_h do |op, sql|
            [op, Statement.new(name: "pgbus_#{op}_#{Digest::SHA256.hexdigest(sql)[0, 12]}", sql: sql.freeze)]
          end.freeze
        end

        private

        def quote_ident(name)
          %("#{name.gsub('"', '""')}")
        end

        # pgmq.read, pgmq.archive (both overloads), pgmq.delete (both),
        # pgmq.send and pgmq._send_batch of pgmq 1.13.1, with the table names
        # filled in. The delay is computed once per statement, as the pgmq
        # wrappers do before calling the implementation.
        def sql_for(queue, archive)
          archived = "RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers"
          archive_insert = "INSERT INTO #{archive} (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers) " \
                           "SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived " \
                           "RETURNING msg_id"
          {
            read: "WITH cte AS (SELECT msg_id FROM #{queue} WHERE vt <= clock_timestamp() " \
                  "ORDER BY msg_id ASC LIMIT $1::integer FOR UPDATE SKIP LOCKED) " \
                  "UPDATE #{queue} m SET last_read_at = clock_timestamp(), " \
                  "vt = clock_timestamp() + make_interval(secs => $2::integer), read_ct = read_ct + 1 " \
                  "FROM cte WHERE m.msg_id = cte.msg_id " \
                  "RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers",
            archive: "WITH archived AS (DELETE FROM #{queue} WHERE msg_id = $1::bigint #{archived}) #{archive_insert}",
            archive_batch: "WITH archived AS (DELETE FROM #{queue} WHERE msg_id = ANY($1::bigint[]) #{archived}) " \
                           "#{archive_insert}",
            delete: "DELETE FROM #{queue} WHERE msg_id = $1::bigint RETURNING msg_id",
            delete_batch: "DELETE FROM #{queue} WHERE msg_id = ANY($1::bigint[]) RETURNING msg_id",
            send: "INSERT INTO #{queue} (vt, message, headers) " \
                  "VALUES (clock_timestamp() + make_interval(secs => $3::integer), $1::jsonb, $2::jsonb) " \
                  "RETURNING msg_id",
            send_batch: "INSERT INTO #{queue} (vt, message, headers) " \
                        "SELECT d.vt, unnest($1::jsonb[]), unnest(coalesce($2::jsonb[], ARRAY[]::jsonb[])) " \
                        "FROM (SELECT clock_timestamp() + make_interval(secs => $3::integer) AS vt) d " \
                        "RETURNING msg_id"
          }
        end
      end

      # Whether this queue runs the native statements. Checked on first use
      # (not at queue creation), so a queue created by another process is
      # covered the same way.
      def native_ready?(full_name)
        native_statements_for(full_name) ? true : false
      end

      private

      # Checks the queue's q_ and a_ columns and caches the outcome: the
      # statements when every column is there, false when one is missing.
      # A queue table that does not exist yet is not cached, so the queue is
      # checked again once something creates it. Runs on @pgmq.with_connection
      # without taking @pgmq_mutex: every caller already runs inside
      # `synchronized`.
      def detect_native_support!(full_name)
        return false unless config.native_statements

        queue_table = "q_#{full_name.downcase}"
        archive_table = "a_#{full_name.downcase}"
        columns = native_table_columns(queue_table, archive_table)
        return false unless columns.key?(queue_table)

        missing = native_missing_columns(columns, queue_table, archive_table)
        value = missing.empty? ? NativeStatements.statements_for(queue_table, archive_table) : false
        previous = @native_statements.put_if_absent(full_name, value)
        return previous unless previous.nil?

        log_native_unsupported(full_name, missing) unless value
        value
      end

      def native_statements_for(full_name)
        return unless config.native_statements

        cached = @native_statements[full_name]
        return cached unless cached.nil?

        detect_native_support!(full_name)
      end

      def native_table_columns(queue_table, archive_table)
        rows = @pgmq.with_connection { |conn| conn.exec_params(COLUMNS_SQL, [queue_table, archive_table]).to_a }
        rows.each_with_object(Hash.new { |h, k| h[k] = [] }) do |row, by_table|
          by_table[row["table_name"]] << row["column_name"]
        end
      end

      def native_missing_columns(columns, queue_table, archive_table)
        missing = (REQUIRED_COLUMNS - columns[queue_table]).map { |c| "#{queue_table}.#{c}" }
        missing + (REQUIRED_COLUMNS - columns[archive_table]).map { |c| "#{archive_table}.#{c}" }
      end

      def log_native_unsupported(full_name, missing)
        Pgbus.logger.info do
          "[Pgbus::Client] Queue #{full_name} uses the pgmq functions, not native statements: " \
            "pgmq.#{missing.join(", pgmq.")} missing (a pgmq install older than the headers column?)"
        end
      end

      # The seven operations, native when the queue is ready, the pgmq-ruby
      # call otherwise. Return values match pgmq-ruby's: Messages with String
      # fields, String ids, true/false for one archive/delete.

      def exec_produce(name, message, headers:, delay:)
        statements = delay.is_a?(Numeric) && native_statements_for(name)
        return @pgmq.produce(name, message, headers: headers, delay: delay) unless statements

        run_native(statements[:send], [message, headers, delay])[0]["msg_id"]
      end

      def exec_produce_batch(name, messages, headers:, delay:)
        statements = messages.any? && (headers.nil? || headers.length == messages.length) &&
                     delay.is_a?(Numeric) && native_statements_for(name)
        return @pgmq.produce_batch(name, messages, headers: headers, delay: delay) unless statements

        encoder = PG::TextEncoder::Array.new
        params = [encoder.encode(messages), headers && encoder.encode(headers), delay]
        run_native(statements[:send_batch], params).map { |row| row["msg_id"] }
      end

      def exec_read_batch(name, vt:, qty:)
        statements = native_statements_for(name)
        return @pgmq.read_batch(name, vt: vt, qty: qty) unless statements

        run_native(statements[:read], [qty, vt]).map { |row| PGMQ::Message.new(row) }
      end

      def exec_archive(name, msg_id)
        statements = native_statements_for(name)
        return @pgmq.archive(name, msg_id) unless statements

        run_native(statements[:archive], [msg_id]).ntuples.positive?
      end

      def exec_archive_batch(name, msg_ids)
        statements = msg_ids.any? && native_statements_for(name)
        return @pgmq.archive_batch(name, msg_ids) unless statements

        run_native(statements[:archive_batch], [PG::TextEncoder::Array.new.encode(msg_ids)]).map { |row| row["msg_id"] }
      end

      def exec_delete(name, msg_id)
        statements = native_statements_for(name)
        return @pgmq.delete(name, msg_id) unless statements

        run_native(statements[:delete], [msg_id]).ntuples.positive?
      end

      def exec_delete_batch(name, msg_ids)
        statements = msg_ids.any? && native_statements_for(name)
        return @pgmq.delete_batch(name, msg_ids) unless statements

        run_native(statements[:delete_batch], [PG::TextEncoder::Array.new.encode(msg_ids)]).map { |row| row["msg_id"] }
      end

      def run_native(statement, params)
        @pgmq.with_connection { |conn| exec_native(conn, statement, params) }
      end

      def exec_native(conn, statement, params)
        return conn.exec_params(statement.sql, params) if NativeStatements.unprepared? || inside_caller_transaction?(conn)

        begin
          result = conn.exec_prepared(statement.name, params)
        rescue PG::InvalidSqlStatementName
          return prepare_and_exec(conn, statement, params)
        end
        NativeStatements.note_executed
        result
      end

      def prepare_and_exec(conn, statement, params)
        pid = conn.backend_pid
        NativeStatements.note_missing(pid, statement.name)
        begin
          conn.prepare(statement.name, statement.sql)
        rescue PG::DuplicatePstatement
          # Prepared on this session between our two calls; run it.
        end
        NativeStatements.note_prepared(pid, statement.name)
        begin
          conn.exec_prepared(statement.name, params)
        rescue PG::InvalidSqlStatementName
          # Gone again right after its own prepare: the pooler ran the two calls
          # on different server connections.
          NativeStatements.unprepare!("a statement disappeared right after it was prepared")
          conn.exec_params(statement.sql, params)
        end
      end
    end
  end
end
