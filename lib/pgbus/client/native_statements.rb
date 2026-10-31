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
    # Inside a transaction the server has begun (the shared-AR path, an
    # enqueue inside the app's transaction once Rails has sent its BEGIN) a
    # failed exec_prepared would abort the caller's transaction, so there the
    # same SQL runs through exec_params.
    #
    # Connections built from ActiveRecord's config whose database.yml sets
    # `prepared_statements: false` (what a transaction-mode PgBouncer asks
    # of Rails) send the same SQL through exec_params from the start.
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

      # Raised when an archive or delete re-run after a lost connection comes
      # back short (run_native). It carries the original error's text, which
      # can match a stale-connection pattern, so with_stale_connection_retry
      # checks completion_lost? first and never re-runs it.
      COMPLETION_NOT_RESENT = "Database connection lost while an archive or delete ran, " \
                              "and the re-run found ids gone: it may have committed"

      # Reads of up to this many messages run a statement with the LIMIT
      # written in: with `LIMIT $1` the generic plan assumes a tenth of the
      # table and hash-joins a full scan, so Postgres plans a custom plan on
      # every call; a literal LIMIT gets the cached generic plan. A larger qty
      # runs the `LIMIT $1` statement.
      LITERAL_READ_LIMIT = 64

      # Entries kept in @prepared_on before it is cleared. Every reconnect
      # brings a new backend pid, so a long-lived process would otherwise grow
      # it for good. Clearing only forgets history: a statement then missing
      # reads as a first prepare, which can delay the switch, never cause it.
      PREPARED_ON_LIMIT = 10_000

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
      # Whether the database.yml prepared_statements: false notice was logged.
      @ar_unprepared_logged = Concurrent::AtomicBoolean.new(false)

      # rubocop:disable ThreadSafety/ClassInstanceVariable -- the four are
      # Concurrent primitives built once at load and never reassigned; every
      # read and write below goes through their atomic operations.
      class << self
        # The error run_native raises for a completion whose re-run came back
        # short (the client's stale retry and the consumer key on it).
        def completion_lost?(error)
          defined?(PGMQ::Errors::ConnectionError) && error.is_a?(PGMQ::Errors::ConnectionError) &&
            error.message.start_with?(COMPLETION_NOT_RESENT)
        end

        # True once the process has given up on prepared statements.
        def unprepared?
          @unprepared.true?
        end

        def note_prepared(backend_pid, name)
          @prepared_on.clear if @prepared_on.size >= PREPARED_ON_LIMIT
          @prepared_on["#{backend_pid}:#{name}"] = true
        end

        def prepared_count
          @prepared_on.size
        end

        # A statement this session needed to prepare. Counts toward the switch
        # only when this backend had prepared it before.
        def note_missing(backend_pid, name)
          return unless @prepared_on.key?("#{backend_pid}:#{name}")

          unprepare!("#{LOST_STATEMENT_LIMIT} prepared statements in a row disappeared from their connection") if
            @lost_in_a_row.increment >= LOST_STATEMENT_LIMIT
        end

        # Logs, once per process, that database.yml turned preparing off.
        def note_ar_unprepared
          return unless @ar_unprepared_logged.make_true

          Pgbus.logger.info do
            "[Pgbus::Client] Native statements run unprepared: database.yml sets prepared_statements: false"
          end
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
          @ar_unprepared_logged.make_false
        end
        # rubocop:enable ThreadSafety/ClassInstanceVariable

        # The seven statements for one queue, keyed by op. Names are
        # pgbus_<op>_<12 hex of SHA-256(sql)>: 63 bytes is the identifier limit,
        # and hashing the SQL (which names the table) means two pgbus versions
        # behind one pooler never run each other's text under one name.
        #
        # :reads holds the literal-LIMIT reads, indexed by qty (1 to
        # LITERAL_READ_LIMIT); their one parameter is the visibility timeout.
        def statements_for(queue_table, archive_table)
          queue = "pgmq.#{quote_ident(queue_table)}"
          statements = sql_for(queue, "pgmq.#{quote_ident(archive_table)}").to_h { |op, sql| [op, statement(op, sql)] }
          reads = (1..LITERAL_READ_LIMIT).map { |qty| statement(:read, read_sql(queue, qty.to_s, "$1")) }
          statements.merge(reads: [nil, *reads].freeze).freeze
        end

        private

        def statement(kind, sql)
          Statement.new(name: "pgbus_#{kind}_#{Digest::SHA256.hexdigest(sql)[0, 12]}", sql: sql.freeze)
        end

        def read_sql(queue, limit, vt)
          "WITH cte AS (SELECT msg_id FROM #{queue} WHERE vt <= clock_timestamp() " \
            "ORDER BY msg_id ASC LIMIT #{limit} FOR UPDATE SKIP LOCKED) " \
            "UPDATE #{queue} m SET last_read_at = clock_timestamp(), " \
            "vt = clock_timestamp() + make_interval(secs => #{vt}::integer), read_ct = read_ct + 1 " \
            "FROM cte WHERE m.msg_id = cte.msg_id " \
            "RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers"
        end

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
            read: read_sql(queue, "$1::integer", "$2"),
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

      # Whether the connections come from ActiveRecord's config with
      # `prepared_statements: false`: the hash pgbus extracts from it, or a
      # connection_params Proc handing out ActiveRecord's own connection. A
      # database_url or a connection_params Hash is pgbus's own route. Read
      # once, in Client#initialize.
      def ar_disables_prepared_statements?
        return false if config.database_url || config.connection_params.is_a?(Hash)
        return false unless defined?(::ActiveRecord::Base)

        base = config.connects_to ? Pgbus::BusRecord : ::ActiveRecord::Base
        return false unless [false, "false"].include?(base.connection_db_config.configuration_hash[:prepared_statements])

        NativeStatements.note_ar_unprepared
        true
      rescue StandardError
        # Client#initialize reads config.connection_options first, whose
        # Configuration#extract_ar_connection_hash makes this same
        # connection_db_config call, logs its failure and falls back to the
        # shared connection; there is no setting to read.
        false
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

      # The send statements bind the delay as $3::integer. is_a?(Integer) also
      # takes a whole-second ActiveSupport::Duration, which binds as its seconds;
      # a fractional delay or a Time goes through pgmq-ruby.
      def exec_produce(name, message, headers:, delay:)
        statements = delay.is_a?(Integer) && native_statements_for(name)
        return @pgmq.produce(name, message, headers: headers, delay: delay) unless statements

        run_native(statements[:send], [message, headers, delay])[0]["msg_id"]
      end

      def exec_produce_batch(name, messages, headers:, delay:)
        statements = messages.any? && (headers.nil? || headers.length == messages.length) &&
                     delay.is_a?(Integer) && native_statements_for(name)
        return @pgmq.produce_batch(name, messages, headers: headers, delay: delay) unless statements

        encoder = PG::TextEncoder::Array.new
        params = [encoder.encode(messages), headers && encoder.encode(headers), delay]
        run_native(statements[:send_batch], params).map { |row| row["msg_id"] }
      end

      def exec_read_batch(name, vt:, qty:)
        statements = native_statements_for(name)
        return @pgmq.read_batch(name, vt: vt, qty: qty) unless statements

        literal = qty.is_a?(Integer) && qty.positive? && statements[:reads][qty]
        result = literal ? run_native(literal, [vt]) : run_native(statements[:read], [qty, vt])
        result.map { |row| PGMQ::Message.new(row) }
      end

      def exec_archive(name, msg_id)
        statements = native_statements_for(name)
        return @pgmq.archive(name, msg_id) unless statements

        run_native(statements[:archive], [msg_id], rows: 1).ntuples.positive?
      end

      def exec_archive_batch(name, msg_ids)
        statements = msg_ids.any? && native_statements_for(name)
        return @pgmq.archive_batch(name, msg_ids) unless statements

        run_native(statements[:archive_batch], [PG::TextEncoder::Array.new.encode(msg_ids)], rows: msg_ids).map { |row| row["msg_id"] }
      end

      def exec_delete(name, msg_id)
        statements = native_statements_for(name)
        return @pgmq.delete(name, msg_id) unless statements

        run_native(statements[:delete], [msg_id], rows: 1).ntuples.positive?
      end

      def exec_delete_batch(name, msg_ids)
        statements = msg_ids.any? && native_statements_for(name)
        return @pgmq.delete_batch(name, msg_ids) unless statements

        run_native(statements[:delete_batch], [PG::TextEncoder::Array.new.encode(msg_ids)], rows: msg_ids).map { |row| row["msg_id"] }
      end

      # rows: the rows an archive or delete returns when every id was still
      # there. pgmq-ruby's with_connection re-runs its block after a lost
      # connection ("server closed the connection"). Usually the socket died
      # idle in the pool and nothing ran, but it can follow a commit whose
      # reply was lost: the re-run then finds those ids gone, and the executor
      # would take its own completed job for a duplicate and skip its signals.
      # So a re-run that comes back short raises instead; the executor's
      # retry-once then reads the gone message as :ambiguous and claims it.
      # A re-run that finds every id is certain: the first run never committed.
      # rows is 1 or the batch's ids (counted once each, only on a re-run). An
      # id another worker had already taken also makes a re-run come back
      # short; every waiter of that batch then claims :ambiguous, the side the
      # executor already errs on.
      def run_native(statement, params, rows: nil)
        attempt = 0
        lost = nil
        @pgmq.with_connection do |conn|
          attempt += 1
          result = exec_native(conn, statement, params)
          if rows && attempt > 1 && result.ntuples < (rows.is_a?(Array) ? rows.uniq.size : rows)
            raise PGMQ::Errors::ConnectionError, "#{COMPLETION_NOT_RESENT}: #{lost}"
          end

          result
        rescue PG::Error => e
          lost = e.message
          raise
        end
      end

      def exec_native(conn, statement, params)
        if @native_unprepared || NativeStatements.unprepared? || inside_caller_transaction?(conn)
          return conn.exec_params(statement.sql, params)
        end

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
