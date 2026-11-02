# frozen_string_literal: true

module Pgbus
  class Client
    # Asynchronous completion commit (issue #619,
    # `config.completion_synchronous_commit = false`). A completion statement
    # (archive, delete and their batches) runs in its own transaction with
    # synchronous_commit off, so the job is acknowledged without waiting for
    # its WAL flush. The four queries go out in one libpq pipeline with one
    # sync, so it is still one round trip (pg writes each query to the socket
    # as it is sent, but waits for a reply only once, after the sync):
    #
    #   BEGIN
    #   SET LOCAL synchronous_commit TO off
    #   <the prepared completion statement>   <- the result the caller gets
    #   COMMIT
    #
    # SET LOCAL ends with the transaction, so the pooled connection never
    # keeps the setting, and the statement stays prepared (no planning cost
    # back, issue #605). Only a job's completion asks for it (the executor and
    # the completer pass job_completion: true; the event consumer and the
    # dashboard keep the durable commit), and NativeStatements#exec_native
    # calls it only outside a caller's transaction: a completion inside one is
    # the caller's to commit.
    #
    # A failed query aborts the rest of the pipeline and leaves the explicit
    # BEGIN open in a failed state: the connection leaves pipeline mode, the
    # transaction is rolled back, and the error is raised for run_native's
    # re-run and short-row checks. A statement not yet prepared on this session
    # runs through prepare_and_exec outside the pipeline, committed
    # synchronously, once; the next call pipelines again.
    module AsyncCommit
      ASYNC_COMMIT_BEGIN = "BEGIN"
      ASYNC_COMMIT_OFF = "SET LOCAL synchronous_commit TO off"
      ASYNC_COMMIT_COMMIT = "COMMIT"
      NO_PARAMS = [].freeze

      private

      def exec_async_commit(conn, statement, params, prepared)
        results = pipeline_async_commit(conn, statement, params, prepared)
        failed = results.find { |result| result.result_status == PG::PGRES_FATAL_ERROR }
        unless failed
          NativeStatements.note_executed if prepared
          return results[2]
        end

        rollback_async_commit(conn)
        begin
          failed.check
        rescue PG::InvalidSqlStatementName
          raise unless prepared

          prepare_and_exec(conn, statement, params)
        end
      end

      # Sends the four queries and one sync, and reads every result up to the
      # sync: each query answers its result and then nil.
      def pipeline_async_commit(conn, statement, params, prepared)
        conn.enter_pipeline_mode
        begin
          conn.send_query_params(ASYNC_COMMIT_BEGIN, NO_PARAMS)
          conn.send_query_params(ASYNC_COMMIT_OFF, NO_PARAMS)
          if prepared
            conn.send_query_prepared(statement.name, params)
          else
            conn.send_query_params(statement.sql, params)
          end
          conn.send_query_params(ASYNC_COMMIT_COMMIT, NO_PARAMS)
          conn.pipeline_sync
          read_pipeline(conn)
        ensure
          leave_pipeline(conn)
        end
      end

      def read_pipeline(conn)
        results = []
        ended = false
        loop do
          result = conn.get_result
          if result.nil?
            # One nil closes each query's results; two in a row means libpq
            # has nothing more, so the sync will not come. That session's
            # state cannot be trusted: pgmq-ruby re-runs on a fresh one.
            if ended
              discard_session(conn, "A completion pipeline ended before its sync")
              raise PG::ConnectionBad, "pipeline ended before its sync"
            end

            ended = true
            next
          end
          ended = false
          break if result.result_status == PG::PGRES_PIPELINE_SYNC

          results << result
        end
        results
      end

      # A connection back in the pool in pipeline mode would break the next
      # caller. A lost one is left as it is: pgmq-ruby resets a bad connection
      # at its next checkout, and a reset leaves pipeline mode. One that cannot
      # leave (results left unread by an interrupted read) is reset here.
      def leave_pipeline(conn)
        return if conn.pipeline_status == PG::PQ_PIPELINE_OFF || conn.status == PG::CONNECTION_BAD

        conn.exit_pipeline_mode
      rescue PG::Error => e
        discard_session(conn, "Could not leave pipeline mode after a completion (#{e.message})")
      end

      # Resets a session whose pipeline state cannot be trusted. A reset that
      # fails (the server is gone) is logged and left there, so the error the
      # caller is already raising is the one that surfaces; pgmq-ruby resets a
      # bad connection at its next checkout.
      def discard_session(conn, reason)
        Pgbus.logger.warn { "[Pgbus::Client] #{reason}, resetting the connection" }
        conn.reset
      rescue PG::Error => e
        Pgbus.logger.warn { "[Pgbus::Client] Resetting the connection failed: #{e.message}" }
      end

      # The failed BEGIN block (INERROR) is still open after the sync. A failed
      # COMMIT has already ended it (IDLE); a lost connection reports UNKNOWN
      # and has nothing to roll back.
      def rollback_async_commit(conn)
        conn.exec("ROLLBACK") if [PG::PQTRANS_INERROR, PG::PQTRANS_INTRANS].include?(conn.transaction_status)
      end
    end
  end
end
