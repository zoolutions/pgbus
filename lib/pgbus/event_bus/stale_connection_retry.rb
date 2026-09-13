# frozen_string_literal: true

module Pgbus
  module EventBus
    # Reconnect + one retry for an ActiveRecord socket that died underneath a
    # long-lived consumer thread.
    #
    # A consumer holds its leased connection for the life of the message, and a
    # pooler restart (PgBouncer `server_idle_timeout`), an admin disconnect or a
    # brief failover can kill that socket at any point. Rails reconnects most
    # statements transparently, but `Relation#update_all` is marked
    # `allow_retry: false`, so the drop propagates — and the only call that
    # matters here, the phase-2 claim stamp, is exactly an `update_all`. The
    # host app saw a paging exception on a message whose handler had in fact
    # succeeded.
    #
    # Deliberately narrower than `Client::STALE_CONNECTION_PATTERNS`, and for
    # the opposite reason. That list excludes mid-flight drops because a
    # half-committed *enqueue* would duplicate a message on retry. This one
    # only ever wraps an idempotent `UPDATE ... SET completed_at = <now>`, so a
    # statement that may already have committed is safe to repeat — which is
    # what makes the mid-flight shapes retryable here and not there.
    module StaleConnectionRetry
      # The socket dying under a statement, not a server refusing us. A refused
      # or timed-out connection is an outage: retrying it in-process buys
      # nothing and hides the outage behind a doubled statement timeout.
      #
      # "ssl syscall error" is libpq's wording when the peer vanished without a
      # TLS close_notify — the same drop as "unexpected eof while reading",
      # reported from the syscall layer instead. It is in
      # Client::STALE_CONNECTION_PATTERNS for the same reason.
      TRANSIENT_DROP = /
        PQconsumeInput|
        server\ closed\ the\ connection\ unexpectedly|
        unexpected\ eof\ while\ reading|
        ssl\ syscall\ error
      /ix

      module_function

      # Runs the block, and on a transient drop reconnects this thread's lease
      # and runs it exactly once more. A second drop raises: the socket is not
      # coming back inside this attempt, and PGMQ's visibility timeout is the
      # right recovery — it redelivers to a healthy consumer.
      #
      # context — an identifier for the log line (the handler class name).
      def call(context: nil, &block)
        block.call
      rescue ActiveRecord::ConnectionFailed => e
        raise unless transient_drop?(e)

        reconnect_leased!
        Pgbus.logger.warn do
          "[Pgbus::EventBus] Retrying after stale ActiveRecord connection drop" \
            "#{" (#{context})" if context}: #{e.message}"
        end
        block.call
      end

      def transient_drop?(error)
        return false unless error.is_a?(ActiveRecord::ConnectionFailed)

        [error.message, error.cause&.message].compact.any? { |message| TRANSIENT_DROP.match?(message) }
      end

      # Only the connections this thread has actually leased.
      # `clear_all_connections!` would yank sockets out from under sibling
      # consumers sharing the process, turning one recoverable drop into many.
      # `active_connection?` returns the lease or nil, and is the accessor this
      # gem uses everywhere (Streams#current_open_transaction documents why we
      # never reach for `ActiveRecord::Base.connection`). Its `active_connection`
      # alias is `:nodoc:` and does not exist at all before Rails 7.2 — below
      # our floor, so calling it would raise NoMethodError on the one path that
      # exists to recover from an error.
      def reconnect_leased!
        ActiveRecord::Base.connection_handler.each_connection_pool(:all) do |pool|
          connection = pool.active_connection?
          next unless connection

          connection.reconnect!
        end
      end
    end
  end
end
