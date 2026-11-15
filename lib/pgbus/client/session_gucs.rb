# frozen_string_literal: true

module Pgbus
  class Client
    # The database.yml variables that connection_guc_mode :session applies with
    # SET after connect (issue #332), on every session a pooled connection has.
    #
    # PG::Connection#reset opens a new backend from the same conninfo and does
    # not replay the SETs. pgmq-ruby resets an unhealthy pooled connection at
    # checkout (finished?, CONNECTION_BAD, and since 0.9.0 an intermediate
    # state such as CONNECTION_STARTED), and AsyncCommit#discard_session resets
    # one whose pipeline state cannot be trusted. So each connection #connect
    # opens is extended with this module, whose #reset (and #async_reset,
    # #sync_reset) applies the SETs again (issue #671).
    #
    # A connection whose SETs did not all complete is closed before the error
    # propagates: it is never pooled CONNECTION_OK without them. The next
    # checkout sees it finished? and resets it, which tries again.
    #
    # One instance per pool, built at Client construction; it holds only the
    # frozen SET statements, so concurrent connections share it safely.
    class SessionGucs < Module
      def initialize(variables)
        super()
        statements = variables.map { |name, value| "SET #{name} = '#{value}'" }.freeze

        define_method(:pgbus_apply_session_gucs) do
          applied = false
          statements.each { |sql| exec(sql) }
          applied = true
        ensure
          close unless applied || finished?
        end

        # pg's async_reset is an alias bound to PG::Connection#reset and its
        # sync_reset is C, so a hook on reset alone would not see either. None
        # of the three calls another, so each reset applies the SETs once.
        %i[reset async_reset sync_reset].each do |name|
          define_method(name) do
            super()
            pgbus_apply_session_gucs
            self
          end
        end
      end

      def connect(pg_opts)
        conn = PG.connect(pg_opts).extend(self)
        conn.pgbus_apply_session_gucs
        conn
      end
    end
  end
end
