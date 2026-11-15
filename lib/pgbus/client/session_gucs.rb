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
    # checkout sees it finished? and resets it, which tries again (pg 1.5.9 and
    # later reset a closed connection; an older pg raises, and the pool reload
    # before the last stale-connection retry replaces it).
    #
    # One instance per pool, built at Client construction; it holds only the
    # SET statements, in a frozen array nothing mutates, so concurrent
    # connections share it safely.
    class SessionGucs < Module
      def initialize(variables)
        super()
        statements = variables.map { |name, value| "SET #{name} = '#{value}'" }.freeze

        # Runs the block (a reset, or nothing after a connect), then the SETs.
        # One ensure covers both, so an async raise between the reset and the
        # first SET also closes the connection.
        define_method(:pgbus_with_session_gucs) do |&before|
          applied = false
          before&.call
          statements.each { |sql| exec(sql) }
          applied = true
          self
        ensure
          close unless applied || finished?
        end

        # pg's async_reset is an alias bound to PG::Connection#reset and its
        # sync_reset is C, so a hook on reset alone would not see either. None
        # of the three calls another, so each reset applies the SETs once.
        %i[reset async_reset sync_reset].each do |name|
          define_method(name) do
            pgbus_with_session_gucs { super() }
          end
        end
      end

      def connect(pg_opts)
        PG.connect(pg_opts).extend(self).pgbus_with_session_gucs
      end
    end
  end
end
