# frozen_string_literal: true

module Pgbus
  # The caller's open ActiveRecord transaction, for work that must wait for
  # its COMMIT: a durable stream broadcast (Streams::Stream) and a process
  # signal (Signals). Guarded by defined? so both stay usable without Rails.
  module OpenTransaction
    # Runs nothing itself: hands the commit hook a block that runs after
    # the commit (and never on rollback).
    #
    # Duck-typed as a record for Rails 7.1, whose transactions have no public
    # after_commit: ActiveRecord calls committed! / rolledback! on every record
    # added to a transaction. 7.2+ use Transaction#after_commit instead.
    class CommitHook
      def initialize(block)
        @block = block
      end

      def before_committed!; end

      # should_run_callbacks is false for a duplicate of a record already in
      # the transaction; a hook is only ever equal to itself.
      def committed!(should_run_callbacks: true, **)
        @block.call if should_run_callbacks
      end

      def rolledback!(*, **); end

      def trigger_transactional_callbacks?
        true
      end
    end

    module_function

    # Returns the current AR transaction if one is open, nil otherwise.
    # AR's NullTransaction yields immediately from after_commit, so we have to
    # check #open? explicitly — otherwise every call path would hit the
    # "deferred" branch and the caller would lose its return value.
    #
    # The probe must only inspect a connection the calling thread/fiber
    # ALREADY leases — `connection_pool.active_connection?`, never
    # `ActiveRecord::Base.connection`. On Rails 7.2+ `.connection` takes a
    # sticky executor-scoped lease: on a non-executor thread (the
    # Coalescer's flush thread, app worker threads) it is never released —
    # one pool connection pinned per thread — and inside
    # `with_connection` the sticky flag defeats the block-exit release, so
    # the CALLER's connection leaks when its thread dies. Both variants
    # exhausted an exactly-sized pool deterministically (Zazu fan-out
    # incident, 2026-08-05). Semantics are unchanged: a transaction is
    # per-lease, so a thread holding no connection can have no open
    # transaction to defer on — the old code's fresh checkout always
    # answered nil anyway, at the price of the leak.
    def current
      return nil unless defined?(::ActiveRecord::Base)

      connection = ::ActiveRecord::Base.connection_pool.active_connection?
      return nil unless connection

      transaction = connection.current_transaction
      transaction if transaction.open?
    rescue StandardError => e
      # Defensive: if AR is loaded but not yet connected (e.g. a
      # Rake task invoked before Rails boot), don't let the transaction
      # probe break the caller. Debug-logged so a misconfigured
      # app can diagnose "why isn't my work deferring?" without impacting
      # production performance (debug is off by default).
      Pgbus.logger.debug do
        "[Pgbus::OpenTransaction] transaction probe failed (#{e.class}: #{e.message}); " \
          "falling back to running now"
      end
      nil
    end

    # Registers +block+ to run after the COMMIT of the caller's open,
    # joinable transaction and returns that transaction; returns nil (and
    # registers nothing) when there is none, so the caller runs the work now.
    #
    # A transaction that is not joinable is not deferred on: in an app's
    # transactional tests that is the fixture transaction, which never
    # commits, so a model's `after_commit { Pgbus.signal(...) }` would never
    # send. Inside it, a `transaction do … end` block is joinable and its
    # commit runs the callbacks, as a production commit does.
    def defer(&block)
      transaction = current
      return nil unless transaction&.joinable?

      if transaction.respond_to?(:after_commit)
        transaction.after_commit(&block)
      else
        ::ActiveRecord::Base.connection_pool.active_connection?.add_transaction_record(CommitHook.new(block))
      end
      transaction
    end
  end
end
