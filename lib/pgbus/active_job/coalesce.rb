# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # A unit of work's enqueues, held at the adapter and sent together when
    # the unit ends (issue #682): one send_batch per (queue, priority) group
    # instead of one send per perform_later. Opened by Pgbus.coalesce and the
    # Rack Middleware.
    #
    # Only a bulk-eligible job is held (Adapter#enqueue decides): one with
    # uniqueness or concurrency, a future scheduled_at, a retry re-enqueue, a
    # deploy task run's job, or a batch member whose batch this scope does not
    # hold goes out at once, as it does outside a scope. Active Job's
    # enqueue_after_transaction_commit runs before the adapter, so a job
    # enqueued in a transaction reaches the buffer at commit, and a rolled
    # back one never does.
    #
    # perform_later inside the scope returns the job with successfully_enqueued
    # true and provider_job_id nil until the flush, the same optimistic answer
    # Active Job gives a job deferred to after commit. The flush proves the
    # delivery: Adapter#deliver_coalesced sends the buffer through enqueue_all,
    # retries every job it left unsent on its own, and what still fails goes
    # to on_failure (:raise, :report, or a callable taking (jobs, error)).
    #
    # The scope is fiber-local (Thread.current, like the batch tag) and
    # cleared before the flush starts, so a job enqueued on another thread or
    # fiber, during the flush, or after the block, goes out at once. A nested
    # scope joins the outer one, whose on_failure applies; only the outermost
    # flushes.
    class Coalesce
      KEY = :pgbus_coalesce
      FAILURE_MODES = %i[raise report].freeze

      Result = Data.define(:buffered, :sent, :retried, :failed, :error)

      class << self
        # The scope open on this thread or fiber, nil when there is none.
        def current
          Thread.current[KEY]
        end

        def open(on_failure: :raise, &block)
          raise ArgumentError, "Pgbus.coalesce needs a block" unless block

          validate_on_failure!(on_failure)
          return yield if current

          run(new(on_failure: on_failure), &block)
        end

        # Registers +batch_id+ with the open scope: its members are then
        # buffered, and the batch starts after the flush (Batch#enqueue).
        # Returns the scope, or nil when none is open.
        def hold(batch_id)
          scope = current
          scope if scope&.hold(batch_id)
        end

        def validate_on_failure!(on_failure)
          return if FAILURE_MODES.include?(on_failure) || on_failure.respond_to?(:call)

          raise ArgumentError, "on_failure must be :raise, :report or a callable, got #{on_failure.inspect}"
        end

        private

        def run(scope)
          Thread.current[KEY] = scope
          error = nil
          begin
            yield
          rescue Exception => e # rubocop:disable Lint/RescueException
            error = e
            raise
          ensure
            Thread.current[KEY] = nil
            scope.finish!(block_error: error)
          end
        end
      end

      attr_reader :on_failure

      def initialize(on_failure: :raise)
        @on_failure = on_failure
        @mutex = Mutex.new
        @entries = []
        @held = Set.new
        @hooks = []
        @closed = false
        @flushed = false
      end

      # Buffers +job+ for +adapter+, with the batch it joins captured now.
      # Returns false once the flush has started: the caller sends it itself.
      def push(adapter, job, batch_id)
        @mutex.synchronize do
          return false if @closed

          @entries << [adapter, job, batch_id]
          true
        end
      end

      def hold(batch_id)
        @mutex.synchronize do
          return false if @closed

          @held << batch_id
          true
        end
      end

      def holds?(batch_id)
        @mutex.synchronize { @held.include?(batch_id) }
      end

      # Runs the block after the flush has sent the buffer (at once when it
      # already has): a held batch starts there, once its members are counted.
      def after_flush(&block)
        run_now = @mutex.synchronize do
          @hooks << block unless @flushed
          @flushed
        end
        yield if run_now
      end

      # Closes the buffer, sends it, runs the after_flush hooks, then applies
      # on_failure to whatever could not be sent. With +block_error+ (the
      # block raised) a failure is reported instead of raised, so the block's
      # error is the one that propagates.
      def finish!(block_error: nil)
        started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        entries = close
        result = nil
        begin
          result = deliver(entries)
        ensure
          run_hooks
        end
        instrument(result, started)
        handle_failure(result, block_error) if result.failed.any?
        result
      end

      private

      def close
        @mutex.synchronize do
          @closed = true
          entries = @entries
          @entries = []
          entries
        end
      end

      def deliver(entries)
        results = entries.group_by(&:first).map do |adapter, group|
          adapter.deliver_coalesced(group.map { |_, job, batch_id| [job, batch_id] })
        end
        Result.new(
          buffered: entries.size,
          sent: results.sum(&:sent), retried: results.sum(&:retried),
          failed: results.flat_map(&:failed), error: results.filter_map(&:error).last
        )
      end

      def run_hooks
        hooks = @mutex.synchronize do
          @flushed = true
          hooks = @hooks
          @hooks = []
          hooks
        end
        hooks.each(&:call)
      end

      def instrument(result, started)
        duration = (::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started) * 1000.0
        Instrumentation.instrument(
          "pgbus.coalesce",
          buffered: result.buffered, sent: result.sent, retried: result.retried,
          failed: result.failed.size, duration: duration
        )
      end

      def handle_failure(result, block_error)
        error = failure_error(result)
        return report(result.failed, error) if block_error

        case on_failure
        when :raise then raise error
        when :report then report(result.failed, error)
        else on_failure.call(result.failed, error)
        end
      end

      def failure_error(result)
        names = result.failed.map { |job| "#{job.class.name} (#{job.job_id})" }
        message = "Pgbus.coalesce could not enqueue #{names.size} job(s): #{names.join(", ")}"
        message += " — #{result.error.class}: #{result.error.message}" if result.error
        # Raised and rescued here so the error carries a backtrace and the
        # send's own error as its cause.
        begin
          raise EnqueueError, message, cause: result.error
        rescue EnqueueError => e
          e
        end
      end

      def report(jobs, error)
        context = { pgbus_coalesce: jobs.map(&:serialize) }
        if defined?(::Rails) && ::Rails.respond_to?(:error)
          Pgbus.logger.error { "[Pgbus] #{error.message}" }
          ::Rails.error.report(error, context: context)
        else
          ErrorReporter.report(error, context)
        end
      end
    end
  end
end
