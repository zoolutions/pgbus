# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # A unit of work's enqueues, held at the adapter and sent together when
    # the unit ends (issue #682): one send_batch per (queue, priority) group
    # instead of one send per perform_later. Opened by Pgbus.coalesce and the
    # Rack Middleware.
    #
    # Only a bulk-eligible job is held (Adapter#enqueue decides), serialized
    # at perform_later. One with uniqueness or concurrency, a future
    # scheduled_at, a retry re-enqueue, a deploy task run's job, a batch
    # member or a Workflow::StepJob goes out at once, as it does outside a
    # scope. Active Job's enqueue_after_transaction_commit runs before the
    # adapter, so a job that defers reaches the buffer at commit, and a rolled
    # back one never does.
    #
    # perform_later inside the scope returns the job with successfully_enqueued
    # true and provider_job_id nil until the flush, the same optimistic answer
    # Active Job gives a job deferred to after commit. The flush proves the
    # delivery: Adapter#deliver_coalesced sends the buffer in bulk, retries
    # every job it left unsent on its own, and what still fails goes to
    # on_failure (:raise, :report, or a callable taking (jobs, error)).
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
        @closed = false
      end

      # Holds +job+ for +adapter+ with the +payload+ serialized at
      # perform_later. Returns false once the flush has started: the caller
      # sends it itself.
      def push(adapter, job, payload)
        @mutex.synchronize do
          return false if @closed

          @entries << [adapter, job, payload]
          true
        end
      end

      # Closes the buffer, sends it, then applies on_failure to whatever could
      # not be sent. With +block_error+ (the block raised) a callable still
      # gets the jobs, but nothing raises: :raise becomes a report, and a
      # callable that raises is reported, so the block's error propagates.
      def finish!(block_error: nil)
        started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        entries = close
        result = deliver(entries)
        instrument(result, started)
        handle_failure(result, entries, block_error) if result.failed.any?
        result
      rescue StandardError => e
        # The block's error is the one that propagates: anything the flush
        # raises after it (a notification subscriber, say) is reported.
        raise unless block_error

        ErrorReporter.report(e, { pgbus_coalesce: :finish })
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
          adapter.deliver_coalesced(group.map { |_, job, payload| [job, payload] })
        end
        Result.new(
          buffered: entries.size,
          sent: results.sum(&:sent), retried: results.sum(&:retried),
          failed: results.flat_map(&:failed), error: results.filter_map(&:error).last
        )
      end

      def instrument(result, started)
        duration = (::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started) * 1000.0
        Instrumentation.instrument(
          "pgbus.coalesce",
          buffered: result.buffered, sent: result.sent, retried: result.retried,
          failed: result.failed.size, duration: duration
        )
      end

      def handle_failure(result, entries, block_error)
        error = failure_error(result)
        payloads = failed_payloads(result.failed, entries)
        case on_failure
        when :raise then block_error ? report(payloads, error) : raise(error)
        when :report then report(payloads, error)
        else call_on_failure(result.failed, payloads, error, block_error)
        end
      end

      def call_on_failure(jobs, payloads, error, block_error)
        on_failure.call(jobs, error)
      rescue StandardError => e
        raise unless block_error

        report(payloads, error)
        ErrorReporter.report(e, { pgbus_coalesce_on_failure: on_failure.to_s })
      end

      # The payloads serialized at perform_later: a job that failed is never
      # serialized again here.
      def failed_payloads(jobs, entries)
        failed = jobs.to_set(&:object_id)
        entries.filter_map { |_, job, payload| payload if failed.include?(job.object_id) }
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

      def report(payloads, error)
        context = { pgbus_coalesce: payloads }
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
