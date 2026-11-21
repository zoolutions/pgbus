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
    #
    # A batch scope (issue #683, opened by Pgbus.batch for a batch it builds,
    # with +batch_id+ and +count_in+) holds only that batch's members and
    # joins nothing: it replaces the current scope for the block, flushes
    # when the block ends and puts the outer scope back. A plain scope opened
    # inside it joins it. A batch scope counts each chunk in before sending
    # it, with one call: count_in.call(payloads, final:). The final call
    # (the block's end) also closes the batch's build, in the same
    # transaction; see Batch#count_in. Every scope flushes early each
    # config.coalesce_chunk_size held jobs, so a huge block stays bounded in
    # memory (a batch scope only once no transaction is open on the batch's
    # connection: until then it keeps holding); on_failure still runs once,
    # at the end, for every chunk.
    class Coalesce
      KEY = :pgbus_coalesce
      FAILURE_MODES = %i[raise report].freeze

      Result = Data.define(:buffered, :sent, :retried, :failed, :error)

      class << self
        # The scope open on this thread or fiber, nil when there is none.
        def current
          Thread.current[KEY]
        end

        def open(on_failure: :raise, batch_id: nil, count_in: nil, &block)
          raise ArgumentError, "Pgbus.coalesce needs a block" unless block

          validate_on_failure!(on_failure)
          return yield if current && batch_id.nil?

          run(new(on_failure: on_failure, batch_id: batch_id, count_in: count_in), &block)
        end

        def validate_on_failure!(on_failure)
          return if FAILURE_MODES.include?(on_failure) || on_failure.respond_to?(:call)

          raise ArgumentError, "on_failure must be :raise, :report or a callable, got #{on_failure.inspect}"
        end

        private

        def run(scope)
          outer = current
          Thread.current[KEY] = scope
          error = nil
          begin
            yield
          rescue Exception => e # rubocop:disable Lint/RescueException
            error = e
            raise
          ensure
            Thread.current[KEY] = outer
            scope.finish!(block_error: error)
          end
        end
      end

      attr_reader :on_failure, :batch_id

      def initialize(on_failure: :raise, batch_id: nil, count_in: nil, chunk_size: Pgbus.configuration.coalesce_chunk_size)
        @on_failure = on_failure
        @batch_id = batch_id
        @count_in = count_in
        @chunk_size = chunk_size
        @mutex = Mutex.new
        @entries = []
        @results = []
        @failed_entries = []
        @closed = false
      end

      # Holds +job+ for +adapter+ with the +payload+ serialized at
      # perform_later, and sends the buffer once it holds a chunk. Returns
      # false once the flush has started: the caller sends it itself.
      def push(adapter, job, payload) # rubocop:disable Naming/PredicateMethod -- holds; the result says whether
        chunk = @mutex.synchronize do
          return false if @closed

          # A full chunk goes when the next job arrives, so a block of
          # exactly chunk_size jobs is still one flush, at its end. A batch
          # scope keeps holding while a transaction is open on the batch's
          # connection: a chunk counted in there and sent on PGMQ's own
          # connection would lose its count, not its messages, on a rollback.
          full = drain if @entries.size >= @chunk_size && !hold_for_transaction?
          @entries << [adapter, job, payload]
          full
        end
        deliver_chunk(chunk) if chunk
        true
      end

      # Closes the buffer, sends it, then applies on_failure to whatever could
      # not be sent. With +block_error+ (the block raised) a callable still
      # gets the jobs, but nothing raises: :raise becomes a report, and a
      # callable that raises is reported, so the block's error propagates.
      def finish!(block_error: nil)
        started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        deliver_chunk(close, final: true)
        failed_entries, results = @mutex.synchronize { [@failed_entries, @results] }
        result = combine(results.sum(&:buffered), results)
        instrument(result, started) if result.buffered.positive?
        handle_failure(result, failed_entries, block_error) if result.failed.any?
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
          drain
        end
      end

      # Takes the buffer; the caller holds the mutex.
      def drain
        entries = @entries
        @entries = []
        entries
      end

      # Sends +entries+ now and keeps their result for finish!. A batch
      # scope counts them in first (count_in); the final chunk also closes
      # the batch's build, so it runs even when the chunk is empty.
      def deliver_chunk(entries, final: false)
        counted = count_chunk_in(entries, final)
        return if entries.empty?

        results = entries.group_by(&:first).map do |adapter, group|
          adapter.deliver_coalesced(group.map { |_, job, payload| [job, payload] }, counted: counted)
        end
        result = combine(entries.size, results)
        failed = result.failed.to_set(&:object_id)
        # Only the counts and the failed jobs are kept: a sent chunk's jobs
        # and payloads go, so chunking bounds memory.
        @mutex.synchronize do
          @results << result
          @failed_entries.concat(entries.select { |_, job, _| failed.include?(job.object_id) })
        end
      end

      def hold_for_transaction?
        return false unless @count_in

        connection = Pgbus::BatchEntry.connection_pool.active_connection?
        connection ? connection.transaction_open? : false
      rescue StandardError => e
        # Unknown is treated as open: holding a chunk to the block's end is
        # safe, counting it inside a transaction that rolls back is not.
        Pgbus.logger.debug { "[Pgbus] Coalesce could not read the batch connection's transaction (#{e.class}); holding the chunk" }
        true
      end

      # True when the chunk was counted in. A count that fails (a batch the
      # sweep reaped, a database error) sends nothing: every job of the chunk
      # fails, with that error, through on_failure.
      def count_chunk_in(entries, final)
        return false unless @count_in

        @count_in.call(entries.map(&:last), final: final)
        true
      rescue StandardError => e
        raise if entries.empty?

        jobs = entries.map { |_, job, _| job }
        jobs.each { |job| job.successfully_enqueued = false }
        @mutex.synchronize do
          @results << Result.new(buffered: entries.size, sent: 0, retried: 0, failed: jobs, error: e)
          @failed_entries.concat(entries)
        end
        entries.clear
        false
      end

      # The held jobs this scope still references, waiting for the flush or
      # failed in it.
      def retained_entries
        @mutex.synchronize { @entries + @failed_entries }
      end

      def combine(buffered, results)
        Result.new(
          buffered: buffered,
          sent: results.sum(&:sent), retried: results.sum(&:retried),
          failed: results.flat_map(&:failed), error: results.filter_map(&:error).last
        )
      end

      # A subscriber that raises is reported: the flush's outcome, and the
      # failed jobs on_failure must still see, do not depend on it.
      def instrument(result, started)
        duration = (::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started) * 1000.0
        Instrumentation.instrument(
          "pgbus.coalesce",
          buffered: result.buffered, sent: result.sent, retried: result.retried,
          failed: result.failed.size, duration: duration
        )
      rescue StandardError => e
        ErrorReporter.report(e, { pgbus_coalesce: :instrument })
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
