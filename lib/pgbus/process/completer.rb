# frozen_string_literal: true

module Pgbus
  module Process
    # Batched completion (issue #606, `config.completion_batching`): one per
    # worker, it archives the messages of jobs that finish close together with
    # one `archive_batch` per physical queue instead of one `pgmq.archive` each.
    #
    # There is no timer. An idle completer flushes a lone id at once; ids that
    # arrive while a flush is in flight wait for the next one, so under load a
    # batch is "what arrived during the previous round trip".
    # `batch_size` only caps the ids per statement.
    #
    # The job thread waits for its own result (`complete` blocks), so the
    # executor's post-archive sequence runs on it as before. Threads only: a
    # blocking wait would stall an Async reactor (Configuration#validate!).
    class Completer
      # Raised on a waiting job thread whose flush can no longer happen: the
      # flusher was stopped past its join budget, or died.
      class Stopped < Pgbus::Error; end

      # One job thread's wait for its id's result.
      class Waiter
        def initialize
          @mutex = Mutex.new
          @done = ConditionVariable.new
          @state = nil
          @value = nil
        end

        def resolve(value) = settle(:value, value)

        def fail(error) = settle(:error, error)

        def value
          @mutex.synchronize do
            @done.wait(@mutex) until @state
            raise @value if @state == :error

            @value
          end
        end

        private

        # First settle wins: a waiter already served is not failed again by
        # the flusher's last-resort cleanup.
        def settle(state, value)
          @mutex.synchronize do
            next if @state

            @state = state
            @value = value
            @done.broadcast
          end
        end
      end

      def initialize(client:, batch_size:)
        @client = client
        @batch_size = batch_size
        @mutex = Mutex.new
        @wake = ConditionVariable.new
        @pending = {}
        @taken = nil
        @stopping = false
        # Owned by this completer: one per worker, built with it. #stop asks
        # it to drain and exit, then joins it (Worker#shutdown, #immediate_shutdown).
        @thread = Thread.new { run } # rubocop:disable ThreadSafety/NewThread
        @thread.name = "pgbus-completer"
      end

      # Archives msg_id from the physical (already prefixed) queue and returns
      # :archived, or :already_archived when the message was gone (another
      # worker archived it). Raises the flush's exception on this thread.
      def complete(queue, msg_id)
        waiter = @mutex.synchronize do
          next if @stopping

          entry = Waiter.new
          (@pending[queue] ||= []) << [msg_id, entry]
          @wake.signal
          entry
        end
        return waiter.value if waiter

        # Stopped: archive on the caller's thread, with the same results.
        @client.archive_batch(queue, [msg_id], prefixed: false).empty? ? :already_archived : :archived
      end

      # Flushes whatever is pending, then joins the flusher. Past timeout the
      # flusher is killed and every waiter it had not served raises Stopped.
      # Later `complete` calls archive on their own thread. Idempotent.
      def stop(timeout: nil)
        @mutex.synchronize do
          @stopping = true
          @wake.signal
        end
        return if @thread.join(timeout)

        @thread.kill
        @thread.join
      end

      def alive? = @thread.alive?

      # Ids waiting for the next flush (test and introspection seam).
      def pending_count
        @mutex.synchronize { @pending.sum { |_queue, entries| entries.size } }
      end

      # True while a flush is running (test and introspection seam).
      def flushing?
        @mutex.synchronize { !@taken.nil? }
      end

      private

      def run
        while (batch = take)
          flush(batch)
          @mutex.synchronize { @taken = nil }
        end
      ensure
        fail_unserved
      end

      # Waits for pending ids and takes all of them; nil once stopping with
      # nothing left.
      def take
        @mutex.synchronize do
          @wake.wait(@mutex) while @pending.empty? && !@stopping
          next if @pending.empty?

          @taken = @pending
          @pending = {}
          @taken
        end
      end

      def flush(batch)
        batch.each do |queue, entries|
          entries.each_slice(@batch_size) { |slice| flush_slice(queue, slice) }
        end
      end

      # pgmq returns one row per archived id (as a String); an id already gone
      # is absent. A duplicated id (the same message held by two threads after
      # a redelivery) is archived once, so only its first waiter gets :archived.
      def flush_slice(queue, slice)
        archived = @client.archive_batch(queue, slice.map(&:first).uniq, prefixed: false).to_set(&:to_i)
        slice.each { |msg_id, waiter| waiter.resolve(archived.delete?(msg_id) ? :archived : :already_archived) }
      rescue Exception => e # rubocop:disable Lint/RescueException -- delivered to the waiters, never swallowed
        log_flush_failure(queue, slice, e) unless e.is_a?(StandardError)
        slice.map(&:last).each { |waiter| waiter.fail(e) }
      end

      def log_flush_failure(queue, slice, error)
        Pgbus.logger.error do
          "[Pgbus::Completer] archive_batch on #{queue} (#{slice.size} ids) raised #{error.class}: #{error.message}"
        end
      end

      # The flusher is exiting: nothing may be left waiting on it.
      def fail_unserved
        leftovers = @mutex.synchronize do
          entries = [@taken, @pending].compact.flat_map(&:values).flatten(1)
          @taken = nil
          @pending = {}
          @stopping = true
          entries
        end
        error = Stopped.new("the completer stopped before archiving this message")
        leftovers.map(&:last).each { |waiter| waiter.fail(error) }
      end
    end
  end
end
