# frozen_string_literal: true

module Pgbus
  module ExecutionPools
    class AsyncPool
      # How long #kill waits for the killed reactor thread (and the job
      # fibers it cancels) to unwind before giving up on it.
      KILL_JOIN_TIMEOUT = 1

      attr_reader :capacity

      def initialize(capacity:, on_state_change: nil)
        @capacity = capacity
        @on_state_change = on_state_change
        @available_capacity = capacity
        @mutex = Mutex.new
        @state_mutex = Mutex.new
        @shutdown_flag = false
        @fatal_error = nil
        @boot_queue = Thread::Queue.new
        @pending = Thread::Queue.new
        @wake_rd, @wake_wr = IO.pipe

        validate_dependencies!
        @reactor_thread = start_reactor
        result = @boot_queue.pop
        raise result if result.is_a?(Exception)
      end

      def post(&block)
        raise_if_fatal!
        raise Pgbus::ExecutionPoolError, "Execution pool is shutting down" if shutdown?

        reserved = false
        reserve_capacity!
        reserved = true
        @pending << block
        wake_reactor
      rescue StandardError
        restore_capacity if reserved
        raise
      end

      def available_capacity
        raise_if_fatal!
        @mutex.synchronize { @available_capacity }
      end

      def idle?
        available_capacity.positive?
      end

      # True only when every slot is free — all in-flight work has finished.
      # idle? answers "can this pool accept work?"; quiesced? answers
      # "has this pool drained?". The worker drain loop needs the latter.
      def quiesced?
        available_capacity >= @capacity
      end

      def shutdown
        @state_mutex.synchronize do
          return false if @shutdown_flag

          @shutdown_flag = true
        end
        wake_reactor
      end

      def shutdown?
        @state_mutex.synchronize { @shutdown_flag }
      end

      def wait_for_termination(timeout)
        @reactor_thread&.join(timeout)
      end

      # Killing the reactor thread cancels its job fibers; wait for them to
      # unwind so the caller can reclaim what they held (ExecutionPools.terminate).
      def kill
        shutdown
        thread = @reactor_thread
        return unless thread

        thread.kill
        join_killed(thread)
      end

      def metadata
        inflight = @mutex.synchronize { @available_capacity }
        {
          mode: :async,
          capacity: @capacity,
          busy: @capacity - inflight
        }
      end

      private

      # The reactor's own failure is already recorded (register_fatal_error);
      # Thread#join would raise it again here.
      def join_killed(thread)
        thread.join(KILL_JOIN_TIMEOUT)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Async pool reactor ended with #{e.class}: #{e.message}" }
      end

      def validate_dependencies!
        require "async"
        require "async/semaphore"
      rescue LoadError => e
        raise LoadError,
              "Async execution mode requires the `async` gem. " \
              "Add `gem \"async\"` to your Gemfile. Original error: #{e.message}"
      end

      # The pool's one reactor thread, owned by this pool: started once by
      # #initialize, it exits when #shutdown lets the reactor drain, and
      # #wait_for_termination / #kill join it.
      # rubocop:disable-next Lint/RescueException
      def start_reactor
        Thread.new do # rubocop:disable ThreadSafety/NewThread
          Thread.current.name = "pgbus-async-reactor-#{object_id}"
          Async do |task|
            semaphore = Async::Semaphore.new(@capacity, parent: task)
            @boot_queue << :ready

            wait_for_executions(semaphore)
            wait_for_inflight(task)
          ensure
            begin
              @wake_rd.close
            rescue IOError
              nil
            end
            begin
              @wake_wr.close
            rescue IOError
              nil
            end
          end
        rescue Exception => e
          register_fatal_error(e)
          raise
        end
      end

      def wait_for_executions(semaphore)
        loop do
          schedule_pending(semaphore)
          break if shutdown? && @pending.empty?

          wait_for_wake if @pending.empty?
        end
      end

      # Fiber-aware wait: yields the reactor fiber until the main thread
      # writes a wake byte via wake_reactor. IO#wait_readable integrates
      # with the Async scheduler so other fibers continue running.
      def wait_for_wake
        return if @wake_rd.closed?

        @wake_rd.wait_readable
        drain_wake_pipe
      rescue IOError, Errno::EBADF
        nil
      end

      def drain_wake_pipe
        @wake_rd.read_nonblock(256)
      rescue IOError, SystemCallError
        nil
      end

      # Thread-safe: called from the main thread to wake the reactor fiber.
      def wake_reactor
        @wake_wr.write_nonblock(".")
      rescue IO::WaitWritable, IOError, Errno::EBADF, Errno::EPIPE
        nil
      end

      def schedule_pending(semaphore)
        while (block = next_pending)
          semaphore.async do
            perform(block)
          end
        end
      end

      def next_pending
        @pending.pop(true)
      rescue ThreadError
        nil
      end

      # Supervisor-level rescue: catch any Exception raised from the user
      # block so capacity is always restored and the failure is logged.
      # The `async` gem uses Async::Stop / Async::Cancel (Exception subclasses,
      # NOT StandardError) to cancel tasks, and prior to issue #126 those
      # would leak past `rescue StandardError` and silently vanish.
      # Process-fatal signals still propagate so the supervisor can react.
      FATAL_EXCEPTIONS = [SystemExit, Interrupt, SignalException, NoMemoryError, SystemStackError].freeze
      private_constant :FATAL_EXCEPTIONS

      def perform(block)
        block.call
      rescue *FATAL_EXCEPTIONS
        raise
      rescue Exception => e # rubocop:disable Lint/RescueException
        Pgbus.logger.error { "[Pgbus] Async pool fiber error: #{e.class}: #{e.message}" }
      ensure
        restore_capacity
      end

      def reserve_capacity!
        @mutex.synchronize do
          raise Pgbus::ExecutionPoolError, "Execution pool is at capacity" if @available_capacity <= 0

          @available_capacity -= 1
        end
      end

      def restore_capacity
        should_notify = @mutex.synchronize do
          @available_capacity += 1
          @available_capacity.positive?
        end
        wake_reactor
        @on_state_change&.call if should_notify
      end

      def register_fatal_error(error)
        @state_mutex.synchronize { @fatal_error ||= error }
        @boot_queue << error if @boot_queue.empty?
        @on_state_change&.call
      end

      def raise_if_fatal!
        error = @state_mutex.synchronize { @fatal_error }
        raise error if error
      end

      def wait_for_inflight(task)
        task.sleep(0.01) while inflight?
      end

      def inflight?
        @mutex.synchronize { @available_capacity < @capacity }
      end
    end
  end
end
