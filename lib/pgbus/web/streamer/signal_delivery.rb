# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # The streamer's signal sink in a web process (issue #674): the Listener
      # (or the HubClient, under :master scope) hands it each process-signal
      # NOTIFY and each LISTEN gap; one thread of its own runs the subscribers
      # (Pgbus::Signals.deliver / deliver_gap), so a slow or raising block
      # never stalls the LISTEN loop or the stream dispatcher.
      #
      # The thread starts on the first signal for a registered name (or the
      # first gap with an on_gap registered): a process without subscribers
      # never has one. Messages still queued at #stop are dropped, as a NOTIFY
      # to a stopped process is.
      class SignalDelivery
        STOP_JOIN_SECONDS = 2

        def initialize(logger: Pgbus.logger, signals: Pgbus::Signals)
          @logger = logger
          @signals = signals
          @queue = Queue.new
          # Guards @thread and @stopped: the first signal starts the thread,
          # #stop ends it, from different threads.
          @lock = Mutex.new
          @thread = nil
          @stopped = false
        end

        # Sink API, called on the listener (or hub reader) thread: a queue
        # push and nothing else.
        def signal(channel, payload)
          name = @signals.name_for(channel)
          return unless name

          enqueue([:signal, name, payload])
        end

        def gap
          return unless @signals.gap_handlers?

          enqueue([:gap])
        end

        def threads
          @lock.synchronize { [@thread].compact }
        end

        def stop
          thread = @lock.synchronize do
            @stopped = true
            @thread
          end
          return unless thread

          @queue << [:stop]
          joined = thread.join(STOP_JOIN_SECONDS)
          if joined
            @lock.synchronize { @thread = nil }
          else
            @logger.warn do
              "[Pgbus::Streamer::SignalDelivery] a signal subscriber is still running after #{STOP_JOIN_SECONDS}s"
            end
          end
          nil
        end

        private

        def enqueue(message)
          @lock.synchronize do
            return if @stopped

            # The delivery thread, owned by this object: started once, on the
            # first message, under @lock; #stop ends and joins it.
            @thread ||= Thread.new { run_loop } # rubocop:disable ThreadSafety/NewThread
          end
          @queue << message
        end

        def run_loop
          loop do
            kind, name, payload = @queue.pop
            case kind
            when :stop then break
            when :signal then @signals.deliver(name, payload)
            when :gap then @signals.deliver_gap
            end
          rescue StandardError => e
            # deliver never raises; a bug here must not end delivery.
            @logger.error { "[Pgbus::Streamer::SignalDelivery] delivery failed: #{e.class}: #{e.message}" }
          end
        end
      end
    end
  end
end
