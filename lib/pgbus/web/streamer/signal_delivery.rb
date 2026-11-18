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
        # Signals waiting behind a slow or stuck subscriber. Past this, a
        # signal is dropped and reported as a gap instead (on_gap is the
        # recovery for a missed signal), so the backlog never grows unbounded.
        BACKLOG_LIMIT = 1_000

        def initialize(logger: Pgbus.logger, signals: Pgbus::Signals)
          @logger = logger
          @signals = signals
          @queue = Queue.new
          # Guards @thread, @stopped and @gap_pending: the first signal starts
          # the thread, #stop ends it, from different threads.
          @lock = Mutex.new
          @thread = nil
          @stopped = false
          # A gap queued and not yet run: a second gap before it runs (a
          # failover during the first LISTEN, say) adds nothing.
          @gap_pending = false
        end

        # Sink API, called on the listener (or hub reader) thread: a queue
        # push and nothing else.
        def signal(channel, payload)
          name = @signals.name_for(channel)
          return unless name
          return overflow(name) if @queue.size >= BACKLOG_LIMIT

          enqueue([:signal, name, payload])
        end

        def gap
          return unless @signals.gap_handlers?

          already_queued = @lock.synchronize do
            pending = @gap_pending
            @gap_pending = true
            pending
          end
          return if already_queued

          begin
            enqueue([:gap])
          rescue StandardError
            # Never queued (the delivery thread could not start): a later gap
            # must not be coalesced into this one.
            @lock.synchronize { @gap_pending = false }
            raise
          end
        end

        def threads
          @lock.synchronize { [@thread].compact }
        end

        def stop
          # Under @lock with enqueue's push: nothing lands behind the sentinel,
          # and what is still queued is dropped rather than run at shutdown.
          thread = @lock.synchronize do
            @stopped = true
            @queue.clear
            @queue << [:stop] if @thread
            @thread
          end
          return unless thread

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
            @queue << message
          end
        end

        # One warning per overflow episode: while a gap is already pending it
        # covers every signal dropped behind it.
        def overflow(name)
          return gap if @lock.synchronize { @gap_pending }

          @logger.warn do
            "[Pgbus::Streamer::SignalDelivery] #{BACKLOG_LIMIT} signals waiting on a slow subscriber; " \
              "dropped #{name.inspect} and reporting a gap"
          end
          gap
        end

        def run_loop
          loop do
            kind, name, payload = @queue.pop
            case kind
            when :stop then break
            when :signal then @signals.deliver(name, payload)
            when :gap
              @lock.synchronize { @gap_pending = false }
              @signals.deliver_gap
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
