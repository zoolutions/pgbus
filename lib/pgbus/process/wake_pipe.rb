# frozen_string_literal: true

module Pgbus
  module Process
    # Fork-side receiver for supervisor-mediated wake-ups (issue #381,
    # worker_notify_scope: :supervisor). The supervisor's NotifyHub holds the
    # write end of a per-fork pipe; this class owns the inherited read end and
    # a single watcher thread that translates the byte protocol into the
    # fork's existing primitives:
    #
    #   W — a NOTIFY arrived for a queue this fork reads → wake_signal.notify!
    #   H — the shared listener is healthy (connected + delivering) → the
    #       fork may sleep up to the NOTIFY poll ceiling
    #   P — the shared listener is degraded (dead thread, mid-reconnect,
    #       pooler-deaf) → the fork falls back to fast polling
    #
    # Status starts optimistic (mirrors NotifyListener's @delivering default)
    # so a just-forked worker isn't pinned to fast polling before the hub's
    # first broadcast. EOF on the pipe means the supervisor is gone: mark
    # not-delivering and let the fork run on plain polling.
    #
    # A nil reader (scope :fork, or the supervisor never armed the pipe)
    # yields an inert instance: start is a no-op and delivering? is false, so
    # wake_timeout math treats it exactly like an absent listener.
    class WakePipe
      WAKE = "W"
      HEALTHY = "H"
      DEGRADED = "P"

      def initialize(reader, wake_signal:, logger: Pgbus.logger)
        @reader = reader
        @wake_signal = wake_signal
        @logger = logger
        @state_mutex = Mutex.new
        @running = false
        @thread = nil
        @delivering = !reader.nil?
      end

      def delivering?
        @state_mutex.synchronize { @delivering }
      end

      def running?
        @state_mutex.synchronize { @running }
      end

      def start
        return self if @reader.nil?

        @state_mutex.synchronize do
          return self if @running

          @running = true
        end
        # The pipe watcher's one thread, owned by this object: started once
        # (the @running check above, under @state_mutex) and stopped by #stop,
        # which closes the pipe under it and joins it.
        @thread = Thread.new { run_loop } # rubocop:disable ThreadSafety/NewThread
        self
      end

      def stop
        @state_mutex.synchronize do
          return self unless @running

          @running = false
        end
        # Closing the pipe FD interrupts the watcher's blocking readpartial —
        # Ruby raises IOError in the blocked thread at the interpreter level.
        # (Safe for a plain IO, unlike PG::Connection#close, which is PQfinish
        # under a concurrent libpq call — issue #375. That constraint is about
        # libpq, not IO.)
        close_reader_quietly
        @thread&.join(2)
        @thread = nil
        self
      end

      private

      def run_loop
        loop do
          break unless running?

          handle_bytes(@reader.readpartial(4096))
        end
      rescue EOFError
        # Supervisor exited: no more wakes will ever arrive on this pipe.
        mark_not_delivering("supervisor wake pipe reached EOF — falling back to polling")
      rescue IOError, Errno::EBADF
        # Reader closed under us. Expected during #stop (running? already
        # false); anything else degrades to polling like EOF.
        mark_not_delivering("supervisor wake pipe closed — falling back to polling") if running?
      rescue StandardError => e
        mark_not_delivering("supervisor wake pipe failed (#{e.class}: #{e.message}) — falling back to polling")
      ensure
        @state_mutex.synchronize { @running = false }
      end

      # One coalesced read may carry several bytes. Status transitions apply
      # in order; any number of W bytes collapses into one notify! (WakeSignal
      # coalesces concurrent notifies anyway).
      def handle_bytes(data)
        wake = false
        data.each_char do |byte|
          case byte
          when WAKE then wake = true
          when HEALTHY then update_delivering(true)
          when DEGRADED then update_delivering(false)
          end
        end
        @wake_signal.notify! if wake
      end

      def update_delivering(value)
        @state_mutex.synchronize { @delivering = value }
      end

      def mark_not_delivering(message)
        update_delivering(false)
        @logger.warn { "[Pgbus::WakePipe] #{message}" }
      end

      def close_reader_quietly
        @reader.close if @reader && !@reader.closed?
      rescue IOError, Errno::EBADF
        nil
      end
    end
  end
end
