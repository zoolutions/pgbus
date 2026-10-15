# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Periodic maintenance loop for SSE connections. Runs three sweeps
      # on every tick:
      #
      #   1. Write an SSE comment (": heartbeat <epoch>\n\n") to each
      #      connection. This keeps proxies and load balancers from timing
      #      out idle HTTP responses; most reverse proxies close HTTP
      #      responses that sit idle for 30-60s, which would silently drop
      #      SSE clients.
      #
      #   2. Mark connections that have been idle longer than the
      #      configured idle_timeout as dead. The StreamEventDispatcher's next pass
      #      picks them up via its disconnect path.
      #
      #   3. Post a DisconnectMessage for any connection already flagged
      #      dead (by IoWriter returning :closed / :blocked, or by the
      #      idle sweep above).
      #
      # The heartbeat runs on its own dedicated thread because it does
      # blocking writes (via IoWriter with a deadline) and we don't want
      # to delay the dispatcher. Writes are serialised per-connection by
      # the Connection's own mutex, so concurrent dispatcher + heartbeat
      # writes are safe.
      class Heartbeat
        # presence_expiry: true (streams_presence_patterns is set) posts the
        # presence touch every tick, even with no presence connection here,
        # so the dispatcher can expire seats left by other processes.
        def initialize(registry:, dispatch_queue:, interval:, idle_timeout:, logger: Pgbus.logger, clock: nil,
                       presence_expiry: false)
          @registry = registry
          @queue = dispatch_queue
          @interval = interval
          @idle_timeout = idle_timeout
          @logger = logger
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @presence_expiry = presence_expiry
          @running = false
          @thread = nil
          @wake = ConditionVariable.new
          @wake_mutex = Mutex.new
        end

        def start
          return if @running

          @running = true
          # The heartbeat's one thread, owned by this object: Instance#start
          # starts it once (under Streamer's @current_mutex); #stop signals
          # and joins it.
          @thread = Thread.new { run_loop } # rubocop:disable ThreadSafety/NewThread
          self
        end

        # Snapshot of this component's live thread(s). Instance#shutdown! captures
        # it BEFORE calling #stop so a join that timed out is still observable
        # after #stop has cleared the reference (issue #443).
        def threads
          [@thread].compact
        end

        def stop
          return unless @running

          @running = false
          @wake_mutex.synchronize { @wake.broadcast }
          @thread&.join(5)
          @thread = nil
          self
        end

        # Runs a single sweep synchronously. Useful for tests — production
        # code goes through the background thread.
        def tick
          now = @clock.call
          presence_conns = []
          @registry.each_connection do |connection|
            if connection.dead?
              # Already dead (e.g. IoWriter returned :closed on a previous
              # dispatcher write). Post the disconnect and skip the rest.
              enqueue_disconnect(connection)
              next
            end

            if connection.idle_for > @idle_timeout
              connection.mark_dead!
              enqueue_disconnect(connection)
              next
            end

            result = connection.write_comment("heartbeat #{now.to_i}")
            if connection.dead? || result != :ok
              enqueue_disconnect(connection)
              next
            end

            presence_conns << connection if presence_member?(connection)
          end

          enqueue_presence_touch(presence_conns)
        end

        private

        def run_loop
          while @running
            begin
              tick
            rescue StandardError => e
              @logger.error { "[Pgbus::Streamer::Heartbeat] tick raised: #{e.class}: #{e.message}" }
            end

            @wake_mutex.synchronize do
              @wake.wait(@wake_mutex, @interval) if @running
            end
          end
        end

        def enqueue_disconnect(connection)
          @queue << StreamEventDispatcher::DisconnectMessage.new(connection: connection)
        end

        def presence_member?(connection)
          connection.respond_to?(:presence_member) && !connection.presence_member.nil?
        end

        # Posts one batched touch per tick so the dispatcher (which owns AR
        # connection release) refreshes last_seen_at for live presence
        # members, keeping them out of the sweeper. No message when no
        # connection has presence and seat expiry is off — avoids waking the
        # dispatcher for nothing.
        def enqueue_presence_touch(connections)
          return if connections.empty? && !@presence_expiry

          @queue << StreamEventDispatcher::PresenceTouchMessage.new(connections: connections)
        end
      end
    end
  end
end
