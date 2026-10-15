# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Owns a single dedicated PG::Connection running LISTEN against every
      # stream channel currently serving at least one SSE subscriber. On
      # NOTIFY, posts a WakeMessage into the dispatch queue; the Dispatcher
      # thread does the actual read_after + fanout.
      #
      # Threading:
      #   - #start spawns ONE listener thread
      #   - ensure_listening / remove_listening are called from the
      #     dispatcher thread, which means the listener thread itself is
      #     only running `wait_for_notify` — all LISTEN/UNLISTEN SQL goes
      #     through a command queue that the listener thread drains between
      #     notifies
      #   - #stop clears @running and joins; it never touches the connection
      #   - the connection is SINGLE-OWNER: the listener thread is the only
      #     thread that may exec, wait, or close on it, from construction
      #     through teardown. PG::Connection is not thread-safe and #close is
      #     PQfinish — freeing the PGconn under a concurrent libpq call is a
      #     process-killing SEGV, not a rescuable PG::Error (issue #375)
      #
      # Health check: `wait_for_notify(timeout)` returns nil on timeout. When
      # it does, the listener runs `SELECT 1` as a TCP keepalive. If that
      # raises, the listener rebuilds a FRESH connection via connection_factory
      # and re-LISTENs every channel in `@listening_to`. This is the fix for
      # design doc §11 #1 (silently dropped LISTEN connections from NAT / PG
      # restart / network blips) — a fresh connect also re-resolves DNS and
      # converges on the promoted primary after a failover.
      #
      # NOTIFY channel naming (from pgmq_v1.11.0.sql:1634):
      #   PG_NOTIFY('pgmq.' || TG_TABLE_NAME || '.' || TG_OP, NULL)
      # For a queue named `pgbus_stream_chat` the trigger table is
      # `q_pgbus_stream_chat`, so the channel is `pgmq.q_pgbus_stream_chat.INSERT`.
      class Listener
        WakeMessage = Data.define(:queue_name, :payload) do
          def initialize(queue_name:, payload: nil)
            super
          end
        end

        # Single-sourced from NotifyListener, which owns the pgmq channel
        # format (issue #381 review — the two copies had already drifted apart
        # once in spirit if not in bytes).
        CHANNEL_PREFIX = Pgbus::Process::NotifyListener::CHANNEL_PREFIX
        CHANNEL_SUFFIX = Pgbus::Process::NotifyListener::CHANNEL_SUFFIX

        RECONNECT_BACKOFF_SECONDS = 0.5

        # Grace added to one health-check cycle when #stop joins the listener
        # thread. See #stop_join_timeout.
        STOP_JOIN_GRACE_SECONDS = 5

        attr_reader :listening_to

        # @param connection_factory [#call] builds a FRESH PG connection on each
        #   reconnect attempt (the Instance passes -> { build_raw_pg_connection }).
        #   Required — the reconnect loop always rebuilds rather than resetting a
        #   possibly-dead socket, so every caller (production and tests) must pass
        #   one.
        # dispatch_queue_limit (issue #315 item 3): 0 = unbounded (default). A
        # positive value makes handle_notify DROP a durable wake when the
        # dispatch queue is at/over the cap — safe because the next durable
        # wake for that stream re-reads from the min cursor. Ephemeral wakes
        # and Connect/Disconnect messages are never dropped.
        # `maintenance:` is an optional periodic callback (issue #323 pool
        # autoscaler). On the listener's idle health-check window — the only place
        # the non-thread-safe LISTEN connection is safely idle — it is invoked at
        # most once per `maintenance.interval` seconds with THIS connection, so a
        # maintenance query (e.g. pg_stat_activity headroom) reuses the existing
        # idle connection instead of opening its own. nil disables it.
        def initialize(pg_connection:, dispatch_queue:, health_check_ms:,
                       connection_factory:, dispatch_queue_limit: 0, maintenance: nil,
                       logger: Pgbus.logger, clock: nil)
          raise ArgumentError, "connection_factory is required" unless connection_factory.respond_to?(:call)

          @conn = pg_connection
          @dispatch_queue = dispatch_queue
          @health_check_ms = health_check_ms
          @logger = logger
          @connection_factory = connection_factory
          @dispatch_queue_limit = dispatch_queue_limit
          @maintenance = maintenance
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @maintenance_last = nil
          @dropped_wakes = 0
          @listening_to = Set.new
          @commands = Queue.new
          @running = false
          @thread = nil
        end

        # Count of durable wakes dropped due to the dispatch-queue cap. Read by
        # tests and available for operator introspection. Only mutated on the
        # Listener thread (in handle_notify), so no synchronization needed.
        attr_reader :dropped_wakes

        def start
          return if @running

          @running = true
          # The LISTEN thread, owned by this object: Instance#start starts it
          # once (under Streamer's @current_mutex); #stop clears @running and
          # joins it.
          @thread = Thread.new { run_loop } # rubocop:disable ThreadSafety/NewThread
          self
        end

        # Health signals for the MasterHub's status broadcasts (issue #382).
        # Read cross-thread without synchronization: ivar assignment is atomic
        # in MRI and a momentarily stale value only delays one status tick —
        # these must never touch the connection itself (single-owner, #375).
        def alive?
          !!@thread&.alive?
        end

        def connected?
          !@conn.nil?
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
          @commands << [:stop]
          # Deliberately does NOT touch @conn. PG::Connection is not
          # thread-safe and #close is PQfinish: it frees the PGconn and its
          # OpenSSL objects out from under whatever libpq call the listener
          # thread is making on the same connection. That is a use-after-free —
          # a process-killing SEGV, not a rescuable PG::Error (issue #375).
          # The listener thread is the sole owner of @conn and closes it in
          # run_loop's ensure; clearing @running above is the whole stop signal.
          # It is observed within one wait_for_notify timeout (health_check_ms),
          # which is what the join budget below is sized against.
          @thread&.join(stop_join_timeout)
          @thread = nil
          self
        end

        # Synchronously enable LISTEN on a queue's NOTIFY channel.
        # Blocks until the listener thread has actually executed the
        # LISTEN SQL — this matters because Dispatcher#handle_connect
        # needs the LISTEN to be active *before* it issues read_after,
        # otherwise a broadcast committed in the gap would be neither
        # in the read_after result nor delivered as a WakeMessage.
        #
        # The wait is bounded by the next wait_for_notify timeout
        # (streams_listen_health_check_ms, default 250ms) plus a small
        # margin so a stuck listener can't hang the dispatcher forever.
        def ensure_listening(queue_name)
          ack = Queue.new
          @commands << [:listen, queue_name, ack]
          ack.pop(timeout: ack_timeout)
        end

        # Asynchronous: lazy GC of LISTENs whose subscriber count
        # has dropped to zero. No correctness path depends on this
        # completing before the caller proceeds, so we don't block.
        def remove_listening(queue_name)
          @commands << [:unlisten, queue_name]
        end

        private

        def run_loop
          loop do
            break unless @running

            drain_commands
            break unless @running

            timeout_s = @health_check_ms / 1000.0
            begin
              got_notify = @conn.wait_for_notify(timeout_s) do |channel, _pid, payload|
                handle_notify(channel, payload)
              end
              # Skip the keepalive when a stop landed during the wait: the loop
              # is about to exit and close this connection anyway, so the
              # round-trip would only add latency to shutdown.
              run_health_check if !got_notify && @running
            rescue IOError => e
              # #stop no longer closes the connection to interrupt the wait
              # (issue #375), so an IOError here is a genuine socket failure,
              # not the expected shutdown signal. Reconnect unless we're
              # stopping, in which case exit cleanly.
              break unless @running

              @logger.warn { "[Pgbus::Streamer::Listener] IO error (#{e.class}: #{e.message}) — reconnecting" }
              reconnect!
            rescue PG::Error => e
              break unless @running

              @logger.warn { "[Pgbus::Streamer::Listener] PG error (#{e.class}: #{e.message}) — reconnecting" }
              reconnect!
            end
          end
        ensure
          # Bookkeeping only — no UNLISTEN round-trip. We close the connection
          # on the next line and closing the session deregisters every LISTEN
          # server-side, so the exec bought nothing while being the statement
          # that raced #stop's PQfinish into a SEGV (issue #375).
          @listening_to.clear
          # The listener thread owns this connection, so the listener thread is
          # the one that closes it — #stop only clears @running.
          close_quietly(@conn)
          @conn = nil
        end

        def drain_commands
          loop do
            cmd = @commands.pop(true)
            case cmd[0]
            when :listen
              ack = cmd[2]
              begin
                do_listen(cmd[1])
              ensure
                # Always ack so the caller is never left blocked, even
                # if do_listen raised. The caller can still detect
                # failure by checking @listening_to or by other means;
                # we just promise not to deadlock the dispatcher.
                ack&.push(:done)
              end
            when :unlisten then do_unlisten(cmd[1])
            when :stop     then @running = false
                                return
            end
          rescue ThreadError
            # empty queue
            return
          end
        end

        # Caller wait budget for ensure_listening's ack. The listener
        # thread will process the command at most one wait_for_notify
        # cycle from now (bounded by health_check_ms); add a small
        # safety margin so the dispatcher fails loud rather than
        # hanging if the listener thread is dead.
        def ack_timeout
          (@health_check_ms / 1000.0) + 1.0
        end

        # How long #stop waits for the listener thread to notice the cleared
        # @running and finish teardown. The thread can be parked in
        # wait_for_notify for one full health-check cycle before it re-checks
        # the flag, so the budget is that cycle plus grace — a flat timeout
        # would expire before a listener with a large health_check_ms had even
        # one chance to observe the stop.
        def stop_join_timeout
          (@health_check_ms / 1000.0) + STOP_JOIN_GRACE_SECONDS
        end

        def do_listen(queue_name)
          channel = channel_for(queue_name)
          return if @listening_to.include?(channel)

          @conn.exec(%(LISTEN "#{channel}"))
          @listening_to.add(channel)
        end

        def do_unlisten(queue_name)
          channel = channel_for(queue_name)
          return unless @listening_to.include?(channel)

          @conn.exec(%(UNLISTEN "#{channel}"))
          @listening_to.delete(channel)
        end

        def handle_notify(channel, payload = nil)
          queue_name = queue_name_from(channel)
          return unless queue_name

          # Backpressure applies ONLY to durable wakes (payload nil): they
          # self-heal because the next durable wake for the stream re-reads
          # from the min cursor. Ephemeral wakes (payload present) carry the
          # only copy of their HTML with no archive to replay, so they are
          # never dropped (issue #315 item 3). @dispatch_queue.size on a stdlib
          # Queue is internally synchronized; this runs only on the Listener
          # thread and touches no dispatcher-owned state.
          if payload.nil? && @dispatch_queue_limit.positive? &&
             @dispatch_queue.size >= @dispatch_queue_limit
            @dropped_wakes += 1
            if (@dropped_wakes % 100) == 1
              @logger.warn do
                "[Pgbus::Streamer::Listener] dispatch queue at limit " \
                  "(#{@dispatch_queue_limit}); dropped durable wake for " \
                  "#{queue_name} (total dropped: #{@dropped_wakes})"
              end
            end
            return
          end

          @dispatch_queue << WakeMessage.new(queue_name: queue_name, payload: payload)
        end

        def run_health_check
          @conn.exec("SELECT 1")
          run_maintenance
        end

        # Periodic maintenance on the idle LISTEN connection, throttled to
        # @maintenance.interval (issue #323). Runs on the listener thread, so it
        # is the only place the non-thread-safe @conn may be queried outside
        # wait_for_notify. Fail-soft: a raising maintenance callback is logged and
        # never disturbs the listen loop.
        def run_maintenance
          return unless @maintenance

          now = @clock.call
          return if @maintenance_last && (now - @maintenance_last) < @maintenance.interval

          @maintenance_last = now
          @maintenance.run(@conn)
        rescue StandardError => e
          @logger.debug { "[Pgbus::Streamer::Listener] maintenance raised: #{e.class}: #{e.message}" }
        end

        # Retry reconnect until we succeed (fresh conn + every channel
        # re-LISTENed) or #stop flips @running to false. Mirrors
        # NotifyListener#reconnect!: a single failed attempt never returns with
        # a dead connection while running, so ensure_listening acks can't
        # succeed vacuously against a broken LISTEN socket and strand SSE
        # clients.
        def reconnect!
          # Snapshot the canonical subscription set BEFORE tearing down the old
          # connection. We rebuild @listening_to from this and only publish it
          # once every channel has been re-LISTENed on the new conn, so a
          # mid-loop LISTEN failure never loses channels.
          channels = @listening_to.to_a
          loop do
            return unless @running

            close_quietly(@conn)
            @conn = nil
            new_conn = nil
            begin
              new_conn = @connection_factory.call
              # Reject a replica before re-LISTENing. A fresh connect re-resolves
              # DNS, so backing off and retrying converges on the promoted primary
              # once DNS catches up after a failover. NOTIFY fires only on the
              # primary; re-LISTENing on a replica registers channels that never wake.
              Pgbus::Process::PrimaryValidator.validate_primary!(new_conn)
              channels.each { |channel| new_conn.exec(%(LISTEN "#{channel}")) }
            rescue PG::Error, Pgbus::Process::ReplicaConnectionError, Pgbus::ConfigurationError => e
              # The factory may raise a ConfigurationError (a bad
              # streams_connection_options, e.g. a pooled AR connection) or return
              # a live conn before a later LISTEN raised (or validate_primary!
              # rejected a replica). Close any partial conn so repeated failures
              # don't leak PG connections, then back off and retry rather than
              # letting the exception kill the listener thread silently.
              close_quietly(new_conn)
              Pgbus::ErrorReporter.report(e, { action: "streamer_reconnect" })
              sleep RECONNECT_BACKOFF_SECONDS
              next
            end

            @conn = new_conn
            @listening_to = Set.new(channels)
            return
          end
        end

        # Close a PG connection we're discarding (the old conn at the top of a
        # reconnect cycle, or a half-built one whose LISTEN raised). Best-effort.
        def close_quietly(conn)
          conn&.close if conn.respond_to?(:close)
        rescue StandardError
          nil
        end

        def channel_for(queue_name)
          "#{CHANNEL_PREFIX}#{queue_name}#{CHANNEL_SUFFIX}"
        end

        def queue_name_from(channel)
          return nil unless channel.start_with?(CHANNEL_PREFIX) && channel.end_with?(CHANNEL_SUFFIX)

          channel[CHANNEL_PREFIX.length..-(CHANNEL_SUFFIX.length + 1)]
        end
      end
    end
  end
end
