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
      #     dispatcher thread (the master hub's reader threads for a hub).
      #     All LISTEN/UNLISTEN SQL goes through a command queue that the
      #     listener thread drains; the caller then writes a byte to a wake
      #     pipe, so the thread, parked in IO.select on the LISTEN socket and
      #     the pipe, drains the command at once instead of at the end of its
      #     current wait (issue #630)
      #   - #stop clears @running, wakes the thread through the pipe and joins;
      #     it never touches the connection, and the thread closes the pipe
      #     itself on its way out
      #   - the connection is SINGLE-OWNER: the listener thread is the only
      #     thread that may exec, wait, or close on it, from construction
      #     through teardown. PG::Connection is not thread-safe and #close is
      #     PQfinish — freeing the PGconn under a concurrent libpq call is a
      #     process-killing SEGV, not a rescuable PG::Error (issue #375). Only
      #     the pipe is touched from other threads
      #
      # Health check: once per health_check_ms the listener runs `SELECT 1` as
      # a keepalive, on a deadline: an idle connection sees exactly one per
      # cycle, and a stream of wakes or notifications cannot postpone it. The
      # interval only bounds how long a silently dropped LISTEN goes unnoticed:
      # no wait budget is derived from it (ack_timeout_ms is, issue #664). If
      # it raises, the listener rebuilds a FRESH connection via
      # connection_factory and re-LISTENs every channel in `@listening_to`.
      # This is the fix for design doc §11 #1 (silently dropped LISTEN
      # connections from NAT / PG restart / network blips) — a fresh connect
      # also re-resolves DNS and converges on the promoted primary after a
      # failover.
      #
      # NOTIFY channel naming (from pgmq_v1.11.0.sql:1634):
      #   PG_NOTIFY('pgmq.' || TG_TABLE_NAME || '.' || TG_OP, NULL)
      # For a queue named `pgbus_stream_chat` the trigger table is
      # `q_pgbus_stream_chat`, so the channel is `pgmq.q_pgbus_stream_chat.INSERT`.
      #
      # Process signals (issue #674) LISTEN on raw channels beside those
      # (ensure_signal_listening, `<queue_prefix>_signal_<name>`). A NOTIFY on
      # a channel outside the pgmq pattern goes to the `signal_sink:`, never to
      # the dispatch queue and never dropped; a reconnect that re-LISTENed a
      # signal channel tells the sink once (#gap), since NOTIFYs sent while the
      # connection was down are lost. The sink runs on this thread: it only
      # hands off (a queue push).
      class Listener
        # Kept Ractor-shareable (issue #527): the hand-off to the dispatcher
        # is the seam a Ractor dispatcher would pass by reference. The Strings
        # are frozen in place: they are the sender's own (the NOTIFY's, or the
        # hub frame's).
        WakeMessage = Data.define(:queue_name, :payload) do
          def initialize(queue_name:, payload: nil)
            queue_name.freeze
            payload&.freeze
            super
          end
        end

        # Single-sourced from NotifyListener, which owns the pgmq channel
        # format (issue #381 review — the two copies had already drifted apart
        # once in spirit if not in bytes).
        CHANNEL_PREFIX = Pgbus::Process::NotifyListener::CHANNEL_PREFIX
        CHANNEL_SUFFIX = Pgbus::Process::NotifyListener::CHANNEL_SUFFIX

        RECONNECT_BACKOFF_SECONDS = 0.5

        # A process signal channel (issue #674): `<queue_prefix>_signal_<name>`,
        # within PostgreSQL's 63-byte identifier limit.
        SIGNAL_CHANNEL_PATTERN = /\A[a-z0-9_]{1,63}\z/

        # Grace added to the ack budget when #stop joins the listener thread.
        # See #stop_join_timeout.
        STOP_JOIN_GRACE_SECONDS = 5

        # Default for ack_timeout_ms: the old 250 ms cycle plus its 1 s margin,
        # now fixed instead of following the keepalive (issue #664).
        DEFAULT_ACK_TIMEOUT_MS = 1_250

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
        # `ack_timeout_ms:` bounds how long ensure_listening waits for this
        # thread's ack, and (plus a grace) how long #stop joins it
        # (streams_listen_ack_timeout_ms; issue #664).
        # `signal_sink:` receives process signals (issue #674): #signal(channel,
        # payload) per NOTIFY on a signal channel and #gap after a reconnect
        # that re-LISTENed one. nil: such NOTIFYs are ignored.
        def initialize(pg_connection:, dispatch_queue:, health_check_ms:,
                       connection_factory:, ack_timeout_ms: DEFAULT_ACK_TIMEOUT_MS, dispatch_queue_limit: 0,
                       maintenance: nil, signal_sink: nil, logger: Pgbus.logger, clock: nil)
          raise ArgumentError, "connection_factory is required" unless connection_factory.respond_to?(:call)

          @conn = pg_connection
          @dispatch_queue = dispatch_queue
          @health_check_ms = health_check_ms
          @ack_timeout_ms = ack_timeout_ms
          @logger = logger
          @connection_factory = connection_factory
          @dispatch_queue_limit = dispatch_queue_limit
          @maintenance = maintenance
          @signal_sink = signal_sink
          @clock = clock || -> { ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) }
          @maintenance_last = nil
          @dropped_wakes = 0
          @listening_to = Set.new
          @commands = Queue.new
          @running = false
          # Set by #stop: no thread will serve a command again (a listener is
          # never restarted in production), so ensure_listening answers at once.
          @stopped = false
          @thread = nil
          @health_check_due_at = nil
          # ensure_listening acks held back while the reconnect that re-LISTENs
          # their channel runs. Listener thread only.
          @acks_after_reconnect = []
          # The wake pipe is created by #start, so a listener that never
          # starts holds no descriptors.
          @wake_reader = nil
          @wake_writer = nil
        end

        # Count of durable wakes dropped due to the dispatch-queue cap. Read by
        # tests and available for operator introspection. Only mutated on the
        # Listener thread (in handle_notify), so no synchronization needed.
        attr_reader :dropped_wakes

        def start
          return if @running

          # A previous thread that outlived #stop's join still owns the
          # connection; a second loop on it is the cross-thread use #375 is
          # about.
          if @thread&.alive?
            @logger.warn { "[Pgbus::Streamer::Listener] previous listener thread still running; not starting another" }
            return
          end

          @wake_reader, @wake_writer = IO.pipe if @wake_reader.nil? || @wake_reader.closed? || @wake_writer.closed?
          @stopped = false
          @running = true
          # The LISTEN thread, owned by this object: Instance#start starts it
          # once (under Streamer's @current_mutex); #stop clears @running,
          # wakes it and joins it.
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
        # it BEFORE calling #stop (issue #443). #stop clears the reference only
        # when its join returned; a thread past its budget stays listed.
        def threads
          [@thread].compact
        end

        def stop
          unless @running
            # Never started (or already stopped): nothing to join, but no
            # thread will serve a command either.
            @stopped = true
            return
          end

          # @stopped before @running: the thread exits (and releases what is
          # queued) only after it sees @running false, so by then @stopped is
          # visible to ensure_listening's second check (MRI's GVL orders these
          # plain ivar writes and reads).
          @stopped = true
          @running = false
          # Deliberately does NOT touch @conn. PG::Connection is not
          # thread-safe and #close is PQfinish: it frees the PGconn and its
          # OpenSSL objects out from under whatever libpq call the listener
          # thread is making on the same connection. That is a use-after-free —
          # a process-killing SEGV, not a rescuable PG::Error (issue #375).
          # The listener thread is the sole owner of @conn and closes it in
          # run_loop's ensure; clearing @running above is the whole stop signal,
          # and the wake makes the thread see it at once. The join budget
          # (the ack budget plus a grace, 6.25 s by default) covers a thread
          # inside a normal keepalive or reconnect attempt. A keepalive stuck
          # on a dead socket can outlast it (libpq's TCP bound is ~11 s): #stop
          # then logs "did not stop" and the thread exits on its own once
          # libpq raises.
          wake
          joined = @thread.nil? || @thread.join(stop_join_timeout)
          # A thread past its budget stays referenced, so #start refuses to
          # run a second loop beside it; it closes its own pipe when it exits.
          if joined
            @thread = nil
          else
            @logger.warn do
              "[Pgbus::Streamer::Listener] listener thread did not stop within #{stop_join_timeout}s; " \
                "it closes its wake pipe when it exits"
            end
          end
          self
        end

        # Synchronously enable LISTEN on a queue's NOTIFY channel.
        # Blocks until the listener thread has actually executed the
        # LISTEN SQL — this matters because Dispatcher#handle_connect
        # needs the LISTEN to be active *before* it issues read_after,
        # otherwise a broadcast committed in the gap would be neither
        # in the read_after result nor delivered as a WakeMessage.
        #
        # The wake pipe makes the typical wait a thread hop (issue #630). The
        # wait is bounded by the ack budget (streams_listen_ack_timeout_ms,
        # default 1.25 s; issue #664), so a stuck listener can't hang the
        # dispatcher forever.
        def ensure_listening(queue_name)
          request_listen(:listen, queue_name)
        end

        # Synchronously LISTEN on a process signal's raw channel (issue #674),
        # with ensure_listening's ack contract: once acked, a NOTIFY on it
        # reaches the signal sink.
        def ensure_signal_listening(channel)
          request_listen(:listen_channel, validate_signal_channel!(channel))
        end

        # Asynchronous UNLISTEN of a signal channel (the master hub, when its
        # last worker subscriber leaves).
        def remove_signal_listening(channel)
          @commands << [:unlisten_channel, validate_signal_channel!(channel)]
          wake
        end

        # Asynchronous: lazy GC of LISTENs whose subscriber count
        # has dropped to zero. No correctness path depends on this
        # completing before the caller proceeds, so we don't block.
        def remove_listening(queue_name)
          @commands << [:unlisten, queue_name]
          wake
        end

        private

        # The channel is interpolated into LISTEN "…": only the identifier
        # characters Pgbus::Signals.channel_for produces may reach it (a hub
        # worker's frame included).
        def validate_signal_channel!(channel)
          return channel if channel.is_a?(String) && SIGNAL_CHANNEL_PATTERN.match?(channel)

          raise ArgumentError, "invalid signal channel #{channel.inspect}"
        end

        def request_listen(kind, name)
          # A connect the dispatcher drains during shutdown (Instance#shutdown!
          # stops the listener first) must not wait out the ack budget.
          return nil if @stopped

          ack = Queue.new
          @commands << [kind, name, ack]
          wake
          # A stop that landed between the check above and the push may have
          # let the thread release its queue and exit before the push: no ack
          # would ever come. Pushed before that release, the command is acked.
          return nil if @stopped

          result = ack.pop(timeout: ack_timeout)
          # Abandoned: the thread sees the closed queue when it gets to the
          # command, and a signal LISTEN that lands this late reports a gap
          # (the caller already went on as if it were listening).
          ack.close if result.nil?
          result
        end

        def run_loop
          schedule_health_check
          loop do
            break unless @running

            begin
              # Inside the rescue: a LISTEN that raises (the connection died
              # under it) reconnects instead of killing this thread.
              drain_commands
              break unless @running

              wait_once
            rescue IOError, Errno::EBADF => e
              # #stop never closes the connection to interrupt the wait (issue
              # #375), so an IOError here — a select on a socket closed under
              # it — is a genuine socket failure, not the shutdown signal.
              # Reconnect unless we're stopping, in which case exit cleanly.
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
          release_held_acks
          release_pending_acks
          # The thread that selects on the pipe is the one that closes it, so
          # no IO is ever closed under a live select (the cross-thread
          # teardown #375 removed), whether #stop's join returned or not.
          close_wake_pipe
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

        # One pass of the wait: dispatch what libpq has buffered, then park in
        # IO.select on the LISTEN socket and the wake pipe until either is
        # readable or the next keepalive is due. A wake returns to run_loop,
        # which drains the commands. This keeps pg's own wait_for_notify order:
        # buffered notifications first, then the socket, consume_input, and the
        # buffer again.
        def wait_once
          # An exec (LISTEN, SELECT 1) may have buffered a notification while
          # reading its reply, leaving nothing on the socket to wake the select.
          drain_notifies
          timeout = @health_check_due_at - @clock.call
          # Never a zero or negative select timeout: zero is a non-blocking
          # poll and would spin.
          return run_health_check unless timeout.positive?

          socket = @conn.socket_io
          ready, = IO.select([socket, @wake_reader], nil, nil, timeout)
          # Idle for a full cycle. Skip the keepalive when a stop landed during
          # the wait: the loop is about to exit and close this connection
          # anyway, so the round-trip would only add latency to shutdown.
          return (run_health_check if @running) if ready.nil?

          drain_wake if ready.include?(@wake_reader)
          read_socket if ready.include?(socket)
        end

        def read_socket
          @conn.consume_input
          # A peer-closed socket stays readable (EOF) whether or not
          # consume_input raised: check the status, or the loop would spin on
          # it with the keepalive no longer the thing that notices.
          raise PG::ConnectionBad, "LISTEN connection lost" unless @conn.status == PG::CONNECTION_OK

          drain_notifies
        end

        # PG::Connection#notifies reports a NOTIFY without a payload as
        # extra: "" (wait_for_notify yields nil for it). A durable PGMQ wake
        # has no payload and must reach handle_notify as nil, or it would be
        # routed as an ephemeral one.
        def drain_notifies
          while (notification = @conn.notifies)
            extra = notification[:extra]
            handle_notify(notification[:relname], extra.nil? || extra.empty? ? nil : extra)
          end
        end

        # Before #start there is no pipe: the command waits in the queue and
        # the thread drains it on its first pass.
        def wake
          @wake_writer&.write_nonblock(".", exception: false)
        rescue IOError, Errno::EPIPE
          # Closed by #stop (IOError), or its reader closed first (EPIPE):
          # nothing is left to wake.
          nil
        end

        # One read empties the pipe of every wake written so far (each is
        # one byte), so a burst of commands costs one extra select return.
        def drain_wake
          @wake_reader.read_nonblock(4096, exception: false)
        end

        def close_wake_pipe
          # Writer first: a late wake then meets a closed writer (IOError), not
          # a writer whose reader is gone (EPIPE). Both are rescued in #wake.
          @wake_writer.close unless @wake_writer.closed?
          @wake_reader.close unless @wake_reader.closed?
        end

        # Acks held back by a LISTEN that raised: the reconnect that
        # re-LISTENed their channel is done (or the thread is exiting).
        def release_held_acks
          @acks_after_reconnect.each { |ack| release_ack(ack) }
          @acks_after_reconnect.clear
        end

        # Commands #stop left in the queue: release every ensure_listening
        # caller now rather than at its ack timeout. Nothing is LISTENed: the
        # connection closes right after this.
        def release_pending_acks
          loop do
            cmd = @commands.pop(true)
            release_ack(cmd[2]) if cmd[2] && %i[listen listen_channel].include?(cmd[0])
          end
        rescue ThreadError
          nil
        end

        def schedule_health_check
          @health_check_due_at = @clock.call + (@health_check_ms / 1000.0)
        end

        def drain_commands
          loop do
            # #stop cleared the flag: run_loop is about to exit and close the
            # connection, so the rest of the queue is moot.
            return unless @running

            # A backlog that never empties must not postpone the keepalive.
            run_health_check unless (@health_check_due_at - @clock.call).positive?
            cmd = @commands.pop(true)
            case cmd[0]
            when :listen, :listen_channel
              ack = cmd[2]
              begin
                do_listen_channel(cmd[0] == :listen ? channel_for(cmd[1]) : cmd[1])
                report_late_signal_listen if cmd[0] == :listen_channel && ack&.closed?
              rescue StandardError
                # The connection failed under the LISTEN. run_loop
                # reconnects, and reconnect! re-LISTENs this channel with the
                # rest; the caller is acked once that is done, so its
                # read_after never runs ahead of an active LISTEN.
                @acks_after_reconnect << ack if ack
                ack = nil
                raise
              ensure
                release_ack(ack) if ack
              end
            when :unlisten then do_unlisten_channel(channel_for(cmd[1]))
            when :unlisten_channel then do_unlisten_channel(cmd[1])
            end
          rescue ThreadError
            # empty queue
            return
          end
        end

        # Caller wait budget for ensure_listening's ack. The wake pipe
        # makes the usual wait a thread hop; a thread busy in a keepalive
        # or a reconnect gets ack_timeout_ms, so the dispatcher fails loud
        # rather than hanging if the thread is dead. Not derived from the
        # keepalive interval: raising that must not slow this failure path
        # (issue #664).
        def ack_timeout
          @ack_timeout_ms / 1000.0
        end

        # How long #stop waits for the listener thread to notice the cleared
        # @running and finish teardown. The wake pipe interrupts its select at
        # once, but it may be inside a keepalive or a reconnect attempt, so the
        # budget is the ack budget plus grace, independent of the keepalive.
        def stop_join_timeout
          ack_timeout + STOP_JOIN_GRACE_SECONDS
        end

        # The channel joins @listening_to BEFORE the exec: if the LISTEN
        # raises, run_loop reconnects, reconnect! re-LISTENs it with the rest,
        # and only then is the caller acked (drain_commands), so an acked
        # ensure_listening means an active LISTEN (the dispatcher's
        # no-second-wait rule rests on that, issue #630). The exception is a
        # thread on its way out (stop, or an unexpected error): run_loop's
        # ensure releases every waiting caller with nothing LISTENed.
        def do_listen_channel(channel)
          return if @listening_to.include?(channel)

          @listening_to.add(channel)
          @conn.exec(%(LISTEN "#{channel}"))
        end

        # The channel leaves @listening_to BEFORE the exec, so a reconnect
        # after a failed UNLISTEN does not LISTEN it again.
        def do_unlisten_channel(channel)
          return unless @listening_to.include?(channel)

          @listening_to.delete(channel)
          @conn.exec(%(UNLISTEN "#{channel}"))
        end

        def handle_notify(channel, payload = nil)
          queue_name = queue_name_from(channel)
          return handle_signal(channel, payload) unless queue_name

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

        # A waiter that timed out closed its queue (request_listen).
        def release_ack(ack)
          ack.push(:done) unless ack.closed?
        rescue ClosedQueueError
          nil
        end

        # ensure_signal_listening timed out before this LISTEN ran, so its
        # caller went on as if the channel were live: whatever was sent in
        # between is a gap.
        def report_late_signal_listen
          @signal_sink&.gap
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::Listener] signal sink gap raised: #{e.class}: #{e.message}" }
        end

        # A NOTIFY outside the pgmq pattern: only signal channels are LISTENed
        # besides those. Never dropped, and a raising sink never stops the loop.
        def handle_signal(channel, payload)
          @signal_sink&.signal(channel, payload)
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::Listener] signal sink raised: #{e.class}: #{e.message}" }
        end

        # After a reconnect: NOTIFYs on a signal channel sent while the
        # connection was down are lost, so the sink hears about the gap once
        # the new LISTENs are in place. Stream channels need no such report
        # (a durable wake re-reads from the cursor).
        def report_gap(channels)
          return unless @signal_sink && channels.any? { |channel| !pgmq_channel?(channel) }

          @signal_sink.gap
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::Listener] signal sink gap raised: #{e.class}: #{e.message}" }
        end

        # The next keepalive is due one cycle after this one ENDS: scheduled
        # from its start, a keepalive slower than a cycle would leave every
        # pass overdue and the loop running SELECT 1 back to back without ever
        # selecting on the socket or the pipe.
        def run_health_check
          @conn.exec("SELECT 1")
          run_maintenance
        ensure
          schedule_health_check
        end

        # Periodic maintenance on the idle LISTEN connection, throttled to
        # @maintenance.interval (issue #323). Runs on the listener thread, so it
        # is the only place the non-thread-safe @conn may be queried outside
        # the listen loop itself. Fail-soft: a raising maintenance callback is logged and
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
            report_gap(channels)
            return
          end
        ensure
          release_held_acks
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

        def pgmq_channel?(channel)
          channel.start_with?(CHANNEL_PREFIX) && channel.end_with?(CHANNEL_SUFFIX)
        end

        def queue_name_from(channel)
          return nil unless pgmq_channel?(channel)

          channel[CHANNEL_PREFIX.length..-(CHANNEL_SUFFIX.length + 1)]
        end
      end
    end
  end
end
