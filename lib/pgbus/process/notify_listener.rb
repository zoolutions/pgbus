# frozen_string_literal: true

module Pgbus
  module Process
    # Owns a single dedicated PG::Connection that LISTENs on the INSERT NOTIFY
    # channel of every queue a Worker/Consumer reads, and fires a WakeSignal the
    # moment any of them receives a row. This converts the worker/consumer loop
    # from "blind-read every polling_interval" into "sleep until a real insert,
    # poll only as a fallback" — eliminating the empty-read storm that dominates
    # DB load on idle queues.
    #
    # pgmq-ruby's `wait_for_notify(queue, timeout:)` is single-queue and wraps
    # the wait in `with_connection`, which only watches one channel and holds the
    # pooled connection for the whole wait. Neither fits a worker that reads N
    # queues on a small shared pool. So we own ONE raw PG::Connection and
    # hand-roll per-channel LISTEN on it.
    #
    # A persistent LISTEN connection silently dies under a transaction-pool
    # PgBouncer (LISTEN does not survive COMMIT boundaries). Point this
    # connection at a DIRECT port via `config.worker_notify_*` overrides.
    # The health-check-on-timeout catches a connection killed out from under us
    # and re-LISTENs everything.
    #
    # NOTIFY channel naming (pgmq trigger): PG_NOTIFY('pgmq.' || table || '.' ||
    # TG_OP). For queue `pgbus_default` the table is `q_pgbus_default`, so the
    # channel is `pgmq.q_pgbus_default.INSERT`.
    #
    # Thread safety: @running, @conn, and @listening_to are guarded by
    # @state_mutex. The listener thread owns @conn during wait_for_notify (a
    # blocking IO call where the mutex MUST NOT be held), so wait_once reads
    # the connection out of the mutex first and operates on a local. Reconnect
    # publishes the new connection + channel set under the mutex.
    #
    # The mutex only makes the ivar READ safe. PG::Connection itself is not
    # thread-safe, so the connection is single-owner: the listener thread is
    # the ONLY thread that may exec, wait, or close on it, from build through
    # teardown. #stop signals by clearing @running and joining — it never
    # touches the connection, because #close is PQfinish and freeing the
    # PGconn under a concurrent libpq call is a process-killing SEGV, not a
    # rescuable PG::Error (issue #375).
    class NotifyListener
      CHANNEL_PREFIX = "pgmq.q_"
      CHANNEL_SUFFIX = ".INSERT"

      # Inverse of #channel_for: map a NOTIFY channel back to the physical
      # queue name. Class-level because the channel format is owned here —
      # NotifyHub (wake routing + union refresh) and Worker (queue-set sync)
      # both consume it.
      def self.physical_for(channel)
        channel.delete_prefix(CHANNEL_PREFIX).delete_suffix(CHANNEL_SUFFIX)
      end

      RECONNECT_BACKOFF_SECONDS = 0.5

      # Grace added to one health-check cycle when #stop joins the listener
      # thread. See #stop_join_timeout.
      STOP_JOIN_GRACE_SECONDS = 5

      def initialize(physical_queues:, on_wake:, connection_options:,
                     health_check_ms: 1000, logger: Pgbus.logger)
        @physical_queues = Array(physical_queues)
        @on_wake = on_wake
        @connection_options = connection_options
        @health_check_ms = health_check_ms
        @logger = logger
        @state_mutex = Mutex.new
        @listening_to = Set.new
        @commands = Queue.new
        @running = false
        @thread = nil
        @conn = nil
        # Optimistic until the start-time self-probe runs: assume NOTIFY delivery
        # works so a not-yet-probed listener isn't mistaken for a pooler-deaf one.
        @delivering = true
      end

      def listening_to
        @state_mutex.synchronize { @listening_to.dup }
      end

      # Whether a live PG connection is currently published. running? stays
      # true during a reconnect (the thread is alive, looping in reconnect!),
      # so this is the signal that distinguishes "parked in wait_for_notify"
      # from "between connections". The supervisor NotifyHub (issue #381)
      # consults it to broadcast degraded status to forks the moment the
      # shared connection drops, and healthy again once it is rebuilt.
      def connected?
        @state_mutex.synchronize { !@conn.nil? }
      end

      # Whether the start-time self-probe confirmed this connection can actually
      # receive a NOTIFY. False when a transaction-mode pooler or replica
      # silently drops LISTEN: the thread is still alive (running? == true) but
      # will never wake the loop. The Worker/Consumer consult this so a
      # live-but-deaf listener is treated as absent for wake-timeout purposes —
      # fast polling, not the 15s NOTIFY ceiling (issue #332).
      def delivering?
        @state_mutex.synchronize { @delivering }
      end

      def start
        @state_mutex.synchronize do
          return self if @running

          @running = true
        end
        @physical_queues.each { |q| @commands << [:listen, q] }
        # The listener's one thread, owned by this object: started once (the
        # @running check above, under @state_mutex), the sole owner of @conn,
        # and joined by #stop after it clears @running.
        @thread = Thread.new { run_loop } # rubocop:disable ThreadSafety/NewThread
        self
      end

      def stop
        @state_mutex.synchronize do
          return self unless @running

          @running = false
        end
        @commands << [:stop]
        # Deliberately does NOT touch @conn. PG::Connection is not thread-safe
        # and #close is PQfinish: it frees the PGconn and its OpenSSL objects
        # out from under whatever libpq call the listener thread is making on
        # the same connection. That is a use-after-free — a process-killing
        # SEGV, not a rescuable PG::Error (issue #375). The listener thread is
        # the sole owner of @conn for its entire life and closes it in
        # run_loop's ensure; clearing @running above is the whole stop signal.
        @thread&.join(stop_join_timeout)
        @thread = nil
        self
      end

      def add_queue(physical_queue)
        @commands << [:listen, physical_queue]
      end

      def remove_queue(physical_queue)
        @commands << [:unlisten, physical_queue]
      end

      # Public so the owning worker can detect a listener whose thread died
      # (run_loop hit a fatal error and cleared @running in its ensure) and
      # restart it. Guarded by @state_mutex like every other @running access.
      def running?
        @state_mutex.synchronize { @running }
      end

      # Called ONLY inside a just-forked child (issues #381 / #437): release
      # this process's copy of the LISTEN socket WITHOUT PQfinish — #close
      # would send a libpq Terminate over the socket shared with the parent,
      # killing the parent's LISTEN session.
      #
      # Closing the IO wrapper is not enough: pg builds socket_io with
      # autoclose=false, so IO#close leaves the fd open, and when GC frees the
      # inherited PG::Connection its PQfinish still writes Terminate on that
      # fd — i.e. on the PARENT's connection (issue #437, one reconnect per
      # fork). Repointing the fd at /dev/null (IO#reopen, the ActiveRecord
      # PostgreSQLAdapter#discard! idiom) makes the eventual PQfinish harmless.
      #
      # The listener thread does not exist in the child (fork copies only the
      # calling thread), so there is no concurrent owner and the single-owner
      # rule (#375) does not apply.
      def close_inherited_socket!
        conn = @state_mutex.synchronize do
          c = @conn
          @conn = nil
          @running = false
          c
        end
        conn&.socket_io&.reopen(IO::NULL)
      rescue StandardError => e
        # Best-effort but never silent: the child keeps booting either way.
        # If this fails the child's GC-time PQfinish will hit the parent's
        # session, which the parent's reconnect! survives.
        @logger.warn do
          "[Pgbus::NotifyListener] inherited socket cleanup failed: #{e.class}: #{e.message}"
        end
        nil
      end

      private

      def run_loop
        conn = build_connection
        # Reject a connection that landed on a read-only replica before doing
        # anything else. After a failover, stale DNS can point this fresh
        # connection at the demoted master; NOTIFY fires only on the primary,
        # so we'd sit deaf forever. A replica here raises ReplicaConnectionError
        # and the rescue below runs the fatal/ensure path (worker-level startup
        # retry is a separate concern); a reconnect converges on the primary.
        PrimaryValidator.validate_primary!(conn)
        # One-shot delivery self-probe on the initial connection only. A pooler
        # or replica that silently breaks LISTEN/NOTIFY is surfaced here with an
        # actionable error; the listener still runs and degrades to polling.
        # Reconnects skip the probe to stay cheap. Record the result so the
        # owning worker can drop back to fast polling instead of the 15s NOTIFY
        # ceiling when this connection can't actually deliver (issue #332).
        delivering = NotifyProbe.probe_notify_delivery!(conn, logger: @logger)
        @state_mutex.synchronize { @delivering = delivering }
        @state_mutex.synchronize { @conn = conn }
        drain_commands

        loop do
          break unless running?

          drain_commands
          break unless running?

          wait_once
        end
      rescue StandardError => e
        # Report, don't just log: a listener that dies at boot silently
        # degrades every worker to polling (issue #352).
        ErrorReporter.report(e, { action: "notify_listener_fatal" }) if running?
      ensure
        # Clear @running so #start can spawn a fresh thread after a fatal exit
        # (e.g. build_connection raising at boot). Without this, the dead
        # thread's @running stays true and #start returns early forever.
        #
        # The LISTEN set is dropped as bookkeeping only — no UNLISTEN
        # round-trip. We close on the next line and closing the session
        # deregisters every LISTEN server-side, so the exec bought nothing
        # while being the statement that raced #stop into a SEGV (issue #375).
        #
        # Capturing @conn in the SAME critical section that clears @running is
        # what makes restart safe. #start is public and guarded only by
        # @running, so a caller watching running? may spawn a fresh thread the
        # instant it flips. If teardown read @conn in a later critical section
        # it could pick up the NEW thread's connection and PQfinish it mid-use
        # — the same use-after-free, reached through restart instead of #stop.
        conn = @state_mutex.synchronize do
          @running = false
          @listening_to.clear
          c = @conn
          @conn = nil
          c
        end
        close_quietly(conn)
      end

      def wait_once
        conn = @state_mutex.synchronize { @conn }
        return reconnect! unless conn

        timeout_s = @health_check_ms / 1000.0
        got_notify = conn.wait_for_notify(timeout_s) do |channel, _pid, _payload|
          # The channel rides along so a hub caller (issue #381) can route the
          # wake to the fork(s) reading that queue; fork-owned listeners take
          # ->(_channel) and ignore it.
          @on_wake.call(channel)
        end
        # Skip the keepalive when a stop landed during the wait: the loop is
        # about to exit and close this connection anyway, so the round-trip
        # would only add latency to shutdown.
        run_health_check(conn) if !got_notify && running?
      rescue IOError, PG::Error => e
        return unless running?

        @logger.warn { "[Pgbus::NotifyListener] connection error (#{e.class}: #{e.message}) — reconnecting" }
        reconnect!
      end

      def drain_commands
        loop do
          cmd = @commands.pop(true)
          case cmd[0]
          when :listen   then do_listen(cmd[1])
          when :unlisten then do_unlisten(cmd[1])
          when :stop
            @state_mutex.synchronize { @running = false }
            return
          end
        rescue ThreadError
          return
        end
      end

      def do_listen(physical_queue)
        channel = channel_for(physical_queue)
        conn = @state_mutex.synchronize do
          return if @listening_to.include?(channel)

          @conn
        end
        return unless conn

        conn.exec(%(LISTEN "#{channel}"))
        @state_mutex.synchronize { @listening_to.add(channel) }
      end

      def do_unlisten(physical_queue)
        channel = channel_for(physical_queue)
        conn = @state_mutex.synchronize do
          return unless @listening_to.include?(channel)

          @conn
        end
        return unless conn

        conn.exec(%(UNLISTEN "#{channel}"))
        @state_mutex.synchronize { @listening_to.delete(channel) }
      end

      def run_health_check(conn)
        conn.exec("SELECT 1")
      end

      # Retry reconnect until either we succeed (new conn + every channel
      # re-LISTENed) or @running flips to false. Without the loop, a single
      # PG::Error during build/LISTEN left @conn nil and the listener degraded
      # silently — wait_once would re-enter and fail forever or run with an
      # incomplete subscription set.
      def reconnect!
        channels = @state_mutex.synchronize { @listening_to.to_a }
        loop do
          return unless running?

          safe_close
          new_conn = nil
          begin
            new_conn = build_connection
            # Reject a replica before re-LISTENing. A fresh PG.connect re-resolves
            # DNS, so backing off and retrying converges on the promoted master
            # once DNS catches up after a failover.
            PrimaryValidator.validate_primary!(new_conn)
            channels.each { |channel| new_conn.exec(%(LISTEN "#{channel}")) }
          rescue PG::Error, ReplicaConnectionError => e
            # build_connection may have succeeded before a later LISTEN raised,
            # or validate_primary! rejected a replica. Without this close, the
            # partially-built conn is orphaned and the next retry just allocates
            # another one — leaking PG connections on repeated failures.
            close_quietly(new_conn)
            ErrorReporter.report(e, { action: "notify_listener_reconnect" })
            sleep RECONNECT_BACKOFF_SECONDS
            next
          end

          @state_mutex.synchronize do
            @conn = new_conn
            @listening_to = Set.new(channels)
          end
          return
        end
      end

      # String/Hash go through DedicatedConnection so a :session-mode
      # `:variables` key never reaches PG.connect (issue #352).
      def build_connection
        case @connection_options
        when String, Hash then Pgbus::DedicatedConnection.connect(@connection_options)
        else
          raise Pgbus::ConfigurationError,
                "NotifyListener cannot build a PG connection from #{@connection_options.class}. " \
                "Set worker_notify_database_url / worker_notify_host / worker_notify_port, " \
                "or a base database_url, so the listener owns a dedicated connection."
        end
      end

      # How long #stop waits for the listener thread to notice the cleared
      # @running and finish teardown. The thread can be parked in
      # wait_for_notify for one full health-check cycle before it re-checks the
      # flag, so the budget is that cycle plus grace — a flat timeout would
      # expire before a listener with a large health_check_ms had even one
      # chance to observe the stop.
      def stop_join_timeout
        (@health_check_ms / 1000.0) + STOP_JOIN_GRACE_SECONDS
      end

      def safe_close
        conn = @state_mutex.synchronize do
          c = @conn
          @conn = nil
          c
        end
        close_quietly(conn)
      end

      # Close a PG::Connection we are done with — a half-built reconnect
      # attempt that never made it into @conn, or the connection captured out
      # of @conn during teardown. Always called on the listener thread, which
      # owns the connection. Best-effort.
      def close_quietly(conn)
        conn&.close if conn.respond_to?(:close)
      rescue StandardError
        nil
      end

      def channel_for(physical_queue)
        "#{CHANNEL_PREFIX}#{physical_queue}#{CHANNEL_SUFFIX}"
      end
    end
  end
end
