# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::NotifyListener do
  subject(:listener) do
    described_class.new(
      physical_queues: %w[pgbus_default pgbus_low],
      # on_wake receives the notifying channel (issue #381) so a hub caller
      # can route the wake; fork callers ignore it with ->(_channel).
      on_wake: ->(channel) { wakes << channel },
      connection_options: { dbname: "fake" },
      health_check_ms: 50,
      logger: logger
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    allow(listener).to receive(:build_connection).and_return(fake_pg)
    # The self-probe owns its own connection dance and is covered by
    # notify_probe_spec. Neutralize it by default so it doesn't consume events
    # from the fake connection's queue; the probe-specific describe re-stubs it.
    allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)
    # PrimaryValidator runs SELECT pg_is_in_recovery() on every built connection.
    # The default fake_pg's #exec returns nil, so neutralize the validator by
    # default (it passes the connection through). The replica-rejection describe
    # re-stubs it to raise ReplicaConnectionError.
    allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) { |conn| conn }
  end

  let(:fake_pg) do
    Class.new do
      attr_reader :executed, :close_count, :close_threads, :exec_after_close

      def initialize
        @executed = []
        @events = Queue.new
        @exec_errors = []
        @close_count = 0
        @close_threads = []
        @exec_after_close = []
      end

      # Real pg is inside PQsendQuery here with the GVL released. If another
      # thread has already run #close (PQfinish), the PGconn and its OpenSSL
      # objects are freed and this is the use-after-free that segfaults the
      # whole process (issue #375). Record it rather than pretending it's a
      # rescuable PG::Error — a C-level SEGV is not catchable.
      def exec(sql)
        @exec_after_close << sql if closed?
        @executed << sql
        raise @exec_errors.shift if @exec_errors.any?

        nil
      end

      # Mirrors PG::Connection#wait_for_notify(timeout): returns nil once the
      # timeout elapses, so a listener whose stop signal is a cleared flag
      # (not a socket close) still gets to observe it.
      def wait_for_notify(timeout)
        event = @events.pop(timeout: timeout)
        case event&.first
        when nil, :timeout
          nil
        when :notify
          yield event[1], 0, nil
          event[1]
        when :raise
          raise event[1]
        when :close
          raise PG::Error, "connection closed"
        end
      end

      # Records the calling thread: the fix for #375 is that ONLY the listener
      # thread may ever touch this connection.
      def close
        @close_count += 1
        @close_threads << Thread.current
        @events << [:close]
      end

      def closed?
        @close_count.positive?
      end

      def push_notify(channel) = @events << [:notify, channel]
      def push_timeout = @events << [:timeout]
      def push_error(error) = @events << [:raise, error]
      def push_exec_error(error) = @exec_errors << error
    end.new
  end

  let(:wakes)  { Queue.new }
  let(:logger) { Logger.new(IO::NULL) }

  after { listener.stop }

  def wait_until(timeout: 2)
    deadline = Time.now + timeout
    until yield
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  describe "#start" do
    it "LISTENs on pgmq.q_<queue>.INSERT for every queue" do
      listener.start
      fake_pg.push_timeout
      wait_until do
        fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_default.INSERT")) &&
          fake_pg.executed.include?(%(LISTEN "pgmq.q_pgbus_low.INSERT"))
      end

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    it "is idempotent" do
      listener.start
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listens = fake_pg.executed.count { |s| s.start_with?("LISTEN") }
      expect(listens).to eq(2)
    end
  end

  describe "NOTIFY handling" do
    it "fires on_wake with the notifying channel" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_notify("pgmq.q_pgbus_default.INSERT")

      expect(wakes.pop).to eq("pgmq.q_pgbus_default.INSERT")
    end

    it "coalesces into a single wake per notification" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_notify("pgmq.q_pgbus_low.INSERT")
      expect(wakes.pop).to eq("pgmq.q_pgbus_low.INSERT")
      expect(wakes).to be_empty
    end
  end

  describe ".physical_for" do
    it "maps a NOTIFY channel back to its physical queue name (inverse of channel_for)" do
      expect(described_class.physical_for("pgmq.q_pgbus_default.INSERT")).to eq("pgbus_default")
    end
  end

  describe "#close_inherited_socket! (forked-child hygiene, issues #381 / #437)" do
    # A just-forked child holds a COPY of the LISTEN socket fd. PQfinish
    # (#close) would send a libpq Terminate over the socket shared with the
    # parent, killing the parent's session. Closing the IO wrapper is NOT
    # enough either: pg builds socket_io with autoclose=false, so the fd stays
    # open and the inherited PG::Connection's GC-time PQfinish still speaks
    # Terminate on it (issue #437). The child must repoint the fd at /dev/null
    # (IO#reopen), the ActiveRecord PostgreSQLAdapter#discard! idiom.
    let(:socket_io) { instance_double(IO, reopen: nil, close: nil) }

    before { allow(fake_pg).to receive(:socket_io).and_return(socket_io) }

    it "repoints the socket fd at /dev/null without PQfinish and drops the connection" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.connected? }

      listener.close_inherited_socket!

      expect(socket_io).to have_received(:reopen).with(IO::NULL)
      expect(fake_pg.close_count).to eq(0)
      expect(listener.connected?).to be false
    end

    it "never closes the IO wrapper (autoclose=false makes it a no-op on the fd)" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.connected? }

      listener.close_inherited_socket!

      expect(socket_io).not_to have_received(:close)
    end

    it "is a no-op before start" do
      expect { listener.close_inherited_socket! }.not_to raise_error
    end

    it "logs (never raises, never silences) when the socket reopen fails" do
      allow(socket_io).to receive(:reopen).and_raise(IOError, "closed stream")
      allow(logger).to receive(:warn)
      listener.start
      fake_pg.push_timeout
      wait_until { listener.connected? }

      expect { listener.close_inherited_socket! }.not_to raise_error
      expect(logger).to have_received(:warn)
    end
  end

  describe "#connected?" do
    # The hub (issue #381) broadcasts degraded status to forks while the
    # listener is between connections — running? stays true during a
    # reconnect, so connected? is the signal that distinguishes "parked in
    # wait_for_notify" from "rebuilding the connection".
    it "is false before start" do
      expect(listener.connected?).to be false
    end

    it "is true once the connection is published" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.connected? }

      expect(listener.connected?).to be true
    end

    it "is false again after stop" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.connected? }

      listener.stop

      expect(listener.connected?).to be false
    end
  end

  describe "health check" do
    it "runs SELECT 1 when wait_for_notify times out" do
      listener.start
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(fake_pg.executed).to include("SELECT 1")
    end

    it "does not fire on_wake on a timeout" do
      listener.start
      fake_pg.push_timeout
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(wakes).to be_empty
    end
  end

  describe "runtime queue-set changes" do
    it "adds a queue via add_queue" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.add_queue("pgbus_webhooks")
      fake_pg.push_timeout
      wait_until { listener.listening_to.include?("pgmq.q_pgbus_webhooks.INSERT") }

      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_pgbus_webhooks.INSERT"))
    end

    it "drops a queue via remove_queue" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.remove_queue("pgbus_low")
      fake_pg.push_timeout
      wait_until { !listener.listening_to.include?("pgmq.q_pgbus_low.INSERT") }

      expect(fake_pg.executed).to include(%(UNLISTEN "pgmq.q_pgbus_low.INSERT"))
    end
  end

  describe "reconnect on connection error" do
    it "rebuilds the connection and re-LISTENs every channel" do
      # Don't use Object#clone — it shallow-copies @events/@executed, so a
      # missed reconnect could pass by reading channels from the first conn.
      second_pg = fake_pg.class.new
      call_count = 0
      allow(listener).to receive(:build_connection) do
        call_count += 1
        call_count == 1 ? fake_pg : second_pg
      end

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      second_pg.push_timeout

      wait_until do
        second_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2
      end

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    context "when re-LISTEN fails mid-rebuild (no leaked conn)" do
      let(:half_built) do
        fake_pg.class.new.tap { |c| c.push_exec_error(PG::Error.new("listen failed mid-rebuild")) }
      end
      let(:good_pg) { fake_pg.class.new }

      before do
        call_sequence = [fake_pg, half_built, good_pg]
        allow(listener).to receive(:build_connection) { call_sequence.shift }
        stub_const("Pgbus::Process::NotifyListener::RECONNECT_BACKOFF_SECONDS", 0.01)
        listener.start
        fake_pg.push_timeout
        wait_until { listener.listening_to.size == 2 }

        fake_pg.push_error(PG::Error.new("connection reset"))
        good_pg.push_timeout
        wait_until { half_built.closed? }
        wait_until { good_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }
      end

      it "closes the partially-built conn before retrying" do
        expect(half_built).to be_closed
      end

      it "ends up subscribed via the successor connection" do
        expect(listener.listening_to).to contain_exactly(
          "pgmq.q_pgbus_default.INSERT",
          "pgmq.q_pgbus_low.INSERT"
        )
      end
    end
  end

  describe "ErrorReporter integration (issue #352)" do
    after { Pgbus.configuration.error_reporters = [] }

    it "reports a fatal run_loop error so APM handlers see it" do
      reported = []
      Pgbus.configuration.error_reporters << ->(ex, ctx) { reported << [ex, ctx] }
      allow(listener).to receive(:build_connection).and_raise(PG::Error.new("boot failed"))

      listener.start
      wait_until { reported.any? }

      expect(reported.first[0]).to be_a(PG::Error)
      expect(reported.first[1]).to include(action: "notify_listener_fatal")
    end

    it "reports reconnect failures so APM handlers see them" do
      reported = []
      Pgbus.configuration.error_reporters << ->(ex, ctx) { reported << [ex, ctx] }
      second_pg = fake_pg.class.new
      call_count = 0
      allow(listener).to receive(:build_connection) do
        call_count += 1
        raise PG::Error, "reconnect refused" if call_count == 2

        call_count == 1 ? fake_pg : second_pg
      end
      stub_const("Pgbus::Process::NotifyListener::RECONNECT_BACKOFF_SECONDS", 0.01)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }
      fake_pg.push_error(PG::Error.new("connection reset"))
      second_pg.push_timeout
      wait_until { reported.any? }

      expect(reported.first[0].message).to eq("reconnect refused")
      expect(reported.first[1]).to include(action: "notify_listener_reconnect")
    end
  end

  describe "#build_connection under :session GUC mode (issue #352)" do
    # The listener's connection_options come from
    # config.worker_notify_connection_options, which in :session GUC mode
    # carries the database.yml `variables:` hash. :variables is not a libpq
    # keyword — passing it through raises `PG::Error: invalid connection
    # option "variables"`, killing NOTIFY wakeups (workers silently fall
    # back to polling).
    subject(:session_listener) do
      described_class.new(
        physical_queues: %w[pgbus_default],
        on_wake: ->(_channel) {},
        connection_options: { host: "pooler.example", dbname: "app",
                              variables: { statement_timeout: "10s", timezone: "UTC" } },
        logger: logger
      )
    end

    before do
      # DedicatedConnection requires "pg" only when PG::Connection is absent;
      # satisfy the check so the stubbed PG module is never clobbered by a
      # real require.
      stub_const("PG::Connection", Class.new) unless defined?(PG::Connection)
    end

    it "strips the non-libpq :variables key before PG.connect" do
      captured = nil
      allow(PG).to receive(:connect) do |**kwargs|
        captured = kwargs
        fake_pg
      end

      session_listener.send(:build_connection)

      expect(captured).to eq(host: "pooler.example", dbname: "app",
                             fallback_application_name: "pgbus-listen")
    end

    it "keeps the GUCs by applying them via post-connect SET" do
      allow(PG).to receive(:connect).and_return(fake_pg)

      session_listener.send(:build_connection)

      expect(fake_pg.executed).to eq(["SET statement_timeout = '10s'", "SET timezone = 'UTC'"])
    end
  end

  describe "replica rejection (failover self-healing)" do
    context "when a reconnect lands on a replica then converges on the primary" do
      let(:replica_pg) { fake_pg.class.new }
      let(:primary_pg) { fake_pg.class.new }

      before do
        # Initial conn (fake_pg) is a primary; the first reconnect attempt lands
        # on a replica; the second reconnect attempt lands on the primary.
        call_sequence = [fake_pg, replica_pg, primary_pg]
        allow(listener).to receive(:build_connection) { call_sequence.shift }
        # validate_primary! passes fake_pg and primary_pg through, but raises
        # for replica_pg — mirroring pg_is_in_recovery() => t on the demoted host.
        allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) do |conn|
          raise Pgbus::Process::ReplicaConnectionError, "on a replica" if conn.equal?(replica_pg)

          conn
        end
        stub_const("Pgbus::Process::NotifyListener::RECONNECT_BACKOFF_SECONDS", 0.01)

        listener.start
        fake_pg.push_timeout
        wait_until { listener.listening_to.size == 2 }

        fake_pg.push_error(PG::Error.new("connection reset"))
        primary_pg.push_timeout
        wait_until { primary_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }
      end

      it "closes the replica connection before retrying" do
        expect(replica_pg).to be_closed
      end

      it "re-LISTENs every channel on the promoted primary" do
        expect(listener.listening_to).to contain_exactly(
          "pgmq.q_pgbus_default.INSERT",
          "pgmq.q_pgbus_low.INSERT"
        )
        expect(primary_pg.executed).to include(
          %(LISTEN "pgmq.q_pgbus_default.INSERT"),
          %(LISTEN "pgmq.q_pgbus_low.INSERT")
        )
      end
    end

    it "validates the initial connection is a primary at start" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(Pgbus::Process::PrimaryValidator).to have_received(:validate_primary!).with(fake_pg)
    end

    it "does not run the delivery probe when the initial connection is a replica" do
      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!)
        .and_raise(Pgbus::Process::ReplicaConnectionError.new("on a replica"))

      listener.start
      # running? flips to false in the run-loop ensure once the thread exits.
      wait_until { !listener.running? }

      # The replica is rejected before the delivery probe runs (probe is only
      # meaningful on a primary), and the thread exits via the fatal/ensure path.
      expect(Pgbus::Process::NotifyProbe).not_to have_received(:probe_notify_delivery!)
      expect(listener.running?).to be(false)
    end
  end

  describe "LISTEN/NOTIFY self-probe at start" do
    it "runs the probe once against the initial connection" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).once
      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).with(fake_pg, logger: logger)
    end

    it "still starts and LISTENs when the probe fails (graceful degradation)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(false)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.listening_to).to contain_exactly(
        "pgmq.q_pgbus_default.INSERT",
        "pgmq.q_pgbus_low.INSERT"
      )
    end

    it "does not re-probe on reconnect (probe is start-only)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      second_pg = fake_pg.class.new
      call_count = 0
      allow(listener).to receive(:build_connection) do
        call_count += 1
        call_count == 1 ? fake_pg : second_pg
      end

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      fake_pg.push_error(PG::Error.new("connection reset"))
      second_pg.push_timeout
      wait_until { second_pg.executed.count { |s| s.start_with?("LISTEN") } >= 2 }

      # Probe ran for the initial connection only, not the reconnect.
      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!).once
    end

    it "reports delivering? true once the probe succeeds (issue #332)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.delivering?).to be(true)
    end

    it "reports delivering? false when the probe fails (pooler-deaf, issue #332)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(false)

      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.delivering?).to be(false)
    end
  end

  describe "#delivering? (public)" do
    it "is true before the probe runs (optimistic default — assume healthy)" do
      # Until the start-time probe has run, treat the listener as delivering so
      # a not-yet-started listener isn't mistaken for a pooler-deaf one.
      expect(listener.delivering?).to be(true)
    end
  end

  describe "#running? (public)" do
    it "is false before start" do
      expect(listener.running?).to be(false)
    end

    it "is true while the listener thread is alive" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      expect(listener.running?).to be(true)
    end

    it "reflects thread death after a fatal run_loop error" do
      # A connection built at start that dies mid-loop drives run_loop through
      # its rescue/ensure path, which clears @running. The worker relies on
      # running? flipping to false to detect a dead listener and restart it.
      allow(listener).to receive(:build_connection)
        .and_raise(PG::Error.new("boot failed"))

      listener.start
      wait_until { !listener.running? }

      expect(listener.running?).to be(false)
    end
  end

  # A PG::Connection is not thread-safe, and #close is PQfinish — it frees the
  # PGconn and its OpenSSL objects. #stop used to call it from the supervisor
  # thread while the listener thread was still using the same connection, so
  # the UNLISTEN in run_loop's ensure walked into freed TLS state and SEGV'd
  # the whole process on every deploy. The invariant these specs pin: the
  # listener thread is the SOLE user of the connection for its entire life.
  describe "shutdown thread-safety (issue #375)" do
    def start_and_wait_for_subscriptions
      listener.start
      wait_until { listener.listening_to.size == 2 }
      listener.instance_variable_get(:@thread)
    end

    it "closes the connection only from the listener thread, never from the stopping thread" do
      listener_thread = start_and_wait_for_subscriptions

      listener.stop

      expect(fake_pg.close_threads).to eq([listener_thread])
    end

    it "never execs on a connection that has already been closed" do
      start_and_wait_for_subscriptions

      listener.stop

      expect(fake_pg.exec_after_close).to be_empty
    end

    it "skips the UNLISTEN round-trip on teardown (closing the session deregisters LISTENs)" do
      start_and_wait_for_subscriptions

      listener.stop

      expect(fake_pg.executed.select { |sql| sql.start_with?("UNLISTEN") }).to be_empty
    end

    it "still releases the connection exactly once" do
      start_and_wait_for_subscriptions

      listener.stop

      expect(fake_pg.close_count).to eq(1)
    end

    it "joins the listener thread within one health-check cycle" do
      listener_thread = start_and_wait_for_subscriptions

      listener.stop

      expect(listener_thread).not_to be_alive
    end

    # run_loop's ensure captures @conn in the SAME critical section that clears
    # @running. #start is public and guarded only by @running, so a caller
    # watching running? can spawn a fresh thread the instant it flips; if
    # teardown read @conn afterwards it could close the NEW thread's connection
    # mid-use — the same use-after-free reached through restart instead of
    # #stop. This is a regression guard for that ordering, not a deterministic
    # reproduction: the atomicity is guaranteed by the single critical section.
    it "does not close the successor connection when the same instance is restarted" do
      second_pg = fake_pg.class.new
      call_sequence = [fake_pg, second_pg]
      allow(listener).to receive(:build_connection) { call_sequence.shift }

      listener.start
      wait_until { listener.listening_to.size == 2 }
      # Kill the first thread the way a fatal error would, then restart the
      # SAME instance the moment running? reports the thread is gone. The
      # reconnect stub is installed BEFORE the error is pushed so the thread
      # can't recover first and leave running? true forever.
      allow(listener).to receive(:reconnect!).and_raise(PG::Error.new("fatal"))
      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until { !listener.running? }

      listener.start
      wait_until { listener.listening_to.size == 2 }

      expect(second_pg.close_threads).to be_empty
      expect(fake_pg).to be_closed
    end
  end

  describe "#stop" do
    it "interrupts the blocking wait and clears the LISTEN set" do
      listener.start
      fake_pg.push_timeout
      wait_until { listener.listening_to.size == 2 }

      listener.stop
      expect(listener.listening_to).to be_empty
    end

    it "clears running state after the thread exits so start can spawn a fresh thread" do
      allow(listener).to receive(:build_connection)
        .and_raise(PG::Error.new("boot failed"))

      listener.start
      # Thread crashes immediately on build_connection; running? flips back to
      # false in the run-loop ensure.
      wait_until { !listener.running? }

      expect(listener.running?).to be(false)
    end

    # stop used to run whole between start's @running check and its
    # Thread.new: it found no thread to join, and start then started one that
    # nobody joins (and that a restart could run beside a second). start is
    # held at its first LISTEN command until stop has finished or is blocked.
    it "joins a thread that a concurrent start was still creating" do
      held = Queue.new
      release = Queue.new
      hold_first_listen_command(held, release)

      starter = Thread.new { listener.start }
      held.pop
      stopper = Thread.new { listener.stop }
      Thread.pass until stopper.stop?
      release << true
      [starter, stopper].each(&:join)

      expect(listener.instance_variable_get(:@thread)).to be_nil
      expect(listener).not_to be_running
    end

    def hold_first_listen_command(held, release)
      first = true
      allow(listener.instance_variable_get(:@commands)).to receive(:<<).and_wrap_original do |original, command|
        if first && command.first == :listen
          first = false
          held << true
          release.pop
        end
        original.call(command)
      end
    end
  end
end
