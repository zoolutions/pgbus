# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::Listener do
  # PG::Error isn't loaded in unit tests (the spec suite stubs pgmq-ruby away).
  # Define a minimal stand-in so Listener's rescue clause has something real
  # to catch. The fake_pg below raises this class when a [:raise] event fires
  # or when #close is called mid-wait.
  subject(:listener) do
    described_class.new(
      pg_connection: fake_pg,
      dispatch_queue: dispatch_queue,
      health_check_ms: 50,
      logger: logger,
      connection_factory: -> { factory_conns.shift }
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    stub_const("PG::ConnectionBad", Class.new(PG::Error)) unless defined?(PG::ConnectionBad)
    stub_const("PG::CONNECTION_OK", 0) unless defined?(PG::CONNECTION_OK)
    stub_const("PG::CONNECTION_BAD", 1) unless defined?(PG::CONNECTION_BAD)
    # reconnect! validates the connection is a primary via pg_is_in_recovery().
    # The fake_pg's #exec returns nil, so neutralize the validator by default
    # (it passes the connection through). The replica-rejection describe re-stubs
    # it to raise ReplicaConnectionError.
    allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) { |conn| conn }
  end

  # A scripted stand-in for PG::Connection, with the surface the listener's
  # select loop uses (issue #630): socket_io (the read end of a pipe this fake
  # owns), consume_input (reads the pending bytes and applies the queued
  # events), notifies (libpq's buffered-notification list) and status. Every
  # push_* writes a byte, so the listener's IO.select sees a readable socket
  # exactly as it would when Postgres sends something. A timeout is the real
  # select timeout: these examples use a small health_check_ms.
  let(:fake_pg_class) do
    Class.new do
      attr_reader :executed, :reset_count, :close_count, :close_threads, :exec_after_close, :socket_io

      def initialize
        @executed = []
        @events = Queue.new
        @notifies = Queue.new
        @reset_count = 0
        @close_count = 0
        @close_threads = []
        @exec_after_close = []
        @raise_on_next_listen = false
        @buffer_on_exec = Queue.new
        @gated_sql = nil
        @exec_gate = Queue.new
        @bad = false
        @socket_io, @socket_writer = IO.pipe
      end

      # Real pg is inside PQsendQuery here with the GVL released. If another
      # thread has already run #close (PQfinish), the PGconn and its OpenSSL
      # objects are freed and this is the use-after-free that segfaults the
      # whole process (issue #375). Record it rather than pretending it's a
      # rescuable PG::Error — a C-level SEGV is not catchable.
      def exec(sql)
        @exec_after_close << sql if @close_count.positive?
        @executed << sql
        @exec_gate.pop if sql == @gated_sql
        sleep @slow_exec[1] if @slow_exec && sql == @slow_exec[0]
        if sql.start_with?("LISTEN") && @raise_on_next_listen
          @raise_on_next_listen = false
          raise PG::Error, "listen failed mid-rebuild"
        end
        # A NOTIFY that arrived with this statement's reply: libpq buffers it
        # while reading the result and the socket has nothing left to read.
        @notifies << @buffer_on_exec.pop unless @buffer_on_exec.empty?
        nil
      end

      # PQconsumeInput: read what the socket has, apply what it carried.
      def consume_input
        @socket_io.read_nonblock(4096, exception: false)
        until @events.empty?
          kind, arg = @events.pop
          case kind
          when :notify then @notifies << arg
          when :raise then raise arg
          when :peer_close then @bad = true
          end
        end
        true
      end

      # PQnotifies, as pg returns it: extra is "" for a NOTIFY without a
      # payload (wait_for_notify yields nil for the same notification).
      def notifies
        return nil if @notifies.empty?

        channel, payload = @notifies.pop
        { relname: channel, be_pid: 0, extra: payload || "" }
      end

      def status = @bad ? PG::CONNECTION_BAD : PG::CONNECTION_OK

      def reset
        @reset_count += 1
      end

      # Called by the listener thread's teardown and by the reconnect loop when
      # it discards the old/partial connection. Records the calling thread: the
      # fix for #375 is that ONLY the listener thread may ever touch this
      # connection.
      def close
        @close_count += 1
        @close_threads << Thread.current
        [@socket_io, @socket_writer].each { |io| io.close unless io.closed? }
      end

      def exec_params(sql, _params)
        @executed << sql
        nil
      end

      def push_notify(channel, payload = nil) = deliver([:notify, [channel, payload]])
      def push_error(error) = deliver([:raise, error])

      # The server went away: the socket turns readable (EOF) and the status
      # goes bad once consume_input has looked.
      def peer_close! = deliver([:peer_close])

      # The next exec buffers a NOTIFY without writing to the socket.
      def buffer_notify_on_next_exec(channel) = @buffer_on_exec << [channel, nil]

      # IO.select on a closed IO raises IOError, as on a socket closed under it.
      def break_socket! = @socket_io.close

      # Every exec of `sql` takes `seconds`.
      def slow_exec!(sql, seconds) = (@slow_exec = [sql, seconds])

      # Park the next exec of `sql` until release_exec!.
      def gate_exec!(sql) = (@gated_sql = sql)
      def release_exec! = @exec_gate << true

      # Arm the connection so its next LISTEN raises PG::Error once — used to
      # simulate a partially-built reconnect connection.
      def fail_next_listen!
        @raise_on_next_listen = true
      end

      private

      def deliver(event)
        @events << event
        @socket_writer.write_nonblock(".", exception: false)
      rescue IOError
        nil
      end
    end
  end

  let(:fake_pg) { fake_pg_class.new }

  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }
  let(:factory_conns)  { [] }

  after { listener.stop }

  def wait_until(timeout: 2, &block)
    deadline = Time.now + timeout
    until block.call
      raise "timed out waiting for condition" if Time.now > deadline

      sleep 0.01
    end
  end

  def build_listener(pg_connection: fake_pg, health_check_ms: 50, **opts)
    described_class.new(
      pg_connection: pg_connection, dispatch_queue: dispatch_queue, health_check_ms: health_check_ms,
      logger: logger, connection_factory: -> { factory_conns.shift }, **opts
    )
  end

  def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  def listens(conn) = conn.executed.count { |sql| sql.start_with?("LISTEN") }

  describe "#start and #stop" do
    it "exposes its thread via #threads while running and none after stop (issue #443)" do
      expect(listener.threads).to eq([])
      listener.start
      expect(listener.threads.size).to eq(1)
      listener.stop
      expect(listener.threads).to eq([])
    end

    it "spawns a thread on start and joins it on stop" do
      listener.start
      listener.stop
      expect(listener.listening_to).to be_empty
    end
  end

  # A PG::Connection is not thread-safe, and #close is PQfinish — it frees the
  # PGconn and its OpenSSL objects. #stop used to call it from the shutting-down
  # request/dispatcher thread while the listener thread was still using the same
  # connection, so the UNLISTEN in run_loop's ensure walked into freed TLS state
  # and SEGV'd the Puma worker on every deploy. Same defect as
  # Process::NotifyListener (issue #375); the invariant these specs pin is the
  # same: the listener thread is the SOLE user of the connection for its life.
  describe "shutdown thread-safety (issue #375)" do
    def start_and_wait_for_subscription
      listener.start
      listener.ensure_listening("pgbus_stream_chat")
      wait_until { listener.listening_to.size == 1 }
      listener.instance_variable_get(:@thread)
    end

    it "closes the connection only from the listener thread, never from the stopping thread" do
      listener_thread = start_and_wait_for_subscription

      listener.stop

      expect(fake_pg.close_threads).to eq([listener_thread])
    end

    it "never execs on a connection that has already been closed" do
      start_and_wait_for_subscription

      listener.stop

      expect(fake_pg.exec_after_close).to be_empty
    end

    it "skips the UNLISTEN round-trip on teardown (closing the session deregisters LISTENs)" do
      start_and_wait_for_subscription

      listener.stop

      expect(fake_pg.executed.select { |sql| sql.start_with?("UNLISTEN") }).to be_empty
    end

    it "releases the listener's connection exactly once so the LISTEN slot is freed" do
      start_and_wait_for_subscription

      listener.stop

      expect(fake_pg.close_count).to eq(1)
    end

    it "joins the listener thread within one health-check cycle" do
      listener_thread = start_and_wait_for_subscription

      listener.stop

      expect(listener_thread).not_to be_alive
    end
  end

  # The listener thread parks in IO.select on the LISTEN socket and a wake
  # pipe; ensure_listening, remove_listening and stop write to the pipe, so a
  # command runs within a thread hop instead of at the end of the current
  # health-check wait (issue #630). Every example here uses a health check far
  # longer than its assertion window: only the pipe can have woken the thread.
  describe "wake pipe (issue #630)" do
    subject(:listener) { build_listener(health_check_ms: 5_000) }

    it "acks a LISTEN queued while the thread is parked in select within a thread hop" do
      listener.start
      wait_until { listener.instance_variable_get(:@thread)&.status == "sleep" }

      started = monotonic
      listener.ensure_listening("chat")
      elapsed = monotonic - started

      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"))
      # Typically well under a millisecond; the bound leaves room for a busy
      # CI runner and is still a tenth of the 5 s select only the pipe can cut.
      expect(elapsed).to be < 0.5
    end

    it "runs an UNLISTEN within a thread hop" do
      listener.start
      listener.ensure_listening("chat")

      started = monotonic
      listener.remove_listening("chat")
      wait_until(timeout: 1) { fake_pg.executed.include?(%(UNLISTEN "pgmq.q_chat.INSERT")) }

      expect(monotonic - started).to be < 0.5
      expect(listener.listening_to).to be_empty
    end

    it "returns from stop in well under a second with a 30 s health check" do
      slow = build_listener(health_check_ms: 30_000)
      slow.start
      slow.ensure_listening("chat")
      thread = slow.threads.first

      started = monotonic
      slow.stop

      expect(monotonic - started).to be < 1.0
      expect(thread).not_to be_alive
    end

    it "creates no pipe until started, so a listener that never starts holds no descriptors" do
      expect(listener.instance_variable_get(:@wake_reader)).to be_nil

      listener.stop

      expect(listener.instance_variable_get(:@wake_reader)).to be_nil
    end

    it "closes both pipe ends after a clean stop" do
      listener.start
      reader = listener.instance_variable_get(:@wake_reader)
      writer = listener.instance_variable_get(:@wake_writer)

      listener.stop

      expect([reader, writer]).to all(be_closed)
    end

    it "recreates the pipe when started again after a stop closed it" do
      listener.start
      listener.stop
      closed_reader = listener.instance_variable_get(:@wake_reader)

      listener.start

      expect(closed_reader).to be_closed
      expect(listener.instance_variable_get(:@wake_reader)).not_to be_closed
      expect(listener.instance_variable_get(:@wake_writer)).not_to be_closed
    end

    it "releases an ensure_listening caller still waiting when stop lands, instead of at its ack timeout" do
      fake_pg.gate_exec!("SELECT 1")
      gated = build_listener(health_check_ms: 20)
      gated.start
      wait_until { fake_pg.executed.include?("SELECT 1") } # the thread is parked inside the keepalive
      result = :pending
      caller_thread = Thread.new { result = gated.ensure_listening("chat") }
      wait_until { caller_thread.status == "sleep" }
      stopper = Thread.new { gated.stop }
      wait_until { !gated.instance_variable_get(:@running) }

      fake_pg.release_exec!
      [caller_thread, stopper].each { |t| t.join(2) }

      expect(result).to eq(:done) # an ack, not the nil of a timeout
      expect(fake_pg.executed).not_to include(%(LISTEN "pgmq.q_chat.INSERT"))
    end

    # Production never restarts a Listener (FailoverListener builds a fresh
    # one); this pins only that nothing left in the queue ends a second run.
    it "answers an ensure_listening after stop at once instead of at its ack timeout" do
      listener.start
      listener.stop

      started = monotonic
      result = listener.ensure_listening("chat")

      expect(monotonic - started).to be < 0.5 # the ack budget is 6 s here
      expect(result).to be_nil
    end

    it "serves commands again when started after a clean stop" do
      listener.start
      listener.stop
      listener.instance_variable_set(:@conn, fake_pg_class.new)

      listener.start
      listener.ensure_listening("chat")

      expect(listener.listening_to).to include("pgmq.q_chat.INSERT")
    end

    it "does not raise when a wake races the pipe's close (the reader already gone)" do
      reader, writer = IO.pipe
      reader.close
      listener.instance_variable_set(:@wake_writer, writer)

      expect { listener.remove_listening("chat") }.not_to raise_error
    ensure
      writer&.close
    end

    it "does not raise when a wake comes after stop closed the pipe" do
      listener.start
      listener.stop

      expect { listener.remove_listening("chat") }.not_to raise_error
    end

    context "when the thread outlives the join budget" do
      let(:log) { StringIO.new }
      # The stuck thread and its pipe, captured before #stop.
      let(:held) { {} }
      let(:stuck) do
        described_class.new(
          pg_connection: fake_pg, dispatch_queue: dispatch_queue, health_check_ms: 20,
          logger: Logger.new(log), connection_factory: -> {}
        )
      end

      before do
        stub_const("#{described_class}::STOP_JOIN_GRACE_SECONDS", 0.05)
        fake_pg.gate_exec!("SELECT 1")
        stuck.start
        wait_until { fake_pg.executed.include?("SELECT 1") }
        held[:thread] = stuck.threads.first
        held[:pipe] = %i[@wake_reader @wake_writer].map { |ivar| stuck.instance_variable_get(ivar) }
        stuck.stop
      end

      after do
        fake_pg.release_exec!
        held[:thread].join(2)
        held[:pipe].each { |io| io.close unless io.closed? }
      end

      it "leaves the pipe open and says so" do
        expect(held[:pipe]).to all(satisfy { |io| !io.closed? })
        expect(log.string).to include("did not stop")
      end

      it "closes the pipe itself once that thread finally exits" do
        fake_pg.release_exec!
        held[:thread].join(2)

        expect(held[:pipe]).to all(be_closed)
      end

      it "refuses to start a second loop beside it" do
        stuck.start

        expect(stuck.threads).to eq([held[:thread]])
        expect(log.string).to include("still running")
      end
    end
  end

  describe "channel naming" do
    it "LISTENs on pgmq.q_<queue>.INSERT for ensure_listening" do
      listener.start
      listener.ensure_listening("pgbus_stream_chat")

      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_pgbus_stream_chat.INSERT"))
    end

    it "UNLISTENs on remove_listening" do
      listener.start
      listener.ensure_listening("pgbus_stream_chat")

      listener.remove_listening("pgbus_stream_chat")
      wait_until { fake_pg.executed.include?(%(UNLISTEN "pgmq.q_pgbus_stream_chat.INSERT")) }
      expect(fake_pg.executed).to include(%(UNLISTEN "pgmq.q_pgbus_stream_chat.INSERT"))
    end

    it "records the channel in listening_to" do
      listener.start
      listener.ensure_listening("chat")
      expect(listener.listening_to).to include("pgmq.q_chat.INSERT")
    end

    it "is idempotent on duplicate ensure_listening" do
      listener.start
      listener.ensure_listening("chat")
      listener.ensure_listening("chat")

      expect(listens(fake_pg)).to eq(1)
      expect(listener.listening_to.size).to eq(1)
    end
  end

  describe "NOTIFY handling" do
    it "posts a durable WakeMessage (no payload) with the queue name extracted from the channel" do
      listener.start
      listener.ensure_listening("chat")

      fake_pg.push_notify("pgmq.q_chat.INSERT")

      msg = dispatch_queue.pop(timeout: 2)
      expect(msg).to be_a(described_class::WakeMessage)
      expect(msg.queue_name).to eq("chat")
      # pg's notifies reports a payload-less NOTIFY as extra: ""; a durable
      # wake carries nil, as wait_for_notify used to yield.
      expect(msg.payload).to be_nil
    end

    it "carries an ephemeral NOTIFY's payload" do
      listener.start
      listener.ensure_listening("chat")

      fake_pg.push_notify("pgmq.q_chat.INSERT", '{"html":"<x/>"}')

      expect(dispatch_queue.pop(timeout: 2).payload).to eq('{"html":"<x/>"}')
    end

    it "dispatches every notification one socket read carried" do
      listener.start
      listener.ensure_listening("chat")

      3.times { fake_pg.push_notify("pgmq.q_chat.INSERT") }

      wait_until { dispatch_queue.size == 3 }
      expect(dispatch_queue.size).to eq(3)
    end

    it "dispatches a NOTIFY an exec buffered without the socket turning readable" do
      slow = build_listener(health_check_ms: 5_000)
      fake_pg.buffer_notify_on_next_exec("pgmq.q_chat.INSERT")
      slow.start

      slow.ensure_listening("chat")

      msg = dispatch_queue.pop(timeout: 1)
      expect(msg&.queue_name).to eq("chat")
    ensure
      slow&.stop
    end

    it "ignores notifications on channels that don't match the pgmq pattern" do
      listener.start
      fake_pg.push_notify("random_channel")

      sleep 0.1
      expect(dispatch_queue).to be_empty
    end
  end

  describe "bounded dispatch queue drop (issue #315 item 3)" do
    # handle_notify is exercised directly: the drop decision is pure and does
    # not need the listener thread.
    it "enqueues every wake when the limit is 0 (unbounded, the default)" do
      l = build_listener(dispatch_queue_limit: 0)
      10.times { l.send(:handle_notify, "pgmq.q_chat.INSERT") }
      expect(dispatch_queue.size).to eq(10)
    end

    it "drops a durable wake (payload nil) when the queue is at/over the limit" do
      l = build_listener(dispatch_queue_limit: 2)
      dispatch_queue << :filler_one
      dispatch_queue << :filler_two # queue size now 2 == limit

      l.send(:handle_notify, "pgmq.q_chat.INSERT") # durable, no payload

      expect(dispatch_queue.size).to eq(2) # dropped, not enqueued
      expect(l.dropped_wakes).to eq(1)
    end

    it "still enqueues durable wakes while below the limit" do
      l = build_listener(dispatch_queue_limit: 5)
      3.times { l.send(:handle_notify, "pgmq.q_chat.INSERT") }
      expect(dispatch_queue.size).to eq(3)
      expect(l.dropped_wakes).to eq(0)
    end

    it "NEVER drops an ephemeral wake (payload present) even at the limit" do
      l = build_listener(dispatch_queue_limit: 1)
      dispatch_queue << :filler # size 1 == limit

      l.send(:handle_notify, "pgmq.q_chat.INSERT", '{"html":"<x/>"}')

      expect(dispatch_queue.size).to eq(2) # ephemeral got through
      dispatch_queue.pop # discard the filler
      msg = dispatch_queue.pop
      expect(msg).to be_a(described_class::WakeMessage)
      expect(msg.payload).to eq('{"html":"<x/>"}')
      expect(l.dropped_wakes).to eq(0)
    end
  end

  describe "health check" do
    it "runs SELECT 1 when a full cycle passes with nothing to read" do
      listener.start
      wait_until { fake_pg.executed.include?("SELECT 1") }
      expect(fake_pg.executed).to include("SELECT 1")
    end

    it "runs one SELECT 1 per cycle, not one per select return" do
      # A 1 s cycle against a ~0.1 s window of wakes: a loaded runner cannot
      # stretch the window into a second cycle.
      idle = build_listener(health_check_ms: 1_000)
      idle.start
      wait_until(timeout: 3) { fake_pg.executed.include?("SELECT 1") }

      # Five wakes inside the cycle: each returns the select early, and none
      # may count as an idle cycle.
      5.times do
        idle.remove_listening("idle")
        sleep 0.02
      end

      expect(fake_pg.executed.count("SELECT 1")).to eq(1)
    ensure
      idle&.stop
    end

    it "still runs the keepalive under a stream of wakes shorter than a cycle" do
      busy = build_listener(health_check_ms: 100)
      busy.start
      stop_at = monotonic + 0.5
      # Each remove_listening wakes the thread (an UNLISTEN of a channel it
      # never LISTENed is a no-op); without a deadline the wait would restart
      # at a full cycle on every wake and the keepalive would never run.
      busy.remove_listening("idle") while monotonic < stop_at && sleep(0.01)

      expect(fake_pg.executed.count("SELECT 1")).to be >= 2
    ensure
      busy&.stop
    end
  end

  describe "a command backlog" do
    it "runs the keepalive between commands once it is due, so a backlog cannot postpone it" do
      clock = [0.0]
      backlog = build_listener(clock: -> { clock[0] })
      backlog.instance_variable_set(:@running, true)
      backlog.instance_variable_set(:@health_check_due_at, 1.0)
      backlog.remove_listening("a")
      clock[0] = 2.0 # overdue while the queue still holds commands
      backlog.remove_listening("b")

      backlog.send(:drain_commands)

      expect(fake_pg.executed.count("SELECT 1")).to eq(1)
    ensure
      backlog&.instance_variable_set(:@running, false)
    end
  end

  describe "a keepalive slower than a cycle" do
    it "still selects on the socket between keepalives" do
      fake_pg.slow_exec!("SELECT 1", 0.03)
      slow = build_listener(health_check_ms: 20)
      slow.start
      wait_until { fake_pg.executed.include?("SELECT 1") }

      fake_pg.push_notify("pgmq.q_chat.INSERT")

      # The next keepalive is scheduled from the end of this one: were it
      # scheduled from the start, every pass would find it overdue and run
      # SELECT 1 again without ever reading the socket.
      expect(dispatch_queue.pop(timeout: 2)&.queue_name).to eq("chat")
    ensure
      slow&.stop
    end
  end

  describe "reconnect" do
    before { stub_const("#{described_class}::RECONNECT_BACKOFF_SECONDS", 0.01) }

    # A connection whose LISTEN always raises — used to prove the reconnect
    # loop spins on failure and only exits when stop() flips @running.
    let(:always_failing_conn) do
      Class.new do
        attr_reader :close_count

        def initialize = (@close_count = 0)

        def exec(sql)
          raise PG::Error, "always fails" if sql.start_with?("LISTEN")

          nil
        end

        def reset = nil
        def close = (@close_count += 1)
      end
    end

    def listen_to_chat_and_presence
      listener.start
      listener.ensure_listening("chat")
      listener.ensure_listening("presence")
    end

    it "rebuilds via the factory and re-LISTENs every channel when consume_input raises PG::Error" do
      new_pg = fake_pg_class.new
      factory_conns << new_pg
      listen_to_chat_and_presence

      fake_pg.push_error(PG::Error.new("connection reset"))

      wait_until { listens(new_pg) >= 2 }
      expect(new_pg.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"), %(LISTEN "pgmq.q_presence.INSERT"))
      # The old connection is discarded, never reset — a fresh connect was used.
      expect(fake_pg.reset_count).to eq(0)
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
    end

    it "reconnects when the socket turns readable but the connection went bad" do
      new_pg = fake_pg_class.new
      factory_conns << new_pg
      listen_to_chat_and_presence

      fake_pg.peer_close!

      wait_until { listens(new_pg) >= 2 }
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
    end

    it "reconnects when IO.select raises IOError on a socket closed under it" do
      new_pg = fake_pg_class.new
      factory_conns << new_pg
      listen_to_chat_and_presence

      fake_pg.break_socket!
      listener.remove_listening("idle") # wake the thread so it selects on the closed socket

      wait_until { listens(new_pg) >= 2 }
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
    end

    it "keeps listening on the fresh connection after a reconnect" do
      new_pg = fake_pg_class.new
      factory_conns << new_pg
      listen_to_chat_and_presence
      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until { listens(new_pg) >= 2 }

      new_pg.push_notify("pgmq.q_chat.INSERT")

      expect(dispatch_queue.pop(timeout: 2)&.queue_name).to eq("chat")
    end

    it "acks a LISTEN that raised only once the reconnect has re-LISTENed it" do
      new_pg = fake_pg_class.new
      new_pg.gate_exec!(%(LISTEN "pgmq.q_chat.INSERT"))
      factory_conns << new_pg
      listener.start
      fake_pg.fail_next_listen!
      result = :pending
      caller_thread = Thread.new { result = listener.ensure_listening("chat") }

      wait_until { new_pg.executed.include?(%(LISTEN "pgmq.q_chat.INSERT")) } # the re-LISTEN, parked
      expect(result).to eq(:pending)

      new_pg.release_exec!
      caller_thread.join(2)
      expect(result).to eq(:done)
    end

    it "reconnects, and re-LISTENs the channel, when a LISTEN itself raises" do
      new_pg = fake_pg_class.new
      factory_conns << new_pg
      listener.start
      fake_pg.fail_next_listen!

      listener.ensure_listening("chat")

      wait_until { new_pg.executed.include?(%(LISTEN "pgmq.q_chat.INSERT")) }
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
      expect(listener.threads.first).to be_alive
    end

    it "preserves the canonical subscription set when a re-LISTEN raises mid-loop" do
      # First factory conn fails its LISTEN mid-rebuild; the loop discards it,
      # backs off, and rebuilds from the second (good) conn.
      partial = fake_pg_class.new.tap(&:fail_next_listen!)
      good = fake_pg_class.new
      factory_conns.push(partial, good)
      listen_to_chat_and_presence

      fake_pg.push_error(PG::Error.new("network blip"))
      wait_until { listens(good) >= 2 }

      # Both channels survive a transient mid-reconnect failure — the earlier
      # version cleared @listening_to before the loop and permanently forgot them.
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT", "pgmq.q_presence.INSERT")
      expect(fake_pg.reset_count).to eq(0) # old conn discarded, never reset
      expect(partial.close_count).to be >= 1 # the half-built conn is closed, no leak
    end

    it "reports reconnect failures through ErrorReporter so APM handlers see them (issue #352)" do
      reported = []
      Pgbus.configuration.error_reporters << ->(ex, ctx) { reported << [ex, ctx] }
      factory_conns.push(always_failing_conn.new, fake_pg_class.new)
      listener.start
      listener.ensure_listening("chat")

      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until { reported.any? }

      expect(reported.first[0].message).to eq("always fails")
      expect(reported.first[1]).to include(action: "streamer_reconnect")
    ensure
      Pgbus.configuration.error_reporters = []
    end

    it "survives a ConfigurationError from the factory and recovers on a later attempt" do
      # build_raw_pg_connection can raise Pgbus::ConfigurationError (e.g. a
      # pooled AR connection in streams_connection_options). The reconnect loop
      # must catch it, back off, and retry rather than let it kill the thread.
      good = fake_pg_class.new
      calls = 0
      config_error_factory = lambda do
        calls += 1
        raise Pgbus::ConfigurationError, "bad streams_connection_options" if calls == 1

        good
      end
      failing_listener = described_class.new(
        pg_connection: fake_pg, dispatch_queue: dispatch_queue, health_check_ms: 50,
        logger: logger, connection_factory: config_error_factory
      )
      failing_listener.start
      failing_listener.ensure_listening("chat")

      fake_pg.push_error(PG::Error.new("connection reset"))

      wait_until { good.executed.include?(%(LISTEN "pgmq.q_chat.INSERT")) }
      expect(failing_listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    ensure
      failing_listener&.stop
    end

    it "exits the reconnect loop promptly when stop is called mid-retry" do
      # The factory returns a brand-new failing connection on every call, so the
      # reconnect loop never runs dry and would spin forever unless stop() breaks
      # it. We prove stop returns and the thread joins within the timeout.
      infinite_listener = described_class.new(
        pg_connection: fake_pg, dispatch_queue: dispatch_queue, health_check_ms: 50,
        logger: logger, connection_factory: -> { always_failing_conn.new }
      )
      infinite_listener.start
      infinite_listener.ensure_listening("chat")

      fake_pg.push_error(PG::Error.new("connection reset"))
      sleep 0.05 # let the loop spin through several failing attempts
      thread = infinite_listener.threads.first
      infinite_listener.stop
      expect(thread.alive?).to be false
    end
  end

  describe "replica rejection on reconnect (failover self-healing)" do
    before { stub_const("#{described_class}::RECONNECT_BACKOFF_SECONDS", 0.01) }

    it "keeps retrying while a fresh conn lands on a replica, then re-LISTENs on the primary" do
      # First factory conn is a replica (validate raises); the next is the
      # promoted primary (validate passes). reconnect! loops internally on
      # validation failure, backing off between cycles.
      replica = fake_pg_class.new
      primary = fake_pg_class.new
      factory_conns.push(replica, primary)
      listener.start
      listener.ensure_listening("chat")

      calls = 0
      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) do |conn|
        (calls += 1) == 1 ? raise(Pgbus::Process::ReplicaConnectionError, "on a replica") : conn
      end

      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until(timeout: 3) { primary.executed.include?(%(LISTEN "pgmq.q_chat.INSERT")) }

      expect(fake_pg.reset_count).to eq(0) # old conn discarded, never reset
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    end

    it "does not re-LISTEN channels while still on a replica" do
      # Every reconnect attempt lands on the same replica; validate_primary!
      # always raises, so the loop spins without ever re-LISTENing.
      replica = fake_pg_class.new
      allow(factory_conns).to receive(:shift).and_return(replica)
      listener.start
      listener.ensure_listening("chat")

      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!)
        .and_raise(Pgbus::Process::ReplicaConnectionError.new("on a replica"))
      fake_pg.push_error(PG::Error.new("connection reset"))
      wait_until { replica.close_count >= 1 } # loop consumed a fresh conn and discarded it
      sleep 0.1

      # A replica reconnect must not re-LISTEN — the channel would register on a
      # host that never NOTIFYs. The subscription set is preserved for the next cycle.
      expect(replica.executed).to be_empty
      expect(fake_pg.reset_count).to eq(0) # old conn discarded, never reset
      expect(listener.listening_to).to contain_exactly("pgmq.q_chat.INSERT")
    end
  end

  describe "ensure_listening synchronous handshake" do
    it "blocks until the listener thread has actually executed the LISTEN" do
      # Not started yet: the command queues and nothing can ack it.
      acked = false
      caller_thread = Thread.new do
        listener.ensure_listening("chat")
        acked = true
      end

      sleep 0.05
      expect(acked).to be false
      expect(fake_pg.executed).to be_empty

      listener.start
      caller_thread.join(2)

      expect(acked).to be true
      expect(fake_pg.executed).to include(%(LISTEN "pgmq.q_chat.INSERT"))
    end
  end

  describe "periodic maintenance on the idle connection (issue #323)" do
    subject(:listener) do
      build_listener(maintenance: maintenance, clock: -> { fake_clock[0] })
    end

    let(:fake_clock) { [0.0] }
    let(:runs) { [] }
    let(:maintenance) do
      interval = 300
      recorder = runs
      Class.new do
        define_method(:interval) { interval }
        define_method(:run) { |conn| recorder << conn }
      end.new
    end

    it "runs maintenance on the health-check window, throttled to the interval, on the LISTEN connection" do
      listener.start

      # First idle window → maintenance runs (never run before).
      wait_until { runs.size >= 1 }
      expect(runs.first).to eq(fake_pg)

      # Later idle windows still inside the interval → NOT run again.
      wait_until { fake_pg.executed.count("SELECT 1") >= 3 }
      expect(runs.size).to eq(1)

      # Advance past the interval → runs again on the next window.
      fake_clock[0] += 301
      wait_until { runs.size >= 2 }
      expect(runs.size).to eq(2)
    end

    it "does not disturb the listen loop when maintenance raises" do
      allow(maintenance).to receive(:run).and_raise(StandardError, "boom")
      listener.start
      wait_until { fake_pg.executed.include?("SELECT 1") }
      fake_pg.push_notify("pgmq.q_chat.INSERT") # loop must still be alive
      wait_until { dispatch_queue.size >= 1 }
      expect(dispatch_queue.size).to be >= 1
    end
  end
end
