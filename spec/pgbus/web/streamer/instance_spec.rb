# frozen_string_literal: true

require "spec_helper"
require "socket"

RSpec.describe Pgbus::Web::Streamer::Instance do
  subject(:streamer) do
    described_class.new(
      client: client,
      config: config,
      pg_connection: fake_pg,
      logger: Logger.new(IO::NULL)
    )
  end

  before do
    # PG::Error is used by Listener and is not loaded in unit tests.
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
  end

  # Reuse the same fake PG connection shape from listener_spec so the
  # Listener thread has something to pop from.
  let(:fake_pg) do
    Class.new do
      attr_reader :executed

      def initialize
        @executed = []
        @events = Queue.new
      end

      def exec(sql)
        @executed << sql
        nil
      end

      # Mirrors PG::Connection#wait_for_notify(timeout): returns nil once the
      # timeout elapses. Honoring the timeout matters because Listener#stop
      # signals shutdown by clearing a flag the listener thread checks between
      # waits — it no longer closes the connection out from under that thread
      # (issue #375).
      def wait_for_notify(timeout)
        event = @events.pop(timeout: timeout)
        case event&.first
        when nil, :timeout
          nil
        when :notify
          yield event[1], 0, nil
          event[1]
        when :close
          raise PG::Error, "closed"
        end
      end

      def reset = nil
      def close = @events << [:close]
      def push_timeout = @events << [:timeout]
    end.new
  end

  let(:client) do
    instance_double(
      Pgbus::Client,
      read_after: [],
      stream_current_msg_id: 0,
      ensure_stream_queue: nil,
      shared_connection?: false,
      config: config
    )
  end

  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.streams_listen_health_check_ms = 50
      c.streams_heartbeat_interval = 0.05
      c.streams_idle_timeout = 60
      c.streams_write_deadline_ms = 1_000
    end
  end

  after { streamer.shutdown! }

  def build_sse_connection(id: "c1", stream: "chat", since_id: 0)
    writer_io, reader_io = UNIXSocket.pair
    conn = Pgbus::Web::Streamer::Connection.new(
      id: id,
      stream_name: stream,
      io: writer_io,
      since_id: since_id,
      writer: Pgbus::Web::Streamer::IoWriter,
      write_deadline_ms: config.streams_write_deadline_ms
    )
    [conn, reader_io]
  end

  describe "#build_pg_connection" do
    let(:fake_pg_module_conn) do
      Class.new do
        def close; end
        def reset = nil
      end.new
    end

    let(:streamer_config) do
      Pgbus::Configuration.new.tap do |c|
        c.connection_params = { host: "pooler.example", port: 6432, dbname: "app", user: "app" }
        c.streams_port = 5432
      end
    end

    # Neutralize the self-probe by default (it owns a real LISTEN/NOTIFY dance
    # covered by notify_probe_spec); probe-specific examples below re-stub it.
    before do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)
      # build_pg_connection validates the freshly built connection is a primary.
      # Neutralize the validator by default (fake connections aren't real);
      # the replica-rejection examples below re-stub it to raise.
      allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!) { |conn| conn }
    end

    it "uses streams_connection_options so the listener can be pointed at a different port" do
      require "pg"
      captured = nil
      allow(PG).to receive(:connect) do |**kwargs|
        captured = kwargs
        fake_pg_module_conn
      end

      described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))

      expect(captured).to eq(host: "pooler.example", port: 5432, dbname: "app", user: "app",
                             fallback_application_name: "pgbus-listen")
    end

    it "runs the LISTEN/NOTIFY self-probe on the freshly built connection" do
      require "pg"
      allow(PG).to receive(:connect).and_return(fake_pg_module_conn)
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))

      expect(Pgbus::Process::NotifyProbe).to have_received(:probe_notify_delivery!)
        .with(fake_pg_module_conn, logger: instance_of(Logger))
    end

    it "still builds the streamer when the probe fails (graceful degradation)" do
      require "pg"
      allow(PG).to receive(:connect).and_return(fake_pg_module_conn)
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(false)

      expect do
        described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))
      end.not_to raise_error
    end

    it "does not probe an injected pg_connection (probe is for self-built connections)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      described_class.new(client: client, config: config, pg_connection: fake_pg, logger: Logger.new(IO::NULL))

      expect(Pgbus::Process::NotifyProbe).not_to have_received(:probe_notify_delivery!)
    end

    it "wires a connection_factory into the Listener that builds a fresh raw connection" do
      require "pg"
      connects = 0
      allow(PG).to receive(:connect) do |**_kwargs|
        connects += 1
        fake_pg_module_conn
      end

      instance = described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))

      factory = instance.listener.instance_variable_get(:@connection_factory)
      expect(factory).to respond_to(:call)

      # Calling the factory builds another raw connection (no probe/validate:
      # the reconnect loop owns those). One connect for the initial build, one
      # for the factory invocation here.
      connects_before = connects
      factory.call
      expect(connects).to eq(connects_before + 1)
    end

    it "still wires a connection_factory when a pg_connection is injected (reconnect rebuilds fresh)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)

      instance = described_class.new(client: client, config: config, pg_connection: fake_pg, logger: Logger.new(IO::NULL))

      expect(instance.listener.instance_variable_get(:@connection_factory)).to respond_to(:call)
    end

    it "passes an injected connection_factory through to the Listener (no real config touched)" do
      allow(Pgbus::Process::NotifyProbe).to receive(:probe_notify_delivery!).and_return(true)
      injected = -> { fake_pg }

      instance = described_class.new(
        client: client, config: config, pg_connection: fake_pg,
        logger: Logger.new(IO::NULL), connection_factory: injected
      )

      expect(instance.listener.instance_variable_get(:@connection_factory)).to be(injected)
    end

    context "when connection_guc_mode is :session with database.yml variables (issue #352)" do
      # In :session mode Configuration#forward_connection_variables leaves the
      # `variables:` hash on the options; :variables is not a libpq keyword, so
      # a raw PG.connect(**opts) fails with `invalid connection option
      # "variables"` — breaking every SSE stream on the worker.
      let(:session_config) do
        Pgbus::Configuration.new.tap do |c|
          c.connection_guc_mode = :session
          c.connection_params = { host: "pooler.example", port: 6432, dbname: "app", user: "app",
                                  variables: { statement_timeout: "10s", timezone: "UTC" } }
          c.streams_port = 5432
        end
      end

      let(:exec_recording_conn) do
        Class.new do
          attr_reader :executed

          def initialize
            @executed = []
          end

          def exec(sql)
            @executed << sql
            nil
          end

          def close; end
          def reset = nil
        end.new
      end

      it "strips the non-libpq :variables key before PG.connect" do
        require "pg"
        captured = nil
        allow(PG).to receive(:connect) do |**kwargs|
          captured = kwargs
          exec_recording_conn
        end

        described_class.new(client: client, config: session_config, logger: Logger.new(IO::NULL))

        expect(captured).to eq(host: "pooler.example", port: 5432, dbname: "app", user: "app",
                               fallback_application_name: "pgbus-listen")
      end

      it "keeps the GUCs by applying them via post-connect SET" do
        require "pg"
        allow(PG).to receive(:connect).and_return(exec_recording_conn)

        described_class.new(client: client, config: session_config, logger: Logger.new(IO::NULL))

        expect(exec_recording_conn.executed).to eq(
          ["SET statement_timeout = '10s'", "SET timezone = 'UTC'"]
        )
      end
    end

    context "when the freshly built connection lands on a read-only replica" do
      before do
        require "pg"
        allow(PG).to receive(:connect).and_return(fake_pg_module_conn)
        allow(Pgbus::Process::PrimaryValidator).to receive(:validate_primary!)
          .and_raise(Pgbus::Process::ReplicaConnectionError.new("on a replica"))
      end

      it "raises a ConfigurationError naming the streams_* override keys" do
        error = nil
        begin
          described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))
        rescue Pgbus::ConfigurationError => e
          error = e
        end

        expect(error).to be_a(Pgbus::ConfigurationError)
        expect(error.message).to include("streams_database_url", "streams_host", "streams_port")
      end

      it "names the read-only replica condition in the message" do
        expect do
          described_class.new(client: client, config: streamer_config, logger: Logger.new(IO::NULL))
        end.to raise_error(Pgbus::ConfigurationError, /replica/i)
      end
    end
  end

  describe "#start" do
    it "starts the listener, dispatcher, and heartbeat threads" do
      streamer.start
      fake_pg.push_timeout
      expect(streamer.listener).to be_a(Pgbus::Web::Streamer::Listener)
      expect(streamer.dispatcher).to be_a(Pgbus::Web::Streamer::StreamEventDispatcher)
      expect(streamer.heartbeat).to be_a(Pgbus::Web::Streamer::Heartbeat)
      expect(streamer.peer_close_watcher.threads.size).to eq(1)
    end

    it "posts a disconnect as soon as a registered client closes its socket (issue #519)" do
      streamer.start
      fake_pg.push_timeout
      conn, reader = build_sse_connection
      streamer.register(conn)
      deadline = Time.now + 2
      sleep 0.01 until streamer.registry.connections_for("chat").include?(conn) || Time.now > deadline

      reader.close

      deadline = Time.now + 3
      sleep 0.01 until !streamer.registry.connections_for("chat").include?(conn) || Time.now > deadline
      expect(streamer.registry.connections_for("chat")).not_to include(conn)
    end

    it "does not start the peer-close watcher when streams_peer_close_interval is nil" do
      config.streams_peer_close_interval = nil
      streamer.start
      fake_pg.push_timeout

      expect(streamer.peer_close_watcher).to be_nil
    end

    it "is idempotent — calling start twice does not spawn duplicate threads" do
      streamer.start
      fake_pg.push_timeout
      expect { streamer.start }.not_to raise_error
    end
  end

  describe "#register" do
    it "enqueues a ConnectMessage that the dispatcher processes" do
      streamer.start
      fake_pg.push_timeout

      conn, reader = build_sse_connection
      streamer.register(conn)

      deadline = Time.now + 2
      sleep 0.01 until streamer.registry.connections_for("chat").include?(conn) || Time.now > deadline

      expect(streamer.registry.connections_for("chat")).to include(conn)
      reader.close
      conn.io.close
    end

    it "is a no-op for a connection that is already dead" do
      streamer.start
      fake_pg.push_timeout
      conn, reader = build_sse_connection
      conn.mark_dead!

      streamer.register(conn)
      sleep 0.1

      expect(streamer.registry.connections_for("chat")).not_to include(conn)
      reader.close
      conn.io.close
    end

    it "rejects a registration that races worker shutdown" do
      # Without the @shutdown_mutex guard in register, a request thread
      # arriving after shutdown! has stopped the dispatcher would push
      # a ConnectMessage onto a queue no one is draining, leaving the
      # socket outside the registry and outside close_all_connections.
      # The fix marks such late connections dead so the caller knows
      # registration was rejected.
      streamer.start
      fake_pg.push_timeout
      streamer.shutdown!

      conn, reader = build_sse_connection(id: "late")
      streamer.register(conn)

      expect(conn.dead?).to be true
      expect(streamer.registry.connections_for("chat")).not_to include(conn)
      expect(streamer.dispatch_queue).to be_empty
      reader.close
      conn.io.close
    end
  end

  describe "#shutdown!" do
    def drain_until(reader, needle, timeout: 2)
      buffer = +""
      Thread.new do
        until buffer.include?(needle)
          begin
            buffer << reader.read_nonblock(4096)
          rescue IO::WaitReadable
            reader.wait_readable(0.1)
          rescue EOFError
            break
          end
        end
      end.join(timeout)
      buffer
    end

    it "writes a pgbus:shutdown sentinel to every registered connection" do
      streamer.start
      fake_pg.push_timeout
      conn, reader = build_sse_connection
      streamer.registry.register(conn)

      Thread.new do
        sleep 0.05
        streamer.shutdown!
      end
      drained = drain_until(reader, "pgbus:shutdown")
      reader.close

      expect(drained).to include("event: pgbus:shutdown")
      expect(drained).to include('data: {"reason":"worker_restart"}')
    end

    it "returns an empty list when every component thread stopped within its join budget (issue #443)" do
      streamer.start
      fake_pg.push_timeout
      expect(streamer.shutdown!).to eq([])
    end

    it "names and logs the components whose threads outlived stop instead of returning silently (issue #443)" do
      log = StringIO.new
      instance = described_class.new(client: client, config: config, pg_connection: fake_pg, logger: Logger.new(log))
      instance.start
      fake_pg.push_timeout
      stuck = Thread.new { sleep }
      allow(instance.dispatcher).to receive(:threads).and_return([stuck])

      leaked = instance.shutdown!

      expect(leaked).to eq(["dispatcher"])
      expect(log.string).to include("dispatcher").and include("still running")
    ensure
      stuck&.kill
    end

    it "returns [] from a second shutdown! call (idempotent) (issue #443)" do
      streamer.start
      fake_pg.push_timeout
      streamer.shutdown!
      expect(streamer.shutdown!).to eq([])
    end

    it "is idempotent — calling shutdown! twice is a no-op the second time" do
      streamer.start
      fake_pg.push_timeout
      streamer.shutdown!
      expect { streamer.shutdown! }.not_to raise_error
    end
  end

  describe "writer offload wiring (issue #321)" do
    context "when streams_writer_threads is 0 (default)" do
      it "builds no pump — fanout stays inline" do
        expect(streamer.pump).to be_nil
      end
    end

    context "when streams_writer_threads is positive" do
      before { config.streams_writer_threads = 2 }

      it "builds an OutboundPump and wires it into the dispatcher" do
        expect(streamer.pump).to be_a(Pgbus::Web::Streamer::OutboundPump)
      end

      it "starts the pump and drains + stops it on shutdown (B3) with no thread leak" do
        streamer.start
        expect(streamer.pump.alive?).to be true # workers running while up

        fake_pg.push_timeout
        streamer.shutdown!

        # Assert on the pump's OWN writer threads, not the global Thread.list
        # (which is noisy). After shutdown every writer thread has exited.
        expect(streamer.pump.alive?).to be false
        expect(streamer.pump.worker_threads).to all(satisfy { |t| !t.alive? })
      end
    end
  end

  describe "module-level Streamer.current / Streamer.reset!" do
    after { Pgbus::Web::Streamer.reset! }

    it "memoises a single instance across calls" do
      fake_pg2 = fake_pg.class.new
      instance1 = described_class.new(
        client: client, config: config, pg_connection: fake_pg2,
        logger: Logger.new(IO::NULL)
      )
      Pgbus::Web::Streamer.current = instance1
      expect(Pgbus::Web::Streamer.current).to be(instance1)
    end

    it "resets and tears down the current instance" do
      fake_pg2 = fake_pg.class.new
      instance1 = described_class.new(
        client: client, config: config, pg_connection: fake_pg2,
        logger: Logger.new(IO::NULL)
      ).tap(&:start)
      fake_pg2.push_timeout
      Pgbus::Web::Streamer.current = instance1

      Pgbus::Web::Streamer.reset!

      expect(Pgbus::Web::Streamer.instance_variable_get(:@current)).to be_nil
    end

    it "returns nil when there is no current instance (issue #443)" do
      expect(Pgbus::Web::Streamer.reset!).to be_nil
    end

    it "returns the leaked component list reported by shutdown! (issue #443)" do
      fake_pg2 = fake_pg.class.new
      instance1 = described_class.new(
        client: client, config: config, pg_connection: fake_pg2,
        logger: Logger.new(IO::NULL)
      ).tap(&:start)
      fake_pg2.push_timeout
      allow(instance1).to(receive(:shutdown!).and_wrap_original do |m|
        m.call
        ["dispatcher"]
      end)
      Pgbus::Web::Streamer.current = instance1

      expect(Pgbus::Web::Streamer.reset!).to eq(["dispatcher"])
    end

    it "serialises concurrent first-callers so only one Instance is built" do
      # Without @current_mutex, two threads racing into Streamer.current
      # would both build and start an Instance, leaking the loser of
      # the `@current ||=` race.
      build_count = Concurrent::AtomicFixnum.new(0)
      original_new = described_class.method(:new)
      allow(described_class).to receive(:new) do |**kwargs|
        build_count.increment
        sleep 0.01 # widen the race window so the GVL doesn't accidentally serialise
        original_new.call(**kwargs)
      end

      fake_pgs = Array.new(10) { fake_pg.class.new }
      instances = Concurrent::Array.new
      threads = Array.new(10) do |i|
        Thread.new do
          instances << Pgbus::Web::Streamer.current(client: client, config: config, pg_connection: fake_pgs[i], logger: Logger.new(IO::NULL))
        end
      end
      threads.each(&:join)
      fake_pgs.each(&:push_timeout)

      expect(instances.uniq.size).to eq(1)
      expect(build_count.value).to eq(1)
    end
  end

  describe "streams-pool autoscaler wiring (issue #323)" do
    let(:autoscale_client) do
      instance_double(
        Pgbus::Client,
        read_after: [], stream_current_msg_id: 0, ensure_stream_queue: nil,
        config: config, shared_connection?: shared
      )
    end
    let(:shared) { false }

    def build_instance
      described_class.new(client: autoscale_client, config: config, pg_connection: fake_pg,
                          logger: Logger.new(IO::NULL))
    end

    context "when autoscale is off (default)" do
      it "does not build an autoscaler (no maintenance, no shared_connection? probe)" do
        instance = build_instance
        expect(instance.autoscaler).to be_nil
      ensure
        instance&.shutdown!
      end
    end

    context "when autoscale is on but the connection is shared (AR path)" do
      let(:shared) { true }

      before { config.streams_pool_autoscale = true }

      it "does not build an autoscaler (resize is a no-op on the shared path)" do
        instance = build_instance
        expect(instance.autoscaler).to be_nil
      ensure
        instance&.shutdown!
      end
    end

    context "when autoscale is on and the connection is dedicated" do
      before { config.streams_pool_autoscale = true }

      it "builds a pure-decision autoscaler (no thread — it has no start/stop)" do
        instance = build_instance
        expect(instance.autoscaler).to be_a(Pgbus::Streams::PoolAutoscaler)
        expect(instance.autoscaler).not_to respond_to(:start)
      ensure
        instance&.shutdown!
      end

      it "wires the autoscaler into the Listener as a throttled maintenance task" do
        maintenance = instance_double(Pgbus::Streams::PoolAutoscaler::Maintenance)
        allow(Pgbus::Streams::PoolAutoscaler::Maintenance).to receive(:new).and_return(maintenance)
        captured = nil
        allow(Pgbus::Web::Streamer::Listener).to receive(:new).and_wrap_original do |orig, **kwargs|
          captured = kwargs[:maintenance]
          orig.call(**kwargs)
        end

        instance = build_instance
        expect(captured).to eq(maintenance)
        expect(Pgbus::Streams::PoolAutoscaler::Maintenance).to have_received(:new)
          .with(autoscaler: instance.autoscaler, interval: config.streams_pool_autoscale_interval,
                application_name_prefix: config.streams_application_name)
      ensure
        instance&.shutdown!
      end
    end
  end

  describe "listener selection by streams_listen_scope (issue #382)" do
    require "tmpdir"

    let(:tmpdir) { Dir.mktmpdir("pgbus-instance-hub") }
    let(:socket_path) { File.join(tmpdir, "hub.sock") }
    let(:hub_server) { UNIXServer.new(socket_path) }

    after do
      hub_server.close if File.socket?(socket_path) && !hub_server.closed?
      FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
    end

    around do |example|
      original = ENV.fetch("PGBUS_STREAMS_HUB_SOCKET", nil)
      example.run
    ensure
      original ? ENV["PGBUS_STREAMS_HUB_SOCKET"] = original : ENV.delete("PGBUS_STREAMS_HUB_SOCKET")
    end

    it "uses a FailoverListener (no per-worker LISTEN connection) when the hub socket is reachable" do
      hub_server # bind before the instance connects
      ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path
      config.streams_listen_scope = :master
      allow(PG).to receive(:connect) # must NOT be called — that's the whole point

      instance = described_class.new(client: client, config: config, logger: Logger.new(IO::NULL))

      expect(instance.listener).to be_a(Pgbus::Web::Streamer::FailoverListener)
      expect(PG).not_to have_received(:connect)
      instance.listener.stop
    end

    it "falls back to a per-worker Listener when the socket path is exported but dead" do
      ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path # nothing bound there
      config.streams_listen_scope = :master

      instance = described_class.new(
        client: client, config: config, pg_connection: fake_pg, logger: Logger.new(IO::NULL)
      )

      expect(instance.listener).to be_a(Pgbus::Web::Streamer::Listener)
    end

    it "uses a per-worker Listener under scope :process even with a live hub socket" do
      hub_server
      ENV["PGBUS_STREAMS_HUB_SOCKET"] = socket_path
      config.streams_listen_scope = :process

      instance = described_class.new(
        client: client, config: config, pg_connection: fake_pg, logger: Logger.new(IO::NULL)
      )

      expect(instance.listener).to be_a(Pgbus::Web::Streamer::Listener)
    end

    it "uses a per-worker Listener when no socket path is exported (single mode)" do
      instance = described_class.new(
        client: client, config: config, pg_connection: fake_pg, logger: Logger.new(IO::NULL)
      )

      expect(instance.listener).to be_a(Pgbus::Web::Streamer::Listener)
    end
  end
end
