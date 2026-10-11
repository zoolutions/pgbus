# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::StreamEventDispatcher do
  subject(:dispatcher) do
    described_class.new(
      client: client,
      registry: registry,
      listener: listener,
      dispatch_queue: dispatch_queue,
      logger: logger,
      read_limit: 500,
      config: dispatcher_config
    )
  end

  # Dispatcher config: independent of Pgbus.configuration so toggling
  # streams_stats_enabled in one test can't bleed into another. Presence
  # is off by default here; the presence describe block injects its own
  # config that enables it.
  let(:dispatcher_config) do
    instance_double(
      Pgbus::Configuration,
      streams_stats_enabled: false,
      stream_presence?: false,
      streams_fanout_write_deadline_ms: 250,
      streams_write_deadline_ms: 5_000
    )
  end

  let(:client) do
    double("Pgbus::Client", config: stream_config)
  end
  let(:stream_config) do
    instance_double(Pgbus::Configuration).tap do |c|
      # Dispatcher#full_table_name_for calls config.queue_name(stream) to
      # translate logical names to PGMQ full table names. In these unit
      # tests we pass the stream name through unchanged — the real
      # prefixing behavior is covered in the integration tests.
      allow(c).to receive(:queue_name) { |name| name }
    end
  end
  # The connection double records enqueued envelopes and exposes a
  # controllable cursor. It implements just the surface the Dispatcher
  # touches, so the test is hermetic and doesn't depend on the real
  # Connection's write path.
  let(:conn_class) do
    Class.new do
      attr_accessor :last_msg_id_sent, :presence_member, :on_connected
      attr_reader :id, :stream_name, :enqueued, :context, :enqueue_deadlines, :connected_writes

      def initialize(id:, stream_name:, last_msg_id_sent: 0, context: nil)
        @id = id
        @stream_name = stream_name
        @last_msg_id_sent = last_msg_id_sent
        @context = context
        @enqueued = []
        # Records the deadline_ms the dispatcher passed on each enqueue so
        # tests can assert fanout uses the short deadline and connect-replay
        # the full one (issue #315 item 3).
        @enqueue_deadlines = []
        @dead = false
        @presence_member = nil
        @connected_writes = 0
      end

      # The pgbus:connected frame (issue #521): counted, and on_connected
      # lets a test look at the dispatcher's state at the moment it is sent.
      def write_connected
        @connected_writes += 1
        on_connected&.call
        :ok
      end

      def enqueue(envelopes, deadline_ms: nil)
        @enqueue_deadlines << deadline_ms
        @enqueued.concat(envelopes)
        envelopes.each { |e| @last_msg_id_sent = e.msg_id if e.msg_id > @last_msg_id_sent }
        envelopes
      end

      def dead? = @dead
      def mark_dead! = @dead = true
    end
  end
  let(:registry)       { Pgbus::Web::Streamer::Registry.new }
  let(:listener)       { double("Listener", ensure_listening: nil, remove_listening: nil) }
  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }

  def build_conn(**kwargs)
    conn_class.new(**kwargs)
  end

  def build_envelope(msg_id)
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: msg_id,
      enqueued_at: "2026-04-07T00:00:00Z",
      payload: "<turbo-stream>#{msg_id}</turbo-stream>",
      source: "live"
    )
  end

  describe "WakeMessage fanout" do
    it "reads from the minimum cursor across registered connections" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 100)
      c2 = build_conn(id: "b", stream_name: "chat", last_msg_id_sent: 200)
      registry.register(c1)
      registry.register(c2)

      allow(client).to receive(:read_after)
        .and_return([build_envelope(150), build_envelope(250)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(client).to have_received(:read_after).with("chat", after_id: 100, limit: 500)
      # c1 sees both (150, 250) because its cursor was at 100
      # c2 only sees 250 (envelope 150 <= c2's cursor of 200 — Connection dedups)
      expect(c1.enqueued.map(&:msg_id)).to contain_exactly(150, 250)
      expect(c2.enqueued.map(&:msg_id)).to contain_exactly(150, 250) # fake conn doesn't dedupe
    end

    it "is a no-op when no connections subscribe to the stream" do
      allow(client).to receive(:read_after)
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "unknown"))
      expect(client).not_to have_received(:read_after)
    end

    it "posts DisconnectMessages for connections that turned dead during enqueue" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      c1.mark_dead!

      allow(client).to receive(:read_after).and_return([build_envelope(10)])
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      msg = dispatch_queue.pop
      expect(msg).to be_a(described_class::DisconnectMessage)
      expect(msg.connection).to be(c1)
    end
  end

  describe "per-phase write deadline (issue #315 item 3)" do
    it "fans out a durable wake with the SHORT fanout deadline" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(c1.enqueue_deadlines).to eq([250])
    end

    it "fans out an ephemeral wake with the SHORT fanout deadline, written inline (not dropped)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      payload = JSON.generate({ "html" => "<turbo-stream>ephemeral</turbo-stream>" })

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat", payload: payload))

      # The ephemeral frame reached the connection (no B1-style silent drop),
      # and it used the short fanout deadline.
      expect(c1.enqueued.length).to eq(1)
      expect(c1.enqueue_deadlines).to eq([250])
    end

    it "replays a new connection with the FULL write deadline, not the short fanout one" do
      conn = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      # handle_connect steps 3 (archive replay) and 4 (in-flight drain) both
      # use the full deadline; a fresh client catching up must not be killed
      # by the 250ms fanout budget. The in-flight drain is a no-op empty
      # buffer here, so only the non-empty step-3 write is recorded.
      expect(conn.enqueue_deadlines).to all(eq(5_000))
      expect(conn.enqueue_deadlines).not_to include(250)
    end
  end

  describe "head-of-line blocking is bounded (issue #315 item 3)" do
    # A connection whose enqueue blocks for `stall` seconds then marks itself
    # dead — models a slow client whose fanout write hit the deadline. The
    # dispatcher's registered.each loop must continue to the next connection
    # after each stall, so K slow clients cost ~K*stall (bounded), and a fast
    # client on the same stream is still serviced in the same wake.
    let(:slow_conn_class) do
      Class.new do
        attr_accessor :last_msg_id_sent, :presence_member
        attr_reader :id, :stream_name, :enqueued, :context, :enqueue_deadlines

        def initialize(id:, stall:)
          @id = id
          @stream_name = "chat"
          @last_msg_id_sent = 0
          @stall = stall
          @enqueued = []
          @enqueue_deadlines = []
          @dead = false
          @presence_member = nil
        end

        def enqueue(envelopes, deadline_ms: nil)
          @enqueue_deadlines << deadline_ms
          sleep(@stall) # models the write blocking up to the fanout deadline
          mark_dead!    # a :blocked write marks the connection dead
          @enqueued.concat(envelopes)
          []
        end

        def dead? = @dead
        def mark_dead! = @dead = true
      end
    end

    it "services a fast client on the same stream after a slow client stalls (loop advances)" do
      slow = slow_conn_class.new(id: "slow", stall: 0.05)
      fast = build_conn(id: "fast", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(slow)
      registry.register(fast)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      # The fast client received the frame despite the slow client stalling
      # first — proof the fanout loop is not abandoned by one slow client.
      expect(fast.enqueued.map(&:msg_id)).to eq([10])
      # The slow client was marked dead (its blocked write hit the deadline)
      # and a DisconnectMessage was posted for it via prune_dead.
      expect(slow.dead?).to be true
      disconnects = []
      disconnects << dispatch_queue.pop until dispatch_queue.empty?
      expect(disconnects.map(&:connection)).to include(slow)
    end

    it "bounds total stall to ~K*stall for K slow clients, not K*full_deadline" do
      k = 3
      stall = 0.02
      slow_conns = Array.new(k) { |i| slow_conn_class.new(id: "slow-#{i}", stall: stall) }
      slow_conns.each { |c| registry.register(c) }
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      # Serial stalls sum to ~K*stall. Generous ceiling (K*stall + slack) to
      # stay non-flaky; the point is it scales with the SHORT deadline, and
      # every slow client was serviced-then-pruned in the one pass.
      expect(elapsed).to be < (k * stall) + 0.5
      expect(slow_conns).to all(be_dead)
    end
  end

  describe "durable fanout offload (issue #321)" do
    # A spy pump records post() calls instead of writing. The dispatcher, when
    # given a pump, must route durable fanout writes to it (not inline) and
    # advance the scan cursor only when it drains a WriteAckMessage.
    subject(:dispatcher) do
      described_class.new(
        client: client, registry: registry, listener: listener,
        dispatch_queue: dispatch_queue, logger: logger, read_limit: 500,
        config: dispatcher_config, pump: pump, ack_queue: ack_queue
      )
    end

    let(:pump) do
      Class.new do
        attr_reader :posts

        def initialize = @posts = []

        def post(connection, envelopes, batch_max, deadline_ms:)
          @posts << { connection: connection, envelopes: envelopes, batch_max: batch_max, deadline_ms: deadline_ms }
        end
      end.new
    end
    let(:ack_queue) { Queue.new }

    it "routes a durable wake to the pump instead of writing inline" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      allow(client).to receive(:read_after).and_return([build_envelope(10), build_envelope(11)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(pump.posts.size).to eq(1)
      expect(pump.posts.first[:connection]).to be(c1)
      expect(pump.posts.first[:envelopes].map(&:msg_id)).to eq([10, 11])
      expect(pump.posts.first[:batch_max]).to eq(11)
      expect(pump.posts.first[:deadline_ms]).to eq(250) # short fanout deadline
      # No inline enqueue happened (the fake conn records enqueue_deadlines).
      expect(c1.enqueue_deadlines).to be_empty
    end

    it "does NOT advance the scan cursor at post time (deferred to the ack, B2)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      allow(client).to receive(:read_after).and_return([build_envelope(10), build_envelope(11)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      # minimum_cursor for the next read should still see cursor 0 (nothing
      # acked yet), NOT 11 — proving the dispatcher didn't speculatively advance.
      expect(dispatcher.send(:cursor_for, c1)).to eq(0)
    end

    it "advances the scan cursor by the ACCEPTED max when it drains an ack (B2)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      ack_queue << described_class::WriteAckMessage.new(connection: c1, accepted_max: 10)

      dispatcher.send(:apply_acks)

      expect(dispatcher.send(:cursor_for, c1)).to eq(10)
    end

    it "drops a late ack for an already-disconnected connection (no @scanned_cursor resurrection)" do
      # Race: the pump acks a successful write, but a DisconnectMessage for the
      # same connection was processed first (heartbeat idle-sweep). The late ack
      # must NOT reinsert the connection into @scanned_cursor — that entry would
      # leak forever since handle_disconnect never runs for it again.
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      # Disconnect first: unregisters + deletes @scanned_cursor[c1].
      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c1))
      # Then a stale ack arrives for the now-gone connection.
      ack_queue << described_class::WriteAckMessage.new(connection: c1, accepted_max: 10)

      dispatcher.send(:apply_acks)

      scanned = dispatcher.instance_variable_get(:@scanned_cursor)
      expect(scanned).not_to have_key(c1)
    end

    it "drains pending acks before computing minimum_cursor on a wake (apply_acks graft)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      # An ack for 10 is queued; the next wake must apply it FIRST so read_after
      # is called with after_id: 10, not 0 (the re-read storm guard).
      ack_queue << described_class::WriteAckMessage.new(connection: c1, accepted_max: 10)
      allow(client).to receive(:read_after).and_return([])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(client).to have_received(:read_after).with("chat", after_id: 10, limit: 500)
    end

    it "keeps EPHEMERAL fanout INLINE even when the pump is present (B1)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      payload = JSON.generate({ "html" => "<turbo-stream>ephemeral</turbo-stream>" })

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat", payload: payload))

      # The pump was NOT used for the ephemeral; it was written inline.
      expect(pump.posts).to be_empty
      expect(c1.enqueued.map(&:payload)).to eq(["<turbo-stream>ephemeral</turbo-stream>"])
    end

    it "still posts (with batch_max) when the filtered batch is empty so the cursor advances (B2b)" do
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      # An audience filter that hides everything.
      allow(dispatcher).to receive(:visible_envelopes_for).and_return([])
      allow(client).to receive(:read_after).and_return([build_envelope(12)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(pump.posts.size).to eq(1)
      expect(pump.posts.first[:envelopes]).to be_empty
      expect(pump.posts.first[:batch_max]).to eq(12)
    end
  end

  describe "ConnectMessage — the 5-step race-free replay sequence" do
    let(:connection) { build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1247) }

    it "ensures the listener is subscribed to the stream (step 1)" do
      allow(client).to receive(:read_after).and_return([])
      allow(listener).to receive(:ensure_listening)

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(listener).to have_received(:ensure_listening).with("chat")
    end

    it "reads the archive from the connection's since_id (step 3)" do
      allow(client).to receive(:read_after)
        .and_return([build_envelope(1248), build_envelope(1249)])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(client).to have_received(:read_after).with("chat", after_id: 1247, limit: 500)
      expect(connection.enqueued.map(&:msg_id)).to eq([1248, 1249])
    end

    it "registers the connection in the main registry (step 5)" do
      allow(client).to receive(:read_after).and_return([])
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(registry.connections_for("chat")).to contain_exactly(connection)
    end

    it "does not register a connection that died during replay" do
      allow(client).to receive(:read_after).and_return([])
      connection.mark_dead!
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(registry.connections_for("chat")).to be_empty
    end

    it "cleans up @full_to_logical and unlistens when a connect dies before registry promotion" do
      # Without the dead-before-register cleanup path, this scenario
      # would pin @full_to_logical[full_name] = "chat" and leave the
      # PG LISTEN active for the life of the worker, since the
      # connection never reaches the registry and no DisconnectMessage
      # is ever emitted.
      allow(client).to receive(:read_after).and_return([])
      connection.mark_dead!

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(dispatcher.instance_variable_get(:@full_to_logical)).to be_empty
      expect(listener).to have_received(:remove_listening).with("chat")
    end

    it "cleans up @full_to_logical and unlistens when step 3 raises" do
      # Same leak surface from a thrown exception path — the rescue
      # must scrub state so a transient failure on a single connect
      # doesn't permanently bloat @full_to_logical.
      allow(client).to receive(:read_after).and_raise(StandardError, "boom")

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(dispatcher.instance_variable_get(:@full_to_logical)).to be_empty
      expect(listener).to have_received(:remove_listening).with("chat")
      expect(connection.dead?).to be true
    end

    it "writes the pgbus:connected frame once the connection is registered (issue #521)" do
      allow(client).to receive(:read_after).and_return([build_envelope(1248)])
      registered_at_write = nil
      connection.on_connected = -> { registered_at_write = registry.connections_for("chat").include?(connection) }

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(connection.connected_writes).to eq(1)
      expect(registered_at_write).to be(true)
    end

    it "writes no pgbus:connected frame to a connection that died during replay (issue #521)" do
      allow(client).to receive(:read_after).and_return([])
      connection.mark_dead!

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(connection.connected_writes).to eq(0)
    end

    it "removes the in-flight buffer entry after register" do
      allow(client).to receive(:read_after).and_return([])
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))
      expect(dispatcher.instance_variable_get(:@in_flight)["chat"]).to be_nil.or eq([])
    end
  end

  describe "the replay race (the rails/rails#52420 fix)" do
    # These are the load-bearing tests for the whole subsystem. They prove
    # that a WakeMessage arriving *between* "subscribe" and "register"
    # during a ConnectMessage is not lost. Without the in-flight buffer
    # pattern, handle_wake finds no connections to fan out to (the new
    # connection isn't in the registry yet) and the archive read in step 3
    # may miss a message that hasn't committed yet.

    it "routes a WakeMessage to in-flight connects, not just registered ones" do
      # Manually install an in-flight entry, then fire a WakeMessage and
      # verify the buffer received the envelopes. This tests the routing
      # half of the race fix in isolation.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)
      buffer = []
      dispatcher.instance_variable_get(:@in_flight)["chat"] << [connection, buffer]

      allow(client).to receive(:read_after)
        .with("chat", after_id: 1000, limit: 500)
        .and_return([build_envelope(1050), build_envelope(1051)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(buffer.map(&:msg_id)).to eq([1050, 1051])
      # The in-flight connection is NOT in the registry, so it should
      # not have been enqueued to directly by the wake path — only the
      # buffer gets populated.
      expect(connection.enqueued).to be_empty
    end

    it "drains the in-flight buffer into the connection on register (step 4)" do
      # Verifies the drain half of the race fix: after handle_connect
      # completes, the connection has received anything that arrived in
      # the buffer while step 3's read_after was still running.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)

      # read_after is called twice during handle_connect:
      #   1. step 3: the initial archive read for the connection
      #   2. NO — step 4 drains the in-flight buffer directly, not via read_after
      # So we only expect one call.
      allow(client).to receive(:read_after) do |*|
        # While step 3 is running, simulate a WakeMessage landing for
        # this stream by invoking handle_wake directly. This populates
        # the in-flight buffer with a newly-broadcast envelope.
        allow(client).to receive(:read_after).and_return([build_envelope(1050)])
        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
        # After the wake, step 3's read still needs to return something.
        # In a real race the archive might or might not have the message
        # yet — we return empty to prove the buffer path is what saves us.
        []
      end

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      # The envelope must have landed on the connection via the buffer
      # drain (step 4), since step 3 returned an empty result.
      expect(connection.enqueued.map(&:msg_id)).to contain_exactly(1050)
    end

    it "handles the simple case where step 3 finds the envelope in the archive" do
      # This is the non-racy path: the broadcast committed before connect
      # started, so the archive read in step 3 returns the envelope and
      # the buffer stays empty.
      connection = build_conn(id: "new", stream_name: "chat", last_msg_id_sent: 1000)

      allow(client).to receive(:read_after)
        .with("chat", after_id: 1000, limit: 500)
        .and_return([build_envelope(1050)])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: connection))

      expect(connection.enqueued.map(&:msg_id)).to contain_exactly(1050)
    end
  end

  describe "audience filtering via visible_to" do
    subject(:dispatcher) do
      described_class.new(
        client: client,
        registry: registry,
        listener: listener,
        dispatch_queue: dispatch_queue,
        logger: logger,
        read_limit: 500,
        filters: filters
      )
    end

    let(:filters) { Pgbus::Streams::Filters.new }
    let(:admin_conn)  { build_conn(id: "admin", stream_name: "chat", context: { role: "admin" }) }
    let(:viewer_conn) { build_conn(id: "viewer", stream_name: "chat", context: { role: "viewer" }) }

    before do
      filters.register(:admin_only) { |ctx| ctx[:role] == "admin" }
      registry.register(admin_conn)
      registry.register(viewer_conn)
    end

    def admin_envelope
      Pgbus::Web::Streamer::StreamEventDispatcher::StreamEnvelope.new(
        msg_id: 100,
        enqueued_at: nil,
        payload: "<turbo-stream>secret</turbo-stream>",
        source: "live",
        visible_to: :admin_only
      )
    end

    def public_envelope
      Pgbus::Web::Streamer::StreamEventDispatcher::StreamEnvelope.new(
        msg_id: 101,
        enqueued_at: nil,
        payload: "<turbo-stream>everyone</turbo-stream>",
        source: "live",
        visible_to: nil
      )
    end

    it "delivers a labeled envelope only to connections matching the filter" do
      raw = double("raw", payload: '{"html":"<turbo-stream>secret</turbo-stream>","visible_to":"admin_only"}',
                          msg_id: 100, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued.map(&:msg_id)).to eq([100])
      expect(viewer_conn.enqueued).to be_empty
    end

    it "delivers an unlabeled envelope to every connection (no filter applied)" do
      raw = double("raw", payload: '{"html":"<turbo-stream>everyone</turbo-stream>"}',
                          msg_id: 101, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued.map(&:msg_id)).to eq([101])
      expect(viewer_conn.enqueued.map(&:msg_id)).to eq([101])
    end

    it "fail-closes on an unknown filter label (drops the envelope and logs)" do
      # Audience filtering is a data-isolation feature; failing open
      # on a typo would turn a restricted broadcast into a public
      # one. The Filters registry is expected to log a warning and
      # return false for unknown labels.
      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"nope"}',
                          msg_id: 102, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued).to be_empty
      expect(viewer_conn.enqueued).to be_empty
    end

    it "fail-closes when the filter predicate raises (drops the envelope)" do
      filters.register(:broken) { |_| raise "boom" }
      allow(logger).to receive(:error)
      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"broken"}',
                          msg_id: 103, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(admin_conn.enqueued).to be_empty
      expect(viewer_conn.enqueued).to be_empty
    end

    it "advances the scanned cursor past filtered-out batches so later public messages are not starved" do
      # Without the scanned cursor, a run of invisible messages pins
      # minimum_cursor on connection.last_msg_id_sent forever and the
      # next read_after returns the same hidden window. Simulate a
      # batch of admin-only messages followed by a public one and
      # assert the viewer eventually gets the public message.
      admin_raw = Array.new(3) do |i|
        double("raw_admin_#{i}",
               payload: %({"html":"<turbo-stream>admin-#{i}</turbo-stream>","visible_to":"admin_only"}),
               msg_id: 200 + i, enqueued_at: nil, source: "live")
      end
      public_raw = double("raw_public",
                          payload: '{"html":"<turbo-stream>public</turbo-stream>"}',
                          msg_id: 210, enqueued_at: nil, source: "live")

      # First handle_wake: return the admin-only batch. The viewer
      # should see nothing but the scanned cursor should advance
      # past 202 so the next read_after starts from > 202.
      allow(client).to receive(:read_after).with("chat", after_id: 0, limit: 500).and_return(admin_raw)
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
      expect(viewer_conn.enqueued).to be_empty

      # Second handle_wake: return the public message from the
      # advanced cursor position. Without scanned cursor advancement,
      # read_after would be called with after_id: 0 again and return
      # the same hidden batch forever.
      allow(client).to receive(:read_after).with("chat", after_id: 202, limit: 500).and_return([public_raw])
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(viewer_conn.enqueued.map(&:msg_id)).to eq([210])
      expect(admin_conn.enqueued.map(&:msg_id)).to eq([200, 201, 202, 210])
    end

    it "applies the filter on the in-flight buffer path during handle_connect" do
      new_conn = build_conn(id: "new", stream_name: "chat", context: { role: "viewer" })

      raw = double("raw", payload: '{"html":"<turbo-stream/>","visible_to":"admin_only"}',
                          msg_id: 200, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: new_conn))

      expect(new_conn.enqueued).to be_empty
    end
  end

  describe "#normalize_sse_event (typed event sanitization, issue #170)" do
    it "passes a clean typed event through" do
      expect(dispatcher.send(:normalize_sse_event, "presence")).to eq("presence")
    end

    it "strips surrounding whitespace" do
      expect(dispatcher.send(:normalize_sse_event, "  reactive  ")).to eq("reactive")
    end

    it "returns nil (default) for a non-String" do
      expect(dispatcher.send(:normalize_sse_event, 42)).to be_nil
      expect(dispatcher.send(:normalize_sse_event, nil)).to be_nil
    end

    it "returns nil for a blank event" do
      expect(dispatcher.send(:normalize_sse_event, "   ")).to be_nil
    end

    it "rejects an event name containing a newline (SSE injection guard)" do
      expect(dispatcher.send(:normalize_sse_event, "presence\nid: 999\ndata: evil")).to be_nil
    end

    it "rejects an event name containing a carriage return" do
      expect(dispatcher.send(:normalize_sse_event, "a\rb")).to be_nil
    end
  end

  describe "actor-echo suppression via exclude" do
    let(:actor_conn)     { build_conn(id: "actor-conn", stream_name: "chat") }
    let(:bystander_conn) { build_conn(id: "bystander-conn", stream_name: "chat") }

    before do
      registry.register(actor_conn)
      registry.register(bystander_conn)
    end

    it "skips delivery to the excluded connection but delivers to everyone else" do
      raw = double("raw",
                   payload: '{"html":"<turbo-stream>msg</turbo-stream>","exclude":"actor-conn"}',
                   msg_id: 100, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(actor_conn.enqueued).to be_empty
      expect(bystander_conn.enqueued.map(&:msg_id)).to eq([100])
    end

    it "delivers to everyone when exclude is absent" do
      raw = double("raw",
                   payload: '{"html":"<turbo-stream>msg</turbo-stream>"}',
                   msg_id: 101, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(actor_conn.enqueued.map(&:msg_id)).to eq([101])
      expect(bystander_conn.enqueued.map(&:msg_id)).to eq([101])
    end

    it "advances the actor's scanned cursor past the excluded message (no re-read loop)" do
      excluded = double("raw",
                        payload: '{"html":"<turbo-stream>own</turbo-stream>","exclude":"actor-conn"}',
                        msg_id: 200, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).with("chat", after_id: 0, limit: 500).and_return([excluded])
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
      expect(actor_conn.enqueued).to be_empty

      later = double("raw_later",
                     payload: '{"html":"<turbo-stream>next</turbo-stream>"}',
                     msg_id: 205, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).with("chat", after_id: 200, limit: 500).and_return([later])
      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(actor_conn.enqueued.map(&:msg_id)).to eq([205])
    end

    it "suppresses the actor's echo on the ephemeral (NOTIFY) path too" do
      msg = described_class::WakeMessage.new(
        queue_name: "chat",
        payload: '{"html":"<turbo-stream>ephemeral</turbo-stream>","exclude":"actor-conn"}'
      )
      dispatcher.send(:handle, msg)

      expect(actor_conn.enqueued).to be_empty
      expect(bystander_conn.enqueued.map(&:payload)).to eq(["<turbo-stream>ephemeral</turbo-stream>"])
    end

    it "composes with visible_to: an excluded actor is skipped even if it matches the filter" do
      filters = Pgbus::Streams::Filters.new
      filters.register(:everyone) { |_ctx| true }
      excluding_dispatcher = described_class.new(
        client: client, registry: registry, listener: listener,
        dispatch_queue: dispatch_queue, logger: logger, read_limit: 500, filters: filters
      )
      raw = double("raw",
                   payload: '{"html":"<turbo-stream/>","visible_to":"everyone","exclude":"actor-conn"}',
                   msg_id: 300, enqueued_at: nil, source: "live")
      allow(client).to receive(:read_after).and_return([raw])

      excluding_dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(actor_conn.enqueued).to be_empty
      expect(bystander_conn.enqueued.map(&:msg_id)).to eq([300])
    end
  end

  describe "DisconnectMessage" do
    it "unregisters the connection" do
      conn = build_conn(id: "a", stream_name: "chat")
      registry.register(conn)

      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: conn))

      expect(registry.connections_for("chat")).to be_empty
    end
  end

  describe "connection-driven presence (issue #169)" do
    subject(:dispatcher) do
      described_class.new(
        client: client, registry: registry, listener: listener,
        dispatch_queue: dispatch_queue, logger: logger, read_limit: 500,
        config: presence_config, presence_provider: presence_provider
      )
    end

    # A fake presence handle recording join/leave/touch calls. One per
    # stream, vended by the injected presence_provider.
    let(:presence_class) do
      Class.new do
        attr_reader :joined, :left, :touched

        def initialize
          @joined = []
          @left = []
          @touched = []
        end

        def join(member_id:, metadata: {}) = @joined << { member_id: member_id, metadata: metadata }
        def leave(member_id:) = @left << member_id
        def touch(member_id:) = @touched << member_id
      end
    end

    let(:presence) { presence_class.new }
    let(:presence_provider) { ->(_stream_name) { presence } }

    # Config that turns on presence for the "room" stream and derives the
    # member from a Hash context's :member_id.
    let(:presence_config) do
      instance_double(
        Pgbus::Configuration,
        streams_stats_enabled: false,
        streams_fanout_write_deadline_ms: 250,
        streams_write_deadline_ms: 5_000,
        streams_presence_patterns: ["room"],
        presence_min_ttl: 60
      ).tap do |c|
        allow(c).to receive(:stream_presence?) { |name| name == "room" }
        allow(c).to receive(:presence_member_for) do |ctx|
          ctx.is_a?(Hash) && ctx[:member_id] ? { id: ctx[:member_id].to_s, metadata: ctx[:metadata] || {} } : nil
        end
      end
    end

    before do
      allow(client).to receive(:read_after).and_return([])
      allow(Pgbus::Streams::Presence).to receive(:expire_seats!).and_return([])
    end

    it "writes the pgbus:connected frame after the presence join (issue #521)" do
      conn = build_conn(id: "c1", stream_name: "room", context: { member_id: "7" })
      joined_at_write = nil
      conn.on_connected = -> { joined_at_write = presence.joined.dup }

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      expect(joined_at_write).to eq([{ member_id: "7", metadata: {} }])
    end

    it "auto-joins on connect when the stream has presence and a member is derivable" do
      conn = build_conn(id: "c1", stream_name: "room", context: { member_id: "7", metadata: { name: "Ann" } })
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      expect(presence.joined).to eq([{ member_id: "7", metadata: { name: "Ann" } }])
      expect(conn.presence_member).to eq("7")
    end

    it "does NOT join when the stream is not a presence stream" do
      conn = build_conn(id: "c1", stream_name: "chat", context: { member_id: "7" })
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      expect(presence.joined).to be_empty
      expect(conn.presence_member).to be_nil
    end

    it "does NOT join when no member can be derived (anonymous connection)" do
      conn = build_conn(id: "c1", stream_name: "room", context: { role: "viewer" })
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      expect(presence.joined).to be_empty
      expect(conn.presence_member).to be_nil
    end

    it "auto-leaves on disconnect for a connection that had joined" do
      conn = build_conn(id: "c1", stream_name: "room", context: { member_id: "7" })
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))
      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: conn))

      expect(presence.left).to eq(["7"])
    end

    # Issue #521: the socket closes between the last replay frame and the
    # pgbus:connected write. The connection must not linger registered with
    # its presence row until the next heartbeat.
    context "when the pgbus:connected write fails" do
      let(:conn) { build_conn(id: "c1", stream_name: "room", context: { member_id: "7" }) }

      before do
        allow(client).to receive(:read_after).and_return([build_envelope(1)])
        conn.on_connected = -> { conn.mark_dead! }
      end

      it "unregisters the connection and leaves its presence seat at once" do
        dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

        expect(conn.enqueued.map(&:msg_id)).to eq([1])
        expect(registry.connections_for("room")).to be_empty
        expect(presence.joined).to eq([{ member_id: "7", metadata: {} }])
        expect(presence.left).to eq(["7"])
      end

      it "leaves only once when a later disconnect arrives (heartbeat or peer-close watcher)" do
        dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))
        dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: conn))

        expect(presence.left).to eq(["7"])
        expect(dispatcher.instance_variable_get(:@full_to_logical)).to be_empty
      end
    end

    it "does NOT leave a connection that never joined presence" do
      conn = build_conn(id: "c1", stream_name: "chat")
      registry.register(conn)
      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: conn))

      expect(presence.left).to be_empty
    end

    it "touches presence members on a PresenceTouchMessage" do
      conn = build_conn(id: "c1", stream_name: "room", context: { member_id: "7" })
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))

      dispatcher.send(:handle, described_class::PresenceTouchMessage.new(connections: [conn]))

      expect(presence.touched).to eq(["7"])
    end

    it "expires silent seats on a PresenceTouchMessage, even with no live presence connections" do
      dispatcher.send(:handle, described_class::PresenceTouchMessage.new(connections: []))

      expect(Pgbus::Streams::Presence).to have_received(:expire_seats!).with(ttl_for: anything, min_ttl: 60)
    end

    it "ignores a touch for a connection without a presence member" do
      conn = build_conn(id: "c1", stream_name: "chat")
      dispatcher.send(:handle, described_class::PresenceTouchMessage.new(connections: [conn]))
      expect(presence.touched).to be_empty
    end

    it "does not let a presence join failure break the connect path" do
      failing = Class.new do
        def join(**) = raise("presence DB down")
        def leave(**) = nil
        def touch(**) = nil
      end.new
      failing_dispatcher = described_class.new(
        client: client, registry: registry, listener: listener,
        dispatch_queue: dispatch_queue, logger: logger, read_limit: 500,
        config: presence_config, presence_provider: ->(_n) { failing }
      )
      conn = build_conn(id: "c1", stream_name: "room", context: { member_id: "7" })

      expect do
        failing_dispatcher.send(:handle, described_class::ConnectMessage.new(connection: conn))
      end.not_to raise_error
      # The connection is still registered despite the presence failure.
      expect(registry.connections_for("room")).to include(conn)
    end
  end

  describe "wake coalescing (drain_wakes_for)" do
    it "collapses consecutive WakeMessages for the same stream into one" do
      first = described_class::WakeMessage.new(queue_name: "chat")
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")

      coalesced, trailing = dispatcher.send(:drain_wakes_for, first)

      expect(coalesced.size).to eq(1)
      expect(coalesced.first.queue_name).to eq("chat")
      expect(trailing).to be_nil
    end

    it "keeps WakeMessages for different streams as separate entries" do
      first = described_class::WakeMessage.new(queue_name: "chat")
      dispatch_queue << described_class::WakeMessage.new(queue_name: "presence")
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat") # dup
      dispatch_queue << described_class::WakeMessage.new(queue_name: "alerts")

      coalesced, trailing = dispatcher.send(:drain_wakes_for, first)

      expect(coalesced.map(&:queue_name)).to contain_exactly("chat", "presence", "alerts")
      expect(trailing).to be_nil
    end

    it "stops at the first non-WakeMessage and returns it as the trailing message" do
      first = described_class::WakeMessage.new(queue_name: "chat")
      conn = build_conn(id: "a", stream_name: "chat")
      connect = described_class::ConnectMessage.new(connection: conn)
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")
      dispatch_queue << connect
      dispatch_queue << described_class::WakeMessage.new(queue_name: "presence")

      coalesced, trailing = dispatcher.send(:drain_wakes_for, first)

      expect(coalesced.map(&:queue_name)).to eq(["chat"])
      expect(trailing).to be(connect)
      # The presence wake is left in the queue for the next iteration
      expect(dispatch_queue.size).to eq(1)
      expect(dispatch_queue.pop.queue_name).to eq("presence")
    end

    it "performs only one read_after per stream when N consecutive wakes arrive" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)

      # Pre-populate the queue with 5 wakes for "chat"
      5.times { dispatch_queue << described_class::WakeMessage.new(queue_name: "chat") }
      first = dispatch_queue.pop

      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      coalesced, = dispatcher.send(:drain_wakes_for, first)
      coalesced.each { |w| dispatcher.send(:handle, w) }

      expect(client).to have_received(:read_after).once
    end
  end

  describe "stream stat recording (opt-in)" do
    # When streams_stats_enabled is false (the default in this suite),
    # the Dispatcher must never touch Pgbus::StreamStat. Otherwise a
    # misconfigured install would start writing rows on upgrade.
    context "when streams_stats_enabled is false" do
      it "does not record a broadcast on handle_wake" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        registry.register(c)
        allow(client).to receive(:read_after).and_return([build_envelope(10)])
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

        expect(Pgbus::StreamStat).not_to have_received(:record!)
      end

      it "records an ephemeral broadcast even when stats are disabled" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        registry.register(c)
        allow(Pgbus::StreamStat).to receive(:record!)
        payload = '{"html":"<turbo-stream>hi</turbo-stream>"}'

        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat", payload: payload))

        expect(Pgbus::StreamStat).to have_received(:record!).with(
          hash_including(stream_name: "chat", event_type: "broadcast")
        )
      end

      it "does not record a connect on handle_connect" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        allow(client).to receive(:read_after).and_return([])
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::ConnectMessage.new(connection: c))

        expect(Pgbus::StreamStat).not_to have_received(:record!)
      end

      it "does not record a disconnect on handle_disconnect" do
        c = build_conn(id: "a", stream_name: "chat")
        registry.register(c)
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c))

        expect(Pgbus::StreamStat).not_to have_received(:record!)
      end
    end

    context "when streams_stats_enabled is true" do
      let(:dispatcher_config) do
        instance_double(
          Pgbus::Configuration,
          streams_stats_enabled: true,
          stream_presence?: false,
          streams_fanout_write_deadline_ms: 250,
          streams_write_deadline_ms: 5_000
        )
      end

      it "records a broadcast with fanout = registered + in-flight subscriber count" do
        c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        c2 = build_conn(id: "b", stream_name: "chat", last_msg_id_sent: 0)
        registry.register(c1)
        registry.register(c2)
        allow(client).to receive(:read_after).and_return([build_envelope(5)])
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

        expect(Pgbus::StreamStat).to have_received(:record!).with(
          hash_including(stream_name: "chat", event_type: "broadcast", fanout: 2)
        )
      end

      it "does not record a broadcast when read_after returns empty" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        registry.register(c)
        allow(client).to receive(:read_after).and_return([])
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

        expect(Pgbus::StreamStat).not_to have_received(:record!)
      end

      it "records a connect with no fanout" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        allow(client).to receive(:read_after).and_return([])
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::ConnectMessage.new(connection: c))

        expect(Pgbus::StreamStat).to have_received(:record!).with(
          hash_including(stream_name: "chat", event_type: "connect", fanout: nil)
        )
      end

      it "records a disconnect" do
        c = build_conn(id: "a", stream_name: "chat")
        registry.register(c)
        allow(Pgbus::StreamStat).to receive(:record!)

        dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c))

        expect(Pgbus::StreamStat).to have_received(:record!).with(
          hash_including(stream_name: "chat", event_type: "disconnect", fanout: nil)
        )
      end

      it "does not propagate StreamStat.record! errors to the dispatcher" do
        c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
        registry.register(c)
        allow(client).to receive(:read_after).and_return([build_envelope(1)])
        allow(Pgbus::StreamStat).to receive(:record!).and_raise(StandardError, "boom")

        expect do
          dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
        end.not_to raise_error
      end
    end
  end

  describe "AR connection release after each run_loop iteration" do
    it "releases ActiveRecord connections after processing a message" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      handler = double("ConnectionHandler")
      allow(Pgbus::BusRecord).to receive(:connection_handler).and_return(handler)
      allow(handler).to receive(:clear_active_connections!)

      dispatcher.start
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")

      deadline = Time.now + 2
      sleep 0.01 until c.enqueued.any? || Time.now > deadline

      dispatcher.stop

      expect(handler).to have_received(:clear_active_connections!).at_least(:once)
    end

    it "releases connections even when handle raises" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      allow(client).to receive(:read_after).and_raise(StandardError, "boom")

      handler = double("ConnectionHandler")
      allow(Pgbus::BusRecord).to receive(:connection_handler).and_return(handler)
      allow(handler).to receive(:clear_active_connections!)

      dispatcher.start
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")

      sleep 0.05

      dispatcher.stop

      expect(handler).to have_received(:clear_active_connections!).at_least(:once)
    end
  end

  describe "in-memory stream counters (always-on)" do
    it "increments broadcast counter on durable wake" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))

      expect(dispatcher.stream_counter.broadcasts("chat")).to eq(1)
    end

    it "increments broadcast counter on ephemeral wake" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      payload = '{"html":"<turbo-stream>hi</turbo-stream>"}'

      dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat", payload: payload))

      expect(dispatcher.stream_counter.broadcasts("chat")).to eq(1)
    end

    it "increments connection counter on connect" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      allow(client).to receive(:read_after).and_return([])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: c))

      expect(dispatcher.stream_counter.active_connections("chat")).to eq(1)
      expect(dispatcher.stream_counter.total_connections("chat")).to eq(1)
    end

    it "decrements connection counter on disconnect" do
      c = build_conn(id: "a", stream_name: "chat")
      registry.register(c)
      allow(client).to receive(:read_after).and_return([])
      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: c))

      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c))

      expect(dispatcher.stream_counter.active_connections("chat")).to eq(0)
    end

    it "is idempotent on duplicate disconnect (does not over-decrement when prune_dead races)" do
      # prune_dead enqueues a DisconnectMessage on every wake when the
      # connection is dead. If two wakes hit before the first
      # DisconnectMessage drains the queue, two DisconnectMessages exist
      # for the same connection. The second one used to keep
      # decrementing, driving the counter negative for the next real
      # connection.
      c1 = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      c2 = build_conn(id: "b", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c1)
      registry.register(c2)
      dispatcher.stream_counter.increment_connections("chat")
      dispatcher.stream_counter.increment_connections("chat")

      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c1))
      dispatcher.send(:handle, described_class::DisconnectMessage.new(connection: c1))

      # c2 is still registered, so the active count must reflect that —
      # not be falsely zero from the duplicate decrement.
      expect(dispatcher.stream_counter.active_connections("chat")).to eq(1)
    end

    it "increments total_connections but not active_connections when connect dies before registry" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      c.mark_dead!
      allow(client).to receive(:read_after).and_return([])

      dispatcher.send(:handle, described_class::ConnectMessage.new(connection: c))

      expect(dispatcher.stream_counter.active_connections("chat")).to eq(0)
      expect(dispatcher.stream_counter.total_connections("chat")).to eq(1)
    end
  end

  describe "#start and #stop" do
    it "processes queued messages on a background thread" do
      c = build_conn(id: "a", stream_name: "chat", last_msg_id_sent: 0)
      registry.register(c)
      allow(client).to receive(:read_after).and_return([build_envelope(10)])

      dispatcher.start
      dispatch_queue << described_class::WakeMessage.new(queue_name: "chat")

      deadline = Time.now + 2
      sleep 0.01 until c.enqueued.any? || Time.now > deadline
      expect(c.enqueued.map(&:msg_id)).to contain_exactly(10)

      dispatcher.stop
    end
  end

  describe "#threads (issue #443)" do
    it "exposes its thread while running and none after stop" do
      expect(dispatcher.threads).to eq([])
      dispatcher.start
      expect(dispatcher.threads.size).to eq(1)
      expect(dispatcher.threads.first).to be_alive
      dispatcher.stop
      expect(dispatcher.threads).to eq([])
    end
  end
end
