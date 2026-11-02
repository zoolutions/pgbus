# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::OutboundPump do
  subject(:pump) do
    described_class.new(
      threads: threads,
      ack_queue: ack_queue,
      on_dead: on_dead,
      buffer_limit: buffer_limit,
      logger: logger
    )
  end

  let(:threads)      { 2 }
  let(:buffer_limit) { 0 }
  let(:ack_queue)    { Queue.new }
  let(:logger)       { Logger.new(IO::NULL) }
  let(:dead_conns)   { [] }
  let(:on_dead)      { ->(conn) { dead_conns << conn } }

  after { pump.stop }

  # A fake connection recording the deadline + order of enqueued frames and the
  # thread each write ran on. Mirrors the real Connection#enqueue contract:
  # returns the written envelopes, advances last_msg_id_sent on :ok, marks dead
  # on the first non-:ok. `result` scripts the per-frame outcome.
  def build_conn(id:, results: nil)
    Class.new do
      attr_reader :id, :written, :write_threads, :frames_seen
      attr_accessor :last_msg_id_sent

      def initialize(id, results)
        @id = id
        @results = results
        @written = []
        @write_threads = []
        @frames_seen = []
        @last_msg_id_sent = 0
        @dead = false
        @mutex = Mutex.new
      end

      def enqueue(envelopes, deadline_ms: nil, frames: nil) # rubocop:disable Lint/UnusedMethodArgument
        accepted = []
        @mutex.synchronize do
          @frames_seen << frames
          envelopes.each do |e|
            outcome = @results ? @results.shift : :ok
            @write_threads << Thread.current
            if outcome == :ok
              @written << e
              @last_msg_id_sent = e.msg_id if e.msg_id > @last_msg_id_sent
              accepted << e
            else
              @dead = true
              break
            end
          end
        end
        accepted
      end

      def dead? = @dead
      def mark_dead! = @dead = true
    end.new(id, results)
  end

  def envelope(msg_id)
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: msg_id, enqueued_at: nil, payload: "<x/>", source: "live"
    )
  end

  # Pop one ack, bounded without Timeout.timeout (forbidden by Pgbus/NoRubyTimeout
  # — it uses Thread#raise). Poll the non-blocking pop against a monotonic deadline.
  def drain_ack
    deadline = monotonic + 2.0
    loop do
      return ack_queue.pop(true)
    rescue ThreadError
      raise "no ack within 2s" if monotonic > deadline

      sleep 0.005
    end
  end

  def wait_until(timeout: 2.0)
    deadline = monotonic + timeout
    sleep 0.005 until yield || monotonic > deadline
    raise "condition not met within #{timeout}s" unless yield
  end

  def monotonic
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  describe "#post — B1 ephemeral guard" do
    it "raises ArgumentError if any envelope has a negative msg_id (ephemerals must stay inline)" do
      pump.start
      conn = build_conn(id: "a")

      expect { pump.post(conn, [envelope(-1)], -1, deadline_ms: 250) }
        .to raise_error(ArgumentError, /ephemeral|negative/i)
    end
  end

  describe "#post — prebuilt frames (issue #621)" do
    it "hands the wake's frames to Connection#enqueue and acks as before" do
      pump.start
      conn = build_conn(id: "a")
      frames = { 10 => "frame 10", 11 => "frame 11" }.freeze

      pump.post(conn, [envelope(10), envelope(11)], 11, deadline_ms: 250, frames: frames)
      ack = drain_ack

      expect(conn.frames_seen).to eq([frames])
      expect(conn.frames_seen.first).to be(frames)
      expect(ack.accepted_max).to eq(11)
    end

    it "passes nil when the caller gives no frames" do
      pump.start
      conn = build_conn(id: "a")

      pump.post(conn, [envelope(10)], 10, deadline_ms: 250)
      drain_ack

      expect(conn.frames_seen).to eq([nil])
    end
  end

  describe "#post — off-thread durable write" do
    it "writes on a worker thread, not the caller's thread" do
      pump.start
      conn = build_conn(id: "a")
      caller_thread = Thread.current

      pump.post(conn, [envelope(10)], 10, deadline_ms: 250)
      drain_ack

      expect(conn.written.map(&:msg_id)).to eq([10])
      expect(conn.write_threads).to all(satisfy { |t| t != caller_thread })
    end

    it "acks the writer's accepted max on success (B2)" do
      pump.start
      conn = build_conn(id: "a")

      pump.post(conn, [envelope(10), envelope(11), envelope(12)], 12, deadline_ms: 250)
      ack = drain_ack

      expect(ack.connection).to be(conn)
      expect(ack.accepted_max).to eq(12)
    end

    it "acks the batch_max for an EMPTY (fully filtered) batch so the scan cursor advances" do
      pump.start
      conn = build_conn(id: "a")

      pump.post(conn, [], 12, deadline_ms: 250)
      ack = drain_ack

      expect(ack.accepted_max).to eq(12)
    end

    it "acks only the accepted prefix when a mid-batch write blocks (B2)" do
      pump.start
      # frame 10 :ok, frame 11 :blocked -> accepted max is 10, conn dead.
      conn = build_conn(id: "a", results: %i[ok blocked])

      pump.post(conn, [envelope(10), envelope(11), envelope(12)], 12, deadline_ms: 250)
      # A failed write yields a disconnect, NOT an ack (the dispatcher must not
      # advance the scan cursor past a frame that never wrote).
      wait_until { dead_conns.include?(conn) }

      expect(conn.written.map(&:msg_id)).to eq([10])
      expect(conn.last_msg_id_sent).to eq(10)
      expect(ack_queue).to be_empty
    end
  end

  describe "#post — B4 death signaling" do
    it "invokes on_dead (not an ack) when the write fails" do
      pump.start
      conn = build_conn(id: "a", results: %i[closed])

      pump.post(conn, [envelope(10)], 10, deadline_ms: 250)
      wait_until { dead_conns.include?(conn) }

      expect(dead_conns).to eq([conn])
      expect(ack_queue).to be_empty
    end
  end

  describe "per-connection ordering" do
    it "writes one connection's frames in strictly ascending order on a single thread" do
      pump.start
      conn = build_conn(id: "stable-id")

      10.times { |i| pump.post(conn, [envelope(i + 1)], i + 1, deadline_ms: 250) }
      10.times { drain_ack }

      expect(conn.written.map(&:msg_id)).to eq((1..10).to_a)
      expect(conn.write_threads.uniq.size).to eq(1) # same partition => same worker
    end
  end

  describe "#stop — B3 drain" do
    it "flushes every posted frame before returning" do
      pump.start
      conn = build_conn(id: "a")

      50.times { |i| pump.post(conn, [envelope(i + 1)], i + 1, deadline_ms: 250) }
      pump.stop

      expect(conn.written.size).to eq(50)
    end

    it "leaves no writer threads alive after stop" do
      before_threads = Thread.list.size
      pump.start
      pump.post(build_conn(id: "a"), [envelope(1)], 1, deadline_ms: 250)
      pump.stop

      expect(Thread.list.size).to be <= before_threads
    end

    it "is idempotent" do
      pump.start
      pump.stop
      expect { pump.stop }.not_to raise_error
    end

    it "exposes its writer threads via #threads while running and none after stop (issue #443)" do
      expect(pump.threads).to eq([])
      pump.start
      expect(pump.threads.size).to eq(2)
      pump.stop
      expect(pump.threads).to eq([])
    end
  end

  describe "buffer limit (streams_writer_buffer_limit)" do
    let(:buffer_limit) { 2 }

    # A connection whose first write blocks on `gate` so its partition buffer
    # fills while later posts queue behind it.
    def gated_slow_conn(gate)
      Class.new do
        attr_reader :written
        attr_accessor :last_msg_id_sent

        def initialize(gate)
          @gate = gate
          @written = []
          @last_msg_id_sent = 0
          @dead = false
        end

        def enqueue(envelopes, deadline_ms: nil, frames: nil) # rubocop:disable Lint/UnusedMethodArgument
          @gate.pop # block the worker until released
          envelopes.each { |e| record(e) }
          envelopes
        end

        def record(envelope)
          @written << envelope
          @last_msg_id_sent = envelope.msg_id
        end

        def id = "slow"
        def dead? = @dead
        def mark_dead! = @dead = true
      end.new(gate)
    end

    it "drops the oldest buffered durable frame when a connection's buffer is over the cap" do
      pump.start
      gate = Queue.new
      slow = gated_slow_conn(gate)

      # First post occupies the worker (blocked on gate); posts 2..6 queue,
      # so with buffer_limit=2 the oldest overflow frames are dropped.
      6.times { |i| pump.post(slow, [envelope(i + 1)], i + 1, deadline_ms: 250) }
      gate.close # release the worker; drain proceeds
      wait_until { slow.written.size.positive? }
      pump.stop

      expect(slow.written.map(&:msg_id)).to include(6) # newest kept (drop-oldest)
      expect(slow.written.size).to be < 6 # some oldest dropped
    end

    it "logs a throttled warning when it drops durable frames (not silent)" do
      allow(logger).to receive(:warn)
      pump.start
      gate = Queue.new
      slow = gated_slow_conn(gate)

      6.times { |i| pump.post(slow, [envelope(i + 1)], i + 1, deadline_ms: 250) }
      gate.close
      wait_until { slow.written.size.positive? }
      pump.stop

      expect(logger).to have_received(:warn).at_least(:once)
    end
  end
end
