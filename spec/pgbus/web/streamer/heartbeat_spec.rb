# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::Heartbeat do
  subject(:heartbeat) do
    described_class.new(
      registry: registry,
      dispatch_queue: dispatch_queue,
      interval: 15,
      idle_timeout: 60,
      logger: logger,
      clock: clock
    )
  end

  let(:registry)       { Pgbus::Web::Streamer::Registry.new }
  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }
  let(:clock_value)    { [0.0] }
  let(:clock)          { -> { clock_value[0] } }

  # Minimal Connection stand-in. Exposes just what Heartbeat touches:
  # dead? / mark_dead! / idle_for / write_raw. The real Connection
  # object uses CLOCK_MONOTONIC; we inject a fake clock so idle_for
  # returns deterministic values. `result` scripts what write_raw returns;
  # a non-:ok result marks it dead, as Connection#write_raw does.
  let(:conn_class) do
    Class.new do
      attr_reader :id, :stream_name, :frames, :deadlines
      attr_accessor :idle_for_value, :presence_member

      def initialize(id:, stream_name:, idle_for: 0, presence_member: nil, result: :ok)
        @id = id
        @stream_name = stream_name
        @idle_for_value = idle_for
        @presence_member = presence_member
        @result = result
        @frames = []
        @deadlines = []
        @dead = false
      end

      def idle_for        = @idle_for_value
      def dead?           = @dead
      def mark_dead!      = @dead = true

      def comments = @frames.map { |frame| frame.delete_prefix(": ").delete_suffix("\n\n") }

      def write_raw(bytes, deadline_ms:)
        @frames << bytes
        @deadlines << deadline_ms
        mark_dead! unless @result == :ok
        @result
      end
    end
  end

  def build_conn(id:, stream_name: "chat", idle_for: 0, presence_member: nil, result: :ok)
    conn_class.new(id: id, stream_name: stream_name, idle_for: idle_for, presence_member: presence_member,
                   result: result)
  end

  describe "#tick" do
    it "writes a heartbeat comment to every live connection" do
      clock_value[0] = 1_700_000_015
      c1 = build_conn(id: "a")
      c2 = build_conn(id: "b")
      registry.register(c1)
      registry.register(c2)

      heartbeat.tick

      expect(c1.comments).to eq(["heartbeat 1700000015"])
      expect(c2.comments).to eq(["heartbeat 1700000015"])
    end

    context "with one frame per tick on the fan-out deadline (issue #621)" do
      subject(:heartbeat) do
        described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 15, idle_timeout: 60,
                            logger: logger, clock: clock, fanout_write_deadline_ms: 250)
      end

      it "builds the comment once per tick however many connections there are" do
        conns = Array.new(5) { |i| build_conn(id: "c#{i}") }
        conns.each { |c| registry.register(c) }
        allow(Pgbus::Streams::Envelope).to receive(:comment).and_call_original

        heartbeat.tick

        expect(Pgbus::Streams::Envelope).to have_received(:comment).once
        expect(conns.map { |c| c.frames.first.object_id }.uniq.size).to eq(1)
        expect(conns.first.frames.first).to be_frozen
      end

      it "writes the frame with the fan-out write deadline" do
        c1 = build_conn(id: "a")
        registry.register(c1)

        heartbeat.tick

        expect(c1.deadlines).to eq([250])
      end

      it "marks a connection whose write times out dead and posts its disconnect" do
        slow = build_conn(id: "slow", result: :timeout)
        fast = build_conn(id: "fast")
        registry.register(slow)
        registry.register(fast)

        heartbeat.tick

        expect(slow).to be_dead
        expect(fast.comments).to eq(["heartbeat 0"])
        msgs = []
        msgs << dispatch_queue.pop(true) until dispatch_queue.empty?
        expect(msgs.map(&:connection)).to eq([slow])
      end

      it "defaults the deadline to the configured streams_fanout_write_deadline_ms" do
        default = described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 15,
                                      idle_timeout: 60, logger: logger, clock: clock)
        c1 = build_conn(id: "a")
        registry.register(c1)

        default.tick

        expect(c1.deadlines).to eq([Pgbus.configuration.streams_fanout_write_deadline_ms])
      end
    end

    it "skips connections that are already dead" do
      c1 = build_conn(id: "a")
      c1.mark_dead!
      registry.register(c1)

      heartbeat.tick

      expect(c1.comments).to be_empty
    end

    it "posts a DisconnectMessage for connections that were already dead" do
      c1 = build_conn(id: "a")
      c1.mark_dead!
      registry.register(c1)

      heartbeat.tick

      msg = dispatch_queue.pop
      expect(msg).to be_a(Pgbus::Web::Streamer::StreamEventDispatcher::DisconnectMessage)
      expect(msg.connection).to be(c1)
    end

    it "marks idle connections dead and posts a DisconnectMessage" do
      c1 = build_conn(id: "a", idle_for: 61) # just over the 60s threshold
      registry.register(c1)

      heartbeat.tick

      expect(c1.dead?).to be true
      expect(c1.comments).to be_empty # idle connections don't get a heartbeat
      msg = dispatch_queue.pop
      expect(msg).to be_a(Pgbus::Web::Streamer::StreamEventDispatcher::DisconnectMessage)
      expect(msg.connection).to be(c1)
    end

    it "does not mark connections idle when exactly at the threshold" do
      c1 = build_conn(id: "a", idle_for: 60) # exactly at threshold, not over
      registry.register(c1)
      heartbeat.tick
      expect(c1.dead?).to be false
      expect(c1.comments).to eq(["heartbeat 0"])
    end

    context "with presence touch (issue #169)" do
      it "posts a PresenceTouchMessage with the live presence connections" do
        presence_conn = build_conn(id: "a", stream_name: "room", presence_member: "7")
        plain_conn = build_conn(id: "b", stream_name: "chat")
        registry.register(presence_conn)
        registry.register(plain_conn)

        heartbeat.tick

        msgs = []
        msgs << dispatch_queue.pop(true) until dispatch_queue.empty?
        touch = msgs.find { |m| m.is_a?(Pgbus::Web::Streamer::StreamEventDispatcher::PresenceTouchMessage) }
        expect(touch).not_to be_nil
        expect(touch.connections).to contain_exactly(presence_conn)
      end

      it "does not post a PresenceTouchMessage when no connection has presence" do
        registry.register(build_conn(id: "a", stream_name: "chat"))

        heartbeat.tick

        msgs = []
        msgs << dispatch_queue.pop(true) until dispatch_queue.empty?
        expect(msgs).not_to(include(be_a(Pgbus::Web::Streamer::StreamEventDispatcher::PresenceTouchMessage)))
      end

      it "posts an empty PresenceTouchMessage when presence streams are configured (seat expiry, issue #515)" do
        sweeping = described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 15,
                                       idle_timeout: 60, logger: logger, clock: clock, presence_expiry: true)
        registry.register(build_conn(id: "a", stream_name: "chat"))

        sweeping.tick

        msgs = []
        msgs << dispatch_queue.pop(true) until dispatch_queue.empty?
        touch = msgs.find { |m| m.is_a?(Pgbus::Web::Streamer::StreamEventDispatcher::PresenceTouchMessage) }
        expect(touch.connections).to eq([])
      end

      it "does not include dead presence connections in the touch" do
        dead_presence = build_conn(id: "a", stream_name: "room", presence_member: "7")
        dead_presence.mark_dead!
        registry.register(dead_presence)

        heartbeat.tick

        msgs = []
        msgs << dispatch_queue.pop(true) until dispatch_queue.empty?
        expect(msgs).not_to(include(be_a(Pgbus::Web::Streamer::StreamEventDispatcher::PresenceTouchMessage)))
      end
    end
  end

  describe "#start and #stop" do
    it "exposes its thread via #threads while running and none after stop (issue #443)" do
      expect(heartbeat.threads).to eq([])
      heartbeat.start
      expect(heartbeat.threads.size).to eq(1)
      expect(heartbeat.threads.first).to be_alive
      heartbeat.stop
      expect(heartbeat.threads).to eq([])
    end

    it "runs tick in the background and joins cleanly on stop" do
      heartbeat_fast = described_class.new(
        registry: registry,
        dispatch_queue: dispatch_queue,
        interval: 0.05,
        idle_timeout: 60,
        logger: logger,
        clock: clock
      )

      c1 = build_conn(id: "a")
      registry.register(c1)

      heartbeat_fast.start
      deadline = Time.now + 2
      sleep 0.01 until c1.comments.any? || Time.now > deadline
      heartbeat_fast.stop

      expect(c1.comments).not_to be_empty
    end

    it "wakes immediately on stop instead of waiting out the interval" do
      heartbeat_slow = described_class.new(
        registry: registry,
        dispatch_queue: dispatch_queue,
        interval: 10,
        idle_timeout: 60,
        logger: logger,
        clock: clock
      )

      heartbeat_slow.start
      sleep 0.05 # let it enter the wait
      start_time = Time.now
      heartbeat_slow.stop
      elapsed = Time.now - start_time

      expect(elapsed).to be < 1.0
    end
  end
end
