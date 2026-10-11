# frozen_string_literal: true

require "spec_helper"

# Connection-driven presence for seat tables (issue #515): one row per SSE
# connection, touched by the heartbeat, expired after the stream's ttl.
RSpec.describe Pgbus::Web::Streamer::PresenceTracker do
  subject(:tracker) do
    described_class.new(config: config, presence_provider: ->(_name) { presence },
                        logger: logger, sweeper: sweeper, clock: -> { now })
  end

  let(:now) { 1_000.0 }
  let(:logger) { instance_double(Logger, error: nil) }
  let(:sweeper) { double("sweeper", call: []) }
  let(:seats) { true }

  let(:presence_class) do
    Class.new do
      attr_reader :calls
      attr_accessor :seats, :live

      def initialize(seats)
        @seats = seats
        @calls = []
        @live = nil
      end

      def seats? = @seats
      def join(**args) = @calls << [:join, args]
      def leave(**args) = @calls << [:leave, args]

      def touch(**args)
        @calls << [:touch, args]
        @live || Array(args[:connection_id])
      end
    end
  end
  let(:presence) { presence_class.new(seats) }

  let(:config) do
    instance_double(Pgbus::Configuration, streams_presence_patterns: ["room"], presence_min_ttl: 60).tap do |c|
      allow(c).to receive(:stream_presence?) { |name| name == "room" }
      allow(c).to receive(:presence_member_for) do |ctx|
        ctx.is_a?(Hash) && ctx[:member_id] ? { id: ctx[:member_id].to_s, metadata: ctx[:metadata] || {} } : nil
      end
    end
  end

  let(:conn_class) do
    Struct.new(:id, :stream_name, :context, :seat, :presence_member, :presence_seat, keyword_init: true)
  end

  def build_conn(id: "c1", stream: "room", member: "7", seat: "s1")
    conn_class.new(id: id, stream_name: stream, context: member && { member_id: member, metadata: { n: 1 } }, seat: seat)
  end

  describe "#join" do
    it "joins the connection's own seat row with its id and seat" do
      conn = build_conn
      tracker.join(conn)

      expect(presence.calls).to eq([[:join, { member_id: "7", metadata: { n: 1 }, connection_id: "c1", seat: "s1" }]])
      expect(conn.presence_member).to eq("7")
    end

    it "joins member-keyed on a table without seats" do
      presence.seats = false
      tracker.join(build_conn)

      expect(presence.calls).to eq([[:join, { member_id: "7", metadata: { n: 1 } }]])
    end

    it "skips non-presence streams and anonymous connections" do
      tracker.join(build_conn(stream: "chat"))
      tracker.join(build_conn(member: nil))

      expect(presence.calls).to be_empty
    end

    it "logs and swallows a failing join" do
      allow(presence).to receive(:join).and_raise("db down")

      expect { tracker.join(build_conn) }.not_to raise_error
      expect(logger).to have_received(:error)
    end
  end

  describe "#leave" do
    it "removes only this connection's seat" do
      conn = build_conn
      tracker.join(conn)
      tracker.leave(conn)

      expect(presence.calls.last).to eq([:leave, { connection_id: "c1" }])
    end

    it "leaves member-keyed on a table without seats" do
      presence.seats = false
      conn = build_conn
      tracker.join(conn)
      tracker.leave(conn)

      expect(presence.calls.last).to eq([:leave, { member_id: "7" }])
    end

    it "removes the member-keyed row of a connection that joined before the table was migrated" do
      presence.seats = false
      conn = build_conn
      tracker.join(conn)
      presence.seats = true
      tracker.leave(conn)

      expect(presence.calls.last).to eq([:leave, { member_id: "7" }])
    end

    it "keeps a touch queued behind the disconnect from re-joining the seat" do
      conn = build_conn
      tracker.join(conn)
      tracker.leave(conn)
      presence.live = []
      presence.calls.clear

      tracker.touch([conn])

      expect(presence.calls).to be_empty
    end

    it "does nothing for a connection that never joined" do
      tracker.leave(build_conn)
      expect(presence.calls).to be_empty
    end
  end

  describe "#touch" do
    it "touches every seat of a stream in one call" do
      a = build_conn(id: "c1").tap { |conn| tracker.join(conn) }
      b = build_conn(id: "c2", member: "8").tap { |conn| tracker.join(conn) }
      presence.calls.clear

      tracker.touch([a, b])

      expect(presence.calls).to eq([[:touch, { connection_id: %w[c1 c2] }]])
    end

    it "re-joins a live connection whose row was expired" do
      a = build_conn(id: "c1").tap { |conn| tracker.join(conn) }
      b = build_conn(id: "c2", member: "8").tap { |conn| tracker.join(conn) }
      presence.calls.clear
      presence.live = ["c1"]

      tracker.touch([a, b])

      expect(presence.calls.last).to eq([:join, { member_id: "8", metadata: { n: 1 }, connection_id: "c2", seat: "s1" }])
    end

    it "moves a connection that joined member-keyed onto a seat once the table is migrated" do
      presence.seats = false
      conn = build_conn.tap { |c| tracker.join(c) }
      presence.seats = true
      presence.live = []
      presence.calls.clear

      tracker.touch([conn])

      expect(presence.calls).to eq([
                                     [:touch, { connection_id: ["c1"] }],
                                     [:leave, { member_id: "7" }],
                                     [:join, { member_id: "7", metadata: { n: 1 }, connection_id: "c1", seat: "s1" }]
                                   ])
      expect(conn.presence_seat).to be(true)
    end

    it "touches member-keyed on a table without seats" do
      presence.seats = false
      a = build_conn.tap { |conn| tracker.join(conn) }
      presence.calls.clear

      tracker.touch([a])

      expect(presence.calls).to eq([[:touch, { member_id: "7" }]])
    end

    it "skips connections without a presence member" do
      tracker.touch([build_conn(stream: "chat")])
      expect(presence.calls).to be_empty
    end
  end

  describe "seat expiry" do
    it "sweeps on the first touch, then at most every min_ttl / 2 seconds" do
      tracker.touch([])
      tracker.touch([])
      expect(sweeper).to have_received(:call).once

      allow(tracker).to receive(:now).and_return(now + 31)
      tracker.touch([])
      expect(sweeper).to have_received(:call).twice
    end

    it "retries a failed sweep on the next touch" do
      calls = 0
      allow(sweeper).to receive(:call) { (calls += 1) == 1 ? raise("db down") : [] }

      tracker.touch([])
      tracker.touch([])
      tracker.touch([])

      expect(sweeper).to have_received(:call).twice
    end

    it "does not sweep when no stream has presence" do
      allow(config).to receive(:streams_presence_patterns).and_return([])
      tracker.touch([])
      expect(sweeper).not_to have_received(:call)
    end

    it "logs and swallows a failing sweep" do
      allow(sweeper).to receive(:call).and_raise("db down")

      expect { tracker.touch([]) }.not_to raise_error
      expect(logger).to have_received(:error)
    end
  end
end
