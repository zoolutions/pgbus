# frozen_string_literal: true

require "spec_helper"
require "protocol/http/body/writable"

RSpec.describe Pgbus::Web::Streamer::FalconConnection do
  subject(:conn) do
    described_class.new(
      id: "fc1",
      stream_name: stream,
      body: body,
      since_id: 100,
      write_deadline_ms: 5_000
    )
  end

  let(:stream) { "chat" }
  let(:body) { Protocol::HTTP::Body::Writable.new }

  def build_envelope(msg_id:, payload: "<turbo-stream>data</turbo-stream>", visible_to: nil)
    Pgbus::Web::Streamer::StreamEventDispatcher::StreamEnvelope.new(
      msg_id: msg_id, enqueued_at: Time.now, payload: payload,
      source: "q_chat", visible_to: visible_to
    )
  end

  describe "#initialize" do
    it "stores attributes" do
      expect(conn.id).to eq("fc1")
      expect(conn.stream_name).to eq("chat")
      expect(conn.last_msg_id_sent).to eq(100)
      expect(conn.context).to be_nil
    end

    it "exposes the body as #io for duck-typing" do
      expect(conn.io).to eq(body)
    end

    it "provides a mutex" do
      expect(conn.mutex).to be_a(Mutex)
    end

    it "accepts a context" do
      c = described_class.new(
        id: "fc2", stream_name: "chat", body: body,
        since_id: 0, write_deadline_ms: 100, context: :admin
      )
      expect(c.context).to eq(:admin)
    end
  end

  describe "#enqueue" do
    it "writes SSE frames to the body and advances the cursor" do
      envelopes = [build_envelope(msg_id: 101), build_envelope(msg_id: 102)]
      written = conn.enqueue(envelopes)

      expect(written.size).to eq(2)
      expect(conn.last_msg_id_sent).to eq(102)
    end

    it "skips envelopes at or below the cursor" do
      written = conn.enqueue([build_envelope(msg_id: 99), build_envelope(msg_id: 100)])
      expect(written).to be_empty
      expect(conn.last_msg_id_sent).to eq(100)
    end

    it "writes valid SSE frame format" do
      conn.enqueue([build_envelope(msg_id: 101, payload: "hello")])

      chunk = body.read
      expect(chunk).to include("id: 101")
      expect(chunk).to include("event: turbo-stream")
      expect(chunk).to include("data: hello")
    end

    it "marks dead when body is closed" do
      body.close
      written = conn.enqueue([build_envelope(msg_id: 101)])

      expect(written).to be_empty
      expect(conn).to be_dead
    end
  end

  describe "#write_comment" do
    it "writes an SSE comment to the body" do
      conn.write_comment("heartbeat 123")

      chunk = body.read
      expect(chunk).to include(": heartbeat 123")
    end

    it "returns :ok on success" do
      expect(conn.write_comment("ping")).to eq(:ok)
    end

    it "marks dead when body is closed" do
      body.close
      result = conn.write_comment("ping")

      expect(result).to eq(:closed)
      expect(conn).to be_dead
    end
  end

  describe "#write_connected (issue #521)" do
    it "writes the pgbus:connected frame carrying the connection id" do
      expect(conn.write_connected).to eq(:ok)
      expect(body.read).to eq(Pgbus::Streams::Envelope.connected(id: "fc1"))
    end

    it "marks dead when body is closed" do
      body.close
      expect(conn.write_connected).to eq(:closed)
      expect(conn).to be_dead
    end
  end

  describe "#write_sentinel" do
    it "writes raw bytes to the body" do
      conn.write_sentinel("sentinel data")

      chunk = body.read
      expect(chunk).to eq("sentinel data")
    end

    it "returns :ok on success" do
      expect(conn.write_sentinel("bytes")).to eq(:ok)
    end

    it "returns :closed when body is closed" do
      body.close
      expect(conn.write_sentinel("bytes")).to eq(:closed)
    end
  end

  describe "#close" do
    it "signals end-of-stream via close_write" do
      conn.close
      # After close_write, the body is closed for writing
      expect(body.closed?).to be true
    end

    it "marks the connection dead" do
      conn.close
      expect(conn).to be_dead
    end

    it "is idempotent" do
      conn.close
      expect { conn.close }.not_to raise_error
    end
  end

  describe "#dead?" do
    it "is false on a fresh connection" do
      expect(conn).not_to be_dead
    end

    it "is true after mark_dead!" do
      conn.mark_dead!
      expect(conn).to be_dead
    end

    it "is true when body is externally closed" do
      body.close
      expect(conn).to be_dead
    end
  end

  describe "#idle_for" do
    it "returns seconds since last write" do
      fake_clock = 0.0
      allow(Process).to receive(:clock_gettime)
        .with(Process::CLOCK_MONOTONIC).and_return(fake_clock)

      conn.enqueue([build_envelope(msg_id: 101)])

      fake_clock = 10.0
      allow(Process).to receive(:clock_gettime)
        .with(Process::CLOCK_MONOTONIC).and_return(fake_clock)

      expect(conn.idle_for).to be_within(0.01).of(10.0)
    end
  end
end
