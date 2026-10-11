# frozen_string_literal: true

require "spec_helper"
require "stringio"

RSpec.describe Pgbus::Web::Streamer::Connection do
  # A test writer that records every call and always succeeds. Models the
  # real Pgbus::Web::Streamer::IoWriter interface (arrives in Phase 2.3):
  #   .write(connection, bytes, deadline_ms:) -> :ok | :closed | :blocked
  subject(:conn) do
    described_class.new(
      id: "c1",
      stream_name: stream,
      io: io,
      since_id: 1247,
      writer: writer,
      write_deadline_ms: 5_000
    )
  end

  let(:writer) do
    Class.new do
      attr_reader :writes

      def initialize(result: :ok)
        @writes = []
        @result = result
      end

      def write(connection, bytes, deadline_ms:)
        @writes << { connection: connection, bytes: bytes, deadline_ms: deadline_ms }
        @result
      end
    end.new
  end

  let(:io)      { StringIO.new }
  let(:stream)  { "chat" }

  def build_envelope(msg_id:, payload: "<turbo-stream>X</turbo-stream>")
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: msg_id,
      enqueued_at: "2026-04-07T00:00:00Z",
      payload: payload,
      source: "live"
    )
  end

  describe "attributes" do
    it "exposes id / stream_name / io" do
      expect(conn.id).to eq("c1")
      expect(conn.stream_name).to eq("chat")
      expect(conn.io).to be(io)
    end

    it "initialises last_msg_id_sent to the caller's since_id" do
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "exposes a mutex for per-io serialization" do
      expect(conn.mutex).to be_a(Mutex)
    end
  end

  describe "#enqueue" do
    it "writes envelopes with msg_id strictly greater than last_msg_id_sent" do
      envelopes = [
        build_envelope(msg_id: 1248, payload: "A"),
        build_envelope(msg_id: 1249, payload: "B")
      ]

      written = conn.enqueue(envelopes)

      expect(written).to eq(envelopes)
      expect(writer.writes.length).to eq(2)
      expect(writer.writes.first[:bytes]).to include("id: 1248")
      expect(writer.writes.first[:bytes]).to include("data: A")
      expect(writer.writes.last[:bytes]).to include("id: 1249")
    end

    it "advances last_msg_id_sent to the highest written id" do
      conn.enqueue([build_envelope(msg_id: 1248), build_envelope(msg_id: 1249)])
      expect(conn.last_msg_id_sent).to eq(1249)
    end

    describe "typed SSE event names (issue #170)" do
      def stream_envelope(msg_id:, event: nil, payload: "<x/>")
        Pgbus::Web::Streamer::StreamEventDispatcher::StreamEnvelope.new(
          msg_id: msg_id, enqueued_at: nil, payload: payload, source: "live", event: event
        )
      end

      it "defaults to the turbo-stream SSE event when the envelope has no event" do
        conn.enqueue([build_envelope(msg_id: 1248)])
        expect(writer.writes.first[:bytes]).to include("event: turbo-stream")
      end

      it "uses the envelope's event name when set" do
        conn.enqueue([stream_envelope(msg_id: 1248, event: "presence")])
        expect(writer.writes.first[:bytes]).to include("event: presence")
        expect(writer.writes.first[:bytes]).not_to include("event: turbo-stream")
      end

      it "falls back to turbo-stream when the envelope's event is nil" do
        conn.enqueue([stream_envelope(msg_id: 1248, event: nil)])
        expect(writer.writes.first[:bytes]).to include("event: turbo-stream")
      end
    end

    it "skips envelopes with msg_id <= last_msg_id_sent (dedup on overlap)" do
      conn.enqueue([build_envelope(msg_id: 1248)])
      writer.writes.clear

      conn.enqueue([
                     build_envelope(msg_id: 1247, payload: "stale"),
                     build_envelope(msg_id: 1248, payload: "same"),
                     build_envelope(msg_id: 1249, payload: "new")
                   ])

      expect(writer.writes.length).to eq(1)
      expect(writer.writes.first[:bytes]).to include("id: 1249")
      expect(conn.last_msg_id_sent).to eq(1249)
    end

    describe "ephemeral envelopes (negative msg_id)" do
      # Ephemeral broadcasts arrive via PG NOTIFY and bypass PGMQ entirely.
      # The Dispatcher tags them with -@ephemeral_seq (-1, -2, ...) so they
      # don't pollute the real PGMQ cursor space. Without special handling,
      # the cursor check `msg_id <= last_msg_id_sent` would reject every
      # negative id against any non-negative cursor, silently dropping
      # every ephemeral broadcast on every fresh connection (since_id: 0).
      subject(:conn) do
        described_class.new(
          id: "c1", stream_name: "chat", io: io, since_id: 0,
          writer: writer, write_deadline_ms: 5_000
        )
      end

      it "delivers ephemeral envelopes through the cursor filter (since_id: 0)" do
        conn.enqueue([build_envelope(msg_id: -1, payload: "ephemeral-1")])

        expect(writer.writes.length).to eq(1)
        expect(writer.writes.first[:bytes]).to include("data: ephemeral-1")
      end

      it "delivers ephemeral envelopes when last_msg_id_sent is positive (post-replay)" do
        # First a durable broadcast advances the cursor to 1248.
        conn.enqueue([build_envelope(msg_id: 1248, payload: "durable")])
        writer.writes.clear

        # Then an ephemeral broadcast with msg_id=-1 must still go through.
        conn.enqueue([build_envelope(msg_id: -1, payload: "ephemeral")])

        expect(writer.writes.length).to eq(1)
        expect(writer.writes.first[:bytes]).to include("data: ephemeral")
      end

      it "does not advance last_msg_id_sent for ephemeral envelopes" do
        # Cursor must stay at 0 (or wherever it was) so that durable replay
        # on reconnect still picks up the right starting point.
        conn.enqueue([build_envelope(msg_id: -1, payload: "x")])
        expect(conn.last_msg_id_sent).to eq(0)
      end

      it "delivers a mix of durable and ephemeral envelopes in one call" do
        envelopes = [
          build_envelope(msg_id: 1248, payload: "durable-1"),
          build_envelope(msg_id: -1, payload: "ephemeral"),
          build_envelope(msg_id: 1249, payload: "durable-2")
        ]

        conn.enqueue(envelopes)

        expect(writer.writes.length).to eq(3)
        expect(conn.last_msg_id_sent).to eq(1249)
      end
    end

    it "passes the configured write deadline through to the writer" do
      conn.enqueue([build_envelope(msg_id: 1248)])
      expect(writer.writes.first[:deadline_ms]).to eq(5_000)
    end

    it "forwards an explicit deadline_ms override to the writer (issue #315 item 3)" do
      # The dispatcher passes the short fanout deadline for hot-loop writes;
      # the override must reach the writer instead of the connection default.
      # (The default-deadline case is covered by "passes the configured write
      # deadline through to the writer" above.)
      conn.enqueue([build_envelope(msg_id: 1248)], deadline_ms: 250)
      expect(writer.writes.first[:deadline_ms]).to eq(250)
    end

    it "accepts an empty array as a no-op" do
      expect { conn.enqueue([]) }.not_to raise_error
      expect(writer.writes).to be_empty
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "wraps each envelope in an SSE frame with event: turbo-stream" do
      conn.enqueue([build_envelope(msg_id: 1248, payload: "X")])
      bytes = writer.writes.first[:bytes]
      expect(bytes).to include("id: 1248\n")
      expect(bytes).to include("event: turbo-stream\n")
      expect(bytes).to include("data: X\n")
      expect(bytes).to end_with("\n\n")
    end

    it "stops writing as soon as the writer returns :closed and marks the connection dead" do
      broken = Class.new do
        def write(*, **)
          :closed
        end
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: broken, write_deadline_ms: 5_000
      )

      c.enqueue([build_envelope(msg_id: 1), build_envelope(msg_id: 2)])

      expect(c.dead?).to be true
    end

    it "stops writing as soon as the writer returns :blocked and marks the connection dead" do
      slow = Class.new do
        def write(*, **)
          :blocked
        end
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: slow, write_deadline_ms: 5_000
      )

      c.enqueue([build_envelope(msg_id: 1)])

      expect(c.dead?).to be true
    end

    it "does not advance last_msg_id_sent for envelopes that fail to write" do
      # First call succeeds, second call reports :closed. We want to prove
      # that the cursor advances for the successful write but NOT for the
      # failed one — so mid-batch failure leaves the cursor on the last
      # confirmed write.
      results = %i[ok closed]
      sequencing_writer = Class.new do
        def initialize(results) = @results = results
        def write(*, **)        = @results.shift
      end.new(results)

      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: sequencing_writer, write_deadline_ms: 5_000
      )
      c.enqueue([build_envelope(msg_id: 1), build_envelope(msg_id: 2)])

      expect(c.last_msg_id_sent).to eq(1)
    end
  end

  describe "#write_comment" do
    it "writes an SSE comment via the writer" do
      conn.write_comment("heartbeat 123")
      expect(writer.writes.length).to eq(1)
      expect(writer.writes.first[:bytes]).to eq(": heartbeat 123\n\n")
    end

    it "does not touch last_msg_id_sent" do
      conn.write_comment("ping")
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "marks the connection dead if the writer reports closed" do
      broken = Class.new do
        def write(*, **) = :closed
      end.new
      c = described_class.new(
        id: "c1", stream_name: "chat", io: io, since_id: 0,
        writer: broken, write_deadline_ms: 5_000
      )
      c.write_comment("ping")
      expect(c.dead?).to be true
    end
  end

  describe "#write_connected (issue #521)" do
    it "writes the pgbus:connected frame carrying the connection id" do
      expect(conn.write_connected).to eq(:ok)
      expect(writer.writes.last[:bytes]).to eq(Pgbus::Streams::Envelope.connected(id: "c1"))
    end

    it "does not touch last_msg_id_sent" do
      conn.write_connected
      expect(conn.last_msg_id_sent).to eq(1247)
    end

    it "marks the connection dead if the writer reports closed" do
      failing_writer = writer.class.new(result: :closed)
      c = described_class.new(
        id: "c3", stream_name: stream, io: io, since_id: 0,
        writer: failing_writer, write_deadline_ms: 100
      )
      expect(c.write_connected).to eq(:closed)
      expect(c.dead?).to be true
    end
  end

  describe "#write_sentinel" do
    it "delegates to the writer with the connection's deadline" do
      result = conn.write_sentinel("sentinel bytes")
      expect(result).to eq(:ok)
      expect(writer.writes.last[:bytes]).to eq("sentinel bytes")
      expect(writer.writes.last[:deadline_ms]).to eq(5_000)
    end

    it "returns :closed when the writer fails" do
      failing_writer = writer.class.new(result: :closed)
      c = described_class.new(
        id: "c2", stream_name: stream, io: io, since_id: 0,
        writer: failing_writer, write_deadline_ms: 100
      )
      expect(c.write_sentinel("bytes")).to eq(:closed)
    end
  end

  describe "#idle_for" do
    it "returns seconds since the last successful write" do
      # Simulate a write at t=0, advance the monotonic clock, check idle_for
      fake_clock = 0.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      conn.enqueue([build_envelope(msg_id: 1248)])

      fake_clock = 42.5
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      expect(conn.idle_for).to be_within(0.01).of(42.5)
    end

    it "counts from construction time when no writes have happened" do
      fake_clock = 100.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      fresh = described_class.new(
        id: "c2", stream_name: "chat", io: io, since_id: 0,
        writer: writer, write_deadline_ms: 5_000
      )
      fake_clock = 105.0
      allow(Process).to receive(:clock_gettime).with(Process::CLOCK_MONOTONIC).and_return(fake_clock)
      expect(fresh.idle_for).to be_within(0.01).of(5.0)
    end
  end

  describe "#dead?" do
    it "is false on a fresh connection" do
      expect(conn.dead?).to be false
    end

    it "is true after #mark_dead! is called" do
      conn.mark_dead!
      expect(conn.dead?).to be true
    end
  end

  describe "#close" do
    it "marks the connection dead and closes the underlying IO" do
      expect(io.closed?).to be false
      conn.close
      expect(conn.dead?).to be true
      expect(io.closed?).to be true
    end

    it "is idempotent — a second close is a no-op" do
      conn.close
      expect { conn.close }.not_to raise_error
    end

    it "holds the connection mutex while closing so writes cannot race" do
      # Prove the close path takes the same mutex as IoWriter.write.
      # Without the mutex, a concurrent write could hit a half-closed
      # socket and corrupt the cursor by marking dead between writes.
      writer_thread = nil
      in_write = Queue.new
      conn.mutex.synchronize do
        writer_thread = Thread.new do
          conn.close
          in_write << :closed
        end
        # Sleep briefly so the writer_thread can attempt to acquire
        # the lock and block on it. If close didn't take the mutex
        # at all, :closed would be pushed immediately.
        sleep 0.05
        expect(in_write).to be_empty
      end
      writer_thread.join(1)
      expect(in_write.pop).to eq(:closed)
      expect(conn.dead?).to be true
    end

    it "swallows IOError from a double-close race" do
      io.close # externally close first
      expect { conn.close }.not_to raise_error
      expect(conn.dead?).to be true
    end
  end
end
