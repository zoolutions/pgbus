# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Envelope do
  describe ".message" do
    it "encodes a basic id + event + data triple" do
      data = '<turbo-stream action="replace"></turbo-stream>'
      result = described_class.message(id: 1248, event: "turbo-stream", data: data)
      expect(result).to eq(
        "id: 1248\n" \
        "event: turbo-stream\n" \
        "data: #{data}\n" \
        "\n"
      )
    end

    it "frames a multiline payload as consecutive data: lines (SSE spec, issue #392)" do
      data = "<turbo-stream>\n  <template>x</template>\n</turbo-stream>"
      result = described_class.message(id: 1, event: "turbo-stream", data: data)
      expect(result).to eq(
        "id: 1\n" \
        "event: turbo-stream\n" \
        "data: <turbo-stream>\n" \
        "data:   <template>x</template>\n" \
        "data: </turbo-stream>\n" \
        "\n"
      )
    end

    it "round-trips a multiline payload through an EventSource-style rejoin" do
      data = "<pre>\n  significant\n\n  whitespace\n</pre>"
      result = described_class.message(id: 1, event: "msg", data: data)
      rejoined = result.lines.grep(/\Adata:/).map { |l| l.chomp.delete_prefix("data:").delete_prefix(" ") }.join("\n")
      expect(rejoined).to eq(data)
    end

    it "treats \\r\\n and lone \\r as line breaks (SSE cannot carry a raw \\r)" do
      data = "a\r\nb\rc"
      result = described_class.message(id: 1, event: "msg", data: data)
      expect(result).to include("data: a\ndata: b\ndata: c\n")
      expect(result).not_to include("\r")
    end

    it "preserves a trailing newline in the payload (empty final data: line)" do
      result = described_class.message(id: 1, event: "msg", data: "a\n")
      expect(result).to include("data: a\ndata: \n\n")
      rejoined = result.lines.grep(/\Adata:/).map { |l| l.chomp.delete_prefix("data:").delete_prefix(" ") }.join("\n")
      expect(rejoined).to eq("a\n")
    end

    it "cannot be used to inject SSE fields via newlines in data (every line is data:-prefixed)" do
      result = described_class.message(id: 1, event: "msg", data: "x\nid: 999\nevent: evil")
      expect(result.scan(/^id:/).size).to eq(1)
      expect(result.scan(/^event:/).size).to eq(1)
      expect(result).to include("data: x\ndata: id: 999\ndata: event: evil\n")
    end

    it "emits a single empty data: line for an empty payload" do
      result = described_class.message(id: 1, event: "msg", data: "")
      expect(result).to eq("id: 1\nevent: msg\ndata: \n\n")
    end

    it "preserves UTF-8 characters in data" do
      data = "héllo 世界"
      result = described_class.message(id: 1, event: "msg", data: data)
      expect(result).to include("data: héllo 世界\n")
      expect(result.encoding).to eq(Encoding::UTF_8)
    end

    it "writes id even when zero" do
      result = described_class.message(id: 0, event: "msg", data: "x")
      expect(result).to start_with("id: 0\n")
    end

    it "raises when id is nil" do
      expect { described_class.message(id: nil, event: "msg", data: "x") }
        .to raise_error(ArgumentError, /id/)
    end

    it "raises when event is nil or empty" do
      expect { described_class.message(id: 1, event: nil, data: "x") }
        .to raise_error(ArgumentError, /event/)
      expect { described_class.message(id: 1, event: "", data: "x") }
        .to raise_error(ArgumentError, /event/)
    end

    it "ends with a blank line (the SSE event terminator)" do
      result = described_class.message(id: 1, event: "msg", data: "x")
      expect(result).to end_with("\n\n")
    end

    it "strips newlines from the event name so a crafted value can't inject SSE fields" do
      # A typed event carrying \n could otherwise terminate the event:
      # line early and inject a forged id:/data: into the frame.
      result = described_class.message(id: 1, event: "presence\nid: 999\ndata: evil", data: "ok")
      expect(result).to eq("id: 1\nevent: presenceid: 999data: evil\ndata: ok\n\n")
      # The crafted "\nid: 999\ndata: evil" cannot become its own SSE fields:
      # there is exactly one id:, one event:, one data: line in the frame.
      expect(result.scan(/^id:/).size).to eq(1)
      expect(result.scan(/^data:/).size).to eq(1)
    end

    it "strips carriage returns from the event name" do
      result = described_class.message(id: 1, event: "a\r\nb", data: "x")
      expect(result).to include("event: ab\n")
    end
  end

  describe ".message data framing (issue #621 fast path)" do
    # The pre-#621 implementation, kept as the reference the fast path must match
    # byte for byte.
    let(:reference) do
      lambda do |str|
        lines = str.split(/\r\n|\r|\n/, -1)
        lines = [""] if lines.empty?
        lines.map { |line| "data: #{line}\n" }.join
      end
    end

    it "frames 200 random payloads exactly as the split/map/join implementation did" do
      random = Random.new(621)
      pieces = ["a", "bc", " ", "\r", "\n", "\r\n", "<p>", "é", "\n\n", ""]
      payloads = ["", "\n", "\r", "\r\n", "x\n", "\nx", "plain"] +
                 Array.new(193) { Array.new(random.rand(0..12)) { pieces.sample(random: random) }.join }

      payloads.each do |payload|
        expected = "id: 1\nevent: msg\n#{reference.call(payload)}\n"
        expect(described_class.message(id: 1, event: "msg", data: payload)).to eq(expected), payload.inspect
      end
    end

    it "does not split a payload with no line break" do
      payload = +"<turbo-stream>one line</turbo-stream>"
      allow(payload).to receive(:split).and_call_original

      described_class.message(id: 1, event: "msg", data: payload)

      expect(payload).not_to have_received(:split)
    end
  end

  describe ".sse_event_for" do
    let(:typed) { Struct.new(:event) }

    it "returns the envelope's event when present" do
      expect(described_class.sse_event_for(typed.new("presence"))).to eq("presence")
    end

    it "falls back to the default event for a nil or empty event" do
      expect(described_class.sse_event_for(typed.new(nil))).to eq(Pgbus::Streams::DEFAULT_SSE_EVENT)
      expect(described_class.sse_event_for(typed.new(""))).to eq(Pgbus::Streams::DEFAULT_SSE_EVENT)
    end

    it "falls back to the default event for an envelope without an event field" do
      plain = Pgbus::Client::ReadAfter::Envelope.new(msg_id: 1, enqueued_at: nil, payload: "x", source: "live")

      expect(described_class.sse_event_for(plain)).to eq(Pgbus::Streams::DEFAULT_SSE_EVENT)
    end
  end

  describe ".comment" do
    it "encodes an SSE comment line" do
      expect(described_class.comment("hello")).to eq(": hello\n\n")
    end

    it "strips newlines from comments" do
      expect(described_class.comment("a\nb")).to eq(": ab\n\n")
    end

    it "handles empty comments (still ends with the terminator)" do
      expect(described_class.comment("")).to eq(": \n\n")
    end
  end

  describe ".retry_directive" do
    it "encodes a retry: directive in milliseconds" do
      expect(described_class.retry_directive(2000)).to eq("retry: 2000\n\n")
    end

    it "rejects negative values" do
      expect { described_class.retry_directive(-1) }
        .to raise_error(ArgumentError, /retry/)
    end

    it "rejects non-integer values" do
      expect { described_class.retry_directive(1.5) }
        .to raise_error(ArgumentError, /retry/)
    end
  end

  describe ".connected" do
    it "encodes a pgbus:connected frame carrying the connection id as JSON" do
      result = described_class.connected(id: "abc123def456")
      expect(result).to eq(
        "event: pgbus:connected\n" \
        "data: {\"connectionId\":\"abc123def456\"}\n" \
        "\n"
      )
    end

    it "does not carry an SSE id: line (the connection id is not a replay cursor)" do
      # A pgbus:connected frame is metadata, not a broadcast. Giving it an
      # id: would advance the client's Last-Event-ID and corrupt the
      # replay cursor on reconnect.
      result = described_class.connected(id: "x")
      expect(result).not_to include("id:")
    end

    it "raises when id is nil or empty" do
      expect { described_class.connected(id: nil) }.to raise_error(ArgumentError, /id/)
      expect { described_class.connected(id: "") }.to raise_error(ArgumentError, /id/)
    end

    it "JSON-escapes a hostile connection id (defense in depth)" do
      result = described_class.connected(id: 'a"b')
      expect(result).to include('data: {"connectionId":"a\\"b"}')
    end
  end

  describe ".http_response_headers" do
    it "returns the SSE response status line and headers as a single string" do
      result = described_class.http_response_headers
      expect(result).to start_with("HTTP/1.1 200 OK\r\n")
      expect(result).to include("content-type: text/event-stream\r\n")
      expect(result).to include("cache-control: no-cache, no-transform\r\n")
      expect(result).to include("x-accel-buffering: no\r\n")
      expect(result).to include("connection: keep-alive\r\n")
      expect(result).to end_with("\r\n\r\n")
    end
  end
end
