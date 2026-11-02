# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Cursor do
  describe ".parse" do
    it "returns 0 when no header and no query param are present" do
      expect(described_class.parse(query_since: nil, last_event_id: nil)).to eq(0)
    end

    it "parses the query param when present" do
      expect(described_class.parse(query_since: "1247", last_event_id: nil)).to eq(1247)
    end

    it "parses the Last-Event-ID header when present" do
      expect(described_class.parse(query_since: nil, last_event_id: "1247")).to eq(1247)
    end

    it "prefers Last-Event-ID over the query param when both are present" do
      # On reconnect, EventSource's Last-Event-ID is the source of truth — it's
      # the most recent message the client actually saw.
      expect(described_class.parse(query_since: "100", last_event_id: "200")).to eq(200)
    end

    it "treats blank values as missing" do
      expect(described_class.parse(query_since: "", last_event_id: nil)).to eq(0)
      expect(described_class.parse(query_since: nil, last_event_id: "")).to eq(0)
      expect(described_class.parse(query_since: "  ", last_event_id: nil)).to eq(0)
    end

    it "raises on non-numeric query param" do
      expect { described_class.parse(query_since: "abc", last_event_id: nil) }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /numeric/)
    end

    it "raises on non-numeric Last-Event-ID" do
      expect { described_class.parse(query_since: nil, last_event_id: "abc") }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /numeric/)
    end

    it "raises on negative cursor" do
      expect { described_class.parse(query_since: "-1", last_event_id: nil) }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /negative/)
    end

    it "skips a negative Last-Event-ID, an ephemeral frame's id, and falls back to ?since= (issue #621)" do
      # Ephemeral broadcasts go out with a negative `id:` (they have no
      # archive position), so a native EventSource that saw one last
      # reconnects with `Last-Event-ID: -N`. That is not a cursor: the
      # element's ?since= carries its last durable id.
      expect(described_class.parse(query_since: "100", last_event_id: "-5")).to eq(100)
      expect(described_class.parse(query_since: "100", last_event_id: -5)).to eq(100)
      expect(described_class.parse(query_since: nil, last_event_id: "-5")).to eq(0)
    end

    it "raises on cursor exceeding 64-bit signed range" do
      # PGMQ msg_id is BIGINT — cap at 2^63 - 1
      too_big = (2**63).to_s
      expect { described_class.parse(query_since: too_big, last_event_id: nil) }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /range/)
    end

    it "accepts cursor at the BIGINT boundary" do
      max = (2**63) - 1
      expect(described_class.parse(query_since: max.to_s, last_event_id: nil)).to eq(max)
    end

    it "accepts integer values directly (Rack params can be coerced)" do
      expect(described_class.parse(query_since: 1247, last_event_id: nil)).to eq(1247)
    end

    it "rejects floats" do
      expect { described_class.parse(query_since: "1.5", last_event_id: nil) }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /numeric/)
    end

    it "rejects leading-plus prefixes" do
      # These would parse via to_i but they're suspicious — strict integer parsing only.
      expect { described_class.parse(query_since: "+10", last_event_id: nil) }
        .to raise_error(Pgbus::Streams::Cursor::InvalidCursor, /numeric/)
    end
  end
end
