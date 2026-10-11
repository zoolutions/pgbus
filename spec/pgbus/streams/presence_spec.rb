# frozen_string_literal: true

require "spec_helper"
require "pg"
require "pgbus/streams/presence"

RSpec.describe Pgbus::Streams::Presence do
  subject(:presence) { described_class.new(stream) }

  let(:stream) { double("Pgbus::Streams::Stream", name: "room:42") }
  let(:raw_connection) { double("PG::Connection") }

  let(:ar_connection) { double("ActiveRecord::ConnectionAdapters::Connection", raw_connection: raw_connection) }

  before do
    # Force the single-database path (Pgbus.configuration.connects_to == nil)
    # so the spec doesn't require BusRecord to be connected. The multi-DB
    # branch is a one-line delegation to BusRecord.connection.raw_connection
    # and is exercised by spec/integration/streams/presence_spec.rb.
    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    stub_const("ActiveRecord::Base", Class.new)
    allow(ActiveRecord::Base).to receive(:connection).and_return(ar_connection)
    # The member-keyed specs below describe a table without the seat columns
    # (pre-#515 schema); the seat specs flip this to true.
    allow(described_class).to receive(:seats_supported?).and_return(false)
  end

  describe "#join" do
    let(:upsert_row) do
      {
        "member_id" => "7",
        "metadata" => '{"name":"Alice"}',
        "joined_at" => "2026-04-09T00:00:00Z",
        "last_seen_at" => "2026-04-09T00:00:00Z"
      }
    end

    before do
      allow(raw_connection).to receive(:exec_params).and_return([upsert_row])
    end

    it "upserts the member and returns a hash with parsed metadata" do
      result = presence.join(member_id: 7, metadata: { name: "Alice" })

      expect(result["id"]).to eq("7")
      expect(result["metadata"]).to eq({ "name" => "Alice" })
    end

    it "coerces member_id to string before inserting" do
      presence.join(member_id: 7)

      expect(raw_connection).to have_received(:exec_params).with(
        anything,
        ["room:42", "7", "{}"]
      )
    end

    it "generates JSON for metadata even when empty" do
      presence.join(member_id: "7")

      expect(raw_connection).to have_received(:exec_params).with(
        anything,
        ["room:42", "7", "{}"]
      )
    end

    it "broadcasts the yielded HTML when the block returns a non-empty string" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { |_record| "<turbo-stream>join</turbo-stream>" }

      expect(stream).to have_received(:broadcast).with("<turbo-stream>join</turbo-stream>")
    end

    it "skips broadcast when the block returns an empty string" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { "" }

      expect(stream).not_to have_received(:broadcast)
    end

    it "skips broadcast when the block returns nil" do
      allow(stream).to receive(:broadcast)
      presence.join(member_id: "7") { nil }

      expect(stream).not_to have_received(:broadcast)
    end
  end

  describe "#leave" do
    context "when the member exists" do
      let(:deleted_row) do
        {
          "member_id" => "7",
          "metadata" => '{"name":"Alice"}',
          "joined_at" => "2026-04-09T00:00:00Z",
          "last_seen_at" => "2026-04-09T00:05:00Z"
        }
      end

      before do
        allow(raw_connection).to receive(:exec_params).and_return([deleted_row])
      end

      it "returns the deleted record" do
        result = presence.leave(member_id: "7")
        expect(result["id"]).to eq("7")
        expect(result["metadata"]).to eq({ "name" => "Alice" })
      end

      it "broadcasts the yielded HTML" do
        allow(stream).to receive(:broadcast)
        presence.leave(member_id: "7") { "<turbo-stream>leave</turbo-stream>" }

        expect(stream).to have_received(:broadcast).with("<turbo-stream>leave</turbo-stream>")
      end
    end

    context "when the member does not exist" do
      before { allow(raw_connection).to receive(:exec_params).and_return([]) }

      it "returns nil" do
        expect(presence.leave(member_id: "missing")).to be_nil
      end

      it "does not yield the block" do
        yielded = false
        presence.leave(member_id: "missing") { |_| yielded = true }
        expect(yielded).to be false
      end

      it "does not broadcast" do
        allow(stream).to receive(:broadcast)
        presence.leave(member_id: "missing") { "<turbo-stream>nope</turbo-stream>" }
        expect(stream).not_to have_received(:broadcast)
      end
    end
  end

  describe "#touch" do
    it "refreshes last_seen_at for the given member" do
      allow(raw_connection).to receive(:exec_params)

      presence.touch(member_id: 42)

      expect(raw_connection).to have_received(:exec_params).with(
        a_string_matching(/UPDATE pgbus_presence_members.*SET last_seen_at = NOW\(\)/m),
        ["room:42", "42"]
      )
    end
  end

  describe "#members" do
    context "with well-formed metadata" do
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => '{"name":"Alice"}',
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    },
                                                                    {
                                                                      "member_id" => "2",
                                                                      "metadata" => '{"name":"Bob"}',
                                                                      "joined_at" => "2026-04-09T00:01:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:01:00Z"
                                                                    }
                                                                  ])
      end

      it "returns an array of hashes with parsed metadata" do
        result = presence.members
        expect(result.size).to eq(2)
        expect(result[0]["id"]).to eq("1")
        expect(result[0]["metadata"]).to eq({ "name" => "Alice" })
        expect(result[1]["id"]).to eq("2")
      end
    end

    context "with corrupt JSON metadata" do
      # parse_metadata must not crash the whole members() call on one bad
      # row — production data can have legacy / manually-edited entries.
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => "{not valid json",
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    }
                                                                  ])
      end

      it "falls back to an empty hash without raising" do
        result = presence.members
        expect(result.size).to eq(1)
        expect(result[0]["metadata"]).to eq({})
      end

      it "logs the failure at debug level with the raw metadata truncated to 200 chars" do
        # Inject a metadata blob much longer than the 200-char cap so
        # we can distinguish "truncated" from "not truncated". The
        # production code does value.to_s[0, 200], then interpolates
        # via .inspect — the logged string contains the first 200
        # source characters and must NOT contain characters past 200.
        long_invalid = "{#{"x" * 500}"
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    {
                                                                      "member_id" => "1",
                                                                      "metadata" => long_invalid,
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z"
                                                                    }
                                                                  ])

        logged_message = nil
        fake_logger = double("logger")
        allow(fake_logger).to receive(:debug) { |&blk| logged_message = blk&.call }
        allow(Pgbus).to receive(:logger).and_return(fake_logger)

        presence.members

        expect(fake_logger).to have_received(:debug)
        expect(logged_message).to include("parse_metadata failed")
        # First 200 chars are preserved in the log line
        expect(logged_message).to include(long_invalid[0, 200])
        # Characters past the 200-char boundary are dropped
        tail = long_invalid[200..]
        expect(logged_message).not_to include(tail)
      end
    end

    context "with nil or empty metadata" do
      before do
        allow(raw_connection).to receive(:exec_params).and_return([
                                                                    { "member_id" => "1", "metadata" => nil,
                                                                      "joined_at" => "2026-04-09T00:00:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:00:00Z" },
                                                                    { "member_id" => "2", "metadata" => "",
                                                                      "joined_at" => "2026-04-09T00:01:00Z",
                                                                      "last_seen_at" => "2026-04-09T00:01:00Z" }
                                                                  ])
      end

      it "returns empty-hash metadata for both without raising" do
        result = presence.members
        expect(result[0]["metadata"]).to eq({})
        expect(result[1]["metadata"]).to eq({})
      end
    end
  end

  describe "#count" do
    it "returns the integer count of members" do
      allow(raw_connection).to receive(:exec_params).and_return([{ "n" => "5" }])
      expect(presence.count).to eq(5)
    end
  end

  describe "#sweep!" do
    it "deletes expired members and returns the number of rows deleted" do
      expired = Time.now - 300
      allow(raw_connection).to receive(:exec_params).and_return([
                                                                  { "member_id" => "1" },
                                                                  { "member_id" => "2" }
                                                                ])

      result = presence.sweep!(older_than: expired)

      expect(result).to eq(2)
      expect(raw_connection).to have_received(:exec_params).with(
        a_string_matching(/DELETE FROM pgbus_presence_members.*RETURNING/m),
        ["room:42", expired]
      )
    end

    it "returns 0 when nothing to sweep" do
      allow(raw_connection).to receive(:exec_params).and_return([])
      expect(presence.sweep!(older_than: Time.now)).to eq(0)
    end

    it "only sweeps member-keyed rows on a seat table (seats expire through the heartbeat)" do
      allow(described_class).to receive(:seats_supported?).and_return(true)
      allow(raw_connection).to receive(:exec_params).and_return([])

      presence.sweep!(older_than: Time.now)

      expect(raw_connection).to have_received(:exec_params)
        .with(a_string_matching(/last_seen_at < \$2 AND connection_id IS NULL/), anything)
    end
  end

  describe "seats (issue #515)" do
    before { allow(described_class).to receive(:seats_supported?).and_return(true) }

    let(:seat_row) do
      {
        "member_id" => "7",
        "connection_id" => "c1",
        "seat" => "s1",
        "state" => '{"selected":3}',
        "metadata" => '{"name":"Alice"}',
        "joined_at" => "2026-04-09T00:00:00Z",
        "last_seen_at" => "2026-04-09T00:00:00Z"
      }
    end

    describe "#seats?" do
      it "reports the schema probe" do
        expect(presence.seats?).to be(true)
      end
    end

    describe "#join with connection_id:" do
      before { allow(raw_connection).to receive(:exec_params).and_return([seat_row]) }

      it "upserts one row per connection, keyed by (stream_name, connection_id)" do
        presence.join(member_id: 7, connection_id: "c1", seat: "s1", metadata: { name: "Alice" })

        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/ON CONFLICT \(stream_name, connection_id\) WHERE connection_id IS NOT NULL/),
          ["room:42", "7", '{"name":"Alice"}', "c1", "s1"]
        )
      end

      it "returns the record with its connection, seat and parsed state" do
        result = presence.join(member_id: 7, connection_id: "c1", seat: "s1")

        expect(result).to include("id" => "7", "connection_id" => "c1", "seat" => "s1",
                                  "state" => { "selected" => 3 }, "metadata" => { "name" => "Alice" })
      end
    end

    describe "#join without connection_id:" do
      before { allow(raw_connection).to receive(:exec_params).and_return([seat_row.merge("connection_id" => nil)]) }

      it "upserts the member-keyed row through the partial index" do
        presence.join(member_id: 7)

        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/ON CONFLICT \(stream_name, member_id\) WHERE connection_id IS NULL/),
          ["room:42", "7", "{}"]
        )
      end
    end

    describe "#leave" do
      it "deletes only the given connection's row" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row])

        result = presence.leave(connection_id: "c1")

        expect(result["connection_id"]).to eq("c1")
        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/DELETE FROM pgbus_presence_members.*connection_id = \$2/m), ["room:42", "c1"]
        )
      end

      it "keeps member-keyed leave on the member-keyed row" do
        allow(raw_connection).to receive(:exec_params).and_return([])

        presence.leave(member_id: "7")

        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/member_id = \$2 AND connection_id IS NULL/), ["room:42", "7"]
        )
      end

      it "requires exactly one of member_id: or connection_id:" do
        expect { presence.leave }.to raise_error(ArgumentError, /member_id: or connection_id:/)
        expect { presence.leave(member_id: "7", connection_id: "c1") }.to raise_error(ArgumentError)
      end
    end

    describe "#touch with connection_id:" do
      it "refreshes every given connection in one statement and returns the ids it touched" do
        allow(raw_connection).to receive(:exec_params).and_return([{ "connection_id" => "c1" }])

        expect(presence.touch(connection_id: %w[c1 c2])).to eq(["c1"])
        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/connection_id = ANY\(\$2::text\[\]\).*RETURNING connection_id/m),
          ["room:42", %({"c1","c2"})]
        )
      end

      it "accepts a single id" do
        allow(raw_connection).to receive(:exec_params).and_return([{ "connection_id" => "c1" }])

        expect(presence.touch(connection_id: "c1")).to eq(["c1"])
      end
    end

    describe "#set_state" do
      it "merges the state into the connection's row and returns the record" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row])

        result = presence.set_state(connection_id: "c1", state: { selected: 3 })

        expect(result["state"]).to eq({ "selected" => 3 })
        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/SET state = state \|\| \$3::jsonb/), ["room:42", "c1", '{"selected":3}']
        )
      end

      it "returns nil when the connection has no row" do
        allow(raw_connection).to receive(:exec_params).and_return([])

        expect(presence.set_state(connection_id: "gone", state: {})).to be_nil
      end
    end

    describe "#members" do
      it "returns one record per seat with its connection, seat and state" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row, seat_row.merge("connection_id" => "c2")])

        result = presence.members

        expect(result.map { |row| row["connection_id"] }).to eq(%w[c1 c2])
        expect(result.first).to include("id" => "7", "seat" => "s1", "state" => { "selected" => 3 })
      end
    end

    describe "#set_state with a non-Hash" do
      it "raises ArgumentError instead of writing a non-mergeable state" do
        expect { presence.set_state(connection_id: "c1", state: nil) }.to raise_error(ArgumentError, /Hash/)
        expect { presence.set_state(connection_id: "c1", state: [1]) }.to raise_error(ArgumentError, /Hash/)
      end
    end

    describe "change hook (streams_presence_on_change)" do
      before { allow(Pgbus.configuration).to receive(:notify_presence_change) }

      it "reports a seat join once" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row])

        presence.join(member_id: 7, connection_id: "c1", seat: "s1")

        expect(Pgbus.configuration).to have_received(:notify_presence_change).once
                                                                             .with("room:42", { type: :join, connection_id: "c1",
                                                                                                seat: "s1", member_id: "7" })
      end

      it "reports a seat leave once" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row])

        presence.leave(connection_id: "c1")

        expect(Pgbus.configuration).to have_received(:notify_presence_change).once
                                                                             .with("room:42", { type: :leave, connection_id: "c1",
                                                                                                seat: "s1", member_id: "7" })
      end

      it "does not report a leave for a connection that had no row" do
        allow(raw_connection).to receive(:exec_params).and_return([])

        presence.leave(connection_id: "gone")

        expect(Pgbus.configuration).not_to have_received(:notify_presence_change)
      end

      it "does not report member-keyed joins and leaves" do
        allow(raw_connection).to receive(:exec_params).and_return([seat_row.merge("connection_id" => nil)])

        presence.join(member_id: 7)
        presence.leave(member_id: 7)

        expect(Pgbus.configuration).not_to have_received(:notify_presence_change)
      end

      it "reports each expired seat as a leave" do
        allow(raw_connection).to receive(:exec_params)
          .and_return([{ "stream_name" => "room:1", "connection_id" => "c1" }], [seat_row])

        described_class.expire_seats!(ttl_for: ->(_) { 30 }, min_ttl: 30)

        expect(Pgbus.configuration).to have_received(:notify_presence_change).once
                                                                             .with("room:1", { type: :leave, connection_id: "c1",
                                                                                               seat: "s1", member_id: "7" })
      end
    end

    describe ".expire_seats!" do
      it "deletes seat rows past their stream's ttl and returns them with the stream name" do
        candidates = [
          { "stream_name" => "room:1", "connection_id" => "c1" },
          { "stream_name" => "room:2", "connection_id" => "c2" }
        ]
        allow(raw_connection).to receive(:exec_params).and_return(candidates, [seat_row.merge("stream_name" => "room:1")], [])

        expired = described_class.expire_seats!(ttl_for: ->(name) { name == "room:1" ? 30 : 90 }, min_ttl: 30)

        expect(expired.map { |row| row["stream_name"] }).to eq(["room:1"])
        expect(expired.first).to include("connection_id" => "c1", "seat" => "s1", "id" => "7")
        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/connection_id IS NOT NULL AND last_seen_at < NOW\(\) - make_interval\(secs => \$1\)/), [30]
        )
        expect(raw_connection).to have_received(:exec_params).with(
          a_string_matching(/DELETE FROM pgbus_presence_members.*RETURNING/m), ["room:1", %({"c1"}), 30]
        )
        expect(raw_connection).to have_received(:exec_params).with(anything, ["room:2", %({"c2"}), 90])
      end

      it "returns [] without a query when the schema has no seats" do
        allow(described_class).to receive(:seats_supported?).and_return(false)
        allow(raw_connection).to receive(:exec_params)

        expect(described_class.expire_seats!(ttl_for: ->(_) { 30 }, min_ttl: 30)).to eq([])
        expect(raw_connection).not_to have_received(:exec_params)
      end
    end
  end

  describe ".seats_supported?" do
    before do
      allow(described_class).to receive(:seats_supported?).and_call_original
      described_class.reset_schema_cache!
    end

    after { described_class.reset_schema_cache! }

    it "is true when pgbus_presence_members has the connection_id column, and memoized" do
      allow(raw_connection).to receive(:exec_params).and_return([{ "present" => "1" }])

      2.times { expect(described_class.seats_supported?).to be(true) }
      expect(raw_connection).to have_received(:exec_params).once
    end

    it "is false on the pre-seat schema" do
      allow(raw_connection).to receive(:exec_params).and_return([])

      expect(described_class.seats_supported?).to be(false)
    end

    it "probes again once a false answer is older than the recheck window (table migrated while running)" do
      allow(raw_connection).to receive(:exec_params).and_return([], [{ "present" => "1" }])
      clock = 100.0
      allow(described_class).to receive(:monotonic_now) { clock }

      expect(described_class.seats_supported?).to be(false)
      clock += described_class::SCHEMA_RECHECK_SECONDS + 1
      expect(described_class.seats_supported?).to be(true)
    end
  end

  describe "member-keyed join after the table was migrated under a running process" do
    before do
      allow(described_class).to receive(:seats_supported?).and_call_original
      described_class.reset_schema_cache!
    end

    after { described_class.reset_schema_cache! }

    it "re-probes the schema and retries with the partial-index conflict target" do
      calls = []
      allow(raw_connection).to receive(:exec_params) do |sql, _params|
        calls << sql
        if sql.include?("information_schema")
          calls.one? { |sent| sent.include?("information_schema") } ? [] : [{ "present" => "1" }]
        elsif sql.include?("ON CONFLICT (stream_name, member_id)\n")
          raise PG::InvalidColumnReference, "there is no unique or exclusion constraint matching the ON CONFLICT specification"
        else
          [{ "member_id" => "7", "metadata" => "{}", "joined_at" => "t", "last_seen_at" => "t" }]
        end
      end

      expect(presence.join(member_id: 7)["id"]).to eq("7")
      expect(calls.last).to include("ON CONFLICT (stream_name, member_id) WHERE connection_id IS NULL")
    end
  end

  describe "connection resolution" do
    it "raises ConfigurationError when ActiveRecord is not loaded" do
      hide_const("ActiveRecord::Base")

      expect do
        presence.count
      end.to raise_error(Pgbus::ConfigurationError, /requires ActiveRecord/)
    end

    # The connects_to branch (Pgbus::BusRecord.connection.raw_connection)
    # is exercised by spec/integration/streams/presence_spec.rb against
    # a real DB — it's a 3-line delegation not worth stubbing AR's full
    # internals for here.
  end
end
