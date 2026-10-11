# frozen_string_literal: true

require_relative "../../integration_helper"

# Seats against a real table (issue #515): the partial unique indexes are
# the upsert targets, so only Postgres can prove the ON CONFLICT clauses.
RSpec.describe "Streams: presence seats", :integration do
  let(:stream_name) { "seats_#{SecureRandom.hex(4)}" }
  let(:presence) { Pgbus.stream(stream_name).presence }
  let(:changes) { [] }

  before do
    Pgbus::Streams::Presence.reset_schema_cache!
    Pgbus.configuration.streams_presence_on_change = ->(stream, change) { changes << [stream, change] }
  end

  after do
    Pgbus.configuration.streams_presence_on_change = nil
    ActiveRecord::Base.connection.execute("DELETE FROM pgbus_presence_members")
  end

  it "detects the seat schema" do
    expect(presence.seats?).to be(true)
  end

  it "keeps two connections of one member as two rows; leaving one keeps the other" do
    presence.join(member_id: "7", connection_id: "c1", seat: "s1", metadata: { name: "Ann" })
    presence.join(member_id: "7", connection_id: "c2", seat: "s2", metadata: { name: "Ann" })

    expect(presence.members.map { |member| member["connection_id"] }).to contain_exactly("c1", "c2")

    presence.leave(connection_id: "c1")

    expect(presence.members).to contain_exactly(
      include("id" => "7", "connection_id" => "c2", "seat" => "s2", "state" => {}, "metadata" => { "name" => "Ann" })
    )
  end

  it "re-joining a connection updates its row instead of adding one" do
    presence.join(member_id: "7", connection_id: "c1", seat: "s1")
    presence.join(member_id: "7", connection_id: "c1", seat: "s1", metadata: { v: 2 })

    expect(presence.count).to eq(1)
    expect(presence.members.first["metadata"]).to eq({ "v" => 2 })
  end

  it "keeps member-keyed rows working beside seat rows" do
    presence.join(member_id: "7")
    presence.join(member_id: "7")
    presence.join(member_id: "7", connection_id: "c1", seat: "s1")

    expect(presence.count).to eq(2)
    expect(presence.leave(member_id: "7")["connection_id"]).to be_nil
    expect(presence.members.map { |member| member["connection_id"] }).to eq(["c1"])
  end

  it "set_state merges into the given connection's row only" do
    presence.join(member_id: "7", connection_id: "c1", seat: "s1")
    presence.join(member_id: "8", connection_id: "c2", seat: "s2")

    presence.set_state(connection_id: "c1", state: { selected: 3 })
    record = presence.set_state(connection_id: "c1", state: { mode: "edit" })

    expect(record["state"]).to eq({ "selected" => 3, "mode" => "edit" })
    expect(presence.members.find { |member| member["connection_id"] == "c2" }["state"]).to eq({})
    expect(presence.set_state(connection_id: "nope", state: { a: 1 })).to be_nil
  end

  it "touch returns only the connections that still have a row" do
    presence.join(member_id: "7", connection_id: "c1", seat: "s1")

    expect(presence.touch(connection_id: %w[c1 c2])).to eq(["c1"])
  end

  it "expires silent seats after their stream's ttl, reports each as a leave, and leaves member rows alone" do
    presence.join(member_id: "7", connection_id: "old", seat: "s1")
    presence.join(member_id: "7", connection_id: "fresh", seat: "s2")
    presence.join(member_id: "9")
    ActiveRecord::Base.connection.execute(<<~SQL)
      UPDATE pgbus_presence_members SET last_seen_at = NOW() - INTERVAL '2 minutes'
      WHERE connection_id = 'old' OR connection_id IS NULL
    SQL
    changes.clear

    expired = Pgbus::Streams::Presence.expire_seats!(ttl_for: ->(_) { 60 }, min_ttl: 60)

    expect(expired.map { |row| row["connection_id"] }).to eq(["old"])
    expect(changes).to eq([[stream_name, { type: :leave, connection_id: "old", seat: "s1", member_id: "7" }]])
    expect(presence.members.map { |member| member["connection_id"] }).to contain_exactly("fresh", nil)
  end

  it "recovers a member-keyed join on a process that probed the table before the migration" do
    klass = Pgbus::Streams::Presence
    klass.instance_variable_set(:@seats_supported, false)
    klass.instance_variable_set(:@probed_at, klass.monotonic_now)

    expect(presence.join(member_id: "7")["id"]).to eq("7")
    expect(presence.seats?).to be(true)
  end

  it "reports joins and leaves through the change hook" do
    presence.join(member_id: "7", connection_id: "c1", seat: "s1")
    presence.leave(connection_id: "c1")

    expect(changes.map { |change| change.last[:type] }).to eq(%i[join leave])
  end
end
