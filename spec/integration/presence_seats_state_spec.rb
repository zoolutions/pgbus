# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #665: the doctor reports a pgbus_presence_members table without the
# seat columns. The table is moved aside for each example and put back after
# it, as failed_events_unmigrated_spec.rb does.
RSpec.describe "Presence.seats_state (integration)", :integration do
  let(:presence) { Pgbus::Streams::Presence }
  let(:conn) { ActiveRecord::Base.connection }

  it "is :present on the migrated table" do
    expect(presence.seats_state).to eq(:present)
  end

  context "with the table moved aside" do
    around do |example|
      conn.execute("ALTER TABLE pgbus_presence_members RENAME TO pgbus_presence_members_665")
      example.run
    ensure
      conn.execute(<<~SQL)
        DROP TABLE IF EXISTS pgbus_presence_members;
        ALTER TABLE pgbus_presence_members_665 RENAME TO pgbus_presence_members;
      SQL
    end

    it "is :absent without the table" do
      expect(presence.seats_state).to eq(:absent)
    end

    it "is :missing on a table from before add_presence_seats, and the runtime probe agrees" do
      conn.execute(<<~SQL)
        CREATE TABLE pgbus_presence_members (
          stream_name VARCHAR NOT NULL, member_id VARCHAR NOT NULL, metadata JSONB NOT NULL DEFAULT '{}',
          joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      SQL
      presence.reset_schema_cache!

      expect(presence.seats_state).to eq(:missing)
      expect(presence.seats_supported?).to be(false)
    ensure
      presence.reset_schema_cache!
    end
  end
end
