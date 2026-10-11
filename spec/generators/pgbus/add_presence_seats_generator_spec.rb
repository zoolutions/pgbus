# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_presence_seats_generator"

RSpec.describe Pgbus::Generators::AddPresenceSeatsGenerator do
  it_behaves_like "a pgbus generator", /seat/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_presence_seats.rb" }

    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: basename) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(
        described_class,
        options: { database: "pgbus" },
        migrate_dir: "db/pgbus_migrate",
        basename: basename
      ) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "defines a versioned, reversible migration class" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusPresenceSeats < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("def up")
        expect(content).to include("def down")
      end
    end

    it "adds the connection_id, seat and state columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_column :pgbus_presence_members, :connection_id, :string")
        expect(content).to include("add_column :pgbus_presence_members, :seat, :string")
        expect(content).to include("add_column :pgbus_presence_members, :state, :jsonb, null: false, default: {}")
      end
    end

    it "swaps the member primary-key index for partial unique keys on member and connection" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include('remove_index :pgbus_presence_members, name: "idx_pgbus_presence_members_pk"')
        expect(content).to include('name: "idx_pgbus_presence_members_connection_key"')
        expect(content).to include('where: "connection_id IS NOT NULL"')
        expect(content).to include('name: "idx_pgbus_presence_members_member_key"')
        expect(content).to include('where: "connection_id IS NULL"')
        expect(content).to include('name: "idx_pgbus_presence_members_member"')
        expect(content).to include('name: "idx_pgbus_presence_members_seat_expiry"')
      end
    end
  end
end
