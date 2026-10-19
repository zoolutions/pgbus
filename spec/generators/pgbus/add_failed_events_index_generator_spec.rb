# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_failed_events_index_generator"

RSpec.describe Pgbus::Generators::AddFailedEventsIndexGenerator do
  it_behaves_like "a pgbus generator", /pgbus_failed_events/

  describe "generated migration" do
    let(:basename) { "_add_pgbus_failed_events_unique_index.rb" }

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

    it "defines a versioned migration class" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusFailedEventsUniqueIndex < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    it "adds a unique idempotent index on (queue_name, msg_id)" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_index :pgbus_failed_events, [:queue_name, :msg_id]")
        expect(content).to include("unique: true")
        expect(content).to include('name: "idx_pgbus_failed_events_queue_msg"')
        expect(content).to include("if_not_exists: true")
      end
    end

    # Issue #583: the (queue_name, msg_id, source_queue) index supersedes it;
    # the old one would collapse two priority sub-queues' failures again.
    it "adds nothing once the source_queue unique index exists" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include(
          'return if index_name_exists?(:pgbus_failed_events, "idx_pgbus_failed_events_queue_msg_source")'
        )
      end
    end
  end
end
