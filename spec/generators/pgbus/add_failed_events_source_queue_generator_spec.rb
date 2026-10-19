# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_failed_events_source_queue_generator"

RSpec.describe Pgbus::Generators::AddFailedEventsSourceQueueGenerator do
  it_behaves_like "a pgbus generator", /pgbus_failed_events/

  describe "generated migration" do
    let(:basename) { "_add_pgbus_failed_events_source_queue.rb" }

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
        expect(content).to match(/class AddPgbusFailedEventsSourceQueue < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    # Issue #583: two _pN sub-queues number their messages from their own
    # sequences, so (queue_name, msg_id) cannot stay unique.
    it "adds source_queue and moves uniqueness to (queue_name, msg_id, source_queue)" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("add_column :pgbus_failed_events, :source_queue, :string, if_not_exists: true")
        expect(content).to include("add_index :pgbus_failed_events, %i[queue_name msg_id source_queue]")
        expect(content).to include('name: "idx_pgbus_failed_events_queue_msg_source"')
        expect(content).to include('remove_index :pgbus_failed_events, name: "idx_pgbus_failed_events_queue_msg", if_exists: true')
      end
    end
  end
end
