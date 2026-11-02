# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/fold_queue_name_case_generator"

RSpec.describe Pgbus::Generators::FoldQueueNameCaseGenerator do
  it_behaves_like "a pgbus generator", /lower case/

  describe "generated migration" do
    let(:basename) { "_fold_pgbus_queue_name_case.rb" }

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
        expect(content).to match(/class FoldPgbusQueueNameCase < ActiveRecord::Migration\[\d+\.\d+\]/)
      end
    end

    # Issue #633: both spellings share one table, so the fold may only touch
    # pgmq.meta rows, never the tables.
    it "folds through Pgbus::QueueNameFold, which never drops a table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("Pgbus::QueueNameFold.run!(connection)")
      end
      source = File.read(File.expand_path("../../../lib/pgbus/queue_name_fold.rb", __dir__))
      expect(source).not_to match(/pgmq\.drop_queue\(|DROP TABLE/i)
    end
  end
end
