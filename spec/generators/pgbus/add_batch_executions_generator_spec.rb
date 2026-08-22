# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_batch_executions_generator"

RSpec.describe Pgbus::Generators::AddBatchExecutionsGenerator do
  it_behaves_like "a pgbus generator", /batch_executions/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_batch_executions.rb" }

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

    it "creates pgbus_batch_executions and renames failure columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusBatchExecutions < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_batch_executions")
        expect(content).to include("t.string :job_id, null: false")
        expect(content).to include("rename_column :pgbus_batches, :on_discard_class, :on_failure_class")
        expect(content).to include("idx_pgbus_batch_executions_orphans")
        expect(content).to include("UPDATE pgbus_batches SET failed_jobs = failed_jobs + discarded_jobs")
        expect(content).to include("remove_column :pgbus_batches, :discarded_jobs")
        expect(content).to include("UPDATE pgbus_batches SET failed_jobs = 0")
      end
    end
  end
end
