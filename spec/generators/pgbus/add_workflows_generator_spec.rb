# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_workflows_generator"

RSpec.describe Pgbus::Generators::AddWorkflowsGenerator do
  it_behaves_like "a pgbus generator", /workflow/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_workflows.rb" }

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

    it "creates pgbus_workflows with the one-ongoing and idempotency partial unique indexes" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusWorkflows < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_workflows, if_not_exists: true")
        expect(content).to include("t.string :workflow_id, null: false")
        expect(content).to include("t.jsonb :arguments, null: false, default: {}")
        expect(content).to include('t.string :status, null: false, default: "ready"')
        expect(content).to include("t.string :definition_digest, null: false")
        expect(content).to include("add_index :pgbus_workflows, [:status, :updated_at]")
        expect(content).to include("t.string :rerun_of_workflow_id")
        expect(content).to match(/\[:workflow_class, :subject_type, :subject_id\],\s+unique: true/)
        expect(content).to match(/\[:workflow_class, :idempotency_key\],\s+unique: true/)
        expect(content).to include('name: "idx_pgbus_workflows_one_ongoing"')
        # status is a varchar: the ::text cast keeps the predicate stable through schema.rb (issue #573).
        expect(content).to include(%(where: "status::text IN ('ready','performing','waiting','paused')"))
        expect(content).to include('name: "idx_pgbus_workflows_idempotency"')
        expect(content).to include('where: "idempotency_key IS NOT NULL"')
      end
    end

    it "creates pgbus_workflow_steps with the attempt key, the stall index and a cascading foreign key" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("create_table :pgbus_workflow_steps, if_not_exists: true")
        expect(content).to include("t.integer :attempt, null: false, default: 1")
        expect(content).to include("t.datetime :stall_at")
        expect(content).to include("t.jsonb :output")
        expect(content).to include("t.jsonb :error_causes")
        expect(content).to include("t.string :app_revision")
        expect(content).to include('name: "idx_pgbus_workflow_steps_attempt"')
        expect(content).to include('name: "idx_pgbus_workflow_steps_stall"')
        expect(content).to include("add_foreign_key :pgbus_workflow_steps, :pgbus_workflows")
        parts = ["add_foreign_key :pgbus_workflow_steps, :pgbus_workflows,", "column: :workflow_id,",
                 "primary_key: :workflow_id,", "on_delete: :cascade"]
        steps_fk = Regexp.new(parts.map { |part| Regexp.escape(part) }.join("\\s+"))
        expect(content).to match(steps_fk)
      end
    end

    # A fresh install needs one generator: the waiting-step columns and the
    # signals table (pgbus:add_workflow_signals, issue #541) are in it too.
    it "includes the waiting-step columns and the signals table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        %w[batch_id wait_for].each { |column| expect(content).to include("t.string :#{column}") }
        expect(content).to include("t.bigint :signal_id", "t.datetime :waiting_since", "t.datetime :timeout_at")
        expect(content).to include('name: "idx_pgbus_workflow_steps_timeout"', 'name: "idx_pgbus_workflow_steps_batch"')
        expect(content).to include("create_table :pgbus_workflow_signals, if_not_exists: true")
        expect(content).to include('name: "idx_pgbus_workflow_signals_idempotency"')
        expect(content).to include("drop_table :pgbus_workflow_signals, if_exists: true")
        expect(content).to include('name: "idx_pgbus_workflow_signals_claimed"')
        # A steps table from before waiting steps has no such columns yet.
        expect(content).to include("if column_exists?(:pgbus_workflow_steps, :batch_id)",
                                   "if column_exists?(:pgbus_workflow_steps, :timeout_at)")
      end
    end

    it "drops the three tables on rollback, children first" do
      generate_migration(described_class, basename: basename) do |_path, content|
        down = content[/def down.*?\n  end/m]
        expect(down).to include("drop_table :pgbus_workflow_events, if_exists: true")
        expect(down.index("pgbus_workflow_events")).to be < down.index("drop_table :pgbus_workflows,")
      end
    end

    it "creates the append-only pgbus_workflow_events table" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("create_table :pgbus_workflow_events, if_not_exists: true")
        expect(content).to include("t.string :actor, null: false")
        expect(content).to include("t.jsonb :details, null: false, default: {}")
        expect(content).to include("add_foreign_key :pgbus_workflow_events, :pgbus_workflows")
      end
    end
  end
end
