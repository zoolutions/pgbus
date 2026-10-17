# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_workflow_signals_generator"

RSpec.describe Pgbus::Generators::AddWorkflowSignalsGenerator do
  it_behaves_like "a pgbus generator", /signal/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_workflow_signals.rb" }

    it "writes the migration into db/migrate by default, and db/pgbus_migrate with --database" do
      generate_migration(described_class, basename: basename) { |path, _content| expect(path).not_to be_nil }
      generate_migration(described_class, options: { database: "pgbus" }, migrate_dir: "db/pgbus_migrate",
                                          basename: basename) { |path, _content| expect(path).not_to be_nil }
    end

    it "adds the five waiting columns and their two indexes to pgbus_workflow_steps, idempotently" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusWorkflowSignals < ActiveRecord::Migration\[\d+\.\d+\]/)
        { batch_id: :string, wait_for: :string, signal_id: :bigint, waiting_since: :datetime, timeout_at: :datetime }
          .each do |column, type|
          expect(content).to include("add_column :pgbus_workflow_steps, :#{column}, :#{type}, if_not_exists: true")
        end
        expect(content).to include("add_index :pgbus_workflow_steps, :batch_id,")
        expect(content).to include('name: "idx_pgbus_workflow_steps_batch", if_not_exists: true')
        expect(content).to include("add_index :pgbus_workflow_steps, [:status, :timeout_at],")
        expect(content).to include('name: "idx_pgbus_workflow_steps_timeout", if_not_exists: true')
      end
    end

    it "creates pgbus_workflow_signals with the idempotency index and a cascading foreign key" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("create_table :pgbus_workflow_signals, if_not_exists: true")
        expect(content).to include("t.jsonb :payload, null: false, default: {}")
        expect(content).to include('t.string :status, null: false, default: "pending"')
        expect(content).to include("t.bigint :claimed_by_step_id")
        expect(content).to match(/\[:workflow_id, :idempotency_key\],\s+unique: true, where: "idempotency_key IS NOT NULL"/)
        expect(content).to include("add_index :pgbus_workflow_signals, [:workflow_id, :name, :status]")
        expect(content).to include("add_index :pgbus_workflow_signals, [:workflow_id, :status, :claimed_by_step_id]")
        foreign_key = /add_foreign_key :pgbus_workflow_signals, :pgbus_workflows,\s+column: :workflow_id,\s+primary_key: :workflow_id,/
        expect(content).to match(foreign_key)
        expect(content).to include("primary_key: :workflow_id, on_delete: :cascade, if_not_exists: true")
      end
    end

    it "drops the table and the columns on rollback" do
      generate_migration(described_class, basename: basename) do |_path, content|
        down = content[/def down.*?\n  end/m]
        expect(down).to include("drop_table :pgbus_workflow_signals, if_exists: true")
        expect(down).to include("remove_column :pgbus_workflow_steps, :timeout_at, if_exists: true")
      end
    end
  end
end
