# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_deploy_task_run_outcomes_generator"

RSpec.describe Pgbus::Generators::AddDeployTaskRunOutcomesGenerator do
  it_behaves_like "a pgbus generator", /deploy task run outcome/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_deploy_task_run_outcomes.rb" }

    it "writes the migration into db/migrate by default" do
      generate_migration(described_class, basename: basename) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "routes into db/pgbus_migrate when --database is set" do
      generate_migration(described_class, options: { database: "pgbus" }, migrate_dir: "db/pgbus_migrate",
                                          basename: basename) do |path, _content|
        expect(path).not_to be_nil
      end
    end

    it "adds the outcome columns, the dependencies column and the completed-run index, each if missing" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusDeployTaskRunOutcomes < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("add_column :pgbus_deploy_task_runs, :completed_at, :datetime, if_not_exists: true")
        expect(content).to include("add_column :pgbus_deploy_task_runs, :failed_at, :datetime, if_not_exists: true")
        expect(content).to include("add_column :pgbus_deploy_tasks, :dependencies, :text, if_not_exists: true")
        expect(content).to include("add_index :pgbus_deploy_task_runs, [:task_key, :completed_at]")
        expect(content).to include('name: "idx_pgbus_deploy_task_runs_completed", if_not_exists: true')
      end
    end
  end
end
