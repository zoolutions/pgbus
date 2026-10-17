# frozen_string_literal: true

require "spec_helper"
require "rails/generators"
require "generators/pgbus/add_deploy_tasks_generator"

RSpec.describe Pgbus::Generators::AddDeployTasksGenerator do
  it_behaves_like "a pgbus generator", /deploy tasks/i

  describe "generated migration" do
    let(:basename) { "_add_pgbus_deploy_tasks_tables.rb" }

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

    it "creates the pgbus_deploy_tasks table with the arm columns" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to match(/class AddPgbusDeployTasksTables < ActiveRecord::Migration\[\d+\.\d+\]/)
        expect(content).to include("create_table :pgbus_deploy_tasks, if_not_exists: true")
        expect(content).to include('name: "idx_pgbus_deploy_tasks_key", if_not_exists: true')
        expect(content).to include("t.string :key, null: false")
        expect(content).to include('t.string :version, null: false, default: "1"')
        expect(content).to include("t.boolean :every_deploy, null: false, default: false")
        expect(content).to include("t.boolean :enabled, null: false, default: true")
        expect(content).to include("t.datetime :armed_at")
        expect(content).to include("t.string :armed_for")
        expect(content).to include('name: "idx_pgbus_deploy_tasks_key"')
        expect(content).to include('name: "idx_pgbus_deploy_tasks_armed"')
      end
    end

    it "creates the pgbus_deploy_task_runs table with task and cleanup indexes" do
      generate_migration(described_class, basename: basename) do |_path, content|
        expect(content).to include("create_table :pgbus_deploy_task_runs")
        expect(content).to include("t.string :triggered_by, null: false")
        expect(content).to include("t.bigint :msg_id")
        expect(content).to include('name: "idx_pgbus_deploy_task_runs_task"')
        expect(content).to include('name: "idx_pgbus_deploy_task_runs_cleanup"')
      end
    end
  end

  describe "deploy tasks config" do
    let(:basename) { "_add_pgbus_deploy_tasks_tables.rb" }

    it "also renders config/deploy_tasks.yml with the version and every_deploy keys" do
      generate_migration(described_class, basename: basename) do |_path, _content, root|
        path = File.join(root, "config", "deploy_tasks.yml")
        expect(File.exist?(path)).to be(true)
        expect(File.read(path)).to include("version:", "every_deploy:")
      end
    end
  end
end
