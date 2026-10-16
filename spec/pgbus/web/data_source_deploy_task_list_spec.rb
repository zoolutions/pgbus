# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Web::DataSource::DeployTaskList do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: double("Pgbus::Client")) }

  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }
  let(:record) do
    double("Pgbus::DeployTask", id: 7, key: "backfill", class_name: "BackfillJob", command: nil, version: "2",
                                every_deploy: false, queue_name: "maintenance", priority: 0, description: "Totals",
                                enabled: true, static: true, armed_at: now, armed_for: "2", created_at: now - 86_400,
                                updated_at: now - 60, parsed_arguments: [{ "batch_size" => 500 }])
  end

  def run(triggered_by, revision: "rev-a", run_at: now - 300)
    double("Pgbus::DeployTaskRun", task_key: "backfill", triggered_by: triggered_by, revision: revision,
                                   version: "2", run_at: run_at, queue_name: "maintenance", msg_id: 42)
  end

  describe "#deploy_tasks" do
    let(:relation) { double("relation", to_a: [record]) }

    before do
      allow(Pgbus::DeployTask).to receive(:order).with(:key).and_return(relation)
      allow(Pgbus::DeployTaskRun).to receive(:latest_for).with(["backfill"])
                                                         .and_return([run("rollover"), run("manual", run_at: now - 60)])
    end

    it "returns each task with its last rollover and last manual run" do
      row = data_source.deploy_tasks.sole

      expect(row).to include(id: 7, key: "backfill", version: "2", every_deploy: false, queue_name: "maintenance",
                             enabled: true, armed_at: now, armed_for: "2")
      expect(row[:last_rollover_run]).to eq(run_at: now - 300, revision: "rev-a", version: "2", triggered_by: "rollover")
      expect(row[:last_manual_run]).to include(run_at: now - 60, triggered_by: "manual")
    end

    it "pages with limit and offset" do
      allow(relation).to receive(:limit).with(25).and_return(relation)
      allow(relation).to receive(:offset).with(25).and_return(relation)

      data_source.deploy_tasks(page: 2, per_page: 25)

      expect(relation).to have_received(:offset).with(25)
    end

    it "leaves the run columns nil without runs" do
      allow(Pgbus::DeployTaskRun).to receive(:latest_for).and_return([])

      expect(data_source.deploy_tasks.sole).to include(last_rollover_run: nil, last_manual_run: nil)
    end

    it "returns [] on error" do
      allow(Pgbus::DeployTask).to receive(:order).and_raise(StandardError, "db down")

      expect(data_source.deploy_tasks).to eq([])
    end
  end

  describe "#deploy_task" do
    it "returns the task with its arguments and the last 25 runs" do
      runs = [run("manual", run_at: now - 60), run("rollover")]
      recent = double("recent", to_a: runs)
      for_task = double("for_task")
      allow(Pgbus::DeployTask).to receive(:find_by).with(id: "7").and_return(record)
      allow(Pgbus::DeployTaskRun).to receive(:for_task).with("backfill").and_return(for_task)
      allow(for_task).to receive(:recent).with(25).and_return(recent)

      task = data_source.deploy_task("7")

      expect(task).to include(key: "backfill", arguments: [{ "batch_size" => 500 }])
      expect(task[:runs].size).to eq(2)
      expect(task[:runs].first).to include(triggered_by: "manual", queue_name: "maintenance", msg_id: 42)
      expect(task[:last_rollover_run]).to include(triggered_by: "rollover")
    end

    it "is nil for an unknown id or an error" do
      allow(Pgbus::DeployTask).to receive(:find_by).and_return(nil)
      expect(data_source.deploy_task("99")).to be_nil

      allow(Pgbus::DeployTask).to receive(:find_by).and_raise(StandardError)
      expect(data_source.deploy_task("1")).to be_nil
    end
  end

  describe "#toggle_deploy_task" do
    it "flips enabled and reports the new state" do
      toggled = double("Pgbus::DeployTask", enabled: true)
      allow(toggled).to receive(:update!) { allow(toggled).to receive(:enabled).and_return(false) }
      allow(Pgbus::DeployTask).to receive(:find_by).with(id: 1).and_return(toggled)

      expect(data_source.toggle_deploy_task(1)).to eq(:disabled)
      expect(toggled).to have_received(:update!).with(enabled: false)
    end

    it "is nil for an unknown id or an error" do
      allow(Pgbus::DeployTask).to receive(:find_by).and_return(nil)
      expect(data_source.toggle_deploy_task(9)).to be_nil

      allow(Pgbus::DeployTask).to receive(:find_by).and_raise(StandardError)
      expect(data_source.toggle_deploy_task(9)).to be_nil
    end
  end

  describe "#enqueue_deploy_task_now" do
    let(:runner) { instance_double(Pgbus::Deploy::Runner) }
    let(:task) { Pgbus::Deploy::Task.from_configuration("backfill", class: "BackfillJob") }

    before do
      allow(Pgbus::Deploy::Runner).to receive(:new).and_return(runner)
      allow(record).to receive(:to_task).and_return(task)
      allow(Pgbus::DeployTask).to receive(:find_by).with(id: 7).and_return(record)
    end

    it "returns the runner's real outcome for a manual run" do
      allow(runner).to receive(:enqueue).with(task, triggered_by: "manual").and_return(:enqueued)
      expect(data_source.enqueue_deploy_task_now(7)).to eq(:enqueued)

      allow(runner).to receive(:enqueue).and_return(:failed)
      expect(data_source.enqueue_deploy_task_now(7)).to eq(:failed)
    end

    it "is :failed for an unknown id or an error" do
      allow(Pgbus::DeployTask).to receive(:find_by).with(id: 8).and_return(nil)
      expect(data_source.enqueue_deploy_task_now(8)).to eq(:failed)

      allow(runner).to receive(:enqueue).and_raise(StandardError)
      expect(data_source.enqueue_deploy_task_now(7)).to eq(:failed)
    end
  end

  describe "#deploy_rollover" do
    let(:status) do
      Pgbus::Deploy::Rollover::Status.new(revision: "rev-b", lagging: [], workers_live: 2, reason: :ready,
                                          live_by_revision: { "rev-b" => 3, "rev-a" => 2 })
    end

    it "judges against the newest live scheduler's revision" do
      allow(Pgbus::Deploy::Rollover).to receive(:scheduler_revision).with(now: now).and_return([true, "rev-b"])
      allow(Pgbus::Deploy::Rollover).to receive(:check).with(revision: "rev-b", now: now).and_return(status)

      expect(data_source.deploy_rollover(now: now)).to eq(
        revision: "rev-b", reason: :ready, lagging: [], workers_live: 2,
        live_by_revision: { "rev-b" => 3, "rev-a" => 2 }, scheduler: true
      )
    end

    it "falls back to the web process's revision without a live scheduler, and says so" do
      allow(Pgbus::Deploy::Rollover).to receive(:scheduler_revision).and_return([false, nil])
      allow(Pgbus::Deploy::Revision).to receive(:current).and_return("rev-web")
      allow(Pgbus::Deploy::Rollover).to receive(:check).with(revision: "rev-web", now: now).and_return(status)

      expect(data_source.deploy_rollover(now: now)).to include(scheduler: false)
    end

    it "is nil when the processes cannot be read" do
      allow(Pgbus::Deploy::Rollover).to receive(:scheduler_revision).and_raise(StandardError)

      expect(data_source.deploy_rollover(now: now)).to be_nil
    end
  end

  describe "#list_count(:deploy_tasks)" do
    it "counts deploy tasks" do
      limited = double("limited", count: 3)
      allow(Pgbus::DeployTask).to receive(:limit).and_return(limited)

      expect(data_source.list_count(:deploy_tasks).total).to eq(3)
    end
  end
end
