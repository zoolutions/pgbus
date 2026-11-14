# frozen_string_literal: true

require_relative "../../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #660 on the dashboard's data source: what an after: task waits on,
# each run's outcome, and Run now refusing a blocked task unless told to
# ignore its dependencies.
RSpec.describe Pgbus::Web::DataSource::DeployTaskList, :integration do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: Pgbus.client) }

  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }
  let(:queue) { "deploy_task_list_deps" }

  def task(key, **options)
    Pgbus::Deploy::Task.from_configuration(key, class: "DeployListDepsJob", queue: queue, **options)
  end

  def run(key, version: "1", revision: "rev-a", **outcome)
    Pgbus::DeployTaskRun.create!(task_key: key, version: version, revision: revision, triggered_by: "rollover",
                                 run_at: now, queue_name: queue, **outcome)
  end

  def row(key) = data_source.deploy_tasks.find { |r| r[:key] == key }

  before do
    stub_const("DeployListDepsJob", Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      def perform(*); end
    end)
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    Pgbus::Deploy::Schema.reset!
    Pgbus.client.ensure_queue(queue)
    Pgbus::DeployTask.sync_from_config!(
      [task("widgets"), task("gadgets", version: 2), task("warm", every_deploy: true),
       task("totals", after: %w[widgets gadgets warm])],
      revision: "rev-b", now: now
    )
    Pgbus::ProcessEntry.create!(kind: "scheduler", hostname: "h", pid: 1, metadata: { revision: "rev-b" },
                                last_heartbeat_at: Time.current)
  end

  it "lists each unsatisfied dependency with its state: not run, running or failed" do
    run("widgets") # no outcome yet: running
    run("gadgets", version: "1", completed_at: now) # completed, but at an older version: not run
    run("warm", revision: "rev-b", failed_at: now)

    expect(row("totals")).to include(
      dependencies: %w[widgets gadgets warm],
      waiting_on: [{ key: "widgets", state: "running" }, { key: "gadgets", state: "not_run" },
                   { key: "warm", state: "failed" }]
    )
    expect(row("widgets")).to include(dependencies: [], waiting_on: [])
  end

  it "waits on nothing once every dependency completed at its version (every_deploy: on the scheduler's revision)" do
    run("widgets", completed_at: now)
    Pgbus::DeployTaskRun.create!(task_key: "gadgets", version: "2", triggered_by: "manual", run_at: now,
                                 queue_name: queue, completed_at: now)
    run("warm", revision: "rev-b", completed_at: now)

    expect(row("totals")[:waiting_on]).to eq([])
  end

  it "carries each run's outcome on the task page" do
    run("widgets", completed_at: now)
    id = Pgbus::DeployTask.find_by!(key: "widgets").id

    expect(data_source.deploy_task(id)[:runs].sole).to include(completed_at: now, failed_at: nil)
  end

  it "refuses Run now while a dependency is unsatisfied, and runs it when told to ignore dependencies" do
    id = Pgbus::DeployTask.find_by!(key: "totals").id

    expect(data_source.enqueue_deploy_task_now(id)).to eq(:blocked)
    expect(Pgbus::DeployTaskRun.where(task_key: "totals")).to be_empty

    expect(data_source.enqueue_deploy_task_now(id, ignore_dependencies: true)).to eq(:enqueued)
    expect(Pgbus::DeployTaskRun.find_by!(task_key: "totals")).to have_attributes(triggered_by: "manual")
  end
end
