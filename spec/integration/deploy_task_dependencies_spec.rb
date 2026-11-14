# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #660 end to end: run outcomes stamped by the real executor from the
# payload's run id, and an after: dependent enqueued only once its dependency
# has completed, on the real rollover gate and PGMQ.
RSpec.describe "Deploy task dependencies (integration)", :integration do
  let(:queue) { "deploy_task_deps" }
  let(:config) { Pgbus.configuration }
  let(:now) { Time.current.change(usec: 0) }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: Pgbus.client) }
  let(:performed) { [] }

  def job_class(name, &body)
    log = performed
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::DeployTaskRunId
      include Pgbus::Concurrency
      include Pgbus::Uniqueness

      self.queue_adapter = :pgbus
      define_singleton_method(:name) { name }
      define_method(:perform) { |*| log << name }
      class_eval(&body) if body
    end
  end

  around do |example|
    saved = config.deploy_tasks
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    Pgbus::Deploy::Schema.reset!
    example.run
  ensure
    config.deploy_tasks = saved
    Pgbus::Deploy::Schema.reset!
  end

  before do
    stub_const("WidgetsJob", job_class("WidgetsJob"))
    stub_const("TotalsJob", job_class("TotalsJob"))
    Pgbus.client.ensure_queue(queue)
    Pgbus.client.purge_queue(queue)
    config.deploy_tasks = {
      "widgets" => { "class" => "WidgetsJob", "queue" => queue },
      "totals" => { "class" => "TotalsJob", "queue" => queue, "after" => "widgets" }
    }
    %w[scheduler worker].each_with_index do |kind, pid|
      Pgbus::ProcessEntry.create!(kind: kind, hostname: "host-a", pid: pid + 1, metadata: { revision: "rev-b" },
                                  last_heartbeat_at: Time.current)
    end
  end

  def runner = Pgbus::Deploy::Runner.new(config: config, revision: "rev-b")

  def work_one
    message = Pgbus.client.read_batch(queue, qty: 1, vt: 30).first
    message && executor.execute(message, queue)
  end

  def run_for(key) = Pgbus::DeployTaskRun.find_by!(task_key: key)

  it "enqueues the dependent only after its dependency's job has completed, in the same rollover" do
    scheduler = runner
    scheduler.sync!(now: now)

    scheduler.tick(now)
    expect(Pgbus::DeployTaskRun.pluck(:task_key)).to eq(%w[widgets])

    scheduler.tick(now)
    expect(Pgbus::DeployTaskRun.pluck(:task_key)).to eq(%w[widgets])
    expect(Pgbus::DeployTask.find_by!(key: "totals").armed_at).not_to be_nil

    expect(work_one).to eq(:success)
    expect(performed).to eq(%w[WidgetsJob])
    expect(run_for("widgets").completed_at).not_to be_nil

    scheduler.tick(now)
    expect(work_one).to eq(:success)

    expect(performed).to eq(%w[WidgetsJob TotalsJob])
    expect(run_for("totals")).to have_attributes(triggered_by: "rollover", queue_name: queue)
    expect(run_for("totals").completed_at).not_to be_nil
  end

  it "stamps only the final attempt of a retry_on job, on the run its payload names" do
    attempts = []
    stub_const("WidgetsJob", job_class("WidgetsJob") do
      retry_on RuntimeError, wait: 0, attempts: 2
      define_method(:perform) do |*|
        attempts << executions
        raise "not yet" if attempts.one?
      end
    end)
    scheduler = runner
    scheduler.sync!(now: now)
    scheduler.tick(now)

    expect(work_one).to eq(:success) # the retried attempt: re-enqueued, archived, no outcome
    expect(run_for("widgets").completed_at).to be_nil

    expect(work_one).to eq(:success)
    expect(attempts.size).to eq(2)
    expect(run_for("widgets").completed_at).not_to be_nil
  end

  it "stamps failed_at when the adapter drops a retry_on retry (its concurrency limit, on_conflict: :discard)" do
    stub_const("WidgetsJob", job_class("WidgetsJob") do
      limits_concurrency to: 1, key: ->(*) { "deploy-deps-limited" }, on_conflict: :discard
      retry_on RuntimeError, wait: 0, attempts: 3
      define_method(:perform) { |*| raise "not yet" }
    end)
    runner.enqueue(Pgbus::Deploy::Task.from_configuration("widgets", class: "WidgetsJob", queue: queue),
                   triggered_by: "manual")

    expect(work_one).to eq(:success) # the attempt holds the slot, so its own retry is dropped
    expect(Pgbus.client.read_batch(queue, qty: 1, vt: 30)).to be_empty
    expect(run_for("widgets").failed_at).not_to be_nil
    expect(run_for("widgets").completed_at).to be_nil
  end

  it "leaves the outcome to a retry parked behind the job's concurrency limit, which stamps it when it runs" do
    attempts = []
    stub_const("WidgetsJob", job_class("WidgetsJob") do
      limits_concurrency to: 1, key: ->(*) { "deploy-deps-parked" }
      retry_on RuntimeError, wait: 0, attempts: 2
      define_method(:perform) do |*|
        attempts << executions
        raise "not yet" if attempts.one?
      end
    end)
    runner.enqueue(Pgbus::Deploy::Task.from_configuration("widgets", class: "WidgetsJob", queue: queue),
                   triggered_by: "manual")

    expect(work_one).to eq(:success) # the attempt: its retry is parked, then released when its slot frees
    expect(run_for("widgets")).to have_attributes(completed_at: nil, failed_at: nil)

    expect(work_one).to eq(:success)
    expect(attempts.size).to eq(2)
    expect(run_for("widgets").completed_at).not_to be_nil
  end

  it "fails Run now, leaving no run, when the adapter drops the job as a uniqueness duplicate" do
    stub_const("WidgetsJob", job_class("WidgetsJob") do
      ensures_uniqueness strategy: :until_executed, key: ->(*) { "deploy-deps-unique" }, on_conflict: :discard
    end)
    WidgetsJob.perform_later # the duplicate already queued
    allow(Pgbus::ErrorReporter).to receive(:report)

    result = runner.enqueue(Pgbus::Deploy::Task.from_configuration("widgets", class: "WidgetsJob", queue: queue),
                            triggered_by: "manual")

    expect(result).to eq(:failed)
    expect(Pgbus::DeployTaskRun.count).to eq(0)
    expect(Pgbus::ErrorReporter).to have_received(:report).with(kind_of(Pgbus::EnqueueError), hash_including(task: "widgets"))
  end

  it "stamps failed_at when the job dead-letters" do
    Pgbus::Deploy::Runner.new(config: config, revision: "rev-b").enqueue(
      Pgbus::Deploy::Task.from_configuration("widgets", class: "WidgetsJob", queue: queue), triggered_by: "manual"
    )
    message = Pgbus.client.read_batch(queue, qty: 1, vt: 30).first.with(read_ct: (config.max_retries + 1).to_s)

    expect(executor.execute(message, queue)).to eq(:dead_lettered)
    expect(run_for("widgets").failed_at).not_to be_nil
    expect(run_for("widgets").completed_at).to be_nil
  end

  describe "before pgbus:add_deploy_task_run_outcomes is migrated" do
    let(:conn) { ActiveRecord::Base.connection }

    around do |example|
      conn.execute(<<~SQL)
        DROP INDEX IF EXISTS idx_pgbus_deploy_task_runs_completed;
        ALTER TABLE pgbus_deploy_task_runs DROP COLUMN IF EXISTS completed_at;
        ALTER TABLE pgbus_deploy_task_runs DROP COLUMN IF EXISTS failed_at;
        ALTER TABLE pgbus_deploy_tasks DROP COLUMN IF EXISTS dependencies;
      SQL
      reset_schema!
      example.run
    ensure
      conn.execute(<<~SQL)
        ALTER TABLE pgbus_deploy_task_runs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP;
        ALTER TABLE pgbus_deploy_task_runs ADD COLUMN IF NOT EXISTS failed_at TIMESTAMP;
        ALTER TABLE pgbus_deploy_tasks ADD COLUMN IF NOT EXISTS dependencies TEXT;
        CREATE INDEX IF NOT EXISTS idx_pgbus_deploy_task_runs_completed
          ON pgbus_deploy_task_runs (task_key, completed_at);
      SQL
      reset_schema!
    end

    def reset_schema!
      [Pgbus::DeployTask, Pgbus::DeployTaskRun].each(&:reset_column_information)
      Pgbus::Deploy::Schema.reset!
    end

    it "skips the after: task with the generator hint, and runs and completes the rest" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
      scheduler = runner

      expect(scheduler.tasks.map(&:key)).to eq(%w[widgets])
      expect(warnings.join).to include("rails g pgbus:add_deploy_task_run_outcomes")

      scheduler.sync!(now: now)
      scheduler.tick(now)
      expect(work_one).to eq(:success)
      expect(performed).to eq(%w[WidgetsJob])
      expect(Pgbus::DeployTaskRun.sole).to have_attributes(task_key: "widgets", queue_name: queue)
    end
  end
end
