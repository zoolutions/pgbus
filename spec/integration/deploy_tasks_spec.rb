# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# End to end (issue #536): the real rollover gate over pgbus_processes, the
# real armed_at claim on pgbus_deploy_tasks and a real PGMQ message enqueued
# through the pgbus ActiveJob adapter.
RSpec.describe "Deploy tasks (integration)", :integration do
  let(:queue) { "deploy_tasks" }
  let(:config) { Pgbus.configuration }
  let(:now) { Time.current.change(usec: 0) }
  let(:job_class) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      def self.name = "DeployTasksSpecJob"
      def perform(*); end
    end
  end

  around do |example|
    saved = config.deploy_tasks
    example.run
  ensure
    config.deploy_tasks = saved
  end

  before do
    stub_const("DeployTasksSpecJob", job_class)
    Pgbus.client.ensure_queue(queue)
    declare(version: 2)
  end

  def declare(version:)
    config.deploy_tasks = { "backfill" => { "class" => "DeployTasksSpecJob", "queue" => queue, "version" => version } }
  end

  def runner(revision)
    Pgbus::Deploy::Runner.new(config: config, revision: revision)
  end

  def process(kind, revision:, pid:)
    Pgbus::ProcessEntry.create!(kind: kind, hostname: "host-a", pid: pid, metadata: { revision: revision },
                                last_heartbeat_at: Time.current)
  end

  def messages
    Pgbus.client.read_batch(queue, qty: 10, vt: 30)
  end

  it "waits while an old-revision worker is live, then enqueues exactly once" do
    new_runner = runner("rev-b")
    process("scheduler", revision: "rev-b", pid: 1)
    old_worker = process("worker", revision: "rev-a", pid: 2)
    new_runner.sync!(now: now)

    new_runner.tick(now)
    expect(Pgbus::DeployTaskRun.count).to eq(0)

    old_worker.destroy!
    process("worker", revision: "rev-b", pid: 3)
    fresh = runner("rev-b") # a new instance: no throttled gate status
    fresh.tick(now)
    fresh.tick(now + 10)

    expect(messages.size).to eq(1)
    expect(Pgbus::DeployTaskRun.sole).to have_attributes(
      task_key: "backfill", version: "2", revision: "rev-b", triggered_by: "rollover", queue_name: queue
    )
    expect(Pgbus::DeployTaskRun.sole.msg_id).to be_positive
    expect(Pgbus::DeployTask.sole.armed_at).to be_nil
  end

  it "enqueues once when two same-revision runners race on one arm" do
    process("worker", revision: "rev-b", pid: 3)
    runners = Array.new(2) { runner("rev-b") }
    runners.first.sync!(now: now)

    barrier = Concurrent::CyclicBarrier.new(runners.size)

    runners.map { |r| Thread.new { barrier.wait && r.tick(now) } }.each(&:join)

    expect(messages.size).to eq(1)
    expect(Pgbus::DeployTaskRun.count).to eq(1)
  end

  it "produces one run for the new version when an old scheduler re-syncs mid-deploy" do
    Pgbus::DeployTaskRun.create!(task_key: "backfill", version: "1", revision: "rev-a",
                                 triggered_by: "rollover", run_at: now - 86_400)
    process("scheduler", revision: "rev-b", pid: 1)
    process("worker", revision: "rev-a", pid: 2)
    new_runner = runner("rev-b")
    new_runner.sync!(now: now)

    declare(version: 1)
    old_runner = runner("rev-a")
    old_runner.sync!(now: now) # crash-restart of the old scheduler inside the overlap window
    old_runner.tick(now)
    expect(Pgbus::DeployTaskRun.where(revision: "rev-b").count).to eq(0)

    Pgbus::ProcessEntry.where(pid: 2).delete_all
    process("worker", revision: "rev-b", pid: 3)
    declare(version: 2)
    runner("rev-b").tick(now)

    expect(messages.size).to eq(1)
    expect(Pgbus::DeployTaskRun.rollover.order(:id).last).to have_attributes(version: "2", revision: "rev-b")
    expect(Pgbus::DeployTaskRun.rollover.count).to eq(2)
  end

  # The take-over (issue #536 deviation): v1 never fired (disabled, or its
  # gate never opened), so an old-revision scheduler that crash-restarts
  # mid-deploy re-arms "1". Once the fleet is on rev-b, the new runner owes
  # v2, takes the arm over and fires v2 exactly once.
  it "takes over an arm an old scheduler re-armed for an unfired version" do
    process("scheduler", revision: "rev-b", pid: 1)
    process("worker", revision: "rev-a", pid: 2)
    runner("rev-b").sync!(now: now)

    declare(version: 1)
    runner("rev-a").sync!(now: now)
    expect(Pgbus::DeployTask.sole.armed_for).to eq("1")

    Pgbus::ProcessEntry.where(pid: 2).delete_all
    process("worker", revision: "rev-b", pid: 3)
    declare(version: 2)
    new_runner = runner("rev-b")
    new_runner.tick(now)
    new_runner.tick(now + 10)

    expect(messages.size).to eq(1)
    expect(Pgbus::DeployTaskRun.sole).to have_attributes(version: "2", revision: "rev-b", triggered_by: "rollover")
    expect(Pgbus::DeployTask.sole.armed_at).to be_nil
  end

  it "re-arms a task whose enqueue fails" do
    process("worker", revision: "rev-b", pid: 3)
    new_runner = runner("rev-b")
    new_runner.sync!(now: now)
    job_class.before_enqueue { throw :abort }

    new_runner.tick(now)

    expect(Pgbus::DeployTask.sole).to have_attributes(armed_for: "2")
    expect(Pgbus::DeployTask.sole.armed_at).not_to be_nil
    expect(Pgbus::DeployTaskRun.count).to eq(0)
  end
end
