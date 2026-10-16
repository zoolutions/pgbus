# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Deploy::Runner do
  subject(:runner) { described_class.new(config: config, revision: "rev-b") }

  let(:config) { Pgbus::Configuration.new }
  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }
  let(:adapter) { recording_adapter.new }
  let(:recording_adapter) do
    Class.new do
      attr_reader :jobs

      def self.name = "RecordingAdapter"

      def initialize
        @jobs = []
      end

      def enqueue(job)
        job.provider_job_id = 4242 + @jobs.size
        @jobs << job
      end

      def enqueue_at(job, _timestamp) = enqueue(job)
      def enqueue_after_transaction_commit? = false
    end
  end
  let(:job_class) do
    Class.new(ActiveJob::Base) do
      def self.name = "DeployBackfillJob"
      def perform(*) = nil
    end
  end
  let(:ready) { status(:ready) }
  let(:rows) { {} }
  let(:runs) { [] }

  def status(reason, lagging: [])
    Pgbus::Deploy::Rollover::Status.new(revision: "rev-b", lagging: lagging, workers_live: 1, reason: reason,
                                        live_by_revision: {})
  end

  def row(key, armed_for: "1", enabled: true, armed: true, version: "1", claim: true)
    # A plain double: AR attribute readers need the schema, which unit specs have no database for.
    rows[key] = double("Pgbus::DeployTask", id: rows.size + 1, key: key, enabled: enabled, armed?: armed,
                                            armed_for: armed_for, version: version, claim!: claim, rearm!: 1)
  end

  around do |example|
    command_job_adapter = Pgbus::Recurring::CommandJob.queue_adapter
    example.run
  ensure
    Pgbus::Recurring::CommandJob.queue_adapter = command_job_adapter
  end

  before do
    stub_const("DeployBackfillJob", job_class)
    job_class.queue_adapter = adapter
    Pgbus::Recurring::CommandJob.queue_adapter = adapter
    config.deploy_tasks = { "backfill" => { "class" => "DeployBackfillJob", "queue" => "maintenance",
                                            "priority" => 2, "args" => [{ "batch_size" => 500 }] } }
    allow(Pgbus::DeployTask).to receive_messages(pending?: true, owed?: false)
    allow(Pgbus::DeployTask).to receive(:where) { |key:| key.filter_map { |k| rows[k] } }
    allow(Pgbus::DeployTaskRun).to receive(:create!) { |attrs| runs << attrs }
    allow(Pgbus::Deploy::Rollover).to receive(:check).and_return(ready)
    allow(Pgbus::ErrorReporter).to receive(:report)
  end

  describe "#tasks" do
    it "loads the declared tasks in key order" do
      config.deploy_tasks = { "zeta" => { "command" => "Z.run" }, "alpha" => { "command" => "A.run" } }

      expect(runner.tasks.map(&:key)).to eq(%w[alpha zeta])
    end

    it "skips an invalid task or a class that is not an ActiveJob, with a warning" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
      config.deploy_tasks = { "invalid" => { "version" => 2 }, "plain" => { "class" => "String" },
                              "ok" => { "command" => "A.run" } }

      expect(runner.tasks.map(&:key)).to eq(%w[ok])
      expect(warnings.join).to include("invalid", "plain", "not an ActiveJob")
    end

    it "logs a task's warnings" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&blk| warnings << blk.call }
      config.deploy_tasks = { "warm" => { "command" => "A.run", "every_deploy" => true, "version" => 3 } }

      runner.tasks

      expect(warnings.join).to include("warm", "version is ignored")
    end
  end

  describe "#sync!" do
    it "syncs the declared tasks with the runner's revision" do
      allow(Pgbus::DeployTask).to receive(:sync_from_config!)

      runner.sync!(now: now)

      expect(Pgbus::DeployTask).to have_received(:sync_from_config!).with(runner.tasks, revision: "rev-b", now: now)
    end

    it "warns once when no revision is configured" do
      allow(Pgbus::DeployTask).to receive(:sync_from_config!)
      allow(Pgbus.logger).to receive(:warn)

      described_class.new(config: config, revision: nil).sync!(now: now)

      expect(Pgbus.logger).to have_received(:warn).once
    end

    it "logs and swallows a sync failure" do
      allow(Pgbus::DeployTask).to receive(:sync_from_config!).and_raise(StandardError, "db down")

      expect { runner.sync!(now: now) }.not_to raise_error
      expect(Pgbus::ErrorReporter).to have_received(:report)
    end
  end

  describe "#tick" do
    it "enqueues an armed task once the gate is open and records the rollover run" do
      backfill = row("backfill")

      runner.tick(now)

      job = adapter.jobs.sole
      expect(job).to have_attributes(class: job_class, queue_name: "maintenance", priority: 2,
                                     arguments: [{ "batch_size" => 500 }])
      expect(backfill).to have_received(:claim!).with(armed_for: "1", now: now)
      expect(runs.sole).to include(task_key: "backfill", version: "1", revision: "rev-b", triggered_by: "rollover",
                                   queue_name: "maintenance", msg_id: 4242, run_at: now)
    end

    it "does nothing while the gate is closed, and logs the reason once" do
      row("backfill")
      lagging = [{ kind: "worker", hostname: "host-a", pid: 41, revision: "rev-a" }]
      allow(Pgbus::Deploy::Rollover).to receive(:check).and_return(status(:lagging, lagging: lagging))
      messages = []
      allow(Pgbus.logger).to receive(:info) { |&blk| messages << blk.call }

      runner.tick(now)
      runner.tick(now + 10)

      expect(adapter.jobs).to be_empty
      expect(messages.grep(/waiting for rollover/).size).to eq(1)
      expect(messages.join).to include("1 process(es)", "worker@host-a pid 41 (rev-a)")
    end

    it "checks nothing when no enabled task is armed" do
      allow(Pgbus::DeployTask).to receive(:pending?).and_return(false)

      runner.tick(now)

      expect(Pgbus::Deploy::Rollover).not_to have_received(:check)
    end

    it "checks the gate at most every ROLLOVER_CHECK_INTERVAL seconds" do
      row("backfill", claim: false)

      3.times { runner.tick(now) }

      expect(Pgbus::Deploy::Rollover).to have_received(:check).once
    end

    it "skips a disabled task" do
      row("backfill", enabled: false)

      runner.tick(now)

      expect(adapter.jobs).to be_empty
    end

    it "skips a task that is not armed" do
      row("backfill", armed: false)

      runner.tick(now)

      expect(adapter.jobs).to be_empty
    end

    it "enqueues a command task through CommandJob" do
      config.deploy_tasks = { "warm" => { "command" => "Cache.warm!", "queue" => "low" } }
      row("warm")

      runner.tick(now)

      expect(adapter.jobs.sole).to have_attributes(class: Pgbus::Recurring::CommandJob, queue_name: "low",
                                                   arguments: ["Cache.warm!"])
    end

    describe "retry backoff after a failed enqueue" do
      let(:clock) { [1_000.0] }

      before do
        allow(runner).to receive(:monotonic_now) { clock.first }
        job_class.before_enqueue { throw :abort }
      end

      def advance(seconds) = clock[0] += seconds

      it "does not retry a task inside its backoff window" do
        backfill = row("backfill")

        runner.tick(now)
        advance(1)
        runner.tick(now + 1)

        expect(backfill).to have_received(:claim!).once
        expect(Pgbus::ErrorReporter).to have_received(:report).once
      end

      it "retries once the window has passed, doubling it up to the cap" do
        backfill = row("backfill")

        runner.tick(now)
        advance(described_class::RETRY_BACKOFF_BASE)
        runner.tick(now)
        advance(described_class::RETRY_BACKOFF_BASE)
        runner.tick(now) # inside the doubled window
        advance(described_class::RETRY_BACKOFF_BASE)
        runner.tick(now)

        expect(backfill).to have_received(:claim!).exactly(3).times
        expect(runner.send(:retry_delay, 20)).to eq(described_class::RETRY_BACKOFF_MAX)
      end

      it "forgets the backoff once the task enqueues" do
        backfill = row("backfill")
        runner.tick(now)
        job_class.reset_callbacks(:enqueue)
        advance(described_class::RETRY_BACKOFF_BASE)

        runner.tick(now)

        expect(adapter.jobs.size).to eq(1)
        expect(runner.send(:backing_off?, "backfill")).to be(false)
        expect(backfill).to have_received(:claim!).twice
      end
    end

    it "re-arms and reports when perform_later raises, then carries on with the next task" do
      config.deploy_tasks = { "a" => { "class" => "DeployBackfillJob" }, "b" => { "command" => "B.run" } }
      first = row("a")
      row("b")
      allow(adapter).to receive(:enqueue).and_wrap_original do |original, job|
        raise Pgbus::JobNotUnique, "already locked" if job.is_a?(job_class)

        original.call(job)
      end

      runner.tick(now)

      expect(first).to have_received(:rearm!).with(armed_for: "1", now: kind_of(Time))
      expect(Pgbus::ErrorReporter).to have_received(:report).with(kind_of(Pgbus::JobNotUnique), hash_including(task: "a"))
      expect(adapter.jobs.map(&:class)).to eq([Pgbus::Recurring::CommandJob])
      expect(runs.map { |r| r[:task_key] }).to eq(["b"])
    end

    it "re-arms when a before_enqueue callback aborts (perform_later returns false)" do
      backfill = row("backfill")
      job_class.before_enqueue { throw :abort }

      runner.tick(now)

      expect(backfill).to have_received(:rearm!)
      expect(runs).to be_empty
      expect(Pgbus::ErrorReporter).to have_received(:report).with(kind_of(Pgbus::EnqueueError), hash_including(task: "backfill"))
    end

    it "re-arms when the adapter reports an enqueue error" do
      backfill = row("backfill")
      allow(adapter).to receive(:enqueue).and_raise(ActiveJob::EnqueueError, "pgmq unreachable")

      runner.tick(now)

      expect(backfill).to have_received(:rearm!)
      expect(Pgbus::ErrorReporter).to have_received(:report) do |error, _context|
        expect(error.message).to include("pgmq unreachable")
      end
    end

    it "enqueues nothing when another scheduler won the claim" do
      row("backfill", claim: false)

      runner.tick(now)

      expect(adapter.jobs).to be_empty
      expect(runs).to be_empty
    end

    it "does not fire an arm for another version that it does not owe" do
      backfill = row("backfill", armed_for: "2", claim: false)

      runner.tick(now)

      expect(backfill).to have_received(:claim!).with(armed_for: "1", now: now)
      expect(adapter.jobs).to be_empty
    end

    it "takes over an arm for another version when its own version is owed" do
      backfill = row("backfill", armed_for: "2")
      allow(backfill).to receive(:take_over!).and_return(true)
      allow(Pgbus::DeployTask).to receive(:owed?).and_return(true)

      runner.tick(now)

      expect(backfill).to have_received(:take_over!).with(armed_for: "1", now: now)
      expect(adapter.jobs.size).to eq(1)
    end

    it "fires its own declared version when the stored row's version differs" do
      config.deploy_tasks = { "backfill" => { "class" => "DeployBackfillJob", "version" => 3 } }
      row("backfill", armed_for: "3", version: "2")

      runner.tick(now)

      expect(runs.sole).to include(version: "3")
    end

    it "logs and reports a run-row insert failure after a successful enqueue" do
      row("backfill")
      allow(Pgbus::DeployTaskRun).to receive(:create!).and_raise(StandardError, "insert failed")

      runner.tick(now)

      expect(adapter.jobs.size).to eq(1)
      expect(Pgbus::ErrorReporter).to have_received(:report).with(kind_of(StandardError), hash_including(task: "backfill"))
    end

    it "emits pgbus.deploy_task.enqueue" do
      row("backfill")
      allow(Pgbus::Instrumentation).to receive(:instrument).and_call_original

      runner.tick(now)

      expect(Pgbus::Instrumentation).to have_received(:instrument).with(
        "pgbus.deploy_task.enqueue", task: "backfill", version: "1", revision: "rev-b", triggered_by: "rollover"
      )
    end
  end

  describe "#enqueue" do
    let(:task) { runner.tasks.first }

    it "ignores the gate and the claim and records a manual run" do
      allow(Pgbus::Deploy::Rollover).to receive(:check).and_return(status(:lagging))

      expect(runner.enqueue(task, triggered_by: "manual", now: now)).to eq(:enqueued)
      expect(adapter.jobs.size).to eq(1)
      expect(runs.sole).to include(triggered_by: "manual")
      expect(Pgbus::Deploy::Rollover).not_to have_received(:check)
    end

    it "returns :failed when the job was not enqueued" do
      job_class.before_enqueue { throw :abort }

      expect(runner.enqueue(task, triggered_by: "manual", now: now)).to eq(:failed)
      expect(runs).to be_empty
    end

    it "returns :failed for a class that no longer resolves" do
      missing = Pgbus::Deploy::Task.from_configuration("gone", class: "NoSuchDeployJob")

      expect(runner.enqueue(missing, triggered_by: "manual", now: now)).to eq(:failed)
    end
  end

  describe "#task_count" do
    it "reports the declared task count" do
      expect(runner.task_count).to eq(1)
    end
  end
end
