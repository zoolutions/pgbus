# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Recurring::Schedule do
  let(:config) { Pgbus::Configuration.new }
  let(:mock_client) { instance_double(Pgbus::Client) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:ensure_queue)
    allow(mock_client).to receive(:send_message)
    config.recurring_tasks = {
      "daily_cleanup" => {
        "class" => "CleanupJob",
        "schedule" => "0 2 * * *",
        "queue" => "maintenance"
      },
      "hourly_sync" => {
        "class" => "SyncJob",
        "schedule" => "0 * * * *"
      }
    }
  end

  describe "#initialize" do
    it "loads tasks from configuration" do
      schedule = described_class.new(config: config)
      expect(schedule.tasks.size).to eq(2)
      expect(schedule.tasks.map(&:key)).to contain_exactly("daily_cleanup", "hourly_sync")
    end

    it "filters out invalid tasks with logging" do
      config.recurring_tasks = {
        "valid" => { "class" => "MyJob", "schedule" => "0 * * * *" },
        "invalid" => { "class" => "MyJob", "schedule" => nil }
      }

      schedule = described_class.new(config: config)
      expect(schedule.tasks.size).to eq(1)
      expect(schedule.tasks.first.key).to eq("valid")
    end

    it "handles empty recurring tasks" do
      config.recurring_tasks = {}
      schedule = described_class.new(config: config)
      expect(schedule.tasks).to be_empty
    end

    it "handles nil recurring tasks" do
      config.recurring_tasks = nil
      schedule = described_class.new(config: config)
      expect(schedule.tasks).to be_empty
    end
  end

  describe "#due_tasks" do
    it "returns [task, run_at] pairs for due tasks" do
      schedule = described_class.new(config: config)

      # Both tasks should have some next_time
      due = schedule.due_tasks(Time.now + 7200) # 2 hours from now
      expect(due).to be_an(Array)
      due.each do |entry|
        expect(entry).to be_an(Array)
        expect(entry.size).to eq(2)
        expect(entry.first).to be_a(Pgbus::Recurring::Task)
        expect(entry.last).to be_a(Time)
      end
    end

    it "returns empty array when no tasks are due" do
      # Use a once-a-year schedule so we can easily find a non-matching time
      config.recurring_tasks = {
        "yearly" => {
          "class" => "YearlyJob",
          "schedule" => "0 0 1 1 *" # Jan 1st midnight only
        }
      }
      schedule = described_class.new(config: config)

      # June 15 at noon — nowhere near Jan 1 midnight
      due = schedule.due_tasks(Time.utc(2026, 6, 15, 12, 30, 0))
      expect(due).to be_empty
    end
  end

  describe "#enqueue_task" do
    let(:schedule) { described_class.new(config: config) }
    let(:task) { schedule.tasks.first }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }

    before do
      # Mock the RecurringExecution to allow recording
      allow(Pgbus::RecurringExecution).to receive(:record).and_yield
    end

    it "sends a message through the client" do
      schedule.enqueue_task(task, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        anything,
        hash_including("job_class" => "CleanupJob"),
        headers: hash_including("pgbus.recurring_key" => "daily_cleanup")
      )
    end

    it "uses the task's configured queue" do
      schedule.enqueue_task(task, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        "maintenance",
        anything,
        headers: anything
      )
    end

    it "uses default queue when task has no queue" do
      task_no_queue = schedule.tasks.find { |t| t.key == "hourly_sync" }

      schedule.enqueue_task(task_no_queue, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        "default",
        anything,
        headers: anything
      )
    end

    it "records the execution for deduplication" do
      allow(Pgbus::RecurringExecution).to receive(:record)
        .with("daily_cleanup", run_at)
        .and_yield

      schedule.enqueue_task(task, run_at: run_at)

      expect(Pgbus::RecurringExecution).to have_received(:record)
        .with("daily_cleanup", run_at)
    end
  end

  describe "#enqueue_task uniqueness with a base-class declaration (issue #357)" do
    let(:schedule) { described_class.new(config: config) }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }

    before do
      allow(Pgbus::RecurringExecution).to receive(:record).and_yield
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
      allow(Pgbus::UniquenessKey).to receive(:bind!)
      allow(mock_client).to receive(:send_message).and_return(11)

      stub_const("MaintenanceBaseJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, on_conflict: :discard
      end)
      stub_const("NightlySweepJob", Class.new(MaintenanceBaseJob))

      config.recurring_tasks = {
        "nightly_sweep" => {
          "class" => "NightlySweepJob",
          "schedule" => "0 2 * * *"
        }
      }
    end

    it "acquires the lock with the SUBCLASS name as the default key" do
      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(Pgbus::UniquenessKey).to have_received(:acquire!)
        .with("NightlySweepJob", queue_name: "default", msg_id: 0)
    end

    it "injects the subclass-name key into the payload metadata" do
      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        anything,
        hash_including(Pgbus::Uniqueness::METADATA_KEY => "NightlySweepJob"),
        headers: anything
      )
    end

    it "does not enqueue when execution already recorded" do
      allow(Pgbus::RecurringExecution).to receive(:record)
        .and_raise(Pgbus::Recurring::AlreadyRecorded)

      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(mock_client).not_to have_received(:send_message)
    end
  end

  describe "#enqueue_task no-key default with task arguments" do
    let(:schedule) { described_class.new(config: config) }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }

    before do
      allow(Pgbus::RecurringExecution).to receive(:record).and_yield
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
      allow(Pgbus::UniquenessKey).to receive(:bind!)
      allow(mock_client).to receive(:send_message).and_return(12)

      stub_const("SiteSyncJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, on_conflict: :discard
      end)

      config.recurring_tasks = {
        "sync_a" => { "class" => "SiteSyncJob", "schedule" => "0 2 * * *", "args" => ["site_a"] },
        "sync_b" => { "class" => "SiteSyncJob", "schedule" => "0 2 * * *", "args" => ["site_b"] }
      }
    end

    it "qualifies the default key with the task's arguments so distinct-arg tasks don't share a lock" do
      schedule.tasks.each { |task| schedule.enqueue_task(task, run_at: run_at) }

      expect(Pgbus::UniquenessKey).to have_received(:acquire!)
        .with('SiteSyncJob:["site_a"]', queue_name: "default", msg_id: 0)
      expect(Pgbus::UniquenessKey).to have_received(:acquire!)
        .with('SiteSyncJob:["site_b"]', queue_name: "default", msg_id: 0)
    end

    it "injects the same args-qualified key into the payload metadata" do
      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(mock_client).to have_received(:send_message).with(
        anything,
        hash_including(Pgbus::Uniqueness::METADATA_KEY => 'SiteSyncJob:["site_a"]'),
        headers: anything
      )
    end

    it "fails closed (skips the tick) when the default key cannot be built" do
      # Scope the stub to the task's args array: the JSON log formatter also
      # calls ::JSON.generate (with a Hash), and a blanket raise would blow up
      # the fail-closed rescue's own log line instead of testing the rescue.
      allow(JSON).to receive(:generate).and_call_original
      allow(JSON).to receive(:generate).with(["site_a"]).and_raise(StandardError, "boom")

      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(mock_client).not_to have_received(:send_message)
    end
  end

  describe "#enqueue_task with a failing user-supplied key proc" do
    let(:schedule) { described_class.new(config: config) }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }

    before do
      allow(Pgbus::RecurringExecution).to receive(:record).and_yield
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
      allow(Pgbus::ErrorReporter).to receive(:report)

      stub_const("BrokenKeyJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed,
                           key: ->(*) { raise "kaput" },
                           on_conflict: :discard
      end)

      config.recurring_tasks = {
        "broken" => { "class" => "BrokenKeyJob", "schedule" => "0 2 * * *" }
      }
    end

    it "fails closed (skips the tick) instead of enqueuing without a lock" do
      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(mock_client).not_to have_received(:send_message)
    end

    it "reports the failure through error_reporters" do
      schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

      expect(Pgbus::ErrorReporter).to have_received(:report)
        .with(kind_of(StandardError), hash_including(action: "recurring_uniqueness_lock", task: "broken"))
    end
  end

  describe "uniqueness integration" do
    let(:schedule) { described_class.new(config: config) }
    let(:task) { schedule.tasks.first }
    let(:run_at) { Time.utc(2026, 3, 31, 2, 0, 0) }
    let(:uniqueness_key_class) { stub_const("Pgbus::UniquenessKey", Class.new) }

    before do
      allow(Pgbus::RecurringExecution).to receive(:record).and_yield
      uniqueness_key_class
      allow(Pgbus::UniquenessKey).to receive_messages(acquire!: true, release!: 1, locked?: false, bind!: 1)
      allow(mock_client).to receive(:send_message).and_return(77)
    end

    context "when job class has ensures_uniqueness" do
      let(:unique_job_class) do
        Class.new do
          include Pgbus::Uniqueness

          def self.name
            "CleanupJob"
          end

          ensures_uniqueness strategy: :until_executed, on_conflict: :discard
        end
      end

      before do
        stub_const("CleanupJob", unique_job_class)
      end

      it "skips enqueue when uniqueness lock is already held" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)

        schedule.enqueue_task(task, run_at: run_at)

        expect(mock_client).not_to have_received(:send_message)
        expect(Pgbus::UniquenessKey).to have_received(:acquire!).with(
          "CleanupJob", queue_name: "maintenance", msg_id: 0
        )
      end

      it "enqueues and acquires lock when no lock is held" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)

        schedule.enqueue_task(task, run_at: run_at)

        expect(mock_client).to have_received(:send_message)
        expect(Pgbus::UniquenessKey).to have_received(:acquire!).with(
          "CleanupJob", queue_name: "maintenance", msg_id: 0
        )
      end

      it "binds the lock to the produced msg_id after send (issue #418)" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)

        schedule.enqueue_task(task, run_at: run_at)

        expect(Pgbus::UniquenessKey).to have_received(:bind!).with(
          "CleanupJob", queue_name: "maintenance", msg_id: 77
        )
      end

      it "does not bind when the uniqueness lock is already held" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)

        schedule.enqueue_task(task, run_at: run_at)

        expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
      end

      it "releases the lock when send_message raises" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
        allow(mock_client).to receive(:send_message).and_raise(StandardError, "connection refused")

        expect do
          schedule.enqueue_task(task, run_at: run_at)
        end.to raise_error(StandardError, "connection refused")

        expect(Pgbus::UniquenessKey).to have_received(:release!).with("CleanupJob")
      end

      it "releases the lock when execution already recorded" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
        allow(Pgbus::RecurringExecution).to receive(:record)
          .and_raise(Pgbus::Recurring::AlreadyRecorded)

        schedule.enqueue_task(task, run_at: run_at)

        expect(Pgbus::UniquenessKey).to have_received(:release!).with("CleanupJob")
        expect(mock_client).not_to have_received(:send_message)
      end

      it "injects uniqueness metadata into the payload" do
        allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)

        schedule.enqueue_task(task, run_at: run_at)

        expect(mock_client).to have_received(:send_message).with(
          anything,
          hash_including(
            Pgbus::Uniqueness::METADATA_KEY => "CleanupJob",
            Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
          ),
          headers: anything
        )
      end
    end

    context "when job class does not have ensures_uniqueness" do
      before do
        plain_job = Class.new do
          def self.name
            "CleanupJob"
          end
        end
        stub_const("CleanupJob", plain_job)
      end

      it "enqueues without checking locks" do
        schedule.enqueue_task(task, run_at: run_at)

        expect(mock_client).to have_received(:send_message)
      end

      it "does not inject uniqueness metadata" do
        schedule.enqueue_task(task, run_at: run_at)

        expect(mock_client).to have_received(:send_message).with(
          anything,
          hash_not_including(Pgbus::Uniqueness::METADATA_KEY),
          headers: anything
        )
      end
    end

    context "when job class cannot be constantized" do
      before do
        config.recurring_tasks = {
          "missing_class" => {
            "class" => "NonexistentJob",
            "schedule" => "0 * * * *"
          }
        }
      end

      it "enqueues without uniqueness check (fail open)" do
        schedule_with_missing = described_class.new(config: config)
        task_missing = schedule_with_missing.tasks.first
        allow(Pgbus::RecurringExecution).to receive(:record).and_yield

        schedule_with_missing.enqueue_task(task_missing, run_at: run_at)

        expect(mock_client).to have_received(:send_message)
      end
    end
  end

  describe "#build_payload" do
    let(:schedule) { described_class.new(config: config) }

    it "builds a serialized job payload for class-based tasks" do
      task = schedule.tasks.find { |t| t.key == "daily_cleanup" }
      payload = schedule.build_payload(task)

      expect(payload["job_class"]).to eq("CleanupJob")
      expect(payload["arguments"]).to eq([])
      expect(payload["queue_name"]).to eq("maintenance")
      expect(payload["priority"]).to be_nil
    end

    it "builds a command payload for command-based tasks" do
      config.recurring_tasks = {
        "cleanup_cmd" => {
          "command" => "OldRecord.cleanup!",
          "schedule" => "0 3 * * *"
        }
      }
      schedule_with_cmd = described_class.new(config: config)
      task = schedule_with_cmd.tasks.first
      payload = schedule_with_cmd.build_payload(task)

      expect(payload["job_class"]).to eq("Pgbus::Recurring::CommandJob")
      expect(payload["arguments"]).to eq(["OldRecord.cleanup!"])
    end
  end
end
