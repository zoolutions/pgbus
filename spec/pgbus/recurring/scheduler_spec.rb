# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Recurring::Scheduler do
  let(:config) { Pgbus::Configuration.new }
  let(:mock_client) { instance_double(Pgbus::Client) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:ensure_queue)
    allow(mock_client).to receive(:send_message)
    config.recurring_tasks = {
      "every_minute" => {
        "class" => "MinuteJob",
        "schedule" => "* * * * *"
      }
    }
  end

  describe "#initialize" do
    it "creates a scheduler with a schedule" do
      scheduler = described_class.new(config: config)
      expect(scheduler.schedule).to be_a(Pgbus::Recurring::Schedule)
      expect(scheduler.schedule.tasks.size).to eq(1)
    end
  end

  describe "#tick" do
    it "enqueues due tasks" do
      scheduler = described_class.new(config: config)

      allow(Pgbus::RecurringExecution).to receive(:record) do |_key, _time, &block|
        block&.call
      end

      # Use a time that's exactly on a minute boundary — the task is "every minute",
      # and cron.match? returns true at any :00 second boundary
      now = Time.utc(2026, 3, 31, 12, 1, 0)
      scheduler.tick(now)

      expect(mock_client).to have_received(:send_message).at_least(:once)
    end

    it "does not enqueue tasks that are not due" do
      config.recurring_tasks = {
        "future_task" => {
          "class" => "FutureJob",
          "schedule" => "0 0 1 1 *" # Jan 1st only
        }
      }
      scheduler = described_class.new(config: config)

      # Use a time that's not Jan 1st midnight — task won't be due
      scheduler.tick(Time.utc(2026, 6, 15, 12, 0, 30))

      expect(mock_client).not_to have_received(:send_message)
    end

    it "handles errors during enqueue gracefully" do
      scheduler = described_class.new(config: config)

      tasks_with_run_at = scheduler.schedule.tasks.map { |t| [t, Time.utc(2026, 3, 31, 12, 1, 0)] }
      allow(scheduler.schedule).to receive(:due_tasks).and_return(tasks_with_run_at)
      allow(scheduler.schedule).to receive(:enqueue_task).and_raise(StandardError, "DB connection lost")

      # Should not raise
      expect { scheduler.tick(Time.utc(2026, 3, 31, 12, 1, 0)) }.not_to raise_error
    end

    it "tracks last_run_at per task" do
      scheduler = described_class.new(config: config)
      allow(Pgbus::RecurringExecution).to receive(:record) do |_key, _time, &block|
        block&.call
      end

      now = Time.utc(2026, 3, 31, 12, 1, 0)
      scheduler.tick(now)

      expect(scheduler.last_run_at("every_minute")).not_to be_nil
    end
  end

  describe "sync_recurring_tasks (private)" do
    it "syncs recurring tasks from config on startup" do
      scheduler = described_class.new(config: config)
      allow(Pgbus::RecurringTask).to receive(:sync_from_config!)

      scheduler.send(:sync_recurring_tasks)

      expect(Pgbus::RecurringTask).to have_received(:sync_from_config!).with(config.recurring_tasks)
    end

    it "skips sync when no recurring tasks configured" do
      config.recurring_tasks = nil
      scheduler = described_class.new(config: config)
      allow(Pgbus::RecurringTask).to receive(:sync_from_config!)

      scheduler.send(:sync_recurring_tasks)

      expect(Pgbus::RecurringTask).not_to have_received(:sync_from_config!)
    end

    it "syncs empty hash to remove stale tasks" do
      config.recurring_tasks = {}
      scheduler = described_class.new(config: config)
      allow(Pgbus::RecurringTask).to receive(:sync_from_config!)

      scheduler.send(:sync_recurring_tasks)

      expect(Pgbus::RecurringTask).to have_received(:sync_from_config!).with({})
    end

    it "handles errors gracefully" do
      scheduler = described_class.new(config: config)
      allow(Pgbus::RecurringTask).to receive(:sync_from_config!).and_raise(StandardError, "DB error")

      expect { scheduler.send(:sync_recurring_tasks) }.not_to raise_error
    end
  end

  describe "#task_statuses" do
    it "returns status information for all tasks" do
      scheduler = described_class.new(config: config)

      statuses = scheduler.task_statuses
      expect(statuses).to be_an(Array)
      expect(statuses.size).to eq(1)

      status = statuses.first
      expect(status[:key]).to eq("every_minute")
      expect(status[:class_name]).to eq("MinuteJob")
      expect(status[:schedule]).to eq("* * * * *")
      expect(status[:next_run_at]).to be_a(Time)
      expect(status[:last_run_at]).to be_nil
    end
  end

  describe "#start_heartbeat (private)" do
    let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }

    before { allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat) }

    it "creates and starts a heartbeat with a loop_tick_supplier" do
      scheduler = described_class.new(config: config)
      scheduler.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new).with(
        kind: "scheduler",
        metadata: { pid: Process.pid, tasks: 1, started_at: a_value_within(5).of(Time.now.to_f),
                    supervisor_pid: Process.ppid },
        loop_tick_supplier: kind_of(Proc)
      )
      expect(heartbeat).to have_received(:start)
    end

    it "supplies the latest stamped loop tick to the heartbeat" do
      supplier = nil
      allow(Pgbus::Process::Heartbeat).to receive(:new) do |**kwargs|
        supplier = kwargs[:loop_tick_supplier]
        heartbeat
      end

      scheduler = described_class.new(config: config)
      scheduler.send(:start_heartbeat)
      expect(supplier.call).to be_nil

      scheduler.send(:stamp_loop_tick)
      expect(supplier.call).to be_within(1).of(Time.now.to_f)
    end
  end

  describe "#stamp_loop_tick (private)" do
    it "advances the beacon on each call" do
      scheduler = described_class.new(config: config)
      scheduler.send(:stamp_loop_tick)
      beacon = scheduler.instance_variable_get(:@loop_tick_at).get
      expect(beacon).to be_within(1).of(Time.now.to_f)
    end
  end
end
