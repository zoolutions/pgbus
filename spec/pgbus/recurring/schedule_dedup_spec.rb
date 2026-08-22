# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Recurring::Schedule do # deduplication
  let(:config) { Pgbus::Configuration.new }
  let(:mock_client) { instance_double(Pgbus::Client) }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:ensure_queue)
    allow(mock_client).to receive(:send_message)
    allow(Pgbus::RecurringExecution).to receive(:record) do |_key, _time, &block|
      block&.call
    end

    config.recurring_tasks = {
      "every_minute" => {
        "class" => "MinuteJob",
        "schedule" => "* * * * *"
      }
    }
  end

  describe "#due_tasks" do
    it "returns the task at the exact cron boundary" do
      schedule = described_class.new(config: config)
      due = schedule.due_tasks(Time.utc(2026, 4, 6, 7, 5, 0))
      expect(due.size).to eq(1)
      task, run_at = due.first
      expect(task.key).to eq("every_minute")
      expect(run_at).to eq(Time.utc(2026, 4, 6, 7, 5, 0))
    end

    it "returns the task within the scheduler interval window after the boundary" do
      schedule = described_class.new(config: config)
      due = schedule.due_tasks(Time.utc(2026, 4, 6, 7, 5, 0) + 0.5)
      expect(due.size).to eq(1)
      _, run_at = due.first
      expect(run_at).to eq(Time.utc(2026, 4, 6, 7, 5, 0))
    end

    it "does NOT return the task outside the scheduler interval window" do
      schedule = described_class.new(config: config)
      due = schedule.due_tasks(Time.utc(2026, 4, 6, 7, 5, 2))
      expect(due).to be_empty
    end

    it "returns the same run_at at exact boundary and 1s after" do
      schedule = described_class.new(config: config)
      due_at_boundary = schedule.due_tasks(Time.utc(2026, 4, 6, 7, 5, 0))
      due_after = schedule.due_tasks(Time.utc(2026, 4, 6, 7, 5, 1))

      expect(due_at_boundary.size).to eq(1)
      expect(due_after.size).to eq(1)

      _, run_at_first = due_at_boundary.first
      _, run_at_second = due_after.first
      expect(run_at_first).to eq(run_at_second),
                              "Expected same run_at, got #{run_at_first.iso8601} vs #{run_at_second.iso8601}"
    end
  end

  describe "run_at consistency across ticks" do
    let(:scheduler) { Pgbus::Recurring::Scheduler.new(config: config) }

    it "produces the same run_at for ticks at and just after the cron boundary" do
      recorded_run_ats = []
      allow(Pgbus::RecurringExecution).to receive(:record) do |_key, run_at, &block|
        recorded_run_ats << run_at
        block&.call
      end

      # Tick at exact boundary: match?=true, previous_time=07:04:00
      scheduler.tick(Time.utc(2026, 4, 6, 7, 5, 0))
      # Tick 1s later: match?=false, previous_time=07:05:00 (within 1s window)
      # BUG: produces DIFFERENT run_at than first tick
      scheduler.tick(Time.utc(2026, 4, 6, 7, 5, 1))

      # Both ticks should produce the SAME run_at so RecurringExecution
      # dedup can catch the duplicate
      expect(recorded_run_ats.uniq.size).to eq(1),
                                            "Expected same run_at for both ticks, got: #{recorded_run_ats.map(&:iso8601)}"
    end

    it "does not enqueue a second message for the same cron occurrence" do
      # Track unique (task_key, run_at) pairs to simulate real DB dedup
      seen = Set.new
      allow(Pgbus::RecurringExecution).to receive(:record) do |key, run_at, &block|
        dedup_key = [key, run_at.to_i]
        raise Pgbus::Recurring::AlreadyRecorded, "already recorded" if seen.include?(dedup_key)

        seen << dedup_key
        block&.call
      end

      # Two ticks: exact boundary and 1 second later
      scheduler.tick(Time.utc(2026, 4, 6, 7, 5, 0))
      scheduler.tick(Time.utc(2026, 4, 6, 7, 5, 1))

      # Only one message should be sent
      expect(mock_client).to have_received(:send_message).once
    end
  end

  describe "uniqueness lock on AlreadyRecorded" do
    let(:unique_job_class) do
      Class.new do
        include Pgbus::Uniqueness

        def self.name
          "UniqueMinuteJob"
        end

        ensures_uniqueness strategy: :until_executed
      end
    end

    before do
      stub_const("UniqueMinuteJob", unique_job_class)
      config.recurring_tasks = {
        "unique_task" => {
          "class" => "UniqueMinuteJob",
          "schedule" => "* * * * *"
        }
      }
    end

    it "releases lock on AlreadyRecorded since prior job already completed" do
      # AlreadyRecorded + acquire! returning true means: the prior job
      # already completed (released its lock), we acquired a new one,
      # but the execution record already exists. Release our orphaned lock.
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(true)
      allow(Pgbus::UniquenessKey).to receive(:release!)
      allow(Pgbus::UniquenessKey).to receive(:bind!)
      allow(Pgbus::RecurringExecution).to receive(:record)
        .and_raise(Pgbus::Recurring::AlreadyRecorded)

      schedule = described_class.new(config: config)
      task = schedule.tasks.first
      schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 6, 7, 5, 0))

      expect(Pgbus::UniquenessKey).to have_received(:release!).with("UniqueMinuteJob")
    end

    it "skips enqueue when lock is held by a running job" do
      # acquire! returns false → :already_locked → early return, no release
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
      allow(Pgbus::UniquenessKey).to receive(:release!)

      schedule = described_class.new(config: config)
      task = schedule.tasks.first
      schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 6, 7, 5, 0))

      expect(mock_client).not_to have_received(:send_message)
      expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    end

    it "skips enqueue when lock acquisition raises (fail closed)" do
      allow(Pgbus::UniquenessKey).to receive(:acquire!)
        .and_raise(ActiveRecord::StatementInvalid, "PG::UndefinedTable")

      schedule = described_class.new(config: config)
      task = schedule.tasks.first
      schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 6, 7, 5, 0))

      expect(mock_client).not_to have_received(:send_message)
    end
  end
end
