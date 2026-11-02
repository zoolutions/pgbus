# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# Issue #629: the executor takes a :while_executing lock only when the payload
# carries the key. The scheduler used to inject none for this strategy, so a
# recurring run that outlasted its interval overlapped itself unprotected. It
# still takes no lock at schedule time (the strategy allows duplicate
# enqueues), but carries key, strategy and on_conflict for the executor.
RSpec.describe "Recurring scheduler with :while_executing strategy (integration)", :integration do
  let(:config) { Pgbus.configuration }
  let(:schedule) { Pgbus::Recurring::Schedule.new(config: config) }
  let(:run_at) { Time.utc(2026, 4, 1, 2, 0, 0) }
  let(:client) { Pgbus.client }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:started) { Queue.new }
  let(:finish) { Queue.new }
  let(:runs) { Concurrent::Array.new }

  before do
    started_q = started
    finish_q = finish
    runs_log = runs
    stub_const("WhileExecutingJob", Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :while_executing

      # The first run blocks until the example lets it finish, so the second
      # run is read while the first still executes.
      define_method(:perform) do
        runs_log << :start
        if runs_log.count(:start) == 1
          started_q << true
          finish_q.pop
        end
        runs_log << :finish
      end
    end)

    config.recurring_tasks = {
      "while_exec_task" => {
        "class" => "WhileExecutingJob",
        "schedule" => "*/5 * * * *",
        "queue" => "default"
      }
    }
  end

  it "takes no lock at schedule time, and allows a second run's enqueue" do
    task = schedule.tasks.first

    schedule.enqueue_task(task, run_at: run_at)
    schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 1, 2, 5, 0))

    expect(Pgbus::UniquenessKey.count).to eq(0)
    expect(client.read_batch("default", qty: 10, vt: 0).size).to eq(2)
  end

  it "carries key, strategy and on_conflict in the payload" do
    schedule.enqueue_task(schedule.tasks.first, run_at: run_at)

    payload = JSON.parse(client.read_batch("default", qty: 1).first.message)
    expect(payload).to include(
      "job_class" => "WhileExecutingJob",
      Pgbus::Uniqueness::METADATA_KEY => "WhileExecutingJob",
      Pgbus::Uniqueness::STRATEGY_KEY => "while_executing",
      Pgbus::Uniqueness::ON_CONFLICT_KEY => "reject"
    )
  end

  it "runs two overlapping runs one at a time, and holds no row between them" do
    task = schedule.tasks.first
    schedule.enqueue_task(task, run_at: run_at)
    schedule.enqueue_task(task, run_at: Time.utc(2026, 4, 1, 2, 5, 0))
    first, second = client.read_batch("default", qty: 10, vt: 30).sort_by { |m| m.msg_id.to_i }

    holder = Thread.new { executor.execute(first, "default") }
    # Bounded: an execute that fails before perform never signals, and the
    # example must fail rather than hang the run.
    expect(started.pop(timeout: 30)).to be(true), "first run never started (holder: #{holder.join(0)&.value.inspect})"

    expect(Pgbus::UniquenessKey.locked?("WhileExecutingJob")).to be(true)
    expect(executor.execute(second, "default")).to eq(:skipped)

    finish << true
    expect(holder.value).to eq(:success)
    expect(Pgbus::UniquenessKey.count).to eq(0)

    client.set_visibility_timeout("default", second.msg_id.to_i, vt: 0)
    expect(executor.execute(client.read_message("default", vt: 30), "default")).to eq(:success)

    expect(runs).to eq(%i[start finish start finish])
    expect(Pgbus::UniquenessKey.count).to eq(0)
  ensure
    finish << true if holder&.alive?
  end
end
