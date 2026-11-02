# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# ensures_uniqueness strategy: :until_executing — a debounce (issue #628).
# The lock is taken at enqueue, so while a copy for the key is queued every
# further enqueue is dropped; the executor releases it before perform, so an
# enqueue that arrives while the copy runs is accepted and one trailing run
# observes what was written during the current one.
RSpec.describe "Uniqueness :until_executing (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:key) { "rollup-42" }
  # What each run saw: the children finished when it started, and whether
  # its key was still locked then.
  let(:runs) { [] }
  let(:children) { (1..9).to_a }

  before do
    client.ensure_queue("default")
    Pgbus::UniquenessKey.delete_all
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    ActiveJob::Base.queue_adapter = :pgbus
  end

  after do
    ActiveJob::Base.queue_adapter = :test
  end

  # during_run is called inside the first run, after it read the children:
  # it stands in for whatever finishes while the rollup is in flight.
  def define_job(concurrency: nil, on_conflict: :discard, &during_run)
    seen = runs
    state = children
    lock_key = key
    stub_const("RollupJob", Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness
      include Pgbus::Concurrency

      ensures_uniqueness strategy: :until_executing, on_conflict: on_conflict, key: ->(id) { "rollup-#{id}" }
      limits_concurrency(**concurrency) if concurrency

      define_method(:perform) do |_parent_id|
        seen << { children: state.dup, locked: Pgbus::UniquenessKey.locked?(lock_key) }
        during_run&.call if seen.size == 1
      end
    end)
  end

  def read_all
    client.read_batch("default", qty: 10, vt: 30)
  end

  def read_one
    messages = read_all
    expect(messages.size).to eq(1)
    messages.first
  end

  # Every message carrying the key, delayed ones included.
  def queued_msg_ids_for_key
    physical = Pgbus.configuration.queue_name("default")
    ActiveRecord::Base.connection.select_values(
      "SELECT msg_id FROM pgmq.q_#{physical} WHERE message->>'pgbus_uniqueness_key' = $1 ORDER BY msg_id",
      "spec", [key]
    ).map(&:to_i)
  end

  def lock_row
    Pgbus::UniquenessKey.find_by(lock_key: key)
  end

  it "(a) sends one message for a burst of five enqueues while none runs" do
    define_job

    5.times { RollupJob.perform_later(42) }

    expect(queued_msg_ids_for_key.size).to eq(1)
    expect(lock_row.msg_id).to eq(queued_msg_ids_for_key.first)
  end

  it "(b) accepts an enqueue during a running perform, and that trailing run sees the state written before it" do
    define_job do
      children << 10
      RollupJob.perform_later(42)
    end
    RollupJob.perform_later(42)

    expect(executor.execute(read_one, "default")).to eq(:success)

    expect(queued_msg_ids_for_key.size).to eq(1)
    expect(lock_row.msg_id).to eq(queued_msg_ids_for_key.first)

    expect(executor.execute(read_one, "default")).to eq(:success)

    expect(runs).to eq([{ children: (1..9).to_a, locked: false }, { children: (1..10).to_a, locked: false }])
    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    expect(read_all).to be_empty
  end

  it "(c) a message whose worker died after the read keeps the key, and its redelivery releases it and runs" do
    define_job
    RollupJob.perform_later(42)
    crashed = read_one # read, then the worker dies before the start-release

    RollupJob.perform_later(42)
    expect(queued_msg_ids_for_key).to eq([crashed.msg_id.to_i])
    expect(lock_row.msg_id).to eq(crashed.msg_id.to_i)

    client.set_visibility_timeout("default", crashed.msg_id.to_i, vt: 0) # VT expiry
    redelivered = read_one
    expect(redelivered.read_ct.to_i).to eq(2)

    expect(executor.execute(redelivered, "default")).to eq(:success)

    expect(runs).to eq([{ children: (1..9).to_a, locked: false }])
    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
  end

  it "(d) with limits_concurrency to: 1, :block, a run-time enqueue parks, a third is dropped, the parked one runs after" do
    define_job(concurrency: { to: 1, key: ->(id) { "rollup-slot-#{id}" }, on_conflict: :block }) do
      children << 10
      RollupJob.perform_later(42) # parks behind the running copy's slot, takes the free key
      expect(Pgbus::BlockedExecution.count).to eq(1)
      RollupJob.perform_later(42) # dropped by the key
    end
    RollupJob.perform_later(42)

    expect(executor.execute(read_one, "default")).to eq(:success)

    # The first run archived and signalled: the parked copy was promoted,
    # the third enqueue never existed.
    expect(Pgbus::BlockedExecution.count).to eq(0)
    expect(queued_msg_ids_for_key.size).to eq(1)
    expect(Pgbus::UniquenessKey.locked?(key)).to be(true)

    expect(executor.execute(read_one, "default")).to eq(:success)

    # The promoted copy ran on an unbound row; its start-release took it.
    expect(runs).to eq([{ children: (1..9).to_a, locked: false }, { children: (1..10).to_a, locked: false }])
    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    expect(read_all).to be_empty
  end

  describe "(e) a recurring task" do
    let(:config) { Pgbus.configuration }
    let(:schedule) { Pgbus::Recurring::Schedule.new(config: config) }
    let(:recurring_key) { "NightlyRollupJob" }

    before do
      seen = runs
      lock_key = recurring_key
      stub_const("NightlyRollupJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executing

        define_method(:perform) { seen << { locked: Pgbus::UniquenessKey.locked?(lock_key) } }
      end)
      config.recurring_tasks = { "nightly_rollup" => { "class" => "NightlyRollupJob", "schedule" => "0 2 * * *" } }
    end

    after { config.recurring_tasks = nil }

    it "takes the lock, carries key + strategy, skips a tick while queued, and releases when the copy starts" do
      task = schedule.tasks.first
      schedule.enqueue_task(task, run_at: Time.utc(2026, 11, 2, 2, 0, 0))
      schedule.enqueue_task(task, run_at: Time.utc(2026, 11, 3, 2, 0, 0))

      message = read_one
      payload = JSON.parse(message.message)
      expect(payload).to include(Pgbus::Uniqueness::METADATA_KEY => recurring_key,
                                 Pgbus::Uniqueness::STRATEGY_KEY => "until_executing")
      expect(Pgbus::UniquenessKey.find_by(lock_key: recurring_key).msg_id).to eq(message.msg_id.to_i)

      expect(executor.execute(message, "default")).to eq(:success)

      expect(runs).to eq([{ locked: false }])
      expect(Pgbus::UniquenessKey.locked?(recurring_key)).to be(false)
    end
  end

  describe "(f) the orphan-lock reaper" do
    let(:dispatcher) { Pgbus::Process::Dispatcher.new }

    def backdate_lock
      Pgbus::UniquenessKey.where(lock_key: key).update_all(created_at: 10.minutes.ago)
    end

    def reap
      dispatcher.send(:reap_orphaned_uniqueness_keys)
    end

    before { define_job }

    it "keeps the row of a delayed copy, bound or not, and drops it once the message is gone" do
      RollupJob.set(wait: 300).perform_later(42)
      msg_id = queued_msg_ids_for_key.sole
      backdate_lock

      expect(reap).to eq(0)
      expect(lock_row.msg_id).to eq(msg_id)

      # A bind that failed leaves the row unbound: the payload scan keeps it.
      Pgbus::UniquenessKey.where(lock_key: key).update_all(msg_id: 0)
      expect(reap).to eq(0)
      expect(Pgbus::UniquenessKey.locked?(key)).to be(true)

      client.delete_message("default", msg_id)

      expect(reap).to eq(1)
      expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    end
  end
end
