# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #564: discarding a failed job releases only the uniqueness lock its
# message holds, bounded by when that message was sent. A failed event
# records the logical queue only, and under priority routing each `_pN`
# sub-queue numbers its messages from its own sequence, so the same msg_id
# can address a different message in every sub-queue.
RSpec.describe "Failed-job discard under priority routing (integration)", :integration do
  let(:queue) { "prio_failed_locks" }
  let(:data_source) { Pgbus::Web::DataSource.new(client: Pgbus.client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:msg_id) { 9_000_001 }
  let(:key) { "prio-failed-1" }

  # drop_queue takes one physical name: under priority routing every _pN
  # sub-queue is dropped on its own, or the tables outlive the spec.
  around do |example|
    original = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client!
    example.run
  ensure
    Pgbus.client.physical_queue_names(queue).each { |name| Pgbus.client.drop_queue(name, prefixed: false) }
    Pgbus.configuration.priority_levels = original
    Pgbus.reset_client!
  end

  before do
    Pgbus::UniquenessKey.delete_all
    Pgbus.client.ensure_queue(queue)
  end

  def physical(level)
    Pgbus.client.physical_queue_names(queue).fetch(level)
  end

  # A message planted with a chosen msg_id and send time, the way two
  # sub-queues' sequences collide.
  def plant(level, lock_key, enqueued_at)
    payload = { "job_class" => "PrioJob", Pgbus::Uniqueness::METADATA_KEY => lock_key,
                Pgbus::Uniqueness::STRATEGY_KEY => "until_executed" }
    conn.exec_insert(
      "INSERT INTO pgmq.q_#{physical(level)} (msg_id, vt, enqueued_at, message) OVERRIDING SYSTEM VALUE " \
      "VALUES ($1, NOW() + interval '1 hour', $2, $3::jsonb)",
      "spec", [msg_id, enqueued_at, JSON.generate(payload)]
    )
  end

  # The failed job's message is the one planted in sub-queue 1.
  def record_failure
    Pgbus::FailedEventRecorder.record!(
      queue_name: queue, msg_id: msg_id, source_queue: physical(1),
      payload: { "job_class" => "PrioJob", Pgbus::Uniqueness::METADATA_KEY => key },
      headers: nil, error: RuntimeError.new("boom"), retry_count: 0
    )
    data_source.failed_events.find { |e| e["msg_id"].to_i == msg_id }
  end

  def lock_created_at
    conn.select_value(
      "SELECT created_at::timestamptz FROM pgbus_uniqueness_keys WHERE lock_key = $1", "spec", [key]
    )
  end

  it "takes the send time from the sub-queue whose message carries the job's key" do
    Pgbus::UniquenessKey.acquire!(key, queue_name: queue, msg_id: 0)
    acquired = lock_created_at
    plant(0, "someone-else", acquired - 60) # same msg_id, another job, sent before the lock
    plant(1, key, acquired + 1)             # the failed job, sent after its lock was taken

    data_source.discard_failed_event(record_failure["id"])

    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
  end

  it "keeps the lock when the key's earliest send in any sub-queue predates it" do
    Pgbus::UniquenessKey.acquire!(key, queue_name: queue, msg_id: 0)
    acquired = lock_created_at
    plant(0, key, acquired - 60)
    plant(1, key, acquired + 1)

    data_source.discard_failed_event(record_failure["id"])

    expect(Pgbus::UniquenessKey.locked?(key)).to be(true)
  end

  # The adapter binds the lock for the job's queue_as; the dashboard knows only
  # the _pN table it shows. Both have to land on the same spelling.
  context "with a queue whose name its table spells differently" do
    let(:hyphenated) { "prio-orders" }

    before do
      Pgbus.client.ensure_queue(hyphenated)
      stub_const("PrioOrderJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        self.queue_adapter = :pgbus
        queue_as "prio-orders"
        ensures_uniqueness strategy: :until_executed, on_conflict: :discard, key: ->(id) { "prio-order-#{id}" }

        def perform(*); end
      end)
    end

    after do
      Pgbus.client.physical_queue_names(hyphenated).each { |name| Pgbus.client.drop_queue(name, prefixed: false) }
    end

    it "frees the job's lock when its message is discarded from its priority sub-queue" do
      PrioOrderJob.set(priority: 1).perform_later(7)
      table = physical(1).sub(queue, "prio_orders")
      msg_id = conn.select_value("SELECT msg_id FROM pgmq.q_#{table} ORDER BY msg_id DESC LIMIT 1")
      expect(Pgbus::UniquenessKey.find_by(lock_key: "prio-order-7")&.queue_name).to eq("prio_orders")

      data_source.discard_job(table, msg_id)

      expect(Pgbus::UniquenessKey.locked?("prio-order-7")).to be(false)
    end
  end

  # A sub-queue the strategy names but that does not exist must not abort an
  # enclosing transaction, or the discard's own DELETE fails after it. The
  # table is moved aside, not dropped, and put back, so pgmq.meta and the
  # tables still agree for every spec that runs after this one.
  it "skips a missing sub-queue table without aborting an enclosing transaction" do
    Pgbus::UniquenessKey.acquire!(key, queue_name: queue, msg_id: 0)
    plant(1, key, lock_created_at + 1)
    event = record_failure
    conn.execute("ALTER TABLE pgmq.q_#{physical(0)} RENAME TO q_#{physical(0)}_aside")

    ActiveRecord::Base.transaction { data_source.discard_failed_event(event["id"]) }

    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    expect(data_source.failed_events.map { |e| e["id"] }).not_to include(event["id"])
  ensure
    conn.execute("ALTER TABLE IF EXISTS pgmq.q_#{physical(0)}_aside RENAME TO q_#{physical(0)}")
  end
end
