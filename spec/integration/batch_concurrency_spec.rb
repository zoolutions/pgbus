# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# A batch whose children are concurrency-limited with `on_conflict: :block`,
# driven end to end by the real executor: children park at enqueue, stay
# counted, are promoted one at a time as each predecessor finishes, get their
# execution rows backfilled on promotion, and the batch finishes only after
# the last one. The stalled-batch sweep must never mistake a parked child for
# an orphan — the double-encoded parked payload used to make exactly that
# happen once the stall threshold passed.
RSpec.describe "Batch + limits_concurrency :block (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "batch_limited_work" }
  let(:callback_queue) { "batch_limited_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:limited_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      self.queue_adapter = :pgbus
      queue_as(queue)
      limits_concurrency to: 1, key: ->(*) { "batch-limited" }, on_conflict: :block
      def self.name = "BatchConcurrencySpec::LimitedJob"
      def perform(*); end
    end
  end

  let(:on_finish_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchConcurrencySpec::OnFinishJob"
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("BatchConcurrencySpec", Module.new)
    stub_const("BatchConcurrencySpec::LimitedJob", limited_job)
    stub_const("BatchConcurrencySpec::OnFinishJob", on_finish_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
  end

  def parked_count
    Pgbus::BlockedExecution.where(concurrency_key: "batch-limited").count
  end

  # Drive the work queue the way a worker would, one message at a time, and
  # return how many messages were ever visible together (the concurrency the
  # limit actually allowed).
  def drain_work_queue
    executed = 0
    max_visible = 0
    loop do
      visible = Array(client.read_batch(work_queue, qty: 10, vt: 0))
      max_visible = [max_visible, visible.size].max
      message = client.read_message(work_queue, vt: 30)
      break unless message

      expect(executor.execute(message, work_queue)).to eq(:success)
      executed += 1
      raise "runaway drain" if executed > 10
    end
    [executed, max_visible]
  end

  it "parks the children, promotes them one at a time, and finishes the batch after the last" do
    batch = Pgbus::Batch.new(on_finish: on_finish_job)
    batch.enqueue { 3.times { limited_job.perform_later } }

    record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
    expect(record.total_jobs).to eq(3)
    expect(parked_count).to eq(2)
    rows = Pgbus::BatchExecution.where(batch_id: batch.batch_id)
    expect(rows.count).to eq(3)
    expect(rows.where(msg_id: nil).count).to eq(2)

    # The stall sweep, with no grace period at all, must leave the two parked
    # children counted: they are parked, not lost.
    Pgbus::Batch::Sweep.run(stalled_for: 0, client: client)
    expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).total_jobs).to eq(3)
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(3)

    executed, max_visible = drain_work_queue

    expect(executed).to eq(3)
    expect(max_visible).to eq(1)
    expect(parked_count).to eq(0)
    expect(Pgbus::Concurrency::Semaphore.current_value("batch-limited")).to eq(0)

    finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
    expect(finished.status).to eq("finished")
    expect(finished.completed_jobs).to eq(3)
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)

    callback = client.read_message(callback_queue, vt: 30)
    expect(callback).not_to be_nil
    expect(JSON.parse(callback.message)["job_class"]).to eq("BatchConcurrencySpec::OnFinishJob")
  end

  it "does not finish the batch while a child is still parked" do
    batch = Pgbus::Batch.new(on_finish: on_finish_job)
    batch.enqueue { 2.times { limited_job.perform_later } }

    message = client.read_message(work_queue, vt: 30)
    expect(executor.execute(message, work_queue)).to eq(:success)

    # First child done, second promoted and now visible — but not yet run.
    expect(parked_count).to eq(0)
    expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("processing")
    expect(client.read_message(callback_queue, vt: 0)).to be_nil
  end
end
