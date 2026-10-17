# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #553: a batch opened inside a database transaction, with jobs that
# defer their enqueue to after commit (ActiveJob's
# enqueue_after_transaction_commit). The batch counted nothing (the thread tag
# was gone by the time the deferred enqueue ran), finished inside the block and
# fired on_complete before a single member job existed.
RSpec.describe "Batch inside a transaction with enqueue_after_transaction_commit (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:work_queue) { "txn_batch_work" }
  let(:callback_queue) { "txn_batch_callbacks" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:batches_seen) { [] }

  let(:worker_job) { build_job("WorkerJob") }
  let(:immediate_job) { build_job("ImmediateJob", defer: false) }

  let(:callback_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "TxnBatchSpec::OnCompleteJob"
      def perform(*); end
    end
  end

  let(:counted_and_open) { { jobs_sent: 2, tagged: 2, total_jobs: 2, status: "processing", callbacks_sent: 0 } }

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("TxnBatchSpec", Module.new)
    stub_const("TxnBatchSpec::WorkerJob", worker_job)
    stub_const("TxnBatchSpec::ImmediateJob", immediate_job)
    stub_const("TxnBatchSpec::OnCompleteJob", callback_job)
    client.ensure_queue(work_queue)
    client.ensure_queue(callback_queue)
  end

  def build_job(name, defer: true, &body)
    queue = work_queue
    seen = batches_seen
    Class.new(ActiveJob::Base) do
      # The ActiveJob railtie includes this module into ActiveJob::Base in a
      # Rails app; outside Rails it has to be included by hand.
      include ActiveJob::EnqueueAfterTransactionCommit
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = :pgbus
      self.enqueue_after_transaction_commit = defer
      queue_as(queue)
      define_singleton_method(:name) { "TxnBatchSpec::#{name}" }
      define_method(:perform) { |*| seen << batch&.batch_id }
      class_eval(&body) if body
    end
  end

  def open_batch(*jobs)
    Pgbus.batch(description: "import") do |b|
      b.on_complete(callback_job)
      b.add(*jobs)
    end
  end

  def in_transaction(&)
    ActiveRecord::Base.transaction(&)
  end

  def drain(queue)
    messages = []
    while (message = client.read_message(queue, vt: 30))
      messages << JSON.parse(message.message)
      client.delete_message(queue, message.msg_id.to_i)
    end
    messages
  end

  def run_all
    while (message = client.read_message(work_queue, vt: 30))
      executor.execute(message, work_queue)
    end
  end

  def queue_depth(queue)
    physical = Pgbus.configuration.queue_name(queue)
    ActiveRecord::Base.connection.select_value("SELECT count(*) FROM pgmq.q_#{physical}").to_i
  end

  # Drains the work queue, so call it last.
  def observed(batch)
    jobs = drain(work_queue)
    batch.reload
    { jobs_sent: jobs.size, tagged: jobs.count { |job| job[Pgbus::Batch::METADATA_KEY] == batch.batch_id },
      total_jobs: batch.total_jobs, status: batch.status, callbacks_sent: drain(callback_queue).size }
  end

  it "counts the jobs b.add added and waits for them" do
    batch = in_transaction { open_batch(worker_job.new(1), worker_job.new(2)) }

    expect(observed(batch)).to eq(counted_and_open)
  end

  it "counts the same jobs when the batch is opened outside a transaction" do
    batch = open_batch(worker_job.new(1), worker_job.new(2))

    expect(observed(batch)).to eq(counted_and_open)
  end

  it "fires on_complete only after both members ran, and each member sees its batch" do
    batch = in_transaction { open_batch(worker_job.new(1), worker_job.new(2)) }
    expect(queue_depth(callback_queue)).to eq(0)

    run_all

    expect(batch.reload).to have_attributes(status: "finished", total_jobs: 2, completed_jobs: 2)
    expect(batches_seen).to eq([batch.batch_id, batch.batch_id])
    expect(drain(callback_queue).map { |job| job["job_class"] }).to eq(["TxnBatchSpec::OnCompleteJob"])
  end

  it "counts a plain perform_later and a scheduled set(wait:).perform_later" do
    batch = in_transaction do
      Pgbus.batch do |b|
        b.on_complete(callback_job)
        worker_job.perform_later(1)
        worker_job.set(wait: 10).perform_later(2)
      end
    end

    expect(batch.reload).to have_attributes(total_jobs: 2, status: "processing")
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(2)
    expect(queue_depth(callback_queue)).to eq(0)
  end

  it "counts jobs that take the individual enqueue path (uniqueness configured)" do
    unique_job = build_job("UniqueJob") do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :until_executed, key: ->(id) { "txn-batch-#{id}" }
    end
    stub_const("TxnBatchSpec::UniqueJob", unique_job)

    batch = in_transaction { open_batch(unique_job.new(1), unique_job.new(2)) }

    expect(observed(batch)).to eq(counted_and_open)
  end

  it "counts deferring and immediate jobs added together" do
    batch = in_transaction { open_batch(worker_job.new(1), immediate_job.new(2)) }

    expect(observed(batch)).to eq(counted_and_open)
  end

  it "starts processing only after the outermost commit when the transactions are nested" do
    batch = nil
    status_after_inner_commit = nil
    in_transaction do
      ActiveRecord::Base.transaction(requires_new: true) { batch = open_batch(worker_job.new(1)) }
      status_after_inner_commit = batch.reload.status
    end

    expect(status_after_inner_commit).to eq("pending")
    expect(observed(batch)).to eq(counted_and_open.merge(jobs_sent: 1, tagged: 1, total_jobs: 1))
  end

  it "sends nothing and leaves no batch behind when the transaction rolls back" do
    batch = nil
    in_transaction do
      batch = Pgbus.batch do |b|
        b.on_complete(callback_job)
        b.add(worker_job.new(1))
        worker_job.perform_later(2)
      end
      raise ActiveRecord::Rollback
    end

    expect(batch.reload.status).to be_nil
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    expect(queue_depth(work_queue)).to eq(0)
    expect(queue_depth(callback_queue)).to eq(0)
  end

  it "uncounts a deferring job discarded at enqueue and still finishes once the rest ran" do
    discarding_job = build_job("DiscardingJob") do
      include Pgbus::Uniqueness

      ensures_uniqueness strategy: :until_executed, key: ->(*) { "txn-batch-dup" }, on_conflict: :discard
    end
    stub_const("TxnBatchSpec::DiscardingJob", discarding_job)

    # Same key twice: the second is discarded when the deferred enqueue runs.
    batch = in_transaction { open_batch(discarding_job.new(1), discarding_job.new(2)) }
    expect(batch.reload).to have_attributes(total_jobs: 1, status: "processing")

    run_all

    expect(batch.reload).to have_attributes(status: "finished", total_jobs: 1, completed_jobs: 1)
    expect(drain(callback_queue).size).to eq(1)
  end

  describe "batch.add on an open batch inside a transaction" do
    it "counts the job at commit and keeps the batch open until it ran" do
      batch = open_batch(worker_job.new(1))

      in_transaction do
        batch.add(worker_job.new(2))
        expect(batch.reload).to have_attributes(total_jobs: 1, status: "processing")
      end

      expect(batch.reload).to have_attributes(total_jobs: 2, status: "processing")
      run_all
      expect(batch.reload).to have_attributes(status: "finished", completed_jobs: 2)
      expect(drain(callback_queue).size).to eq(1)
    end

    it "lets a member add a sibling from inside its own transaction" do
      sibling_job = worker_job
      parent_job = build_job("ParentJob") do
        define_method(:perform) { |*| ActiveRecord::Base.transaction { batch.add(sibling_job.new(2)) } }
      end
      stub_const("TxnBatchSpec::ParentJob", parent_job)
      batch = open_batch(parent_job.new(1))

      run_all

      expect(batch.reload).to have_attributes(status: "finished", total_jobs: 2, completed_jobs: 2)
      expect(batches_seen).to eq([batch.batch_id])
      expect(drain(callback_queue).size).to eq(1)
    end

    it "raises AlreadyFinished only when the batch has really finished" do
      batch = open_batch(worker_job.new(1))
      run_all
      drain(callback_queue)

      expect { in_transaction { batch.add(worker_job.new(2)) } }.to raise_error(Pgbus::Batch::AlreadyFinished)
      expect(queue_depth(work_queue)).to eq(0)
    end
  end

  it "keeps the retry path: a retry_on re-enqueue of a deferring member re-joins without a second count" do
    attempts = []
    flaky_job = build_job("FlakyJob") do
      retry_on RuntimeError, attempts: 3, wait: 0
      define_method(:perform) do |*|
        attempts << executions
        raise "boom" if attempts.size == 1
      end
    end
    stub_const("TxnBatchSpec::FlakyJob", flaky_job)

    batch = in_transaction { open_batch(flaky_job.new) }
    run_all

    expect(attempts).to eq([1, 2])
    expect(batch.reload).to have_attributes(status: "finished", total_jobs: 1, completed_jobs: 1)
    expect(drain(callback_queue).size).to eq(1)
  end
end
