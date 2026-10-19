# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# End-to-end coverage of batches: real jobs enqueued inside a batch, real
# per-job completion signals, and a real callback job landing on a PGMQ queue
# only after the final job completes. Previously batch_spec.rb stubbed
# BatchEntry entirely (receive_message_chain), so the finish-detection and
# callback-enqueue paths never ran against a database.
RSpec.describe "Batch flow (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:callback_queue) { "batch_callbacks" }
  let(:work_queue) { "batch_work" }

  # Worker jobs go to their own queue so reads of the callback queue only ever
  # see callback jobs — never the batch's member jobs.
  let(:worker_job) do
    queue = work_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def perform(*); end
    end
  end

  let(:on_finish_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnFinishJob"
      def perform(*); end
    end
  end

  let(:on_success_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnSuccessJob"
      def perform(*); end
    end
  end

  let(:on_discard_job) do
    queue = callback_queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue)
      def self.name = "BatchFlowSpec::OnDiscardJob"
      def perform(*); end
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("BatchFlowSpec", Module.new)
    stub_const("BatchFlowSpec::OnFinishJob", on_finish_job)
    stub_const("BatchFlowSpec::OnSuccessJob", on_success_job)
    stub_const("BatchFlowSpec::OnDiscardJob", on_discard_job)
    client.ensure_queue(callback_queue)
    client.ensure_queue(work_queue)
  end

  # Drain the callback queue and return the ActiveJob class names of every
  # message on it, deleting each as it is read so a re-read never sees it again.
  def drain_callback_job_classes
    classes = []
    while (message = client.read_message(callback_queue, vt: 30))
      classes << JSON.parse(message.message).fetch("job_class")
      client.delete_message(callback_queue, message.msg_id.to_i)
    end
    classes
  end

  # The class name of the single callback job currently on the queue, or nil.
  def next_callback_job_class
    drain_callback_job_classes.first
  end

  def enqueue_batch(**callbacks)
    batch = Pgbus::Batch.new(**callbacks)
    batch.enqueue do
      worker_job.perform_later
      worker_job.perform_later
    end
    batch
  end

  def complete_next(batch_id)
    row = Pgbus::BatchExecution.where(batch_id: batch_id).order(:id).first
    raise "no execution row for #{batch_id}" unless row

    Pgbus::Batch.job_completed(batch_id, job_id: row.job_id)
  end

  def discard_next(batch_id)
    row = Pgbus::BatchExecution.where(batch_id: batch_id).order(:id).first
    raise "no execution row for #{batch_id}" unless row

    Pgbus::Batch.job_discarded(batch_id, job_id: row.job_id)
  end

  describe "on_success path" do
    it "fires the callback exactly once, only on the final completion" do
      batch = enqueue_batch(on_finish: on_finish_job, on_success: on_success_job)

      # Two jobs enqueued means total_jobs == 2 and status == processing.
      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(2)
      expect(record.status).to eq("processing")

      # First completion: not finished yet, no callback enqueued.
      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("processing")

      # Final completion: batch finishes and callbacks enqueue.
      complete_next(batch.batch_id)
      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(finished.finished_at).not_to be_nil

      # on_finish + on_success both land on the callback queue (all succeeded).
      expect(drain_callback_job_classes).to contain_exactly(
        "BatchFlowSpec::OnFinishJob",
        "BatchFlowSpec::OnSuccessJob"
      )
    end

    it "is idempotent — an extra completion signal fires no second callback" do
      batch = enqueue_batch(on_finish: on_finish_job)

      2.times { complete_next(batch.batch_id) }
      # Drain the single expected on_finish callback.
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")

      # A stray extra completion after finish must not enqueue anything more.
      Pgbus::Batch.job_completed(batch.batch_id, job_id: "already-gone")
      expect(next_callback_job_class).to be_nil
    end
  end

  describe "bulk enqueue via perform_all_later (issue #413)" do
    it "counts bulk-enqueued jobs into the batch and tags their payloads" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      batch.enqueue do
        ActiveJob.perform_all_later([worker_job.new, worker_job.new])
      end

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.total_jobs).to eq(2)
      expect(record.status).to eq("processing")

      # Every bulk payload on the work queue carries the batch id.
      batch_ids = []
      while (message = client.read_message(work_queue, vt: 30))
        batch_ids << JSON.parse(message.message)[Pgbus::Batch::METADATA_KEY]
        client.delete_message(work_queue, message.msg_id.to_i)
      end
      expect(batch_ids).to eq([batch.batch_id, batch.batch_id])

      # The batch finishes only after BOTH bulk jobs complete.
      complete_next(batch.batch_id)
      expect(next_callback_job_class).to be_nil
      complete_next(batch.batch_id)
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end
  end

  describe "completions racing the total publish (PR #417 review)" do
    it "finishes a batch whose jobs all complete before total_jobs is published" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      batch.enqueue do
        worker_job.perform_later
        worker_job.perform_later
        # Simulate fast workers: both jobs reach their terminal state while the
        # enqueue block is still open, i.e. while total_jobs is still 0. Their
        # completion signals cannot finish the batch (1 != 0, 2 != 0), so the
        # total publish must run the completion check itself.
        complete_next(batch.batch_id)
        complete_next(batch.batch_id)
      end

      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end
  end

  describe "on_discard path" do
    it "fires on_discard instead of on_success when a job is discarded" do
      batch = enqueue_batch(on_success: on_success_job, on_discard: on_discard_job)

      # One completes, one is discarded (dead-lettered) — batch still finishes.
      complete_next(batch.batch_id)
      discard_next(batch.batch_id)

      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(finished.failed_jobs).to eq(1)
      expect(finished.discarded_jobs).to eq(1)

      expect(drain_callback_job_classes).to contain_exactly("BatchFlowSpec::OnDiscardJob")
    end
  end

  describe "crashed enqueue-block sweep (issue #414)" do
    it "finishes a batch whose block raised before any job was enqueued" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      expect { batch.enqueue { raise "boom" } }.to raise_error("boom")
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("pending")

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end

    it "leaves a pre-migration in-flight batch (no rows, unfinished counters) alone" do
      batch_id = SecureRandom.uuid
      Pgbus::BatchEntry.create!(
        batch_id: batch_id, status: "processing", total_jobs: 3,
        completed_jobs: 1, failed_jobs: 0, properties: "{}"
      )

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch_id).status).to eq("processing")
    end
  end

  describe "stalled-pending sweep keeps the counters it already has (issue #423)" do
    it "finishes a crashed pending batch whose early jobs already completed" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      expect do
        batch.enqueue do
          3.times { worker_job.perform_later }
          # Two jobs finish while the block is still open, then the block dies.
          2.times { complete_next(batch.batch_id) }
          raise "crash mid-block"
        end
      end.to raise_error("crash mid-block")

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.status).to eq("pending")
      expect(record.completed_jobs).to eq(2)
      Pgbus::BatchEntry.where(batch_id: batch.batch_id).update_all(created_at: 1.hour.ago)

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      record = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(record.status).to eq("processing")
      expect(record.total_jobs).to eq(3)

      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end

    it "leaves a pending batch alone while its block is still actively enqueuing" do
      batch = Pgbus::Batch.new
      batch.enqueue do
        worker_job.perform_later
        Pgbus::BatchEntry.where(batch_id: batch.batch_id).update_all(created_at: 1.hour.ago)
        # Row inserted just now => activity inside the threshold => not stalled.
        Pgbus::Batch.sweep_stalled(stalled_for: 300)
        expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("pending")
      end
    end
  end

  describe "orphan-row sweep probes the queue before un-counting (issue #423)" do
    it "keeps a msg_id-less row whose message is live, and resolves it as failed once dead-lettered" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job, on_failure: on_discard_job)
      batch.enqueue { worker_job.perform_later }
      row = Pgbus::BatchExecution.find_by!(batch_id: batch.batch_id)
      # Simulate a backfill that never landed: message is live, the row keeps
      # the insert's logical queue name and no msg_id.
      msg_id = row.msg_id
      row.update_columns(msg_id: nil, queue_name: work_queue, created_at: 1.hour.ago)

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(1)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(status: "processing", total_jobs: 1)

      message = client.read_message(work_queue, vt: 30)
      expect(message.msg_id.to_i).to eq(msg_id)
      client.move_to_dead_letter(work_queue, message)

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(status: "finished", failed_jobs: 1)
      expect(drain_callback_job_classes).to contain_exactly("BatchFlowSpec::OnFinishJob", "BatchFlowSpec::OnDiscardJob")
    end

    it "still un-counts a row whose message exists nowhere" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job)
      batch.enqueue do
        worker_job.perform_later
        worker_job.perform_later
      end
      row = Pgbus::BatchExecution.where(batch_id: batch.batch_id).order(:id).first
      client.delete_message(work_queue, row.msg_id.to_i)
      row.update_columns(msg_id: nil, queue_name: work_queue, created_at: 1.hour.ago)

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(status: "processing", total_jobs: 1)
      complete_next(batch.batch_id)
      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
    end
  end

  # Issue #569: a logical queue whose own name starts with the prefix. An
  # orphan row holds the logical name, a backfilled row the physical table
  # (pgbus_int_pgbus_int_sweep_work); each probe must use the name it holds.
  describe "sweep of a queue whose logical name starts with the prefix" do
    let(:work_queue) { "pgbus_int_sweep_work" }

    it "un-counts an orphan row whose message exists nowhere" do
      batch = enqueue_batch(on_finish: on_finish_job)
      row = Pgbus::BatchExecution.where(batch_id: batch.batch_id).order(:id).first
      client.delete_message(work_queue, row.msg_id.to_i)
      row.update_columns(msg_id: nil, queue_name: work_queue, created_at: 1.hour.ago)

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(total_jobs: 1)
    end

    it "resolves an orphan row as failed once its message is dead-lettered" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job, on_failure: on_discard_job)
      batch.enqueue { worker_job.perform_later }
      row = Pgbus::BatchExecution.find_by!(batch_id: batch.batch_id)
      row.update_columns(msg_id: nil, queue_name: work_queue, created_at: 1.hour.ago)
      client.move_to_dead_letter(work_queue, client.read_message(work_queue, vt: 30))

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(status: "finished", failed_jobs: 1)
    end

    it "resolves a backfilled row as failed once its message is dead-lettered" do
      batch = Pgbus::Batch.new(on_finish: on_finish_job, on_failure: on_discard_job)
      batch.enqueue { worker_job.perform_later }
      row = Pgbus::BatchExecution.find_by!(batch_id: batch.batch_id)
      expect(row.queue_name).to eq("pgbus_int_pgbus_int_sweep_work")
      row.update_columns(created_at: 1.hour.ago)
      client.move_to_dead_letter(work_queue, client.read_message(work_queue, vt: 30))

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)).to have_attributes(status: "finished", failed_jobs: 1)
    end
  end

  describe "stalled-execution sweep (issue #414)" do
    it "finishes a batch whose last message is gone but the execution row remains" do
      batch = enqueue_batch(on_finish: on_finish_job)
      complete_next(batch.batch_id)

      row = Pgbus::BatchExecution.find_by!(batch_id: batch.batch_id)
      client.delete_message(work_queue, row.msg_id.to_i) if row.msg_id

      Pgbus::Batch.sweep_stalled(stalled_for: 0)

      finished = Pgbus::BatchEntry.find_by(batch_id: batch.batch_id)
      expect(finished.status).to eq("finished")
      expect(next_callback_job_class).to eq("BatchFlowSpec::OnFinishJob")
    end
  end
end
