# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::BatchExecution do
  let(:batch_id) { "batch-abc" }
  let(:job_id) { "job-1" }

  before do
    Pgbus::Batch.reset_executions_migrated_cache!
    allow(described_class).to receive(:table_exists?).and_return(true)
  end

  after { Pgbus::Batch.reset_executions_migrated_cache! }

  describe "Pgbus::Batch.executions_migrated?" do
    it "is true when the executions table exists" do
      expect(Pgbus::Batch.executions_migrated?).to be true
    end

    it "is false when the executions table is missing" do
      allow(described_class).to receive(:table_exists?).and_return(false)
      Pgbus::Batch.reset_executions_migrated_cache!

      expect(Pgbus::Batch.executions_migrated?).to be false
    end

    it "is false when the table check raises (unmigrated app, no connection)" do
      allow(described_class).to receive(:table_exists?).and_raise(ActiveRecord::StatementInvalid)
      Pgbus::Batch.reset_executions_migrated_cache!

      expect(Pgbus::Batch.executions_migrated?).to be false
    end
  end

  describe ".insert_for!" do
    it "inserts a row keyed by job_id before send" do
      conn = double("connection")
      allow(described_class).to receive_messages(connection: conn, table_name: "pgbus_batch_executions")
      allow(conn).to receive(:exec_query)

      described_class.insert_for!(batch_id: batch_id, job_id: job_id)

      expect(conn).to have_received(:exec_query).with(
        a_string_matching(/ON CONFLICT \(job_id\) DO NOTHING/),
        "BatchExecution Insert",
        [batch_id, job_id, anything]
      )
    end
  end

  describe ".backfill!" do
    it "writes msg_id and queue_name after send" do
      relation = double("relation")
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(relation).to receive(:update_all)

      described_class.backfill!(job_id, msg_id: 42, queue_name: "default")

      expect(relation).to have_received(:update_all).with(msg_id: 42, queue_name: "default")
    end
  end

  describe "Pgbus::Batch.track_enqueue / untrack_enqueue / backfill_execution" do
    let(:payload) { { "job_id" => job_id, Pgbus::Batch::METADATA_KEY => batch_id } }

    before do
      allow(described_class).to receive(:insert_for!)
      allow(described_class).to receive(:backfill!)
      allow(described_class).to receive(:where).and_return(double(delete_all: 1))
    end

    it "inserts an execution row when tracking an enqueue" do
      Pgbus::Batch.track_enqueue(payload)

      expect(described_class).to have_received(:insert_for!).with(batch_id: batch_id, job_id: job_id)
    end

    it "does not insert when the executions table is missing" do
      allow(described_class).to receive(:table_exists?).and_return(false)
      Pgbus::Batch.reset_executions_migrated_cache!

      Pgbus::Batch.track_enqueue(payload)

      expect(described_class).not_to have_received(:insert_for!)
    end

    it "deletes the row when untracking an enqueue-time discard" do
      relation = double("relation", delete_all: 1)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)

      Pgbus::Batch.untrack_enqueue(payload)

      expect(relation).to have_received(:delete_all)
    end

    it "backfills msg_id after send" do
      Pgbus::Batch.backfill_execution(payload, 99, "default")

      expect(described_class).to have_received(:backfill!).with(job_id, msg_id: 99, queue_name: "default")
    end
  end

  describe "Pgbus::Batch.job_completed with execution rows" do
    before do
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
    end

    it "deletes the execution row, increments completed_jobs, and tries to finish" do
      relation = double("relation", delete_all: 1)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(
        { record: double("BatchEntry", failed_jobs: 0, on_finish_class: nil, on_success_class: nil,
                                       on_failure_class: nil, properties: "{}"), just_finished: false }
      )
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(relation).to have_received(:delete_all)
      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "completed_jobs")
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id)
    end

    it "does not increment counters when the row was already gone (idempotent signal)" do
      relation = double("relation", delete_all: 0)
      remaining = double("remaining", exists?: true)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).not_to have_received(:increment_counter!)
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id)
    end

    it "increments counters for a pre-migration in-flight batch with no execution rows" do
      relation = double("relation", delete_all: 0)
      remaining = double("remaining", exists?: false)
      record = double("BatchEntry", total_jobs: 3, completed_jobs: 1, failed_jobs: 0, discarded_jobs: 0)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(
        { record: record, just_finished: false }
      )
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "completed_jobs")
    end

    it "does not increment a vanished row when counters are already terminal" do
      relation = double("relation", delete_all: 0)
      remaining = double("remaining", exists?: false)
      record = double("BatchEntry", total_jobs: 2, completed_jobs: 2, failed_jobs: 0, discarded_jobs: 0)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).not_to have_received(:increment_counter!)
    end
  end

  describe "Pgbus::Batch.job_discarded with execution rows" do
    before do
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
    end

    it "deletes the row and increments failed_jobs (not discarded_jobs)" do
      relation = double("relation", delete_all: 1)
      allow(described_class).to receive(:where).with(job_id: job_id).and_return(relation)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(
        { record: double("BatchEntry"), just_finished: false }
      )
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_discarded(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "failed_jobs")
    end

    it "increments failed_jobs and re-checks finish when job_id is omitted" do
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(
        { record: double("BatchEntry"), just_finished: false }
      )
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(
        { just_finished: false, record: double("BatchEntry") }
      )

      Pgbus::Batch.job_discarded(batch_id)

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "failed_jobs")
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id)
    end
  end

  describe "Pgbus::Batch.try_finish!" do
    let(:record) do
      double("BatchEntry", batch_id: batch_id, total_jobs: 2, completed_jobs: 2, failed_jobs: 0,
                           on_finish_class: nil, on_success_class: nil, on_failure_class: nil, properties: "{}")
    end

    it "finishes when the CAS update wins and a fresh exists? check is empty" do
      executions = double("executions", exists?: false)
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(1)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(executions)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)
      allow(Pgbus::Instrumentation).to receive(:instrument).and_yield({})

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result[:just_finished]).to be true
    end

    it "does not report finished when a fresh exists? check finds rows (READ COMMITTED hazard)" do
      executions = double("executions", exists?: true)
      allow(Pgbus::BatchEntry).to receive(:transaction) do |&block|
        block.call
      rescue ActiveRecord::Rollback
        nil
      end
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(1)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(executions)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result[:just_finished]).to be false
    end

    it "is a no-op when the CAS update matches zero rows" do
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(0)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result[:just_finished]).to be false
    end
  end

  describe "fallback when not migrated" do
    before do
      allow(described_class).to receive(:table_exists?).and_return(false)
      Pgbus::Batch.reset_executions_migrated_cache!
    end

    it "job_completed uses counter-based completion" do
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "completed_jobs")
    end
  end
end
