# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::BatchExecution do
  let(:batch_id) { "batch-abc" }
  let(:job_id) { "job-1" }

  before do
    # The bookkeeping leases through connection_pool.with_connection (issue #649).
    stub_bus_record_pool
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
      pool = instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool)
      allow(pool).to receive(:with_connection).and_yield(conn)
      allow(described_class).to receive_messages(connection_pool: pool, table_name: "pgbus_batch_executions")
      allow(conn).to receive(:exec_query)

      described_class.insert_for!(batch_id: batch_id, job_id: job_id, queue_name: "default")

      expect(conn).to have_received(:exec_query).with(
        a_string_matching(/ON CONFLICT \(job_id\) DO NOTHING/),
        "BatchExecution Insert",
        [batch_id, job_id, "default", anything]
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
    let(:payload) { { "job_id" => job_id, "queue_name" => "default", Pgbus::Batch::METADATA_KEY => batch_id } }

    before do
      allow(described_class).to receive(:insert_for!)
      allow(described_class).to receive(:backfill!)
      allow(described_class).to receive(:where).and_return(double(delete_all: 1))
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive_messages(increment_total_jobs!: true, decrement_total_jobs!: 1)
    end

    it "inserts an execution row when tracking an enqueue" do
      Pgbus::Batch.track_enqueue(payload)

      expect(described_class).to have_received(:insert_for!).with(batch_id: batch_id, job_id: job_id, queue_name: "default")
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

    # It runs after the send: a cold process whose pool cannot hand out a
    # connection must not fail an enqueue whose message is already live,
    # exactly as the fail-soft schema probe never did.
    it "takes one lease once the schema probe is memoized" do
      Pgbus::Batch.executions_migrated?
      pool = stub_bus_record_pool

      Pgbus::Batch.backfill_execution(payload, 99, "default")

      expect(pool.leases).to eq(1)
    end

    it "skips the backfill without raising when the schema probe cannot get a connection" do
      Pgbus::Batch.reset_executions_migrated_cache!
      pool = instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool)
      allow(pool).to receive(:with_connection).and_raise(ActiveRecord::ConnectionNotEstablished, "no database")
      allow(Pgbus::BusRecord).to receive(:connection_pool).and_return(pool)

      expect(Pgbus::Batch.backfill_execution(payload, 99, "default")).to be_nil
      expect(described_class).not_to have_received(:backfill!)
    end
  end

  describe "Pgbus::Batch.job_completed with execution rows" do
    let(:counted) { double("BatchEntry", batch_id: batch_id) }

    it "resolves and counts the job in one statement, then tries to finish with that row" do
      allow(Pgbus::BatchEntry).to receive(:count_and_resolve!).and_return(counted)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:count_and_resolve!).with(job_id, "completed_jobs")
      expect(Pgbus::BatchEntry).not_to have_received(:increment_counter!)
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id, record: counted)
    end

    it "does not increment counters when the row was already gone (idempotent signal)" do
      remaining = double("remaining", exists?: true)
      allow(Pgbus::BatchEntry).to receive(:count_and_resolve!).and_return(nil)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).not_to have_received(:increment_counter!)
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id)
    end

    it "increments counters for a pre-migration in-flight batch with no execution rows" do
      remaining = double("remaining", exists?: false)
      record = double("BatchEntry", total_jobs: 3, completed_jobs: 1, failed_jobs: 0, discarded_jobs: 0)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)
      allow(Pgbus::BatchEntry).to receive_messages(
        count_and_resolve!: nil, increment_counter!: { record: record, just_finished: false }
      )
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with(batch_id, "completed_jobs")
    end

    it "does not increment a vanished row when counters are already terminal" do
      remaining = double("remaining", exists?: false)
      record = double("BatchEntry", total_jobs: 2, completed_jobs: 2, failed_jobs: 0, discarded_jobs: 0)
      allow(Pgbus::BatchEntry).to receive(:count_and_resolve!).and_return(nil)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(remaining)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_completed(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).not_to have_received(:increment_counter!)
    end
  end

  describe "Pgbus::Batch.job_discarded with execution rows" do
    it "resolves the row and counts failed_jobs (not discarded_jobs)" do
      counted = double("BatchEntry", batch_id: batch_id)
      allow(Pgbus::BatchEntry).to receive(:count_and_resolve!).and_return(counted)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return(nil)

      Pgbus::Batch.job_discarded(batch_id, job_id: job_id)

      expect(Pgbus::BatchEntry).to have_received(:count_and_resolve!).with(job_id, "failed_jobs")
      expect(Pgbus::Batch).to have_received(:try_finish!).with(batch_id, record: counted)
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

    before { allow(Pgbus::BatchEntry).to receive(:find_by) }

    it "finishes with the RETURNING row when the CAS update wins and a fresh exists? check is empty" do
      executions = double("executions", exists?: false)
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(record)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(executions)

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result).to eq(just_finished: true, record: record)
      expect(Pgbus::BatchEntry).not_to have_received(:find_by)
    end

    it "does not report finished when a fresh exists? check finds rows (READ COMMITTED hazard)" do
      executions = double("executions", exists?: true)
      allow(Pgbus::BatchEntry).to receive(:transaction) do |&block|
        block.call
      rescue ActiveRecord::Rollback
        nil
      end
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(record)
      allow(described_class).to receive(:where).with(batch_id: batch_id).and_return(executions)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result[:just_finished]).to be false
    end

    it "hands back the caller's row without a query when the CAS update matches zero rows" do
      counted = double("BatchEntry")
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(nil)

      result = Pgbus::Batch.try_finish!(batch_id, record: counted)

      expect(result).to eq(just_finished: false, record: counted)
      expect(Pgbus::BatchEntry).not_to have_received(:find_by)
    end

    it "reads the row when the CAS update matches zero rows and the caller has none (the sweep)" do
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:finish_if_empty!).with(batch_id).and_return(nil)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: batch_id).and_return(record)

      result = Pgbus::Batch.try_finish!(batch_id)

      expect(result).to eq(just_finished: false, record: record)
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
