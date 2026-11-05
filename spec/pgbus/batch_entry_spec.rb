# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::BatchEntry do
  # The bookkeeping leases through connection_pool.with_connection (issue #649).
  before { stub_bus_record_pool }

  describe ".increment_total_jobs!" do
    it "raises AlreadyFinished when no unfinished row is updated" do
      relation = double("relation", update_all: 0)
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: %w[pending processing])
        .and_return(relation)

      expect do
        described_class.increment_total_jobs!("b1", 2)
      end.to raise_error(Pgbus::Batch::AlreadyFinished, /already finished/)
    end

    it "returns true when the unfinished row is updated" do
      relation = double("relation", update_all: 1)
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: %w[pending processing])
        .and_return(relation)

      expect(described_class.increment_total_jobs!("b1", 2)).to be true
    end
  end

  describe ".decrement_total_jobs!" do
    it "subtracts one, floored at zero, only on an unfinished row" do
      relation = double("relation", update_all: 1)
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: %w[pending processing])
        .and_return(relation)

      described_class.decrement_total_jobs!("b1")

      expect(relation).to have_received(:update_all).with(["total_jobs = GREATEST(total_jobs - 1, 0)"])
    end
  end

  describe ".increment_counter! (legacy counter path)" do
    # With per-job counting (issue #423) total_jobs grows while the block is
    # still open, so completed == total can be momentarily true on a pending
    # batch. Only a processing batch may auto-finish; check_finished! at the
    # end of the block handles the pending case.
    it "does not finish a pending batch even when the counters match" do
      record = double("BatchEntry", status: "pending", completed_jobs: 1, discarded_jobs: 0, total_jobs: 1)
      allow(record).to receive(:increment!)
      allow(record).to receive(:update!)
      lock = double("lock", find_by: record)
      allow(described_class).to receive_messages(lock: lock)
      allow(described_class).to receive(:transaction).and_yield
      allow(Pgbus::Batch).to receive(:executions_migrated?).and_return(false)

      result = described_class.increment_counter!("b1", "completed_jobs")

      expect(record).not_to have_received(:update!)
      expect(result[:just_finished]).to be(false)
    end
  end

  describe ".count_and_resolve!" do
    let(:connection) { double("ActiveRecord connection") }
    let(:row) { { "id" => 1, "batch_id" => "b1", "total_jobs" => 3, "completed_jobs" => 2, "failed_jobs" => 0 } }

    before { stub_bus_record_pool(connection) }

    # instantiate builds the model from the row without a query; the column
    # types it needs come from a real schema (spec/integration/batch_statement_count_spec.rb).
    it "deletes the execution row and counts it into its batch in one statement" do
      record = double("BatchEntry")
      allow(connection).to receive(:exec_query).and_return([row])
      allow(described_class).to receive(:instantiate).with(row).and_return(record)

      expect(described_class.count_and_resolve!("job-1", "completed_jobs")).to be(record)
      expect(connection).to have_received(:exec_query) do |sql, name, binds|
        expect(sql).to include("DELETE FROM pgbus_batch_executions WHERE job_id = $1 RETURNING batch_id")
        expect(sql).to include("SET completed_jobs = b.completed_jobs + 1")
        expect(sql).to include("WHERE b.batch_id = gone.batch_id RETURNING b.*")
        expect(name).to eq("Pgbus Batch Count")
        expect(binds).to eq(["job-1"])
      end
    end

    it "returns nil when the job had no execution row" do
      allow(connection).to receive(:exec_query).and_return([])

      expect(described_class.count_and_resolve!("job-1", "failed_jobs")).to be_nil
    end

    it "refuses a column outside COUNTER_COLUMNS before any query" do
      allow(connection).to receive(:exec_query)

      expect { described_class.count_and_resolve!("job-1", "total_jobs = 0; --") }
        .to raise_error(ArgumentError, /Invalid column/)
      expect(connection).not_to have_received(:exec_query)
    end
  end

  describe ".finish_if_empty!" do
    let(:connection) { double("ActiveRecord connection") }

    before { stub_bus_record_pool(connection) }

    it "requires counters already terminal so a legacy empty table is not closed" do
      allow(connection).to receive(:exec_query).and_return([])

      expect(described_class.finish_if_empty!("b1")).to be_nil
      expect(connection).to have_received(:exec_query) do |sql, name, binds|
        expect(sql).to include("status = 'processing'")
        expect(sql).to include("completed_jobs + failed_jobs = total_jobs")
        expect(sql).to include("NOT EXISTS ( SELECT 1 FROM pgbus_batch_executions e")
        expect(sql).to include("RETURNING *")
        expect(name).to eq("Pgbus Batch Finish")
        expect(binds.first).to eq("b1")
      end
    end

    it "returns the finished row the winner's callbacks use" do
      row = { "id" => 1, "batch_id" => "b1", "status" => "finished", "total_jobs" => 2 }
      record = double("BatchEntry")
      allow(connection).to receive(:exec_query).and_return([row])
      allow(described_class).to receive(:instantiate).with(row).and_return(record)

      expect(described_class.finish_if_empty!("b1")).to be(record)
    end
  end
end
