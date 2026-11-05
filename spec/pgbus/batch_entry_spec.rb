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

  describe ".finish_if_empty!" do
    it "requires counters already terminal so a legacy empty table is not closed" do
      processing = double("processing")
      empty = double("empty")
      terminal = double("terminal")
      allow(described_class).to receive(:where)
        .with(batch_id: "b1", status: "processing")
        .and_return(processing)
      allow(processing).to receive(:without_executions).and_return(empty)
      allow(empty).to receive(:where)
        .with("completed_jobs + failed_jobs = total_jobs")
        .and_return(terminal)
      allow(terminal).to receive(:update_all).and_return(1)

      expect(described_class.finish_if_empty!("b1")).to eq(1)
      expect(empty).to have_received(:where)
        .with("completed_jobs + failed_jobs = total_jobs")
      expect(terminal).to have_received(:update_all)
        .with(hash_including(status: "finished"))
    end
  end
end
