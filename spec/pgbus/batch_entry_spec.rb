# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::BatchEntry do
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
        .with("completed_jobs + failed_jobs = total_jobs AND total_jobs > 0")
        .and_return(terminal)
      allow(terminal).to receive(:update_all).and_return(1)

      expect(described_class.finish_if_empty!("b1")).to eq(1)
      expect(empty).to have_received(:where)
        .with("completed_jobs + failed_jobs = total_jobs AND total_jobs > 0")
      expect(terminal).to have_received(:update_all)
        .with(hash_including(status: "finished"))
    end
  end
end
