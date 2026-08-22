# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Batch::Sweep do
  after { Pgbus::Batch.reset_executions_migrated_cache! }

  it "is a no-op when the executions table is missing" do
    allow(Pgbus::BatchExecution).to receive(:table_exists?).and_return(false)
    Pgbus::Batch.reset_executions_migrated_cache!
    allow(Pgbus::BatchExecution).to receive(:where)

    described_class.run(client: double("client"))

    expect(Pgbus::BatchExecution).not_to have_received(:where)
  end

  it "defines a 5-minute stall threshold matching solid_queue" do
    expect(described_class::STALL_THRESHOLD).to eq(300)
  end

  describe ".finish_stalled_processing" do
    it "skips a processing batch whose counters are not terminal" do
      record = double("BatchEntry", batch_id: "legacy", total_jobs: 3, completed_jobs: 0, discarded_jobs: 0)
      relation = double("relation")
      allow(Pgbus::BatchEntry).to receive_message_chain(:processing, :without_executions).and_return(relation) # rubocop:disable RSpec/MessageChain
      allow(relation).to receive(:find_each).and_yield(record)
      allow(Pgbus::Batch).to receive(:try_finish!)

      described_class.send(:finish_stalled_processing, batch_size: 50)

      expect(Pgbus::Batch).not_to have_received(:try_finish!)
    end

    it "tries to finish when counters already match total_jobs" do
      record = double("BatchEntry", batch_id: "stalled", total_jobs: 2, completed_jobs: 2, discarded_jobs: 0)
      relation = double("relation")
      allow(Pgbus::BatchEntry).to receive_message_chain(:processing, :without_executions).and_return(relation) # rubocop:disable RSpec/MessageChain
      allow(relation).to receive(:find_each).and_yield(record)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return({ just_finished: true, record: nil })

      described_class.send(:finish_stalled_processing, batch_size: 50)

      expect(Pgbus::Batch).to have_received(:try_finish!).with("stalled")
    end
  end
end
