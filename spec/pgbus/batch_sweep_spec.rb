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

  describe ".start_stalled_pending (issue #423)" do
    let(:record) { double("BatchEntry", batch_id: "p1") }
    let(:pending_scope) { double("pending_scope") }
    let(:flip) { double("flip", update_all: 1) }

    before do
      allow(Pgbus::BatchEntry).to receive(:pending).and_return(pending_scope)
      allow(pending_scope).to receive(:where).and_return(pending_scope)
      allow(pending_scope).to receive(:find_each).and_yield(record)
      allow(Pgbus::BatchEntry).to receive(:where).with(batch_id: "p1", status: "pending").and_return(flip)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return({ just_finished: false, record: nil })
    end

    it "flips pending to processing without recomputing total_jobs (the jobs counted themselves)" do
      described_class.send(:start_stalled_pending, stalled_for: 300, batch_size: 50)

      expect(flip).to have_received(:update_all).with(status: "processing")
      expect(flip).not_to have_received(:update_all).with(hash_including(:total_jobs))
      expect(Pgbus::Batch).to have_received(:try_finish!).with("p1")
    end

    it "only treats a batch as stalled when no execution row was inserted within the threshold" do
      described_class.send(:start_stalled_pending, stalled_for: 300, batch_size: 50)

      expect(pending_scope).to have_received(:where).with("created_at < ?", anything)
      expect(pending_scope).to have_received(:where)
        .with(a_string_matching(/NOT EXISTS.*pgbus_batch_executions.*created_at >= /m), anything)
    end
  end

  describe ".sweep_orphan_rows (issue #423)" do
    let(:client) { double("client") }
    let(:row) { double("BatchExecution", id: 9, batch_id: "b1", job_id: "j1", queue_name: "default") }
    let(:relation) { double("relation") }

    before do
      allow(Pgbus::BatchExecution).to receive(:where).with(msg_id: nil).and_return(relation)
      allow(relation).to receive(:where).and_return(relation)
      allow(relation).to receive(:find_each).and_yield(row)
      allow(described_class).to receive(:blocked_job_ids).and_return(Set.new)
      allow(client).to receive(:dead_letter_physical_name).with("default").and_return("pgbus_test_default_dlq")
      allow(Pgbus::Batch).to receive_messages(job_discarded: nil, try_finish!: { just_finished: false, record: nil })
      allow(Pgbus::BatchEntry).to receive(:decrement_total_jobs!)
    end

    it "keeps a row whose message is still on its queue" do
      allow(client).to receive(:message_with_job_id?).with("default", job_id: "j1").and_return(true)

      swept = described_class.send(:sweep_orphan_rows, stalled_for: 300, batch_size: 50, client: client)

      expect(swept).to eq(0)
      expect(Pgbus::BatchEntry).not_to have_received(:decrement_total_jobs!)
    end

    it "keeps a row when the queue probe is inconclusive" do
      allow(client).to receive(:message_with_job_id?).with("default", job_id: "j1").and_return(nil)

      described_class.send(:sweep_orphan_rows, stalled_for: 300, batch_size: 50, client: client)

      expect(Pgbus::BatchEntry).not_to have_received(:decrement_total_jobs!)
    end

    it "resolves a row as failed when its message sits in the DLQ" do
      allow(client).to receive(:message_with_job_id?).with("default", job_id: "j1").and_return(false)
      allow(client).to receive(:message_with_job_id?)
        .with("pgbus_test_default_dlq", job_id: "j1", prefixed: false).and_return(true)

      described_class.send(:sweep_orphan_rows, stalled_for: 300, batch_size: 50, client: client)

      expect(Pgbus::Batch).to have_received(:job_discarded).with("b1", job_id: "j1")
      expect(Pgbus::BatchEntry).not_to have_received(:decrement_total_jobs!)
    end

    it "un-counts a row only when the message exists nowhere" do
      allow(client).to receive(:message_with_job_id?).and_return(false)
      cas = double("cas", delete_all: 1)
      allow(Pgbus::BatchExecution).to receive(:where).with(id: 9, msg_id: nil).and_return(cas)

      swept = described_class.send(:sweep_orphan_rows, stalled_for: 300, batch_size: 50, client: client)

      expect(swept).to eq(1)
      expect(Pgbus::BatchEntry).to have_received(:decrement_total_jobs!).with("b1")
      expect(Pgbus::Batch).to have_received(:try_finish!).with("b1")
    end
  end

  describe ".sweep_stale_executions (issue #569)" do
    let(:client) { double("client") }
    # A row with a msg_id was backfilled with the physical table it was sent to.
    let(:row) do
      double("BatchExecution", batch_id: "b1", job_id: "j1", msg_id: 42, queue_name: "pgbus_test_default_p1")
    end
    let(:relation) { double("relation") }

    before do
      allow(Pgbus::BatchExecution).to receive_message_chain(:where, :not).and_return(relation) # rubocop:disable RSpec/MessageChain
      allow(relation).to receive(:where).and_return(relation)
      allow(relation).to receive(:find_each).and_yield(row)
      allow(client).to receive(:dead_letter_physical_name)
        .with("pgbus_test_default_p1", prefixed: false).and_return("pgbus_test_default_dlq")
      allow(Pgbus::Batch).to receive_messages(job_completed: nil, job_discarded: nil)
    end

    it "probes the row's physical table as is: queue, DLQ and archive" do
      allow(client).to receive(:message_in_queue?).with("pgbus_test_default_p1", msg_id: 42, prefixed: false)
                                                  .and_return(false)
      allow(client).to receive(:message_with_job_id?).with("pgbus_test_default_dlq", job_id: "j1", prefixed: false)
                                                     .and_return(false)
      allow(client).to receive(:message_archived?).with("pgbus_test_default_p1", msg_id: 42, prefixed: false)
                                                  .and_return(true)

      described_class.send(:sweep_stale_executions, batch_size: 50, client: client, stalled_for: 300)

      expect(Pgbus::Batch).to have_received(:job_completed).with("b1", job_id: "j1")
    end

    it "resolves the row as failed when the DLQ holds its job" do
      allow(client).to receive(:message_in_queue?).with("pgbus_test_default_p1", msg_id: 42, prefixed: false)
                                                  .and_return(false)
      allow(client).to receive(:message_with_job_id?).with("pgbus_test_default_dlq", job_id: "j1", prefixed: false)
                                                     .and_return(true)

      described_class.send(:sweep_stale_executions, batch_size: 50, client: client, stalled_for: 300)

      expect(Pgbus::Batch).to have_received(:job_discarded).with("b1", job_id: "j1")
    end

    # Issue #607: under completed_jobs :delete a finished job leaves no
    # archive row, so a message in neither queue nor DLQ is a completion, not
    # an anomaly worth a warning per row.
    context "with completed_jobs: :delete" do
      before do
        Pgbus.configuration.completed_jobs = :delete
        allow(client).to receive(:message_in_queue?).with("pgbus_test_default_p1", msg_id: 42, prefixed: false)
                                                    .and_return(false)
        allow(client).to receive(:message_archived?)
        allow(Pgbus.logger).to receive(:warn)
      end

      after { Pgbus.configuration.completed_jobs = :archive }

      it "resolves the row as completed without probing the archive or warning" do
        allow(client).to receive(:message_with_job_id?).with("pgbus_test_default_dlq", job_id: "j1", prefixed: false)
                                                       .and_return(false)

        described_class.send(:sweep_stale_executions, batch_size: 50, client: client, stalled_for: 300)

        expect(Pgbus::Batch).to have_received(:job_completed).with("b1", job_id: "j1")
        expect(client).not_to have_received(:message_archived?)
        expect(Pgbus.logger).not_to have_received(:warn)
      end

      it "still resolves the row as failed when the DLQ holds its job" do
        allow(client).to receive(:message_with_job_id?).with("pgbus_test_default_dlq", job_id: "j1", prefixed: false)
                                                       .and_return(true)

        described_class.send(:sweep_stale_executions, batch_size: 50, client: client, stalled_for: 300)

        expect(Pgbus::Batch).to have_received(:job_discarded).with("b1", job_id: "j1")
        expect(Pgbus::Batch).not_to have_received(:job_completed)
      end
    end
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

    it "finishes a batch whose enqueue block crashed before enqueuing anything" do
      record = double("BatchEntry", batch_id: "crashed", total_jobs: 0, completed_jobs: 0, discarded_jobs: 0)
      relation = double("relation")
      allow(Pgbus::BatchEntry).to receive_message_chain(:processing, :without_executions).and_return(relation) # rubocop:disable RSpec/MessageChain
      allow(relation).to receive(:find_each).and_yield(record)
      allow(Pgbus::Batch).to receive(:try_finish!).and_return({ just_finished: true, record: nil })

      described_class.send(:finish_stalled_processing, batch_size: 50)

      expect(Pgbus::Batch).to have_received(:try_finish!).with("crashed")
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
