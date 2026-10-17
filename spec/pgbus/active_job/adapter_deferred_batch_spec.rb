# frozen_string_literal: true

require "spec_helper"

# Issue #553: a job that enqueues after commit reaches the adapter after its
# Pgbus.batch block has returned, so the thread tag is gone. The batch it was
# added to rides on the instance (enqueue_batch_id) instead.
RSpec.describe Pgbus::ActiveJob::Adapter do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:job_id) { SecureRandom.uuid }
  let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
  let(:serialized_hash) { { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] } }

  def added_to(job, batch_id)
    allow(job).to receive(:enqueue_batch_id).and_return(batch_id)
    allow(job).to receive(:enqueue_batch_id=)
    job
  end

  def tagged(payload, batch_id)
    payload.merge(Pgbus::Batch::METADATA_KEY => batch_id)
  end

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
    allow(Pgbus::Batch).to receive(:track_enqueue)
    allow(Pgbus::Batch).to receive(:untrack_enqueue)
    allow(Pgbus::Batch).to receive(:track_retry)
    allow(Pgbus::Batch).to receive(:forget_retry_reenqueued)
    allow(mock_client).to receive(:send_message).and_return(7)
  end

  after { Thread.current[:pgbus_batch_id] = nil }

  describe "#enqueue of a job added to a batch whose block has returned" do
    it "tags and counts the job into the batch it was added to, before the send" do
      added_to(job, "b-1")

      adapter.enqueue(job)

      expect(Pgbus::Batch).to have_received(:track_enqueue).with(tagged(serialized_hash, "b-1")).ordered
      expect(mock_client).to have_received(:send_message)
        .with("default", tagged(serialized_hash, "b-1"), delay: 0, priority: nil).ordered
    end

    it "prefers the batch the job was added to over a block active when the transaction commits" do
      added_to(job, "inner")
      Thread.current[:pgbus_batch_id] = "outer"

      adapter.enqueue(job)

      expect(Pgbus::Batch).to have_received(:track_enqueue).with(tagged(serialized_hash, "inner"))
    end

    it "forgets the batch once the enqueue is done, so a later re-enqueue stays out of it" do
      added_to(job, "b-1")

      adapter.enqueue(job)

      expect(job).to have_received(:enqueue_batch_id=).with(nil)
    end

    it "forgets the batch when the enqueue raises too" do
      added_to(job, "b-1")
      allow(Pgbus::Batch).to receive(:track_enqueue).and_raise(Pgbus::Batch::AlreadyFinished)

      expect { adapter.enqueue(job) }.to raise_error(Pgbus::Batch::AlreadyFinished)
      expect(job).to have_received(:enqueue_batch_id=).with(nil)
      expect(mock_client).not_to have_received(:send_message)
    end

    it "uncounts a first attempt discarded as a duplicate instead of taking it for a retry" do
      added_to(job, "b-1")
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive_messages(
        inject_metadata: serialized_hash, extract_key: "UniqJob-42", acquire_enqueue_lock: :locked,
        uniqueness_config: { strategy: :until_executed, on_conflict: :discard }
      )

      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
      expect(Pgbus::Batch).to have_received(:untrack_enqueue).with(tagged(serialized_hash, "b-1"))
      expect(Pgbus::Batch).not_to have_received(:forget_retry_reenqueued)
    end

    it "uncounts a first attempt whose send fails before a message exists" do
      added_to(job, "b-1")
      allow(mock_client).to receive(:send_message).and_raise(ArgumentError, "bad payload")

      expect { adapter.enqueue(job) }.to raise_error(ArgumentError)
      expect(Pgbus::Batch).to have_received(:untrack_enqueue).with(tagged(serialized_hash, "b-1"))
    end
  end

  describe "#enqueue_at of a job added to a batch whose block has returned" do
    it "tags and counts a scheduled job into the batch it was added to" do
      added_to(job, "b-1")

      adapter.enqueue_at(job, Time.now.to_f + 60)

      expect(Pgbus::Batch).to have_received(:track_enqueue).with(tagged(serialized_hash, "b-1"))
      expect(job).to have_received(:enqueue_batch_id=).with(nil)
    end
  end

  describe "#enqueue_all of jobs added to a batch whose block has returned" do
    let(:second_job_id) { SecureRandom.uuid }
    let(:job2) { build_job_double(job_class: "OtherJob", queue_name: "default", job_id: second_job_id) }
    let(:second_hash) { { "job_class" => "OtherJob", "job_id" => second_job_id, "queue_name" => "default", "arguments" => [] } }

    before do
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job).and_return(serialized_hash)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job2).and_return(second_hash)
      allow(mock_client).to receive(:send_batch).and_return([1, 2])
    end

    it "counts a bulk of jobs added to one batch once, before the send" do
      added_to(job, "b-1")
      added_to(job2, "b-1")
      payloads = [tagged(serialized_hash, "b-1"), tagged(second_hash, "b-1")]

      adapter.enqueue_all([job, job2])

      expect(Pgbus::Batch).to have_received(:track_enqueue).with(payloads).once.ordered
      expect(mock_client).to have_received(:send_batch).with("default", payloads, priority: nil).ordered
      expect([job, job2]).to all(have_received(:enqueue_batch_id=).with(nil))
    end

    it "counts each batch separately and leaves an unbatched job out" do
      added_to(job, "b-1")
      added_to(job2, nil)

      adapter.enqueue_all([job, job2])

      expect(Pgbus::Batch).to have_received(:track_enqueue).with([tagged(serialized_hash, "b-1")]).once
      expect(mock_client).to have_received(:send_batch)
        .with("default", [tagged(serialized_hash, "b-1"), second_hash], priority: nil)
    end

    it "never re-tags a retry on the bulk path" do
      allow(job).to receive_messages(batch_id: "b-1", executions: 1)

      adapter.enqueue_all([job, job2])

      expect(Pgbus::Batch).not_to have_received(:track_retry)
      expect(Pgbus::Batch).not_to have_received(:track_enqueue)
      expect(mock_client).to have_received(:send_batch).with("default", [serialized_hash, second_hash], priority: nil)
    end
  end
end
