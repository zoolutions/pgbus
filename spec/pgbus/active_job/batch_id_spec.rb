# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::ActiveJob::BatchId do
  let(:job_class) do
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      def self.name = "BatchIdSpec::Job"
      def perform(*); end
    end
  end

  before { stub_const("BatchIdSpec::Job", job_class) }

  describe "serialization" do
    it "omits both keys for an unbatched job so payloads stay lean" do
      data = job_class.new.serialize

      expect(data).not_to have_key("batch_id")
      expect(data).not_to have_key("callback_batch_id")
    end

    it "round-trips batch_id" do
      job = job_class.new
      job.batch_id = "b-1"

      restored = ActiveJob::Base.deserialize(job.serialize)

      expect(restored.batch_id).to eq("b-1")
    end

    it "round-trips callback_batch_id" do
      job = job_class.new
      job.callback_batch_id = "b-2"

      restored = ActiveJob::Base.deserialize(job.serialize)

      expect(restored.callback_batch_id).to eq("b-2")
      expect(restored.batch_id).to be_nil
    end
  end

  # Issue #553: the batch a job was added to rides on the instance, so it
  # survives an enqueue Active Job defers to after commit.
  describe "#enqueue_batch_id" do
    let(:captured) { [] }
    let(:adapter) do
      seen = captured
      stub_const("BatchIdSpec::CapturingAdapter", Class.new do
        define_method(:enqueue) { |job| seen << job.enqueue_batch_id }
        def enqueue_at(*) = nil
        def enqueue_after_transaction_commit? = false
      end).new
    end

    before { job_class.queue_adapter = adapter }
    after { Thread.current[:pgbus_batch_id] = nil }

    it "is never serialized: the executor's copy of a member must not look like a first attempt" do
      job = job_class.new
      job.enqueue_batch_id = "b-1"

      expect(job.serialize.values).not_to include("b-1")
      expect(ActiveJob::Base.deserialize(job.serialize).enqueue_batch_id).to be_nil
    end

    it "captures the block's batch tag when the job is enqueued" do
      Thread.current[:pgbus_batch_id] = "b-1"

      job_class.perform_later

      expect(captured).to eq(["b-1"])
    end

    it "keeps the batch the job was explicitly added to" do
      Thread.current[:pgbus_batch_id] = "outer"
      job = job_class.new
      job.enqueue_batch_id = "inner"

      job.enqueue

      expect(captured).to eq(["inner"])
    end

    it "tags each instance with the batch it is added to" do
      jobs = [job_class.new, job_class.new]

      described_class.capture(jobs, "b-1")

      expect(jobs.map(&:enqueue_batch_id)).to eq(%w[b-1 b-1])
    end

    it "skips instances without the mixin" do
      plain = Class.new(ActiveJob::Base) { def perform; end }.new

      expect { described_class.capture([plain], "b-1") }.not_to raise_error
    end
  end

  describe "#batch" do
    it "is nil when the job belongs to no batch" do
      expect(job_class.new.batch).to be_nil
      expect(Pgbus::Batch).not_to receive(:find) # rubocop:disable RSpec/MessageSpies
    end

    it "looks the batch up once and memoizes it" do
      handle = instance_double(Pgbus::Batch)
      allow(Pgbus::Batch).to receive(:find).with("b-1").and_return(handle)
      job = job_class.new
      job.batch_id = "b-1"

      expect(job.batch).to be(handle)
      expect(job.batch).to be(handle)
      expect(Pgbus::Batch).to have_received(:find).once
    end

    it "prefers callback_batch_id so a callback job reads the batch it reports on" do
      handle = instance_double(Pgbus::Batch)
      allow(Pgbus::Batch).to receive(:find).with("cb-1").and_return(handle)
      job = job_class.new
      job.batch_id = "b-1"
      job.callback_batch_id = "cb-1"

      expect(job.batch).to be(handle)
    end
  end

  describe "#batch with a block" do
    it "opens a new batch through Pgbus.batch with the options and the block" do
      started = instance_double(Pgbus::Batch)
      received = nil
      allow(Pgbus).to receive(:batch) do |**options, &block|
        received = [options, block]
        started
      end
      block = proc { |_b| }

      expect(job_class.new.batch(description: "import", &block)).to be(started)
      expect(received).to eq([{ description: "import" }, block])
    end

    it "leaves the job's own membership and its memoized batch alone" do
      own = instance_double(Pgbus::Batch)
      allow(Pgbus::Batch).to receive(:find).with("b-1").and_return(own)
      allow(Pgbus).to receive(:batch).and_return(instance_double(Pgbus::Batch))
      job = job_class.new
      job.batch_id = "b-1"
      job.batch

      job.batch { |_b| nil }

      expect(job.batch_id).to eq("b-1")
      expect(job.batch).to be(own)
    end

    it "raises when options are given without a block" do
      expect { job_class.new.batch(description: "import") }.to raise_error(ArgumentError, /block/)
    end
  end
end
