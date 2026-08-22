# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Concurrency::BlockedExecution do
  describe ".insert" do
    it "creates a blocked execution record" do
      allow(Pgbus::BlockedExecution).to receive(:create!).and_return(double("record"))

      described_class.insert(
        concurrency_key: "TestJob-42",
        queue_name: "default",
        payload: { "job_class" => "TestJob", "arguments" => [42] },
        priority: 0,
        duration: 900
      )

      expect(Pgbus::BlockedExecution).to have_received(:create!).with(
        hash_including(concurrency_key: "TestJob-42", queue_name: "default", priority: 0)
      )
    end
  end

  describe ".release_next" do
    it "delegates to BlockedExecution.release_next!" do
      released = { queue_name: "default", payload: { "job_class" => "TestJob" } }
      allow(Pgbus::BlockedExecution).to receive(:release_next!).with("TestJob-42").and_return(released)

      result = described_class.release_next("TestJob-42")

      expect(result).to eq(released)
    end

    it "returns nil when no blocked executions exist" do
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(nil)

      expect(described_class.release_next("TestJob-42")).to be_nil
    end
  end

  describe ".promote_next" do
    let(:mock_client) { build_mock_client }

    before do
      allow(Pgbus::BlockedExecution).to receive(:transaction).and_yield
    end

    it "deletes the blocked row and enqueues atomically, returning true" do
      released = { queue_name: "default", payload: { "job_class" => "TestJob" } }
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)

      promoted = described_class.promote_next("TestJob-42", client: mock_client)

      expect(promoted).to be true
      expect(mock_client).to have_received(:send_message).with("default", { "job_class" => "TestJob" }, delay: 0)
    end

    it "returns false when no blocked executions exist" do
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(nil)

      promoted = described_class.promote_next("TestJob-42", client: mock_client)

      expect(promoted).to be false
      expect(mock_client).not_to have_received(:send_message)
    end

    it "returns false and logs warning on error" do
      allow(Pgbus::BlockedExecution).to receive(:transaction).and_raise(StandardError, "db error")

      promoted = described_class.promote_next("TestJob-42", client: mock_client)

      expect(promoted).to be false
    end

    it "returns true when post-commit backfill raises" do
      released = { queue_name: "default", payload: { "job_class" => "TestJob", "job_id" => "j1" } }
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)
      allow(Pgbus::Batch).to receive(:backfill_execution).and_raise(StandardError, "backfill failed")
      logger = instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)

      expect(described_class.promote_next("TestJob-42", client: mock_client)).to be true
      expect(logger).to have_received(:warn)
    end
  end

  describe ".expire_stale" do
    it "deletes expired blocked executions" do
      scope = double("scope", delete_all: 2)
      allow(Pgbus::BlockedExecution).to receive(:expired).and_return(scope)

      count = described_class.expire_stale

      expect(count).to eq(2)
    end
  end

  describe ".count_for" do
    it "returns the count of blocked executions for a key" do
      scope = double("scope", count: 5)
      allow(Pgbus::BlockedExecution).to receive(:where).with(concurrency_key: "TestJob-42").and_return(scope)

      expect(described_class.count_for("TestJob-42")).to eq(5)
    end
  end
end
