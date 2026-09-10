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
        hash_including(concurrency_key: "TestJob-42", queue_name: "default", priority: 0,
                       payload: { "job_class" => "TestJob", "arguments" => [42] })
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
    let(:released) { { queue_name: "default", payload: { "job_class" => "TestJob", "job_id" => "j1" }, priority: nil } }

    before do
      allow(Pgbus::BlockedExecution).to receive(:transaction).and_yield
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:acquired)
      allow(Pgbus::Batch).to receive(:backfill_execution)
    end

    it "deletes the blocked row, takes a slot and enqueues in one transaction, returning true" do
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)

      promoted = described_class.promote_next("TestJob-42", client: mock_client)

      expect(promoted).to be true
      # A savepoint, so Semaphore.signal can wrap release + promote in one
      # transaction. (The batch backfill opens a second one of its own.)
      expect(Pgbus::BlockedExecution).to have_received(:transaction).with(requires_new: true).at_least(:once)
      # nil limit: this payload's class carries no concurrency config, so the
      # promotion is judged against the limit the semaphore row records.
      expect(Pgbus::Semaphore).to have_received(:acquire!).with("TestJob-42", nil, a_kind_of(Time))
      expect(mock_client).to have_received(:send_message).with("default", released[:payload], delay: 0, priority: nil)
    end

    # A backfill failure must not poison the transaction the caller may have
    # opened around promote_next (Semaphore.signal does): a poisoned
    # transaction fails to commit, un-deletes the parked row and un-takes the
    # slot while the message is already live — the job runs twice.
    it "isolates a failing batch backfill in its own savepoint" do
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)
      savepoints = 0
      allow(Pgbus::BlockedExecution).to receive(:transaction) do |**opts, &block|
        savepoints += 1 if opts[:requires_new]
        begin
          block.call
        rescue ActiveRecord::Rollback
          nil
        end
      end
      allow(Pgbus::Batch).to receive(:backfill_execution).and_raise(StandardError, "deadlock")
      allow(Pgbus).to receive(:logger).and_return(instance_double(Logger, warn: nil, debug: nil, info: nil, error: nil))

      expect(described_class.promote_next("TestJob-42", client: mock_client)).to be true
      expect(savepoints).to eq(2)
    end

    # Same rule as a direct scheduled enqueue: the heartbeat that renews a
    # lease only starts at dequeue, so a parked `wait:` job promoted while
    # its scheduled time is still far out needs its remaining delay in the
    # lease or the sweep expires it before the message is even visible.
    it "adds a promoted scheduled job's remaining delay to its lease" do
      released[:payload] = released[:payload].merge("scheduled_at" => (Time.current + 1800).iso8601)
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)

      described_class.promote_next("TestJob-42", client: mock_client)

      expect(Pgbus::Semaphore).to have_received(:acquire!) do |_key, _limit, expires_at|
        expect(expires_at - Time.current).to be >= 1800
      end
    end

    it "takes the slot with the limit and duration the job class declares" do
      stub_const("TestJob", Class.new do
        include Pgbus::Concurrency

        limits_concurrency to: 3, key: ->(*) { "k" }, duration: 60
      end)
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)

      described_class.promote_next("TestJob-42", client: mock_client)

      expect(Pgbus::Semaphore).to have_received(:acquire!) do |_key, limit, expires_at|
        expect(limit).to eq(3)
        expect(expires_at - Time.current).to be_within(5).of(60)
      end
    end

    # The slot may be gone by the time the row is picked (a concurrent enqueue,
    # or a sweep that already refilled the semaphore): put the row back.
    it "returns false and keeps the row when no slot is free" do
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:blocked)
      allow(Pgbus::BlockedExecution).to receive(:transaction) do |&block|
        block.call
      rescue ActiveRecord::Rollback
        nil
      end

      promoted = described_class.promote_next("TestJob-42", client: mock_client)

      expect(promoted).to be false
      expect(mock_client).not_to have_received(:send_message)
    end

    # Issue #423 (F7): the row stores the enqueuer's priority for ordering, but
    # the promoted send dropped it — under priority routing the job landed on
    # the default sub-queue.
    it "sends the promoted job with the priority the blocked row carried" do
      released[:priority] = 0
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)
      allow(mock_client).to receive(:target_queue).with("default", 0).and_return("pgbus_test_default_p0")

      described_class.promote_next("TestJob-42", client: mock_client)

      expect(mock_client).to have_received(:send_message).with("default", released[:payload], delay: 0, priority: 0)
      expect(Pgbus::Batch).to have_received(:backfill_execution).with(released[:payload], 42, "pgbus_test_default_p0")
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
      allow(Pgbus::BlockedExecution).to receive(:release_next!).and_return(released)
      allow(mock_client).to receive(:send_message).and_return(42)
      allow(Pgbus::Batch).to receive(:backfill_execution).and_raise(StandardError, "backfill failed")
      logger = instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)

      expect(described_class.promote_next("TestJob-42", client: mock_client)).to be true
      expect(logger).to have_received(:warn)
    end
  end

  describe ".promote_pending" do
    let(:mock_client) { build_mock_client }

    before { allow(Pgbus::BlockedExecution).to receive(:repair_double_encoded!).and_return(0) }

    it "heals double-encoded rows before looking for parked keys" do
      allow(Pgbus::BlockedExecution).to receive(:promotable_keys).and_return([])

      described_class.promote_pending(client: mock_client)

      expect(Pgbus::BlockedExecution).to have_received(:repair_double_encoded!).ordered
      expect(Pgbus::BlockedExecution).to have_received(:promotable_keys).ordered
    end

    # The filtering itself is SQL, pinned by the integration spec "skips keys
    # whose slots are all held". This only pins that the sweep asks for the
    # filtered set rather than every parked key.
    it "asks the model for promotable keys, not for every parked key" do
      allow(Pgbus::BlockedExecution).to receive(:promotable_keys).and_return(%w[b])
      allow(described_class).to receive(:promote_next).and_return(true, false)

      expect(described_class.promote_pending(client: mock_client)).to eq(1)
      expect(Pgbus::BlockedExecution).to have_received(:promotable_keys)
    end

    it "promotes for every parked key until its slots are full and returns the total" do
      allow(Pgbus::BlockedExecution).to receive(:promotable_keys).and_return(%w[a b])
      allow(described_class).to receive(:promote_next).with("a", client: mock_client).and_return(true, true, false)
      allow(described_class).to receive(:promote_next).with("b", client: mock_client).and_return(false)

      expect(described_class.promote_pending(client: mock_client)).to eq(2)
    end

    it "bounds the promotions per key" do
      allow(Pgbus::BlockedExecution).to receive(:promotable_keys).and_return(%w[a])
      allow(described_class).to receive(:promote_next).and_return(true)

      expect(described_class.promote_pending(client: mock_client, per_key: 5)).to eq(5)
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
