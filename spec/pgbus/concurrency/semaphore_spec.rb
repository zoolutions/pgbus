# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Concurrency::Semaphore do
  describe ".acquire" do
    it "returns :acquired when a slot is available" do
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:acquired)

      expect(described_class.acquire("TestJob-42", 2, 900)).to eq(:acquired)
      expect(Pgbus::Semaphore).to have_received(:acquire!).with("TestJob-42", 2, a_kind_of(Time))
    end

    it "returns :blocked when limit is reached" do
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:blocked)

      expect(described_class.acquire("TestJob-42", 1, 900)).to eq(:blocked)
    end
  end

  describe ".release" do
    it "decrements the semaphore value" do
      scope = double("scope", update_all: 1)
      allow(Pgbus::Semaphore).to receive(:where).with(key: "TestJob-42").and_return(scope)

      described_class.release("TestJob-42")

      expect(scope).to have_received(:update_all).with("value = GREATEST(value - 1, 0)")
    end
  end

  describe ".signal" do
    let(:mock_client) { build_mock_client }

    # The decrement takes the semaphore row lock first, so a concurrent
    # enqueue that saw the slot taken and is parking its job must commit
    # before the promote looks for parked rows (rails/solid_queue#712).
    it "releases the slot and promotes the next parked job inside one transaction" do
      order = []
      allow(Pgbus::Semaphore).to receive(:transaction) do |&block|
        order << :begin
        block.call
        order << :commit
      end
      allow(described_class).to receive(:release) { order << :release }
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next) do
        order << :promote
        true
      end

      described_class.signal("TestJob-42", client: mock_client)

      expect(order).to eq(%i[begin release promote commit])
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
    end
  end

  describe ".touch" do
    # The acquire path floors the lease, but a raw duration here would renew
    # it for less than the gap to the next beat — the lease then lapses
    # mid-run and the sweep promotes beside a running job.
    it "renews for at least the floored duration, never the raw one" do
      scope = double("scope", update_all: 1)
      pool = double("pool")
      allow(pool).to receive(:with_connection).and_yield
      allow(Pgbus::Semaphore).to receive_messages(connection_pool: pool)
      allow(Pgbus::Semaphore).to receive(:where).with(key: "TestJob-42").and_return(scope)
      allow(Pgbus.configuration).to receive(:effective_visibility_heartbeat_interval).and_return(10)

      described_class.touch("TestJob-42", 5)

      expect(scope).to have_received(:update_all) do |(_sql, expires_at)|
        expect(expires_at - Time.current).to be_within(2).of(20)
      end
    end

    # Waiting on a busy pool must not eat the lease: computing the expiry
    # before the checkout means the UPDATE writes a lease that has already
    # been partly consumed.
    it "starts the lease when the connection is in hand, not when the wait began" do
      scope = double("scope", update_all: 1)
      pool = double("pool")
      yielded_at = nil
      allow(pool).to receive(:with_connection) do |&block|
        sleep 0.2
        yielded_at = Time.current
        block.call
      end
      allow(Pgbus::Semaphore).to receive_messages(connection_pool: pool)
      allow(Pgbus::Semaphore).to receive(:where).with(key: "TestJob-42").and_return(scope)

      described_class.touch("TestJob-42", 600)

      expect(scope).to have_received(:update_all) do |(_sql, expires_at)|
        expect(expires_at).to be >= yielded_at + 600 - 0.05
      end
    end

    it "pushes the expiry out from now, never pulling it in" do
      scope = double("scope", update_all: 1)
      pool = double("pool")
      allow(pool).to receive(:with_connection).and_yield
      allow(Pgbus::Semaphore).to receive_messages(connection_pool: pool)
      allow(Pgbus::Semaphore).to receive(:where).with(key: "TestJob-42").and_return(scope)

      described_class.touch("TestJob-42", 600)

      expect(scope).to have_received(:update_all) do |(sql, expires_at)|
        expect(sql).to include("GREATEST(expires_at, ?)")
        expect(expires_at - Time.current).to be_within(5).of(600)
      end
    end
  end

  describe ".expire_stale" do
    it "deletes expired semaphores and returns their keys" do
      result = double("result", rows: [["old-key-1"], ["old-key-2"]])
      connection = double("connection")
      allow(Pgbus::Semaphore).to receive(:connection).and_return(connection)
      allow(connection).to receive(:exec_query).and_return(result)

      expired = described_class.expire_stale

      expect(expired).to eq([{ "key" => "old-key-1" }, { "key" => "old-key-2" }])
    end
  end

  describe ".current_value" do
    it "returns the current value for a key" do
      allow(Pgbus::Semaphore).to receive_message_chain(:where, :pick).and_return(3) # rubocop:disable RSpec/MessageChain

      expect(described_class.current_value("TestJob-42")).to eq(3)
    end

    it "returns nil when key does not exist" do
      allow(Pgbus::Semaphore).to receive_message_chain(:where, :pick).and_return(nil) # rubocop:disable RSpec/MessageChain

      expect(described_class.current_value("missing")).to be_nil
    end
  end
end
