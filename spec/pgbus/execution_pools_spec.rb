# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ExecutionPools do
  describe ".build" do
    it "returns a ThreadPool for :threads mode" do
      pool = described_class.build(mode: :threads, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::ThreadPool)
      pool.kill
    end

    it "returns an AsyncPool for :async mode" do
      pool = described_class.build(mode: :async, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::AsyncPool)
      pool.shutdown
      pool.wait_for_termination(2)
    end

    it "returns an AsyncPool for :fiber mode (alias)" do
      pool = described_class.build(mode: :fiber, capacity: 3)
      expect(pool).to be_a(Pgbus::ExecutionPools::AsyncPool)
      pool.shutdown
      pool.wait_for_termination(2)
    end

    it "raises ArgumentError for unknown mode" do
      expect { described_class.build(mode: :unknown, capacity: 3) }
        .to raise_error(ArgumentError, /unknown execution_mode/i)
    end
  end

  describe ".normalize_mode" do
    it "normalizes :fiber to :async" do
      expect(described_class.normalize_mode(:fiber)).to eq(:async)
    end

    it "passes :threads through" do
      expect(described_class.normalize_mode(:threads)).to eq(:threads)
    end

    it "passes :async through" do
      expect(described_class.normalize_mode(:async)).to eq(:async)
    end

    it "accepts string modes" do
      expect(described_class.normalize_mode("threads")).to eq(:threads)
      expect(described_class.normalize_mode("async")).to eq(:async)
    end

    it "raises for unknown modes" do
      expect { described_class.normalize_mode(:bogus) }
        .to raise_error(ArgumentError)
    end
  end

  # Immediate shutdown (issue #527): the pools' real behaviour is in
  # spec/pgbus/process/immediate_shutdown_spec.rb; this pins the order of the
  # steps with a pool double.
  describe ".terminate" do
    let(:calls) { [] }
    let(:pool) do
      log = calls
      double("execution pool").tap do |pool|
        allow(pool).to receive(:shutdown) { log << :shutdown }
        allow(pool).to receive(:kill) { log << :kill }
      end
    end

    before do
      log = calls
      allow(described_class).to receive(:reclaim_connections) { log << :reclaim }
    end

    def wait_returns(finished)
      log = calls
      allow(pool).to receive(:wait_for_termination).with(2) do
        log << :wait
        finished
      end
    end

    it "stops the pool, waits the grace, and reclaims the connections when every job finished" do
      wait_returns(true)

      expect(described_class.terminate(pool, grace: 2)).to be(true)
      expect(calls).to eq(%i[shutdown wait reclaim])
    end

    it "kills what still runs after the grace before it reclaims the connections" do
      wait_returns(false)

      expect(described_class.terminate(pool, grace: 2)).to be(false)
      expect(calls).to eq(%i[shutdown wait kill reclaim])
    end

    # An async pool's wait re-raises a fatal reactor error.
    it "reclaims the connections also when the wait raises" do
      allow(pool).to receive(:wait_for_termination).and_raise(RuntimeError, "reactor died")

      expect { described_class.terminate(pool, grace: 2) }.to raise_error(RuntimeError, "reactor died")
      expect(calls).to eq(%i[shutdown reclaim])
    end
  end
end
