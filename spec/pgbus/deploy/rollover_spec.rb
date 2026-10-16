# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Deploy::Rollover do
  def row(kind, revision: :none, hostname: "host-a", pid: 41)
    metadata = revision == :none ? { "pid" => pid } : { "pid" => pid, "revision" => revision }
    { kind: kind, hostname: hostname, pid: pid, metadata: metadata }
  end

  describe ".evaluate" do
    it "is ready when every live row is on the revision and a worker is live" do
      status = described_class.evaluate([row("scheduler", revision: "b"), row("worker", revision: "b")], revision: "b")

      expect(status).to have_attributes(reason: :ready, workers_live: 1, lagging: [])
      expect(status).to be_rolled_over
    end

    it "lags on a worker that reports another revision" do
      status = described_class.evaluate(
        [row("worker", revision: "b"), row("worker", revision: "a", hostname: "host-z", pid: 7)], revision: "b"
      )

      expect(status.reason).to eq(:lagging)
      expect(status.lagging).to eq([{ kind: "worker", hostname: "host-z", pid: 7, revision: "a" }])
      expect(status).not_to be_rolled_over
    end

    it "lags on a live row with no revision key (a pre-upgrade process)" do
      status = described_class.evaluate([row("worker", revision: "b"), row("dispatcher")], revision: "b")

      expect(status.reason).to eq(:lagging)
      expect(status.lagging.first).to include(kind: "dispatcher", revision: nil)
    end

    it "waits for a worker when only a scheduler is live" do
      status = described_class.evaluate([row("scheduler", revision: "b")], revision: "b")

      expect(status).to have_attributes(reason: :no_workers, workers_live: 0)
    end

    it "is closed without a revision" do
      status = described_class.evaluate([row("worker", revision: "b")], revision: nil)

      expect(status).to have_attributes(reason: :no_revision, revision: nil)
      expect(status).not_to be_rolled_over
    end

    it "counts live processes per revision" do
      rows = [row("worker", revision: "b"), row("worker", revision: "b"), row("scheduler", revision: "a"), row("worker")]

      expect(described_class.evaluate(rows, revision: "b").live_by_revision).to eq("b" => 2, "a" => 1, nil => 1)
    end

    it "reads JSON-string metadata and tolerates a missing hash" do
      rows = [{ kind: "worker", hostname: "h", pid: 1, metadata: '{"revision":"b"}' },
              { kind: "supervisor", hostname: "h", pid: 2, metadata: nil }]

      status = described_class.evaluate(rows, revision: "b")

      expect(status.workers_live).to eq(1)
      expect(status.lagging).to eq([{ kind: "supervisor", hostname: "h", pid: 2, revision: nil }])
    end
  end

  describe ".check" do
    let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }
    let(:scope) { double("live scope") }

    it "evaluates the rows heartbeating within the alive threshold" do
      cutoff = now - Pgbus::Process::Heartbeat::ALIVE_THRESHOLD
      allow(Pgbus::ProcessEntry).to receive(:where).with(last_heartbeat_at: cutoff..).and_return(scope)
      allow(scope).to receive(:pluck).with(:kind, :hostname, :pid, :metadata)
                                     .and_return([["worker", "h", 1, { "revision" => "b" }]])

      expect(described_class.check(revision: "b", now: now)).to be_rolled_over
    end
  end
end
