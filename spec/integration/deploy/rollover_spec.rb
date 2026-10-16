# frozen_string_literal: true

require_relative "../../integration_helper"

RSpec.describe Pgbus::Deploy::Rollover, :integration do
  let(:now) { Time.current.change(usec: 0) }

  def process(kind, revision:, heartbeat_at: now, pid: 41)
    metadata = { pid: pid }
    metadata[:revision] = revision unless revision == :none
    Pgbus::ProcessEntry.create!(kind: kind, hostname: "host-a", pid: pid, metadata: metadata,
                                last_heartbeat_at: heartbeat_at)
  end

  it "is ready when every live process runs the revision and a worker is live" do
    process("scheduler", revision: "rev-b")
    process("worker", revision: "rev-b")

    expect(described_class.check(revision: "rev-b", now: now)).to be_rolled_over
  end

  it "lags on a live worker on another revision" do
    process("worker", revision: "rev-b")
    process("worker", revision: "rev-a", pid: 42)

    status = described_class.check(revision: "rev-b", now: now)

    expect(status.reason).to eq(:lagging)
    expect(status.lagging).to eq([{ kind: "worker", hostname: "host-a", pid: 42, revision: "rev-a" }])
  end

  it "lags on a live row with no revision key" do
    process("worker", revision: "rev-b")
    process("dispatcher", revision: :none, pid: 43)

    expect(described_class.check(revision: "rev-b", now: now).lagging.first).to include(kind: "dispatcher", revision: nil)
  end

  it "ignores a stale old-revision row" do
    process("worker", revision: "rev-b")
    process("worker", revision: "rev-a", pid: 42,
                      heartbeat_at: now - Pgbus::Process::Heartbeat::ALIVE_THRESHOLD - 1)

    expect(described_class.check(revision: "rev-b", now: now)).to be_rolled_over
  end

  describe ".scheduler_revision" do
    it "reads the newest live scheduler's revision" do
      Pgbus::ProcessEntry.create!(kind: "scheduler", hostname: "h", pid: 1, metadata: { revision: "rev-a" },
                                  last_heartbeat_at: now, created_at: now - 600)
      Pgbus::ProcessEntry.create!(kind: "scheduler", hostname: "h", pid: 2, metadata: { revision: "rev-b" },
                                  last_heartbeat_at: now, created_at: now - 60)

      expect(described_class.scheduler_revision(now: now)).to eq([true, "rev-b"])
    end

    it "reports no live scheduler" do
      Pgbus::ProcessEntry.create!(kind: "scheduler", hostname: "h", pid: 1, metadata: { revision: "rev-a" },
                                  last_heartbeat_at: now - 3600)

      expect(described_class.scheduler_revision(now: now)).to eq([false, nil])
    end
  end

  it "reads the revision a real heartbeat registration stamps" do
    allow(Pgbus::Deploy::Revision).to receive(:current).and_return("rev-b")
    heartbeat = Pgbus::Process::Heartbeat.new(kind: "worker", metadata: { pid: Process.pid })
    heartbeat.start

    expect(described_class.check(revision: "rev-b", now: Time.current)).to be_rolled_over
  ensure
    heartbeat&.stop
  end
end
