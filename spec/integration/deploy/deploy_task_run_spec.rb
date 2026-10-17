# frozen_string_literal: true

require_relative "../../integration_helper"

RSpec.describe Pgbus::DeployTaskRun, :integration do
  let(:now) { Time.utc(2026, 10, 16, 12, 0, 0) }

  def run(key, version: "1", revision: "rev-a", triggered_by: "rollover", run_at: now)
    described_class.create!(task_key: key, version: version, revision: revision,
                            triggered_by: triggered_by, run_at: run_at)
  end

  describe ".last_rollover_for" do
    it "returns the newest rollover run by id, ignoring manual runs" do
      run("backfill", revision: "rev-a")
      newest = run("backfill", revision: "rev-b", run_at: now - 60)
      run("backfill", revision: "rev-c", triggered_by: "manual")
      run("other", revision: "rev-d")

      expect(described_class.last_rollover_for("backfill")).to eq(newest)
    end

    it "is nil without a rollover run" do
      run("backfill", triggered_by: "manual")

      expect(described_class.last_rollover_for("backfill")).to be_nil
    end
  end

  describe ".latest_for" do
    it "returns the newest run per key and trigger" do
      run("a", revision: "old")
      newest_rollover = run("a", revision: "new")
      manual = run("a", triggered_by: "manual")
      other = run("b")
      run("c")

      expect(described_class.latest_for(%w[a b]).to_a).to contain_exactly(newest_rollover, manual, other)
    end
  end

  describe ".rollover_run_for?" do
    it "is true only for a rollover run of that key and version" do
      run("backfill", version: "2")
      run("backfill", version: "3", triggered_by: "manual")

      expect(described_class.rollover_run_for?("backfill", "2")).to be(true)
      expect(described_class.rollover_run_for?("backfill", "3")).to be(false)
      expect(described_class.rollover_run_for?("other", "2")).to be(false)
    end
  end

  describe ".older_than" do
    it "selects runs before the cutoff" do
      old = run("backfill", run_at: now - (100 * 86_400))
      run("backfill", run_at: now)

      expect(described_class.older_than(now - (90 * 86_400))).to eq([old])
    end
  end

  describe ".prunable_before" do
    it "keeps the newest rollover run per (task_key, version) however old" do
      cutoff = now - (90 * 86_400)
      old = now - (100 * 86_400)
      superseded = run("a", version: "1", run_at: old)
      kept = run("a", version: "1", run_at: old)
      other_version = run("a", version: "2", run_at: old)
      manual = run("a", version: "2", triggered_by: "manual", run_at: old)
      run("b", run_at: now)

      expect(described_class.prunable_before(cutoff)).to contain_exactly(superseded, manual)
      expect([kept, other_version]).to all(satisfy { |r| described_class.exists?(r.id) })
    end
  end

  describe ".recent" do
    it "returns the newest runs first, limited" do
      runs = Array.new(3) { |i| run("backfill", run_at: now + i) }

      expect(described_class.recent(2)).to eq(runs.last(2).reverse)
    end
  end
end
