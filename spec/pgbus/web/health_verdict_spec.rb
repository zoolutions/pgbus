# frozen_string_literal: true

require "spec_helper"
require "pgbus/mcp/health_analyzer"

RSpec.describe Pgbus::Web::HealthVerdict do
  let(:health) do
    { total_dead_tuples: 1_250, total_live_tuples: 65_000, worst_bloat_ratio: 0.0283, tables_needing_vacuum: 0,
      oldest_vacuum_ago_sec: 1800, oldest_transaction_age_sec: 8,
      tables: [
        { table: "pgmq.q_pgbus_default", bloat_ratio: 0.0175 },
        { table: "pgmq.a_pgbus_default", bloat_ratio: 0.0283 }
      ] }
  end

  def present(**overrides) = described_class.present(health.merge(overrides))

  describe "dead tuples" do
    it "is fine up to the warning threshold" do
      expect(present.dead_tuples).to have_attributes(level: :ok, key: "dead_tuples.ok",
                                                     args: { count: 1_250, limit: 10_000 })
    end

    it "warns above it" do
      expect(present(total_dead_tuples: 10_001).dead_tuples).to have_attributes(level: :warn, key: "dead_tuples.warn")
    end
  end

  describe "bloat" do
    it "names the worst table and both thresholds" do
      expect(present.bloat).to have_attributes(
        level: :ok, key: "bloat.ok",
        args: { ratio: 0.0283, table: "pgmq.a_pgbus_default", warn: 0.10, critical: 0.20 }
      )
    end

    it "warns above 10% and is critical above 20%" do
      tables = [{ table: "pgmq.q_pgbus_events", bloat_ratio: 0.12 }]
      expect(present(worst_bloat_ratio: 0.12, tables: tables, tables_needing_vacuum: 1).bloat)
        .to have_attributes(level: :warn, key: "bloat.warn", args: include(table: "pgmq.q_pgbus_events"))
      expect(present(worst_bloat_ratio: 0.21, tables: tables).bloat).to have_attributes(level: :critical)
      expect(present(worst_bloat_ratio: 0.10, tables: tables).bloat).to have_attributes(level: :ok)
    end

    it "is fine with no tables to measure" do
      expect(present(worst_bloat_ratio: 0.0, tables: []).bloat).to have_attributes(level: :ok, key: "bloat.none")
    end
  end

  describe "vacuum" do
    it "is fine within an hour, warns after it and is critical after a day" do
      expect(present.vacuum).to have_attributes(level: :ok, key: "vacuum.ok",
                                                args: { age: 1800, warn: 3600, critical: 86_400 })
      expect(present(oldest_vacuum_ago_sec: 3601).vacuum).to have_attributes(level: :warn, key: "vacuum.warn")
      expect(present(oldest_vacuum_ago_sec: 86_401).vacuum).to have_attributes(level: :critical, key: "vacuum.critical")
    end

    it "is unknown when no table was ever vacuumed or the stats are unavailable" do
      expect(present(oldest_vacuum_ago_sec: nil).vacuum).to have_attributes(level: :unknown, key: "vacuum.unknown")
    end
  end

  describe "transaction" do
    it "states the card thresholds and the health verdict's own" do
      expect(present.transaction).to have_attributes(level: :ok, key: "transaction.ok",
                                                     args: { age: 8, warn: 30, critical: 60, verdict: 300 })
    end

    it "warns above 30s and is critical above 60s" do
      expect(present(oldest_transaction_age_sec: 31).transaction).to have_attributes(level: :warn)
      expect(present(oldest_transaction_age_sec: 61).transaction).to have_attributes(level: :critical)
    end

    it "is fine with no open transaction" do
      expect(present(oldest_transaction_age_sec: nil).transaction).to have_attributes(level: :ok,
                                                                                      key: "transaction.none")
    end

    # The words promise the verdict turns DEGRADED past VERDICT_TXN_SEC.
    it "matches the threshold the health verdict degrades at" do
      verdict = lambda do |age|
        source = double("DataSource", queues_with_metrics: [], processes: [], stream_queue_names: Set.new,
                                      queue_health_stats: { oldest_transaction_age_sec: age })
        Pgbus::MCP::HealthAnalyzer.new(source).verdict[:status]
      end

      expect(verdict.call(described_class::VERDICT_TXN_SEC)).to eq("OK")
      expect(verdict.call(described_class::VERDICT_TXN_SEC + 1)).to eq("DEGRADED")
    end
  end
end
