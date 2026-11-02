# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::AutovacuumTuning do
  describe ".sql_for_queue" do
    subject(:sql) { described_class.sql_for_queue("pgbus_default") }

    it "returns ALTER TABLE statements for both queue and archive tables" do
      expect(sql).to include("ALTER TABLE pgmq.q_pgbus_default SET")
      expect(sql).to include("ALTER TABLE pgmq.a_pgbus_default SET")
    end

    it "sets queue table to aggressive vacuum scale factor" do
      expect(sql).to include("autovacuum_vacuum_scale_factor = 0.01")
    end

    it "sets archive table to moderate vacuum scale factor" do
      expect(sql).to include("autovacuum_vacuum_scale_factor = 0.05")
    end

    it "sets cost delay for both tables" do
      expect(sql).to include("autovacuum_vacuum_cost_delay = 2")
      expect(sql).to include("autovacuum_vacuum_cost_delay = 5")
    end
  end

  describe ".sql_for_all_queues" do
    subject(:sql) { described_class.sql_for_all_queues }

    it "returns a DO $$ block that iterates pgmq.meta" do
      expect(sql).to include("DO $$")
      expect(sql).to include("SELECT queue_name FROM pgmq.meta")
    end

    it "applies queue settings to q_ tables" do
      expect(sql).to include("'q_' || lower(q.queue_name)")
      expect(sql).to include("autovacuum_vacuum_scale_factor = 0.01")
    end

    it "applies archive settings to a_ tables" do
      expect(sql).to include("'a_' || lower(q.queue_name)")
      expect(sql).to include("autovacuum_vacuum_scale_factor = 0.05")
    end
  end

  describe "QUEUE_SETTINGS" do
    it "includes the three required autovacuum parameters" do
      expect(described_class::QUEUE_SETTINGS).to include(
        "autovacuum_vacuum_scale_factor" => "0.01",
        "autovacuum_vacuum_cost_delay" => "2",
        "autovacuum_analyze_scale_factor" => "0.05"
      )
    end
  end

  describe "ARCHIVE_SETTINGS" do
    it "uses a less aggressive scale factor than queue tables" do
      queue_sf = described_class::QUEUE_SETTINGS["autovacuum_vacuum_scale_factor"].to_f
      archive_sf = described_class::ARCHIVE_SETTINGS["autovacuum_vacuum_scale_factor"].to_f
      expect(archive_sf).to be > queue_sf
    end
  end

  describe ".sql_for_high_churn_tables" do
    subject(:sql) { described_class.sql_for_high_churn_tables }

    it "includes ALTER TABLE IF EXISTS for all high-churn tables" do
      expect(sql).to include("ALTER TABLE IF EXISTS pgbus_semaphores SET")
      expect(sql).to include("ALTER TABLE IF EXISTS pgbus_uniqueness_keys SET")
      expect(sql).to include("ALTER TABLE IF EXISTS pgbus_processed_events SET")
    end

    it "applies the high-churn settings" do
      expect(sql).to include("autovacuum_vacuum_scale_factor = 0.02")
      expect(sql).to include("autovacuum_vacuum_cost_delay = 2")
    end
  end

  describe "HIGH_CHURN_TABLES" do
    it "lists the three highest-churn pgbus tables" do
      expect(described_class::HIGH_CHURN_TABLES).to contain_exactly(
        "pgbus_semaphores",
        "pgbus_uniqueness_keys",
        "pgbus_processed_events"
      )
    end
  end

  describe "HIGH_CHURN_SETTINGS" do
    it "is less aggressive than queue settings but more than archive settings" do
      queue_sf = described_class::QUEUE_SETTINGS["autovacuum_vacuum_scale_factor"].to_f
      high_sf = described_class::HIGH_CHURN_SETTINGS["autovacuum_vacuum_scale_factor"].to_f
      archive_sf = described_class::ARCHIVE_SETTINGS["autovacuum_vacuum_scale_factor"].to_f

      expect(high_sf).to be > queue_sf
      expect(high_sf).to be < archive_sf
    end
  end
end
