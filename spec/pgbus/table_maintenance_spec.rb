# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::TableMaintenance do
  describe ".fillfactor_sql_for_queue" do
    subject(:sql) { described_class.fillfactor_sql_for_queue("pgbus_default") }

    it "returns ALTER TABLE for queue table with fillfactor" do
      expect(sql).to include("ALTER TABLE pgmq.q_pgbus_default SET (fillfactor = 70)")
    end

    it "does not alter the archive table" do
      expect(sql).not_to include("pgmq.a_pgbus_default")
    end
  end

  describe ".fillfactor_sql_for_all_queues" do
    subject(:sql) { described_class.fillfactor_sql_for_all_queues }

    it "returns a DO $$ block that iterates pgmq.meta" do
      expect(sql).to include("DO $$")
      expect(sql).to include("SELECT queue_name FROM pgmq.meta")
    end

    it "applies fillfactor to q_ tables only" do
      expect(sql).to include("'q_' || lower(q.queue_name)")
      expect(sql).to include("fillfactor = 70")
    end

    it "does not apply fillfactor to archive tables" do
      expect(sql).not_to include("'a_' ||")
    end
  end

  describe ".vacuum_candidates" do
    let(:conn) { double("connection") }

    it "returns tables exceeding the bloat threshold" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_default",
          "n_dead_tup" => 500, "n_live_tup" => 1000 },
        { "schemaname" => "pgmq", "relname" => "a_pgbus_default",
          "n_dead_tup" => 10, "n_live_tup" => 5000 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to contain_exactly(
        hash_including(table: "pgmq.q_pgbus_default", dead_ratio: be > 0.1)
      )
    end

    it "returns empty array when no tables exceed threshold" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_default",
          "n_dead_tup" => 5, "n_live_tup" => 5000 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to be_empty
    end

    it "skips tables with zero total tuples" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_empty",
          "n_dead_tup" => 0, "n_live_tup" => 0 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      candidates = described_class.vacuum_candidates(conn, threshold: 0.1)
      expect(candidates).to be_empty
    end
  end

  describe ".vacuum_sql" do
    it "generates VACUUM with quoted identifiers" do
      sql = described_class.vacuum_sql("pgmq.q_pgbus_default")
      expect(sql).to eq('VACUUM "pgmq"."q_pgbus_default"')
    end
  end

  describe ".reindex_sql" do
    it "generates REINDEX TABLE CONCURRENTLY with quoted identifiers" do
      sql = described_class.reindex_sql("pgmq.q_pgbus_default")
      expect(sql).to eq('REINDEX TABLE CONCURRENTLY "pgmq"."q_pgbus_default"')
    end
  end

  describe ".run_maintenance" do
    let(:conn) { double("connection") }

    it "continues processing when one table fails" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_first",
          "n_dead_tup" => 500, "n_live_tup" => 1000 },
        { "schemaname" => "pgmq", "relname" => "q_second",
          "n_dead_tup" => 500, "n_live_tup" => 1000 }
      ]
      allow(conn).to receive(:exec).with(/pg_stat_user_tables/).and_return(rows)

      call_count = 0
      allow(conn).to receive(:exec) do |sql|
        next rows if sql.include?("pg_stat_user_tables")

        if sql.include?("q_first")
          call_count += 1
          raise StandardError, "permission denied"
        end
        call_count += 1
      end

      maintained = described_class.run_maintenance(conn, threshold: 0.1, reindex: false)
      expect(maintained).to eq(1)
      expect(call_count).to be >= 2
    end

    it "returns count of successfully maintained tables" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_ok",
          "n_dead_tup" => 500, "n_live_tup" => 1000 }
      ]
      allow(conn).to receive(:exec).and_return(rows)

      maintained = described_class.run_maintenance(conn, threshold: 0.1, reindex: false)
      expect(maintained).to eq(1)
    end

    it "executes REINDEX TABLE CONCURRENTLY when reindex is true" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_pgbus_default",
          "n_dead_tup" => 500, "n_live_tup" => 1000 }
      ]
      reindexed = false
      allow(conn).to receive(:exec) do |sql|
        next rows if sql.include?("pg_stat_user_tables")

        reindexed = true if sql.include?("REINDEX TABLE CONCURRENTLY")
      end

      described_class.run_maintenance(conn, threshold: 0.1, reindex: true)
      expect(reindexed).to be true
    end

    it "continues with other tables when reindex fails on one" do
      rows = [
        { "schemaname" => "pgmq", "relname" => "q_first",
          "n_dead_tup" => 500, "n_live_tup" => 1000 },
        { "schemaname" => "pgmq", "relname" => "q_second",
          "n_dead_tup" => 500, "n_live_tup" => 1000 }
      ]
      allow(conn).to receive(:exec) do |sql|
        next rows if sql.include?("pg_stat_user_tables")
        next if sql.include?("VACUUM")

        raise StandardError, "reindex failed" if sql.include?("q_first")
      end

      maintained = described_class.run_maintenance(conn, threshold: 0.1, reindex: true)
      expect(maintained).to eq(1)
    end

    context "with a stop_check callable" do
      let(:two_candidates) do
        [
          { "schemaname" => "pgmq", "relname" => "q_first",
            "n_dead_tup" => 500, "n_live_tup" => 1000 },
          { "schemaname" => "pgmq", "relname" => "q_second",
            "n_dead_tup" => 500, "n_live_tup" => 1000 }
        ]
      end

      it "stops before the next table when stop_check returns true" do
        vacuumed = []
        allow(conn).to receive(:exec) do |sql|
          next two_candidates if sql.include?("pg_stat_user_tables")

          vacuumed << sql if sql.include?("VACUUM")
        end

        # Stop after the first candidate is maintained.
        calls = 0
        stop_check = lambda do
          calls += 1
          calls > 1
        end

        maintained = described_class.run_maintenance(conn, threshold: 0.1, reindex: false, stop_check: stop_check)

        expect(maintained).to eq(1)
        expect(vacuumed.size).to eq(1)
        expect(vacuumed.first).to include("q_first")
      end

      it "logs a single summary line when interrupted" do
        allow(conn).to receive(:exec) do |sql|
          next two_candidates if sql.include?("pg_stat_user_tables")
        end
        logged = []
        logger = instance_double(Logger, warn: nil, error: nil, debug: nil)
        allow(logger).to receive(:info) { |&block| logged << block.call }
        allow(Pgbus).to receive(:logger).and_return(logger)

        calls = 0
        stop_check = lambda do
          calls += 1
          calls > 1
        end

        described_class.run_maintenance(conn, threshold: 0.1, reindex: false, stop_check: stop_check)

        summaries = logged.grep(/interrupted by shutdown after 1 of 2/)
        expect(summaries.size).to eq(1)
      end

      it "maintains every table when stop_check never fires" do
        vacuumed = []
        allow(conn).to receive(:exec) do |sql|
          next two_candidates if sql.include?("pg_stat_user_tables")

          vacuumed << sql if sql.include?("VACUUM")
        end

        maintained = described_class.run_maintenance(
          conn, threshold: 0.1, reindex: false, stop_check: -> { false }
        )

        expect(maintained).to eq(2)
        expect(vacuumed.size).to eq(2)
      end
    end
  end

  describe "FILLFACTOR" do
    it "is set to 70" do
      expect(described_class::FILLFACTOR).to eq(70)
    end

    it "leaves room for update churn (must be < 100)" do
      expect(described_class::FILLFACTOR).to be < 100
      expect(described_class::FILLFACTOR).to be >= 50
    end
  end

  describe "BLOAT_THRESHOLD" do
    it "defaults to 10% dead tuple ratio" do
      expect(described_class::BLOAT_THRESHOLD).to eq(0.1)
    end
  end
end
