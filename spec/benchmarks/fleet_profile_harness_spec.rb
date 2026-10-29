# frozen_string_literal: true

require "spec_helper"
require_relative "../../benchmarks/support/fleet_profile_harness"

# Pure-logic specs for the fleet cell of the worker profiling bench (issue
# #602): N worker processes draining one queue on one Postgres. No database and
# no subprocess: the knob parsing, the connection budget, the throughput and
# pg_stat_statements arithmetic and the table are fed hand-built values.
RSpec.describe FleetProfileHarness do
  describe ".parse_processes" do
    it "parses a comma-separated list of process counts" do
      expect(described_class.parse_processes("1,2,4,8")).to eq([1, 2, 4, 8])
      expect(described_class.parse_processes(" 1, 2 ")).to eq([1, 2])
    end

    it "defaults to 1,2,4 (8 is opt-in: it does not fit a stock max_connections)" do
      expect(described_class.parse_processes(described_class::DEFAULT_PROCESSES)).to eq([1, 2, 4])
    end

    it "rejects zero, negatives, non-integers and an empty list" do
      ["0", "1,0", "-2", "two", "1.5", "", "1,,2"].each do |value|
        expect { described_class.parse_processes(value) }
          .to raise_error(ArgumentError, /WP_BENCH_PROCESSES/), "expected #{value.inspect} to be rejected"
      end
    end
  end

  describe ".free_connections" do
    it "is max_connections minus the superuser reserve and the client backends already connected" do
      expect(described_class.free_connections(max_connections: 100, reserved: 3, in_use: 9)).to eq(88)
    end
  end

  describe ".budget_skip_reason" do
    it "is nil when processes × pool_size plus headroom fits the free connections" do
      expect(described_class.budget_skip_reason(processes: 4, pool_size: 14, free_connections: 88)).to be_nil
    end

    it "explains the skip when it does not fit" do
      reason = described_class.budget_skip_reason(processes: 8, pool_size: 14, free_connections: 88)

      expect(reason).to eq("skipped: needs 8 × 14 + 10 = 122 connections, 88 free")
    end
  end

  describe ".throughput" do
    let(:children) do
      [
        { "processed" => 600, "started_at" => 100.002, "finished_at" => 101.5 },
        { "processed" => 400, "started_at" => 100.010, "finished_at" => 102.0 }
      ]
    end

    it "divides the total by the time from the parent's go to the last child's last job" do
      result = described_class.throughput(children, go_at: 100.0)

      expect(result[:processed]).to eq(1000)
      expect(result[:wall_s]).to be_within(1e-9).of(2.0)
      expect(result[:jobs_per_s]).to be_within(1e-9).of(500.0)
    end

    it "reports how far apart the children started, in ms, to prove the barrier held" do
      expect(described_class.throughput(children, go_at: 100.0)[:start_skew_ms]).to be_within(1e-6).of(8.0)
    end
  end

  describe ".summarize_statements" do
    let(:rows) do
      [
        { "query" => "SELECT * FROM pgmq.read($1, $2, $3)", "calls" => 200, "total_exec_time" => 400.0 },
        { "query" => "SELECT pgmq.archive($1, $2)", "calls" => 1000, "total_exec_time" => 300.0 },
        { "query" => "INSERT INTO pgbus_job_stats (job_class) VALUES ($1)", "calls" => 10, "total_exec_time" => 100.0 }
      ]
    end

    it "reports PG ms per job, statements per job and the average busy backends over the wall" do
      summary = described_class.summarize_statements(rows, jobs: 1000, wall_s: 2.0)

      expect(summary[:available]).to be(true)
      expect(summary[:ms_per_job]).to be_within(1e-9).of(0.8)
      expect(summary[:statements_per_job]).to be_within(1e-9).of(1.21)
      expect(summary[:busy_backends]).to be_within(1e-9).of(0.4)
    end

    it "lists the top statements by total time with ms per call" do
      top = described_class.summarize_statements(rows, jobs: 1000, wall_s: 2.0)[:top]

      expect(top.first).to include("query" => "SELECT * FROM pgmq.read($1, $2, $3)", "calls" => 200)
      expect(top.first["ms_per_call"]).to be_within(1e-9).of(2.0)
      expect(top.size).to eq(3)
    end

    it "keeps at most five top statements" do
      many = Array.new(8) { |i| { "query" => "q#{i}", "calls" => 1, "total_exec_time" => 8.0 - i } }

      expect(described_class.summarize_statements(many, jobs: 10, wall_s: 1.0)[:top].size).to eq(5)
    end
  end

  describe ".fleet_table" do
    def cell(**overrides)
      {
        "role" => "fleet", "location" => "local", "jit" => "yjit", "processes" => 2, "jobs" => 10_000,
        "wall_s" => 1.0, "jobs_per_s" => 10_000.0, "start_skew_ms" => 1.3,
        "pg_ms_per_job" => 0.123, "pg_statements_per_job" => 1.5, "pg_busy_backends" => 1.23,
        "pg_top" => [{ "query" => "SELECT pgmq.archive($1, $2)", "calls" => 10_000, "ms_per_call" => 0.05 }]
      }.merge(overrides)
    end

    it "renders one row per process count with jobs/s, per-process jobs/s and the PG cost" do
      lines = described_class.fleet_table([cell])

      expect(lines.first).to start_with("| cell | processes | jobs/s | jobs/s per process |")
      expect(lines.last).to eq("| fleet / local / yjit | 2 | 10000 | 5000 | 0.100 | 0.123 | 1.50 | 1.23 | 1.3 ms | " \
                               "SELECT pgmq.archive($1, $2) |")
    end

    it "renders n/a for the PG columns when pg_stat_statements is unavailable" do
      row = described_class.fleet_table([cell("pg_ms_per_job" => nil, "pg_statements_per_job" => nil,
                                              "pg_busy_backends" => nil, "pg_top" => [])]).last

      expect(row).to eq("| fleet / local / yjit | 2 | 10000 | 5000 | 0.100 | n/a | n/a | n/a | 1.3 ms | n/a |")
    end

    it "renders a skipped row with its reason" do
      row = described_class.fleet_table([cell("processes" => 8, "skip_reason" => "skipped: needs 122, 88 free")]).last

      expect(row).to eq("| fleet / local / yjit | 8 | skipped: needs 122, 88 free | | | | | | | |")
    end

    it "truncates a long top statement and flattens its whitespace" do
      long = "SELECT  *\n FROM pgmq.read(#{"x" * 100})"
      row = described_class.fleet_table([cell("pg_top" => [{ "query" => long, "calls" => 1, "ms_per_call" => 1 }])]).last

      expect(row).to end_with("| SELECT * FROM pgmq.read(#{"x" * 36}… |")
    end
  end

  describe ".wait_shares" do
    it "averages the active backends per sample and splits them by wait event type" do
      summary = described_class.wait_shares({ "CPU" => 6, "LWLock" => 3, "Lock" => 1 }, samples: 4)

      expect(summary["avg_active"]).to be_within(1e-9).of(2.5)
      expect(summary["shares"]).to eq("CPU" => 0.6, "LWLock" => 0.3, "Lock" => 0.1)
    end

    it "is zero active and no shares when nothing was sampled" do
      expect(described_class.wait_shares({}, samples: 0)).to eq("avg_active" => 0.0, "shares" => {})
    end
  end

  describe ".statement_lines" do
    it "leads each cell with the sampled backend wait split when there is one" do
      cell = { "role" => "fleet", "location" => "local", "jit" => "none", "processes" => 4, "pg_top" => [],
               "pg_waits" => { "avg_active" => 4.86, "shares" => { "CPU" => 0.75, "LWLock" => 0.25 } } }

      expect(described_class.statement_lines([cell]))
        .to eq(["fleet / local / none, 4 process(es): 4.9 active backends sampled, CPU 75%, LWLock 25%"])
    end

    it "lists each cell's top statements with ms per call" do
      cell = { "role" => "fleet", "location" => "local", "jit" => "none", "processes" => 1,
               "pg_top" => [{ "query" => "SELECT pgmq.archive($1, $2)", "calls" => 7, "ms_per_call" => 0.0512 }] }

      expect(described_class.statement_lines([cell])).to eq(
        ["fleet / local / none, 1 process(es):", "  0.051 ms/call × 7  SELECT pgmq.archive($1, $2)"]
      )
    end

    it "says why there is nothing when pg_stat_statements is unavailable" do
      cell = { "role" => "fleet", "location" => "local", "jit" => "none", "processes" => 1, "pg_top" => [],
               "pg_unavailable_reason" => "add it to shared_preload_libraries" }

      expect(described_class.statement_lines([cell]))
        .to eq(["fleet / local / none, 1 process(es): pg_stat_statements: n/a (add it to shared_preload_libraries)"])
    end
  end
end
