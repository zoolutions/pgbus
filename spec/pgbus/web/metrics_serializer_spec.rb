# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::MetricsSerializer do
  subject(:serializer) { described_class.new(data_source) }

  let(:data_source) { instance_double(Pgbus::Web::DataSource) }

  let(:queue_metrics) do
    [
      { name: "pgbus_default", queue_length: 42, queue_visible_length: 40,
        total_messages: 1000, oldest_msg_age_sec: 120, oldest_claimable_age_sec: 90,
        newest_msg_age_sec: 1, paused: false },
      { name: "pgbus_default_dlq", queue_length: 3, queue_visible_length: 3,
        total_messages: 50, oldest_msg_age_sec: 3600, oldest_claimable_age_sec: 3600,
        newest_msg_age_sec: 60, paused: false },
      { name: "pgbus_critical", queue_length: 0, queue_visible_length: 0,
        total_messages: 500, oldest_msg_age_sec: nil, oldest_claimable_age_sec: nil,
        newest_msg_age_sec: nil, paused: true }
    ]
  end

  let(:job_status_counts) { { "success" => 150, "failed" => 5, "dead_lettered" => 1 } }

  let(:job_summary) do
    {
      total: 156, success: 150, failed: 5, dead_lettered: 1,
      avg_duration_ms: 42.5, max_duration_ms: 500
    }
  end

  let(:job_summary_with_latency) do
    job_summary.merge(
      avg_latency_ms: 12.3, p50_latency_ms: 10.0,
      p95_latency_ms: 25.0, p99_latency_ms: 50.0,
      avg_retries: 0.3
    )
  end

  let(:processes) do
    [
      { pid: 1, kind: "worker", hostname: "host1",
        metadata: { "capacity" => 5, "busy" => 3, "mode" => "threads" } },
      { pid: 2, kind: "worker", hostname: "host1",
        metadata: { "capacity" => 10, "busy" => 0, "mode" => "threads" } }
    ]
  end

  let(:summary_stats) do
    { total_queues: 3, total_depth: 45, total_visible: 43,
      active_processes: 2, failed_count: 8, dlq_depth: 3,
      recurring_count: 4, throughput_rate: 10.5,
      parked_total: 7, oldest_parked_age_sec: 812, slots_held: 3, keys_at_limit: 2 }
  end

  let(:health_stats) do
    {
      total_dead_tuples: 500,
      total_live_tuples: 10_000,
      worst_bloat_ratio: 0.15,
      tables_needing_vacuum: 1,
      oldest_vacuum_ago_sec: 3600,
      oldest_transaction_age_sec: 12,
      tables: [
        { table: "pgmq.q_pgbus_default", kind: "queue",
          live_tuples: 8000, dead_tuples: 400, bloat_ratio: 0.0476,
          last_vacuum_ago_sec: 120, last_vacuum: "2026-04-10", last_autovacuum: nil },
        { table: "pgmq.a_pgbus_default", kind: "archive",
          live_tuples: 2000, dead_tuples: 100, bloat_ratio: 0.0476,
          last_vacuum_ago_sec: 3600, last_vacuum: nil, last_autovacuum: "2026-04-10" }
      ]
    }
  end

  before do
    allow(data_source).to receive_messages(
      queues_with_metrics: queue_metrics,
      job_status_counts: job_status_counts,
      job_stats_summary: job_summary,
      processes: processes,
      summary_stats: summary_stats,
      stream_stats_available?: false,
      queue_health_stats: health_stats
    )
  end

  describe "#serialize" do
    let(:output) { serializer.serialize }

    it "returns a string" do
      expect(output).to be_a(String)
    end

    it "includes queue depth gauges with queue label" do
      expect(output).to include('pgbus_queue_depth{queue="pgbus_default"} 42')
      expect(output).to include('pgbus_queue_depth{queue="pgbus_default_dlq"} 3')
      expect(output).to include('pgbus_queue_depth{queue="pgbus_critical"} 0')
    end

    it "includes queue visible depth gauges" do
      expect(output).to include('pgbus_queue_visible_depth{queue="pgbus_default"} 40')
    end

    it "includes queue total messages gauges" do
      expect(output).to include('pgbus_queue_total_messages{queue="pgbus_default"} 1000')
    end

    it "includes queue oldest message age" do
      expect(output).to include('pgbus_queue_oldest_message_age_seconds{queue="pgbus_default"} 120')
    end

    it "omits oldest message age when nil" do
      expect(output).not_to include('pgbus_queue_oldest_message_age_seconds{queue="pgbus_critical"}')
    end

    it "includes vt-aware oldest claimable age" do
      expect(output).to include('pgbus_queue_oldest_claimable_age_seconds{queue="pgbus_default"} 90')
    end

    it "omits oldest claimable age when no claimable backlog exists" do
      expect(output).not_to include('pgbus_queue_oldest_claimable_age_seconds{queue="pgbus_critical"}')
    end

    it "includes queue paused gauge (1 for paused, 0 for active)" do
      expect(output).to include('pgbus_queue_paused{queue="pgbus_default"} 0')
      expect(output).to include('pgbus_queue_paused{queue="pgbus_critical"} 1')
    end

    it "includes job status counts with status label" do
      expect(output).to include('pgbus_jobs_total{status="success"} 150')
      expect(output).to include('pgbus_jobs_total{status="failed"} 5')
      expect(output).to include('pgbus_jobs_total{status="dead_lettered"} 1')
    end

    it "includes job duration summary" do
      expect(output).to include("pgbus_job_duration_avg_seconds 0.0425")
      expect(output).to include("pgbus_job_duration_max_seconds 0.5")
    end

    it "includes active processes gauge" do
      expect(output).to include("pgbus_active_processes 2")
    end

    it "includes failed events total" do
      expect(output).to include("pgbus_failed_events_total 8")
    end

    it "includes DLQ depth" do
      expect(output).to include("pgbus_dlq_depth 3")
    end

    it "includes HELP and TYPE comments for each metric family" do
      expect(output).to include("# HELP pgbus_queue_depth")
      expect(output).to include("# TYPE pgbus_queue_depth gauge")
      expect(output).to include("# HELP pgbus_jobs_total")
      expect(output).to include("# TYPE pgbus_jobs_total gauge")
    end

    context "when job stats have latency columns" do
      before do
        allow(data_source).to receive(:job_stats_summary).and_return(job_summary_with_latency)
        allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(true)
      end

      it "includes enqueue latency percentiles" do
        expect(output).to include('pgbus_job_enqueue_latency_seconds{quantile="0.5"} 0.01')
        expect(output).to include('pgbus_job_enqueue_latency_seconds{quantile="0.95"} 0.025')
        expect(output).to include('pgbus_job_enqueue_latency_seconds{quantile="0.99"} 0.05')
      end

      it "includes average retries" do
        expect(output).to include("pgbus_job_avg_retries 0.3")
      end
    end

    context "when stream stats are available" do
      let(:stream_summary) do
        {
          broadcasts: 200, connects: 50, disconnects: 45,
          active_estimate: 5, avg_fanout: 3.2,
          avg_broadcast_ms: 1.5, avg_connect_ms: 8.0
        }
      end

      before do
        allow(data_source).to receive_messages(
          stream_stats_available?: true,
          stream_stats_summary: stream_summary
        )
      end

      it "includes stream event counts" do
        expect(output).to include('pgbus_stream_events_total{event_type="broadcast"} 200')
        expect(output).to include('pgbus_stream_events_total{event_type="connect"} 50')
        expect(output).to include('pgbus_stream_events_total{event_type="disconnect"} 45')
      end

      it "includes active stream connections" do
        expect(output).to include("pgbus_stream_active_connections 5")
      end

      it "includes average fanout" do
        expect(output).to include("pgbus_stream_avg_fanout 3.2")
      end
    end

    context "when stream stats are not available" do
      it "omits stream metrics entirely" do
        expect(output).not_to include("pgbus_stream_")
      end
    end

    it "includes worker pool capacity gauges" do
      expect(output).to include('pgbus_worker_pool_capacity{pid="1",hostname="host1"} 5')
      expect(output).to include('pgbus_worker_pool_capacity{pid="2",hostname="host1"} 10')
    end

    it "includes worker pool busy gauges" do
      expect(output).to include('pgbus_worker_pool_busy{pid="1",hostname="host1"} 3')
      expect(output).to include('pgbus_worker_pool_busy{pid="2",hostname="host1"} 0')
    end

    it "includes worker pool utilization ratio" do
      expect(output).to include('pgbus_worker_pool_utilization{pid="1",hostname="host1"} 0.6')
      expect(output).to include('pgbus_worker_pool_utilization{pid="2",hostname="host1"} 0.0')
    end

    it "includes table dead tuples gauges" do
      expect(output).to include('pgbus_table_dead_tuples{table="pgmq.q_pgbus_default",kind="queue"} 400')
      expect(output).to include('pgbus_table_dead_tuples{table="pgmq.a_pgbus_default",kind="archive"} 100')
    end

    it "includes table live tuples gauges" do
      expect(output).to include('pgbus_table_live_tuples{table="pgmq.q_pgbus_default",kind="queue"} 8000')
    end

    it "includes table bloat ratio gauges" do
      expect(output).to include('pgbus_table_bloat_ratio{table="pgmq.q_pgbus_default",kind="queue"} 0.0476')
    end

    it "includes last vacuum age gauges" do
      expect(output).to include('pgbus_table_last_vacuum_age_seconds{table="pgmq.q_pgbus_default",kind="queue"} 120')
      expect(output).to include('pgbus_table_last_vacuum_age_seconds{table="pgmq.a_pgbus_default",kind="archive"} 3600')
    end

    it "includes oldest transaction age" do
      expect(output).to include("pgbus_oldest_transaction_age_seconds 12")
    end

    it "includes the concurrency gauges, unlabelled" do
      expect(output).to include("pgbus_concurrency_blocked_executions 7")
      expect(output).to include("pgbus_concurrency_blocked_oldest_age_seconds 812")
      expect(output).to include("pgbus_concurrency_slots_held 3")
    end

    context "when nothing is parked and no slot is held" do
      let(:summary_stats) do
        { total_queues: 3, total_depth: 45, total_visible: 43,
          active_processes: 2, failed_count: 8, dlq_depth: 3,
          recurring_count: 4, throughput_rate: 10.5,
          parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0 }
      end

      it "omits the concurrency family entirely" do
        expect(output).not_to include("pgbus_concurrency_blocked_executions")
        expect(output).not_to include("pgbus_concurrency_slots_held")
      end
    end

    context "when a key is at its limit with nothing parked" do
      let(:summary_stats) do
        { total_queues: 3, total_depth: 45, total_visible: 43,
          active_processes: 2, failed_count: 8, dlq_depth: 3,
          recurring_count: 4, throughput_rate: 10.5,
          parked_total: 0, oldest_parked_age_sec: nil, slots_held: 1, keys_at_limit: 1 }
      end

      it "still reports the slot count and a zero backlog" do
        expect(output).to include("pgbus_concurrency_slots_held 1")
        expect(output).to include("pgbus_concurrency_blocked_executions 0")
        expect(output).not_to include("pgbus_concurrency_blocked_oldest_age_seconds")
      end
    end

    context "when summary_stats raises" do
      before { allow(data_source).to receive(:summary_stats).and_raise(StandardError, "db error") }

      it "drops only the concurrency and summary families" do
        expect { output }.not_to raise_error
        expect(output).not_to include("pgbus_concurrency_blocked_executions")
        expect(output).to include("pgbus_queue_depth")
      end
    end

    context "when health stats have no tables and no transactions" do
      let(:health_stats) do
        {
          total_dead_tuples: 0, total_live_tuples: 0, worst_bloat_ratio: 0.0,
          tables_needing_vacuum: 0, oldest_vacuum_ago_sec: nil,
          oldest_transaction_age_sec: nil, tables: []
        }
      end

      it "omits health metrics entirely" do
        expect(output).not_to include("pgbus_table_dead_tuples")
        expect(output).not_to include("pgbus_oldest_transaction_age_seconds")
      end
    end

    context "when data source raises" do
      before do
        allow(data_source).to receive(:queues_with_metrics).and_raise(StandardError, "db error")
      end

      it "does not raise — returns whatever metrics it can" do
        expect { output }.not_to raise_error
        # Other sections still present
        expect(output).to include("pgbus_jobs_total")
      end
    end
  end
end
