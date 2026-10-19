# frozen_string_literal: true

require "spec_helper"
require "pgbus/mcp/health_analyzer"

RSpec.describe Pgbus::Web::DataSource::DashboardSignals do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client") }
  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:queues) do
    [
      { name: "pgbus_test_default", queue_length: 10, queue_visible_length: 8, parked_length: 2,
        oldest_claimable_age_sec: 90, paused: false },
      { name: "pgbus_test_mailers", queue_length: 4, queue_visible_length: 4, parked_length: 0,
        oldest_claimable_age_sec: 30, paused: true },
      { name: "pgbus_test_billing", queue_length: 0, queue_visible_length: 0, parked_length: 0,
        oldest_claimable_age_sec: nil, paused: false },
      { name: "pgbus_test_default_dlq", queue_length: 3, queue_visible_length: 3, parked_length: 0,
        oldest_claimable_age_sec: 600, paused: false }
    ]
  end
  let(:processes) do
    [
      { kind: "worker", status: :healthy, healthy: true, metadata: { "queues" => %w[default] } },
      { kind: "worker", status: :stalled, healthy: false, metadata: { "queues" => %w[default] } },
      { kind: "consumer", status: :stale, healthy: false, metadata: {} },
      { kind: "dispatcher", status: :healthy, healthy: true, metadata: {} }
    ]
  end

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Pgbus.configuration).to receive(:queue_prefix).and_return("pgbus_test")
    allow(data_source).to receive_messages(queues_with_metrics: queues, processes: processes,
                                           handler_queue_physical_names: %w[pgbus_test_billing],
                                           stream_queue_names: Set.new)
    allow(Pgbus::FailedEventRecorder).to receive(:source_queue_column?).and_return(true)
  end

  describe "#retrying_signal" do
    let(:counts) do
      Pgbus::Web::DataSource::JobList::StateCounts.new(counts: { "retrying" => 4 }, capped: Set.new)
    end

    before do
      allow(data_source).to receive(:job_state_counts).and_return(counts)
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Retry Schedule")
        .and_return({ "next_at" => "2026-10-10 12:00:40+00", "due" => 2, "newest_error" => "Net::ReadTimeout" })
    end

    it "counts retrying jobs the way the Jobs page does, outside the handler queues" do
      signal = data_source.retrying_signal

      expect(signal).to include(count: 4, capped: false, due_now_count: 2, newest_error_class: "Net::ReadTimeout")
      expect(signal[:next_attempt_at]).to eq(Time.utc(2026, 10, 10, 12, 0, 40))
      expect(data_source).to have_received(:job_state_counts).with(exclude: %w[pgbus_test_billing])
    end

    it "reads the next attempt and the due count from the retrying rows of every job queue, capped per queue" do
      sql = nil
      allow(mock_connection).to receive(:select_one).with(anything, "Pgbus Retry Schedule") do |query, _|
        sql = query
        { "next_at" => nil, "due" => 0 }
      end

      data_source.retrying_signal

      expect(sql).to include("FROM pgmq.q_pgbus_test_default m", "FROM pgmq.q_pgbus_test_mailers m")
      expect(sql).not_to include("q_pgbus_test_billing", "q_pgbus_test_default_dlq")
      expect(sql).to include(Pgbus::Web::DataSource::JobList::STATE_PREFILTER["retrying"])
      expect(sql).to include("= 'retrying'", "ORDER BY m.vt LIMIT #{Pgbus::Web::DataSource::JobList::COUNT_CAP + 1}")
      expect(sql).to include("MIN(vt) FILTER (WHERE vt > now())", "COUNT(*) FILTER (WHERE vt <= now())")
      # The newest error comes from the rows still backing off, not from any
      # failed event (one that already re-ran or was dead-lettered).
      expect(sql).to include("SELECT m.vt, f.error_class, f.failed_at", "(array_agg(error_class ORDER BY failed_at DESC))[1]")
      # A priority sub-queue's failed row is its own table's (issue #583).
      expect(sql).to include("AND (f.source_queue IS NULL OR f.source_queue = 'pgbus_test_default')")
    end

    it "flags a capped count" do
      allow(data_source).to receive(:job_state_counts)
        .and_return(counts.with(counts: { "retrying" => 10_000 }, capped: Set["retrying"]))

      expect(data_source.retrying_signal).to include(count: 10_000, capped: true)
    end

    it "skips the schedule query when nothing is retrying" do
      allow(data_source).to receive(:job_state_counts).and_return(counts.with(counts: {}))

      expect(data_source.retrying_signal).to eq(count: 0, capped: false, next_attempt_at: nil, due_now_count: 0,
                                                newest_error_class: nil)
      expect(mock_connection).not_to have_received(:select_one)
    end

    it "returns the empty shape and logs when a query fails" do
      allow(mock_connection).to receive(:select_one).and_raise(ActiveRecord::StatementInvalid, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.retrying_signal).to include(count: 0, next_attempt_at: nil)
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#dlq_signal" do
    let(:row) do
      { "msg_id" => 7, "read_ct" => 0, "enqueued_at" => "2026-10-10 10:00:00+00", "last_read_at" => nil,
        "vt" => nil, "message" => '{"job_class":"SendWelcomeEmailJob"}', "headers" => nil,
        "queue_name" => "pgbus_test_default_dlq" }
    end

    it "reports the depth from the cached metrics and the newest message across dead-letter queues" do
      sql = nil
      allow(mock_connection).to receive(:select_one).with(anything, "Pgbus Newest Dead Letter") do |query, _|
        sql = query
        row
      end

      signal = data_source.dlq_signal

      expect(signal[:depth]).to eq(3)
      expect(signal[:newest]).to include(msg_id: 7, queue_name: "pgbus_test_default_dlq",
                                         message: '{"job_class":"SendWelcomeEmailJob"}')
      expect(sql).to include("FROM pgmq.q_pgbus_test_default_dlq ORDER BY msg_id DESC LIMIT 1")
      expect(sql).to include("ORDER BY enqueued_at DESC LIMIT 1")
    end

    it "does not query when every dead-letter queue is empty" do
      allow(data_source).to receive(:queues_with_metrics).and_return(queues.first(3))
      allow(mock_connection).to receive(:select_one)

      expect(data_source.dlq_signal).to eq(depth: 0, newest: nil)
      expect(mock_connection).not_to have_received(:select_one)
    end

    it "keeps the depth and logs when the newest message cannot be read" do
      allow(mock_connection).to receive(:select_one).and_raise(ActiveRecord::StatementInvalid, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.dlq_signal).to eq(depth: 3, newest: nil)
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#process_signal" do
    it "tallies the process rows by status and kind without a query of its own" do
      expect(data_source.process_signal).to eq(total: 4, healthy: 2, stalled: 1, stale: 1, workers: 2, consumers: 1,
                                               dispatchers: 1)
    end
  end

  describe "#queue_signal" do
    before { allow(data_source).to receive(:drained_queue_names).and_return(Set["pgbus_test_billing"]) }

    it "counts paused, dead-letter and undrained queues from the cached metrics" do
      # As in the health verdict: only an active backlog nothing drains counts.
      # default qualifies; mailers is paused, billing is empty, the DLQ is a DLQ.
      expect(data_source.queue_signal).to include(total: 4, paused: 1, dlq: 1, undrained: 1)
    end

    # Paused queues' visible messages wait for a resume, not a worker; the
    # DLQs have their own card.
    it "counts only unpaused job queues as claimable" do
      expect(data_source.queue_signal).to include(depth: 14, claimable: 8, waiting: 2, paused_depth: 4)
    end

    it "leaves stream queues out of the undrained count" do
      allow(data_source).to receive(:stream_queue_names).and_return(Set["pgbus_test_default"])

      expect(data_source.queue_signal).to include(undrained: 0)
    end

    it "counts nothing as undrained under a wildcard capsule" do
      allow(data_source).to receive(:drained_queue_names).and_return(nil)

      expect(data_source.queue_signal).to include(undrained: 0)
    end
  end

  describe "#throughput_signal" do
    it "reports jobs per minute over the last five minutes from job stats" do
      allow(Pgbus::JobStat).to receive(:table_exists?).and_return(true)
      allow(Pgbus::JobStat).to receive(:summary).with(minutes: 5).and_return({ total: 191 })

      expect(data_source.throughput_signal).to eq(available: true, jobs_per_minute: 38.2, window_minutes: 5)
    end

    it "says job stats are unavailable without the table" do
      allow(Pgbus::JobStat).to receive(:table_exists?).and_return(false)

      expect(data_source.throughput_signal).to eq(available: false, jobs_per_minute: nil, window_minutes: 5)
    end
  end

  describe "#verdict_signal" do
    it "returns the health analyzer's verdict" do
      verdict = { status: "OK", reasons: [], checked_at: "now", summary: {} }
      analyzer = instance_double(Pgbus::MCP::HealthAnalyzer, verdict: verdict)
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).with(data_source).and_return(analyzer)

      expect(data_source.verdict_signal).to eq(verdict)
    end

    it "returns nil and logs when the verdict cannot be computed" do
      allow(Pgbus::MCP::HealthAnalyzer).to receive(:new).and_raise(StandardError, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.verdict_signal).to be_nil
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#dashboard_signals" do
    it "collects every signal the home page renders" do
      allow(data_source).to receive_messages(retrying_signal: :retrying, dlq_signal: :dlq, process_signal: :processes,
                                             queue_signal: :queues, throughput_signal: :throughput,
                                             verdict_signal: :verdict)

      expect(data_source.dashboard_signals).to eq(retrying: :retrying, dlq: :dlq, processes: :processes,
                                                  queues: :queues, throughput: :throughput, verdict: :verdict)
    end
  end

  describe "#queue_status_lines" do
    let(:paused_state) do
      double("Pgbus::QueueState", queue_name: "mailers", paused: true, paused_reason: "Mail provider maintenance",
                                  paused_at: Time.utc(2026, 10, 10, 11), circuit_breaker_resume_at: nil,
                                  circuit_breaker_trip_count: 0)
    end

    before do
      allow(Pgbus.configuration).to receive(:workers).and_return([{ queues: %w[default mailers] }])
      allow(mock_client).to receive(:physical_queue_names) { |q| ["pgbus_test_#{q}"] }
      allow(data_source).to receive(:registered_subscribers).and_return([])
      allow(Pgbus::QueueState).to receive(:where).and_return([paused_state])
    end

    it "puts one line of words on every queue row, loading pause states and processes once" do
      lines = data_source.queue_status_lines

      expect(lines.keys).to eq(queues.map { |q| q[:name] })
      expect(lines["pgbus_test_default"].key).to eq("backlog")
      expect(lines["pgbus_test_mailers"]).to have_attributes(key: "paused_operator", tone: :yellow)
      expect(lines["pgbus_test_default_dlq"].key).to eq("dlq")
      expect(Pgbus::QueueState).to have_received(:where).once
      expect(data_source).to have_received(:processes).once
      expect(data_source).to have_received(:stream_queue_names).once
    end

    it "returns no lines and logs when the queue facts cannot be read" do
      allow(Pgbus::QueueState).to receive(:where).and_raise(ActiveRecord::StatementInvalid, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.queue_status_lines).to eq({})
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
