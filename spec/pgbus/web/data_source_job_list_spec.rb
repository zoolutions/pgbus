# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource::JobList do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client") }
  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:queue_names) { %w[pgbus_test_default pgbus_test_mailers pgbus_test_default_dlq] }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Pgbus.configuration).to receive(:queue_prefix).and_return("pgbus_test")
    allow(data_source).to receive(:queues_with_metrics).and_return(queue_names.map { |name| { name: name } })
  end

  def captured_sql(name)
    sql = nil
    binds = nil
    allow(mock_connection).to receive(:select_all) do |query, label, params = []|
      expect(label).to eq(name)
      sql = query
      binds = params
      []
    end
    yield
    expect(mock_connection).to have_received(:select_all).once
    [sql, binds]
  end

  describe "#job_rows" do
    it "unions one state-tagged fragment per non-DLQ queue, sorted by time across queues" do
      sql, binds = captured_sql("Pgbus Job List") { data_source.job_rows(page: 2, per_page: 10) }

      expect(sql).to include("FROM pgmq.q_pgbus_test_default m", "FROM pgmq.q_pgbus_test_mailers m")
      expect(sql).not_to include("pgmq.q_pgbus_test_default_dlq")
      expect(sql).to include(described_class::STATE_CASE)
      expect(sql).to include("LEFT JOIN LATERAL", "FROM pgbus_failed_events")
      expect(sql).to include("f.queue_name IN ('default', 'pgbus_test_default')")
      expect(sql).to include("ORDER BY q.msg_id DESC LIMIT 20")
      expect(sql).to match(/ORDER BY sort_at DESC NULLS LAST, id DESC\s+LIMIT \$1 OFFSET \$2/)
      expect(binds).to eq([10, 10])
    end

    it "adds orphaned failed rows and blocked executions to the All tab" do
      sql, = captured_sql("Pgbus Job List") { data_source.job_rows }

      expect(sql).to include("NOT EXISTS (SELECT 1 FROM pgmq.q_pgbus_test_default m WHERE m.msg_id = f.msg_id)")
      expect(sql).to include("NOT (f.queue_name = ANY(ARRAY['default', 'pgbus_test_default', 'mailers', 'pgbus_test_mailers']))")
      expect(sql).to include("FROM pgbus_blocked_executions b")
      expect(sql).to include("LEFT JOIN pgbus_semaphores s ON s.key = b.concurrency_key")
    end

    it "binds the state filter instead of interpolating it" do
      sql, binds = captured_sql("Pgbus Job List") { data_source.job_rows(state: "running", per_page: 5) }

      expect(sql).to include("WHERE q.state = $1")
      expect(sql).not_to include("= 'running'")
      expect(sql).not_to include("pgbus_blocked_executions")
      expect(sql).not_to include("NOT EXISTS")
      expect(binds).to eq(["running", 5, 0])
    end

    {
      "scheduled" => "WHERE m.read_ct = 0 AND m.vt > now()",
      "ready" => "WHERE m.vt <= now()",
      "running" => "WHERE m.read_ct > 0 AND m.vt > now()",
      "retrying" => "WHERE m.read_ct > 0"
    }.each do |state, prefilter|
      it "narrows the #{state} scan with a predicate the state CASE implies" do
        sql, = captured_sql("Pgbus Job List") { data_source.job_rows(state: state) }

        expect(sql).to include(prefilter)
        expect(sql).to include("WHERE q.state = $1")
      end
    end

    it "keeps orphaned failed rows on the Retrying tab" do
      sql, = captured_sql("Pgbus Job List") { data_source.job_rows(state: "retrying") }

      expect(sql).to include("NOT EXISTS")
      expect(sql).not_to include("pgbus_blocked_executions")
    end

    it "reads only blocked executions on the Blocked tab" do
      sql, binds = captured_sql("Pgbus Job List") { data_source.job_rows(state: "blocked", per_page: 5) }

      expect(sql).to include("FROM pgbus_blocked_executions b")
      expect(sql).not_to include("pgmq.q_")
      expect(binds).to eq([5, 0])
    end

    it "limits the fragments to one queue" do
      sql, = captured_sql("Pgbus Job List") { data_source.job_rows(queue_name: "pgbus_test_mailers") }

      expect(sql).to include("FROM pgmq.q_pgbus_test_mailers m")
      expect(sql).not_to include("pgmq.q_pgbus_test_default ")
      expect(sql).not_to include("ANY(ARRAY")
      expect(sql).to include("b.queue_name IN ('mailers', 'pgbus_test_mailers')")
    end

    it "returns no rows for a queue that does not exist" do
      expect(data_source.job_rows(queue_name: "pgbus_test_nope")).to eq([])
    end

    it "formats rows with symbol keys and integer ids" do
      allow(mock_connection).to receive(:select_all).and_return([
                                                                  { "source" => "queue", "id" => "42", "msg_id" => "42",
                                                                    "read_ct" => "1", "state" => "running",
                                                                    "queue_name" => "pgbus_test_default",
                                                                    "failed_event_id" => nil, "slots_held" => nil,
                                                                    "slots_max" => nil }
                                                                ])

      row = data_source.job_rows.first

      expect(row).to include(source: "queue", id: 42, msg_id: 42, read_ct: 1, state: "running",
                             failed_event_id: nil)
    end

    it "logs and returns [] on error" do
      allow(mock_connection).to receive(:select_all).and_raise(StandardError, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.job_rows).to eq([])
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#job_state_counts" do
    it "counts every state in one capped aggregate query" do
      sql, = captured_sql("Pgbus Job State Counts") { data_source.job_state_counts }

      expect(sql).to include("LIMIT #{described_class::COUNT_CAP + 1}")
      expect(sql).to include("GROUP BY frag, state")
      expect(sql).to include("FROM pgbus_blocked_executions b")
    end

    it "sums counts per state and flags capped fragments" do
      cap = described_class::COUNT_CAP + 1
      allow(mock_connection).to receive(:select_all).and_return([
                                                                  { "frag" => 0, "state" => "ready", "n" => cap - 5 },
                                                                  { "frag" => 0, "state" => "running", "n" => 5 },
                                                                  { "frag" => 1, "state" => "ready", "n" => 2 },
                                                                  { "frag" => 3, "state" => "retrying", "n" => 1 },
                                                                  { "frag" => 5, "state" => "blocked", "n" => 4 }
                                                                ])

      counts = data_source.job_state_counts

      expect(counts["ready"]).to eq(cap - 3)
      expect(counts["running"]).to eq(5)
      expect(counts["scheduled"]).to eq(0)
      expect(counts["all"]).to eq(cap + 7)
      expect(counts.capped?("ready")).to be(true)
      expect(counts.capped?("all")).to be(true)
      expect(counts.capped?("blocked")).to be(false)
    end

    it "logs and returns zero counts on error" do
      allow(mock_connection).to receive(:select_all).and_raise(StandardError, "boom")
      allow(Pgbus.logger).to receive(:error)

      counts = data_source.job_state_counts

      expect(counts["all"]).to eq(0)
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#jobs_ahead" do
    let(:rows) do
      [
        { source: "queue", state: "ready", queue_name: "pgbus_test_default", msg_id: 10 },
        { source: "queue", state: "ready", queue_name: "pgbus_test_default", msg_id: 12 },
        { source: "queue", state: "ready", queue_name: "pgbus_test_mailers", msg_id: 3 },
        { source: "queue", state: "running", queue_name: "pgbus_test_mailers", msg_id: 4 },
        { source: "blocked", state: "blocked", queue_name: "default", msg_id: nil }
      ]
    end

    it "issues one bounded query per distinct queue with ready rows" do
      queries = []
      allow(mock_connection).to receive(:select_all) do |sql, _label, binds|
        queries << [sql, binds]
        ids = binds.first.delete("{}").split(",").map(&:to_i)
        ids.map { |id| { "msg_id" => id, "ahead" => id - 1 } }
      end

      ahead = data_source.jobs_ahead(rows)

      expect(queries.size).to eq(2)
      expect(queries.first.first).to include("FROM pgmq.q_pgbus_test_default", "LIMIT #{Pgbus::Web::JobState::AHEAD_CAP}")
      expect(queries.first.last).to eq(["{10,12}"])
      expect(ahead).to eq(["pgbus_test_default", 10] => 9, ["pgbus_test_default", 12] => 11, ["pgbus_test_mailers", 3] => 2)
    end

    it "memoizes per instance" do
      allow(mock_connection).to receive(:select_all).and_return([])

      2.times { data_source.jobs_ahead(rows) }

      expect(mock_connection).to have_received(:select_all).twice
    end

    it "skips a queue whose count fails" do
      allow(mock_connection).to receive(:select_all).and_raise(StandardError, "boom")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.jobs_ahead(rows)).to eq({})
    end
  end

  describe "#job_list_context" do
    it "collects paused, drained and worker liveness for JobState" do
      allow(Pgbus::QueueState).to receive(:paused).and_return(instance_double(ActiveRecord::Relation, pluck: ["default"]))
      allow(data_source).to receive_messages(drained_queue_names: Set["pgbus_test_default"],
                                             handler_queue_physical_names: ["pgbus_test_events"],
                                             processes: [{ kind: "worker", healthy: true }])

      context = data_source.job_list_context(now: Time.utc(2026, 1, 1))

      expect(context).to have_attributes(paused: Set["default"], drained: Set["pgbus_test_default"], workers_alive: true,
                                         consumers_alive: false, handler_queues: Set["pgbus_test_events"],
                                         max_retries: Pgbus.configuration.max_retries)
    end

    it "reports no live workers when every worker is stale" do
      allow(Pgbus::QueueState).to receive(:paused).and_return(instance_double(ActiveRecord::Relation, pluck: []))
      allow(data_source).to receive_messages(drained_queue_names: nil, handler_queue_physical_names: [],
                                             processes: [{ kind: "worker", healthy: false },
                                                         { kind: "consumer", healthy: true },
                                                         { kind: "dispatcher", healthy: true }])

      expect(data_source.job_list_context).to have_attributes(workers_alive: false, consumers_alive: true)
    end
  end
end
