# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource::QueueSummary do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client") }
  let(:queue_names) { %w[pgbus_test_default pgbus_test_mailers_p0 pgbus_test_mailers_p1 pgbus_test_default_dlq] }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(double("ActiveRecord::Connection"))
    allow(Pgbus.configuration).to receive(:queue_prefix).and_return("pgbus_test")
    allow(data_source).to receive(:queues_with_metrics).and_return(queue_names.map { |name| { name: name } })
  end

  describe "#queue_pause_state" do
    it "reports a running queue when no state row exists" do
      allow(Pgbus::QueueState).to receive(:find_by).with(queue_name: "default").and_return(nil)

      expect(data_source.queue_pause_state("pgbus_test_default"))
        .to eq(paused: false, reason: nil, paused_at: nil, resumes_at: nil, trip_count: 0)
    end

    it "reads the state by the logical name and returns the circuit-breaker fields" do
      paused_at = Time.utc(2026, 10, 10, 11, 59)
      resumes_at = Time.utc(2026, 10, 10, 12, 1)
      state = double("Pgbus::QueueState", paused: true, paused_reason: "circuit_breaker: 5 consecutive failures",
                                          paused_at: paused_at, circuit_breaker_resume_at: resumes_at,
                                          circuit_breaker_trip_count: 2)
      allow(Pgbus::QueueState).to receive(:find_by).with(queue_name: "mailers").and_return(state)

      expect(data_source.queue_pause_state("pgbus_test_mailers_p1"))
        .to eq(paused: true, reason: "circuit_breaker: 5 consecutive failures", paused_at: paused_at,
               resumes_at: resumes_at, trip_count: 2)
    end

    it "reports a resumed queue as running" do
      state = double("Pgbus::QueueState", paused: false)
      allow(Pgbus::QueueState).to receive(:find_by).and_return(state)

      expect(data_source.queue_pause_state("pgbus_test_default")).to include(paused: false)
    end

    it "degrades to running and logs when the state cannot be read" do
      allow(Pgbus::QueueState).to receive(:find_by).and_raise(ActiveRecord::StatementInvalid, "no table")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.queue_pause_state("pgbus_test_default")).to include(paused: false)
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#queue_drainers" do
    let(:workers) { [{ name: "critical", queues: %w[default] }, { queues: %w[mailers] }] }
    let(:processes) do
      [
        { kind: "worker", healthy: true, metadata: { "queues" => %w[default] } },
        { kind: "worker", healthy: true, metadata: { "queues" => "mailers,default" } },
        { kind: "worker", healthy: false, metadata: { "queues" => %w[default] } },
        { kind: "worker", healthy: true, metadata: { "queues" => %w[other] } },
        { kind: "consumer", healthy: true, metadata: {} }
      ]
    end

    before do
      allow(Pgbus.configuration).to receive(:workers).and_return(workers)
      allow(mock_client).to receive(:physical_queue_names).with("default").and_return(%w[pgbus_test_default])
      allow(mock_client).to receive(:physical_queue_names).with("mailers")
                                                          .and_return(%w[pgbus_test_mailers_p0 pgbus_test_mailers_p1])
      allow(data_source).to receive_messages(processes: processes, handler_queue_physical_names: [],
                                             stream_queue_names: Set.new)
    end

    it "names the capsules that drain the queue and counts its healthy workers" do
      expect(data_source.queue_drainers("pgbus_test_default"))
        .to eq(logical: "default", capsules: ["critical"], wildcard: false, handler: false, stream: false,
               live_workers: 2, live_consumers: 1, siblings: [], priority_level: nil)
    end

    it "names an anonymous capsule by its first queue and lists sibling priority tables" do
      expect(data_source.queue_drainers("pgbus_test_mailers_p1"))
        .to include(logical: "mailers", capsules: ["mailers"], live_workers: 1, siblings: %w[pgbus_test_mailers_p0],
                    priority_level: 1)
    end

    it "treats a _pN suffix the queue strategy did not create as part of the queue name" do
      allow(mock_client).to receive(:physical_queue_names).with("reports").and_return(%w[pgbus_test_reports])
      allow(mock_client).to receive(:physical_queue_names).with("reports_p1").and_return(%w[pgbus_test_reports_p1])

      expect(data_source.queue_drainers("pgbus_test_reports_p1"))
        .to include(logical: "reports_p1", priority_level: nil, siblings: [])
    end

    it "matches heartbeat queue names the way queue tables are named" do
      allow(data_source).to receive(:processes).and_return([{ kind: "worker", healthy: true,
                                                              metadata: { "queues" => %w[bulk-imports] } }])

      expect(data_source.queue_drainers("pgbus_test_bulk_imports")).to include(live_workers: 1)
    end

    it "flags a wildcard capsule and counts wildcard workers as live" do
      allow(Pgbus.configuration).to receive(:workers).and_return([{ queues: %w[*] }])
      allow(data_source).to receive(:processes).and_return([{ kind: "worker", healthy: true,
                                                              metadata: { "queues" => %w[*] } }])

      expect(data_source.queue_drainers("pgbus_test_default")).to include(wildcard: true, capsules: [], live_workers: 1)
    end

    it "counts only the consumers whose topics route to this handler queue" do
      allow(data_source).to receive_messages(
        registered_subscribers: [{ pattern: "invoice.*", physical_queue_name: "pgbus_test_billing" },
                                 { pattern: "user.signed_up", physical_queue_name: "pgbus_test_slack" }],
        processes: [{ kind: "consumer", healthy: true, metadata: { "topics" => ["invoice.*"] } },
                    { kind: "consumer", healthy: true, metadata: { "topics" => ["user.signed_up"] } },
                    { kind: "consumer", healthy: true, metadata: { "topics" => ["#"] } },
                    { kind: "consumer", healthy: false, metadata: { "topics" => ["invoice.*"] } }]
      )

      expect(data_source.queue_drainers("pgbus_test_billing")).to include(live_consumers: 2)
    end

    it "flags handler and stream queues" do
      allow(data_source).to receive_messages(handler_queue_physical_names: %w[pgbus_test_default],
                                             stream_queue_names: Set["pgbus_test_default"])

      expect(data_source.queue_drainers("pgbus_test_default")).to include(handler: true, stream: true)
    end

    it "names the logical queue of a dead-letter queue without the suffix" do
      expect(data_source.queue_drainers("pgbus_test_default_dlq")).to include(logical: "default", siblings: [])
    end

    # Fails open like drained_queue_names: a wildcard, never a false "nothing drains this".
    it "degrades to a wildcard and logs when a capsule's queues cannot be expanded" do
      allow(mock_client).to receive(:physical_queue_names).and_raise(ArgumentError, "bad queue")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.queue_drainers("pgbus_test_default")).to include(logical: "default", capsules: [],
                                                                          wildcard: true, live_workers: 2)
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
