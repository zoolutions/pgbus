# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource::ProcessList do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: double("Pgbus::Client")) }

  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }

  def db_row(id, kind, metadata, heartbeat_at: now - 10, hostname: "web-01", pid: 100 + id)
    { "id" => id.to_s, "kind" => kind, "hostname" => hostname, "pid" => pid.to_s,
      "metadata" => JSON.generate(metadata), "last_heartbeat_at" => heartbeat_at,
      "created_at" => (now - 3600).to_s }
  end

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Time).to receive(:now).and_return(now)
  end

  describe "#processes" do
    let(:rows) do
      [
        db_row(1, "supervisor", { "pid" => 101 }, pid: 100),
        db_row(2, "worker", { "loop_tick_at" => now.to_f - 120, "supervisor_pid" => 100,
                              "started_at" => now.to_f - 2520 }),
        db_row(3, "worker", { "loop_tick_at" => now.to_f - 5 }, heartbeat_at: (now - 400).iso8601),
        db_row(4, "dispatcher", { "loop_tick_at" => now.to_f - 1 })
      ]
    end

    before { allow(mock_connection).to receive(:select_all).and_return(double(to_a: rows)) }

    it "derives every status against one now (stale wins over a fresh beacon; stalled past the limit)" do
      expect(data_source.processes.map { |p| p[:status] }).to eq(%i[healthy stalled stale healthy])
    end

    it "keeps the row shape and adds supervisor_pid and started_at" do
      worker = data_source.processes[1]

      expect(worker).to include(id: 2, kind: "worker", hostname: "web-01", pid: 102, healthy: false,
                                status: :stalled, last_heartbeat_at: now - 10, supervisor_pid: 100,
                                started_at: Time.at(now.to_f - 2520), created_at: (now - 3600).to_s)
      expect(worker[:metadata]).to include("supervisor_pid" => 100)
    end

    it "parses a String heartbeat and leaves started_at/supervisor_pid nil when not reported" do
      expect(data_source.processes[2]).to include(last_heartbeat_at: now - 400, supervisor_pid: nil, started_at: nil)
    end

    it "takes already-decoded metadata and a row without metadata" do
      rows << db_row(5, "worker", {}).merge("metadata" => { "supervisor_pid" => 7 })
      rows << db_row(6, "supervisor", {}).merge("metadata" => nil)

      expect(data_source.processes.last(2).map { |p| [p[:status], p[:supervisor_pid]] }).to eq([[:healthy, 7], [:healthy, nil]])
    end

    it "derives with the thresholds it is given, so a caller can reuse them for the words" do
      later = data_source.process_thresholds(now: now + 600)

      expect(data_source.processes(thresholds: later).map { |p| p[:status] }).to all(eq(:stale))
    end

    it "derives against the dashboard's stall_threshold" do
      allow(Pgbus.configuration).to receive(:stall_threshold).and_return(nil)

      expect(data_source.processes[1][:status]).to eq(:healthy)
    end
  end

  describe "#process_thresholds" do
    it "captures the config, the process constants and one now" do
      allow(Pgbus.configuration).to receive_messages(stall_threshold: 60, dispatch_interval: 2.0,
                                                     recurring_schedule_interval: 3.0, polling_interval: 0.5,
                                                     worker_notify_wakeup?: true)

      expect(data_source.process_thresholds).to eq(
        Pgbus::Web::ProcessState::Thresholds.new(
          now: now, alive_threshold: 300, stall_threshold: 60, dispatch_interval: 2.0,
          recurring_schedule_interval: 3.0, polling_interval: 0.5, notify_fallback_poll: 15,
          notify_wakeup: true, reap_interval: 300
        )
      )
    end
  end

  describe "#process_context" do
    it "indexes the given rows without a query" do
      allow(mock_connection).to receive(:select_all)
      rows = [{ kind: "supervisor", hostname: "web-01", pid: 100, metadata: {}, status: :healthy },
              { kind: "dispatcher", hostname: "web-01", pid: 101, metadata: { "supervisor_pid" => 100 },
                status: :healthy }]

      context = data_source.process_context(rows: rows)

      expect(context.supervisors).to eq(["web-01", 100] => :healthy)
      expect(context.children).to eq(["web-01", 100] => { "dispatcher" => 1 })
      expect(context.live_dispatcher).to be(true)
      expect(context.thresholds.now).to eq(now)
      expect(mock_connection).not_to have_received(:select_all)
    end
  end
end
