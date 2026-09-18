# frozen_string_literal: true

require "json"
require_relative "spec_helper"

# Exercises every diagnostic tool: correct DataSource delegation, JSON
# response shape, payload redaction, pagination caps, and error responses.
# Covers the whole tool family in one file, so the outer describe is a string.
RSpec.describe "Pgbus MCP tools" do # rubocop:disable RSpec/DescribeClass
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:context_with_payloads) { { data_source: data_source, allow_payloads: true } }

  def body(response)
    JSON.parse(response.content.first[:text])
  end

  describe Pgbus::MCP::Tools::QueuesTool do
    it "delegates to queues_with_metrics" do
      allow(data_source).to receive(:queues_with_metrics)
        .and_return([{ name: "pgbus_default", queue_length: 3 }])

      result = body(described_class.call(server_context: context))
      expect(result["queues"].first["name"]).to eq("pgbus_default")
    end

    it "is read-only" do
      expect(described_class.annotations_value.to_h).to include(readOnlyHint: true)
    end
  end

  describe Pgbus::MCP::Tools::QueueDetailTool do
    it "merges detail, paused state, and health" do
      allow(data_source).to receive(:queue_detail).with("pgbus_default")
                                                  .and_return({ name: "pgbus_default", queue_length: 1 })
      allow(data_source).to receive(:queue_paused?).with("pgbus_default").and_return(true)
      allow(data_source).to receive(:queue_health_detail).with("pgbus_default")
                                                         .and_return({ tables: [] })

      result = body(described_class.call(name: "pgbus_default", server_context: context))
      expect(result["paused"]).to be(true)
      expect(result["health"]).to eq({ "tables" => [] })
    end

    it "returns an error response when the queue is missing" do
      allow(data_source).to receive(:queue_detail).and_return(nil)

      response = described_class.call(name: "nope", server_context: context)
      expect(response.error?).to be(true)
    end
  end

  describe Pgbus::MCP::Tools::ProcessesTool do
    it "delegates to processes" do
      allow(data_source).to receive(:processes)
        .and_return([{ kind: "worker", status: :stalled }])

      result = body(described_class.call(server_context: context))
      expect(result["processes"].first["status"]).to eq("stalled")
    end
  end

  describe Pgbus::MCP::Tools::HealthTool do
    it "returns a verdict computed by HealthAnalyzer" do
      allow(data_source).to receive_messages(queues_with_metrics: [], processes: [], queue_health_stats: {}, stream_queue_names: Set.new)

      result = body(described_class.call(server_context: context))
      expect(result["status"]).to eq("OK")
    end
  end

  describe Pgbus::MCP::Tools::JobsTool do
    let(:rows) do
      [{ msg_id: 1, read_ct: 0, message: "{\"pii\":\"x\"}", headers: "{}" }]
    end

    it "redacts payloads by default" do
      allow(data_source).to receive(:jobs)
        .with(queue_name: "pgbus_default", page: 1, per_page: 25)
        .and_return(rows)

      result = body(described_class.call(queue: "pgbus_default", server_context: context))
      job = result["jobs"].first
      expect(job["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
      expect(job["read_ct"]).to eq(0)
    end

    it "returns payloads when allowed and include_payloads is set" do
      allow(data_source).to receive(:jobs).and_return(rows)

      result = body(
        described_class.call(queue: "pgbus_default", include_payloads: true, server_context: context_with_payloads)
      )
      expect(result["jobs"].first["message"]).to eq("{\"pii\":\"x\"}")
    end

    it "ignores include_payloads when the server disallows payloads" do
      allow(data_source).to receive(:jobs).and_return(rows)

      result = body(
        described_class.call(queue: "pgbus_default", include_payloads: true, server_context: context)
      )
      expect(result["jobs"].first["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "caps per_page at 100" do
      allow(data_source).to receive(:jobs)
        .with(queue_name: nil, page: 1, per_page: 100)
        .and_return([])

      body(described_class.call(per_page: 5000, server_context: context))
      expect(data_source).to have_received(:jobs).with(queue_name: nil, page: 1, per_page: 100)
    end

    it "floors page and per_page at 1" do
      allow(data_source).to receive(:jobs)
        .with(queue_name: nil, page: 1, per_page: 1)
        .and_return([])

      body(described_class.call(page: 0, per_page: 0, server_context: context))
      expect(data_source).to have_received(:jobs).with(queue_name: nil, page: 1, per_page: 1)
    end

    it "caps page at MAX_PAGE so OFFSET can't be driven to an unbounded scan" do
      allow(data_source).to receive(:jobs)
        .with(queue_name: nil, page: described_class::MAX_PAGE, per_page: 25)
        .and_return([])

      body(described_class.call(page: 10_000_000, server_context: context))
      expect(data_source).to have_received(:jobs)
        .with(queue_name: nil, page: described_class::MAX_PAGE, per_page: 25)
    end
  end

  describe Pgbus::MCP::Tools::JobDetailTool do
    it "redacts the payload by default" do
      allow(data_source).to receive(:job_detail).with("pgbus_default", 7)
                                                .and_return({ msg_id: 7, message: "secret", headers: "h" })

      result = body(described_class.call(queue: "pgbus_default", msg_id: 7, server_context: context))
      expect(result["job"]["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "errors when the message is missing" do
      allow(data_source).to receive(:job_detail).and_return(nil)

      response = described_class.call(queue: "pgbus_default", msg_id: 99, server_context: context)
      expect(response.error?).to be(true)
    end
  end

  describe Pgbus::MCP::Tools::DlqTool do
    it "redacts payloads and paginates" do
      allow(data_source).to receive(:dlq_messages).with(page: 1, per_page: 25)
                                                  .and_return([{ msg_id: 5, message: "secret" }])

      result = body(described_class.call(server_context: context))
      expect(result["messages"].first["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "caps page at MAX_PAGE so OFFSET can't be driven to an unbounded scan" do
      allow(data_source).to receive(:dlq_messages)
        .with(page: described_class::MAX_PAGE, per_page: 25)
        .and_return([])

      body(described_class.call(page: 10_000_000, server_context: context))
      expect(data_source).to have_received(:dlq_messages)
        .with(page: described_class::MAX_PAGE, per_page: 25)
    end
  end

  describe Pgbus::MCP::Tools::DlqDetailTool do
    it "returns a redacted detail when only one DLQ is present" do
      allow(data_source).to receive_messages(queues_with_metrics: [])
      allow(data_source).to receive(:dlq_message_detail).with(5)
                                                        .and_return({ msg_id: 5, message: "secret" })

      result = body(described_class.call(msg_id: 5, server_context: context))
      expect(result["message"]["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
    end

    it "errors when not found" do
      allow(data_source).to receive_messages(queues_with_metrics: [], dlq_message_detail: nil)

      expect(described_class.call(msg_id: 5, server_context: context).error?).to be(true)
    end

    it "queries the named DLQ directly when queue: is supplied (no cross-scan)" do
      allow(data_source).to receive(:job_detail).with("pgbus_orders_dlq", 5)
                                                .and_return({ msg_id: 5, message: "secret" })

      result = body(described_class.call(msg_id: 5, queue: "pgbus_orders_dlq", server_context: context))

      expect(result["message"]["msg_id"]).to eq(5)
      expect(data_source).not_to have_received(:job_detail).with("pgbus_default_dlq", 5)
    end

    it "refuses to guess when msg_id is ambiguous across multiple DLQs" do
      allow(data_source).to receive_messages(queues_with_metrics: [
                                               { name: "pgbus_default_dlq" },
                                               { name: "pgbus_orders_dlq" }
                                             ])
      allow(data_source).to receive(:job_detail).and_return({ msg_id: 5, message: "x" })

      response = described_class.call(msg_id: 5, server_context: context)

      expect(response.error?).to be(true)
      expect(response.content.first[:text]).to include("ambiguous")
    end
  end

  describe Pgbus::MCP::Tools::LocksTool do
    it "delegates to job_locks" do
      allow(data_source).to receive(:job_locks)
        .and_return([{ lock_key: "k", age_seconds: 10 }])

      result = body(described_class.call(server_context: context))
      expect(result["locks"].first["lock_key"]).to eq("k")
    end
  end

  describe Pgbus::MCP::Tools::ConcurrencyTool do
    it "delegates to concurrency_stats" do
      allow(data_source).to receive(:concurrency_stats).and_return(
        parked_total: 7, oldest_parked_age_sec: 812, slots_held: 3, keys_at_limit: 1,
        keys: [{ key: "ProcessOrder-42", value: 1, max_value: 1, lease_fresh: true,
                 parked_count: 7, oldest_parked_age_sec: 812 }]
      )

      result = body(described_class.call(server_context: context))

      expect(result["parked_total"]).to eq(7)
      expect(result["keys"].first["key"]).to eq("ProcessOrder-42")
      expect(result["keys"].first["parked_count"]).to eq(7)
    end

    it "is read-only" do
      expect(described_class.annotations_value.to_h).to include(readOnlyHint: true)
    end
  end

  describe Pgbus::MCP::Tools::ThroughputTool do
    it "delegates with a clamped window" do
      allow(data_source).to receive(:job_throughput).with(minutes: 1440).and_return([])

      result = body(described_class.call(minutes: 99_999, server_context: context))
      expect(result["minutes"]).to eq(1440)
    end
  end

  describe Pgbus::MCP::Tools::StatsTool do
    it "returns status counts and summary" do
      allow(data_source).to receive(:job_status_counts).with(minutes: 60).and_return({ "success" => 5 })
      allow(data_source).to receive(:job_stats_summary).with(minutes: 60).and_return({ total: 5 })

      result = body(described_class.call(server_context: context))
      expect(result["status_counts"]["success"]).to eq(5)
      expect(result["summary"]["total"]).to eq(5)
    end
  end

  describe Pgbus::MCP::Tools::RecurringTool do
    it "delegates to recurring_tasks" do
      allow(data_source).to receive(:recurring_tasks)
        .and_return([{ key: "cleanup", schedule: "0 * * * *" }])

      result = body(described_class.call(server_context: context))
      expect(result["recurring_tasks"].first["key"]).to eq("cleanup")
    end
  end

  describe "default data source" do
    it "builds a DataSource when none is injected" do
      allow(Pgbus::Web::DataSource).to receive(:new).and_return(data_source)
      allow(data_source).to receive(:job_locks).and_return([])

      expect(body(Pgbus::MCP::Tools::LocksTool.call(server_context: nil))).to eq({ "locks" => [] })
    end
  end

  describe "fail-safe redaction at the response boundary" do
    # A tool that returns a payload-bearing key without remembering to redact
    # is still safe: BaseTool.json_response strips payloads by default.
    let(:leaky_tool) do
      Class.new(Pgbus::MCP::BaseTool) do
        def self.call(server_context: nil)
          json_response({ rows: [{ msg_id: 1, message: "secret-body", headers: "h" }] }, server_context: server_context)
        end
      end
    end

    it "redacts payloads even when the tool never calls Redactor itself" do
      result = body(leaky_tool.call(server_context: context))
      row = result["rows"].first

      expect(row["message"]).to eq(Pgbus::MCP::Redactor::REDACTED)
      expect(row["headers"]).to eq(Pgbus::MCP::Redactor::REDACTED)
      expect(row["msg_id"]).to eq(1)
    end
  end
end
