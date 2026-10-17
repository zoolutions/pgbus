# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::WorkflowStatsTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:at) { Time.utc(2026, 10, 17, 12, 0, 0) }
  let(:detail) do
    { workflow_class: "OrderWorkflow", loaded: true, window: { name: "24h", seconds: 86_400, since: at - 86_400 },
      steps: [{ name: "capture", kind: "step", removed: false, declared: true,
                counts: { ready: 0, performing: 2, waiting: 0, paused: 1 },
                run_ms: { p50: 820.0, p95: 4_100.0, p99: 9_800.0, samples: 468 },
                rates: { failed: 0.0833, reattempted: 0.0662, skipped: 0.0 }, sample_size: 468 }],
      throughput: [{ from: at, started: 3, finished: 2, cancelled: 0 }],
      by_revision: [{ revision: "8d41b07", first_seen_at: at - 3_600, last_seen_at: at, attempts: 560 }] }
  end

  before do
    allow(Pgbus.configuration).to receive(:workflow_retention).and_return(30 * 86_400)
    allow(data_source).to receive(:workflow_definition).and_return(detail)
  end

  def result(**args) = JSON.parse(described_class.call(server_context: context, **args).content.first[:text])

  it "is named pgbus_workflow_stats and is read-only" do
    expect(described_class.tool_name).to eq("pgbus_workflow_stats")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true, destructiveHint: false)
    expect(described_class.input_schema.to_h[:required]).to eq(%w[workflow_class])
  end

  it "returns one class's numbers over the window asked for, times as ISO 8601" do
    out = result(workflow_class: "OrderWorkflow", window: "7d")

    expect(data_source).to have_received(:workflow_definition).with("OrderWorkflow", window: having_attributes(name: "7d"))
    stats = out["workflow"]
    expect(stats["steps"].sole).to include("name" => "capture", "counts" => { "ready" => 0, "performing" => 2, "waiting" => 0,
                                                                              "paused" => 1 })
    expect(stats["throughput"].sole["from"]).to eq("2026-10-17T12:00:00Z")
    expect(stats["by_revision"].sole["last_seen_at"]).to eq("2026-10-17T12:00:00Z")
  end

  it "sends the removed steps as rows, so a step named like a payload key keeps its count" do
    detail[:removed_in_flight] = { "message" => 3 }

    expect(result(workflow_class: "OrderWorkflow")["workflow"]["removed_in_flight"]).to eq([{ "step" => "message", "runs" => 3 }])
  end

  it "answers a class nothing knows with an error" do
    allow(data_source).to receive(:workflow_definition).and_return(nil)

    response = described_class.call(workflow_class: "NopeWorkflow", server_context: context)

    expect(response.error?).to be(true)
    expect(response.content.first[:text]).to include("NopeWorkflow")
  end

  it "refuses a name that is not a constant path, without asking the data source" do
    response = described_class.call(workflow_class: "Kernel.exit", server_context: context)

    expect(response.error?).to be(true)
    expect(data_source).not_to have_received(:workflow_definition)
  end

  it "answers an unknown window with an error" do
    expect(described_class.call(workflow_class: "OrderWorkflow", window: "1y", server_context: context).error?).to be(true)
  end
end
