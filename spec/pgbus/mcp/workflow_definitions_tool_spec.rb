# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::WorkflowDefinitionsTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:rows) do
    [{ workflow_class: "OrderWorkflow", loaded: true, ongoing: 11, paused: 1, attention: 1, finished: 412, cancelled: 9,
       e2e_ms: { p50: 48_200.0, p95: 312_000.0, samples: 412 }, removed_in_flight: {} }]
  end

  before do
    allow(Pgbus.configuration).to receive(:workflow_retention).and_return(30 * 86_400)
    allow(data_source).to receive(:workflow_definitions).and_return(rows)
  end

  def result(**args) = JSON.parse(described_class.call(server_context: context, **args).content.first[:text])

  it "is named pgbus_workflow_definitions and is read-only" do
    expect(described_class.tool_name).to eq("pgbus_workflow_definitions")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true, destructiveHint: false)
  end

  it "offers the dashboard's windows" do
    expect(described_class.input_schema.to_h.dig(:properties, :window, :enum)).to eq(%w[1h 24h 7d 30d])
  end

  it "lists every class over the last 24 hours by default, with the window it read" do
    out = result

    expect(data_source).to have_received(:workflow_definitions).with(window: having_attributes(name: "24h"))
    expect(out["window"]).to eq("name" => "24h", "seconds" => 86_400, "bucket_seconds" => 3_600, "capped" => false)
    expect(out["definitions"].sole).to include("workflow_class" => "OrderWorkflow", "ongoing" => 11,
                                               "e2e_ms" => { "p50" => 48_200.0, "p95" => 312_000.0, "samples" => 412 })
  end

  it "sends the removed steps as rows, so a step named like a payload key keeps its count" do
    rows.first[:removed_in_flight] = { "payload" => 2 }

    expect(result["definitions"].sole["removed_in_flight"]).to eq([{ "step" => "payload", "runs" => 2 }])
  end

  it "says when the window is capped at workflow_retention" do
    allow(Pgbus.configuration).to receive(:workflow_retention).and_return(86_400)

    expect(result(window: "7d")["window"]).to include("seconds" => 86_400, "capped" => true)
  end

  it "answers an unknown window with an error" do
    response = described_class.call(window: "90d", server_context: context)

    expect(response.error?).to be(true)
    expect(response.content.first[:text]).to include("unknown window")
  end
end
