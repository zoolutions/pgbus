# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::WorkflowDetailTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:allow_payloads) { false }
  let(:context) { { data_source: data_source, allow_payloads: allow_payloads } }
  let(:at) { Time.utc(2026, 10, 17, 12, 0, 0) }
  let(:detail) do
    { workflow_id: "wf-1", workflow_class: "CheckoutWorkflow", status: "paused", current_step: "capture",
      error_class: "Timeout::Error", error_message: "card 4242", arguments: { "card" => "4242" },
      available_actions: %i[resume resume_at cancel signal], resume_action: :retry, resume_step: "capture",
      steps: [{ step_name: "capture", attempt: 1, status: "failed", error_class: "Timeout::Error",
                error_message: "card 4242", error_backtrace: "app/x.rb:1", output: { "token" => "tok_1" },
                error_causes: [{ "class" => "Net::ReadTimeout", "message" => "card 4242" }],
                durations: { waited: 1.0, queued: 0.5, ran: 2.0, waiting: nil }, started_at: at }],
      signals: [{ name: "shipped", status: "pending", payload: { "address" => "Main St 1" }, created_at: at }],
      events: [{ event: "step_failed", actor: "system", reason: nil, details: { "error_class" => "Timeout::Error",
                                                                                "error_message" => "card 4242" },
                 created_at: at },
               { event: "resumed", actor: "dashboard:alice", reason: "gateway back", details: {}, created_at: at }],
      timeline: { rows: [], paused: [] }, created_at: at, updated_at: at }
  end

  before { allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(detail) }

  def result(**args)
    JSON.parse(described_class.call(workflow_id: "wf-1", server_context: context, **args).content.first[:text])["workflow"]
  end

  it "is named pgbus_workflow, read-only, and needs a workflow_id" do
    expect(described_class.tool_name).to eq("pgbus_workflow")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true)
    expect(described_class.input_schema.to_h[:required]).to eq(["workflow_id"])
  end

  it "returns the steps, signals, events and available actions, without the chart payload" do
    out = result

    expect(out).to include("status" => "paused", "available_actions" => %w[resume resume_at cancel signal],
                           "resume_action" => "retry")
    expect(out["steps"].sole).to include("step_name" => "capture", "durations" => { "waited" => 1.0, "queued" => 0.5,
                                                                                    "ran" => 2.0, "waiting" => nil })
    expect(out["events"].map { |e| e["event"] }).to eq(%w[step_failed resumed])
    expect(out["events"].last["reason"]).to eq("gateway back")
    expect(out).not_to have_key("timeline")
  end

  it "redacts arguments, outputs, signal payloads and error text by default" do
    out = result

    expect(out["arguments"]).to eq("[redacted]")
    expect(out["error_message"]).to eq("[redacted]")
    expect(out["steps"].sole).to include("output" => "[redacted]", "error_message" => "[redacted]",
                                         "error_backtrace" => "[redacted]", "error_class" => "Timeout::Error")
    expect(out["steps"].sole["error_causes"]).to eq([{ "class" => "Net::ReadTimeout" }])
    expect(out["signals"].sole).to include("name" => "shipped", "payload" => "[redacted]")
    expect(out["events"].first["details"]).to eq("error_class" => "Timeout::Error")
  end

  context "when the server allows payloads" do
    let(:allow_payloads) { true }

    it "includes them only with include_payloads" do
      expect(result["arguments"]).to eq("[redacted]")

      out = result(include_payloads: true)
      expect(out["arguments"]).to eq("card" => "4242")
      expect(out["steps"].sole).to include("output" => { "token" => "tok_1" }, "error_message" => "card 4242")
      expect(out["signals"].sole["payload"]).to eq("address" => "Main St 1")
      expect(out["events"].first["details"]).to include("error_message" => "card 4242")
    end
  end

  it "answers an unknown run as a tool error" do
    allow(data_source).to receive(:workflow_detail).with("nope").and_return(nil)

    response = described_class.call(workflow_id: "nope", server_context: context)

    expect(response.error?).to be(true)
    expect(response.content.first[:text]).to include("nope")
  end
end
