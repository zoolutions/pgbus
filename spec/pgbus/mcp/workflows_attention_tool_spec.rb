# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::WorkflowsAttentionTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:allow_payloads) { false }
  let(:context) { { data_source: data_source, allow_payloads: allow_payloads } }
  let(:at) { Time.utc(2026, 10, 17, 12, 0, 0) }
  let(:groups) do
    [{ workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error", error_class: "Timeout::Error",
       count: 3, first_seen_at: at - 60, last_seen_at: at, error_message: "card 4242", status: "paused",
       available_actions: %i[resume resume_at cancel signal] },
     { workflow_class: "SignupWorkflow", step_name: "welcome", reason: "overdue", error_class: nil, count: 1,
       first_seen_at: at, last_seen_at: at, error_message: nil, status: "ready",
       available_actions: %i[pause cancel run_now reschedule signal] }]
  end

  before { allow(data_source).to receive(:workflow_attention_groups).and_return(groups) }

  def result(**args) = JSON.parse(described_class.call(server_context: context, **args).content.first[:text])

  it "is named pgbus_workflows_attention and is read-only" do
    expect(described_class.tool_name).to eq("pgbus_workflows_attention")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true)
  end

  it "returns the groups with their counts, the status their actions follow, and the total" do
    out = result

    expect(out["groups"].first).to include("workflow_class" => "CheckoutWorkflow", "reason" => "paused_error", "count" => 3,
                                           "status" => "paused", "first_seen_at" => "2026-10-17T11:59:00Z",
                                           "available_actions" => %w[resume resume_at cancel signal])
    expect(out["total_runs"]).to eq(4)
  end

  it "redacts the newest error message unless payloads are allowed and asked for" do
    expect(result["groups"].first["error_message"]).to eq("[redacted]")
    expect(result(include_payloads: true)["groups"].first["error_message"]).to eq("[redacted]")
    expect(result["groups"].last["error_message"]).to be_nil
  end

  context "when the server allows payloads" do
    let(:allow_payloads) { true }

    it "includes the message with include_payloads" do
      expect(result(include_payloads: true)["groups"].first["error_message"]).to eq("card 4242")
    end
  end
end
