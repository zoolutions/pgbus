# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::WorkflowsTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:allow_payloads) { false }
  let(:context) { { data_source: data_source, allow_payloads: allow_payloads } }
  let(:count_class) { Pgbus::Web::DataSource::ListCounts::Count }
  let(:at) { Time.utc(2026, 10, 17, 12, 0, 0) }
  let(:row) do
    { workflow_id: "wf-1", workflow_class: "CheckoutWorkflow", status: "paused", current_step: "capture",
      error_class: "Timeout::Error", error_message: "card 4242 timed out", created_at: at, updated_at: at }
  end

  before do
    allow(data_source).to receive_messages(workflows: [row], workflows_count: count_class.new(total: 1, capped: false))
  end

  around { |example| Time.use_zone("UTC") { example.run } }

  def result(**args)
    JSON.parse(described_class.call(server_context: context, **args).content.first[:text])
  end

  it "is named pgbus_workflows and is read-only" do
    expect(described_class.tool_name).to eq("pgbus_workflows")
    expect(described_class.annotations_value.to_h).to include(readOnlyHint: true, destructiveHint: false)
  end

  it "takes the index's filters, a status enum and the pager in its schema" do
    props = described_class.input_schema.to_h[:properties]

    expect(props.keys).to include(:status, :workflow_class, :step, :error_class, :subject_type, :subject_id, :workflow_id,
                                  :created_from, :created_to, :page, :per_page, :include_payloads)
    expect(props[:status][:enum]).to eq(Pgbus::Web::DataSource::WorkflowList::STATUSES)
  end

  it "lists ongoing runs by default, paged, with times as ISO 8601" do
    out = result

    expect(data_source).to have_received(:workflows).with(status: "ongoing", page: 1, per_page: 25)
    expect(out["workflows"].sole).to include("workflow_id" => "wf-1", "status" => "paused", "created_at" => "2026-10-17T12:00:00Z")
    expect(out).to include("page" => 1, "per_page" => 25, "total" => 1, "has_more" => false)
  end

  it "passes the filters through, parsed like the dashboard's" do
    result(status: "paused", workflow_class: "CheckoutWorkflow", error_class: "Timeout::Error", created_from: "2026-10-01",
           page: 2, per_page: 10)

    expect(data_source).to have_received(:workflows)
      .with(status: "paused", page: 2, per_page: 10, workflow_class: "CheckoutWorkflow", error_class: "Timeout::Error",
            created_from: Time.zone.parse("2026-10-01"))
    expect(data_source).to have_received(:workflows_count)
      .with(status: "paused", workflow_class: "CheckoutWorkflow", error_class: "Timeout::Error",
            created_from: Time.zone.parse("2026-10-01"))
  end

  it "clamps the pager" do
    expect(result(page: 0, per_page: 5_000)).to include("page" => 1, "per_page" => 100)
  end

  it "says has_more when the next page has rows" do
    allow(data_source).to receive(:workflows_count).and_return(count_class.new(total: 30, capped: false))

    expect(result(per_page: 25)["has_more"]).to be(true)
  end

  it "answers an unknown status or a bad date as a tool error" do
    response = described_class.call(server_context: context, status: "stuck")

    expect(response.error?).to be(true)
    expect(response.content.first[:text]).to include("unknown status")
    expect(described_class.call(server_context: context, created_to: "soon").error?).to be(true)
  end

  it "redacts the error message unless payloads are allowed and asked for" do
    expect(result["workflows"].sole["error_message"]).to eq("[redacted]")
    expect(result["workflows"].sole["error_class"]).to eq("Timeout::Error")
  end

  context "when the server allows payloads" do
    let(:allow_payloads) { true }

    it "includes the error message only with include_payloads" do
      expect(result["workflows"].sole["error_message"]).to eq("[redacted]")
      expect(result(include_payloads: true)["workflows"].sole["error_message"]).to eq("card 4242 timed out")
    end
  end
end
