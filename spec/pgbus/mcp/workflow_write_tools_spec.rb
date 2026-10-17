# frozen_string_literal: true

require "json"
require_relative "spec_helper"

# The six workflow write tools (issue #550): each calls its Web::DataSource
# action with actor "mcp" and the reason, and answers with the run after it.
RSpec.describe "the workflow write tools" do # rubocop:disable RSpec/DescribeClass
  let(:data_source) { instance_double(Pgbus::Web::DataSource, reset_cache!: nil) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:actions) { %i[resume resume_at cancel run_now reschedule skip signal] }
  let(:before_detail) { { workflow_id: "wf-1", status: status, available_actions: actions, steps: [], signals: [], events: [] } }
  let(:after_detail) { before_detail.merge(status: "ready") }
  let(:status) { "paused" }
  let(:who) { { actor: "mcp", reason: "gateway fixed" } }

  before { allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(before_detail, after_detail) }

  def call(tool, **args)
    response = tool.call(workflow_id: "wf-1", reason: "gateway fixed", server_context: context, **args)
    JSON.parse(response.content.first[:text])
  end

  {
    Pgbus::MCP::Tools::WorkflowResumeTool => ["pgbus_workflow_resume", :resume_workflow],
    Pgbus::MCP::Tools::WorkflowCancelTool => ["pgbus_workflow_cancel", :cancel_workflow],
    Pgbus::MCP::Tools::WorkflowSkipStepTool => ["pgbus_workflow_skip_step", :skip_workflow_step],
    Pgbus::MCP::Tools::WorkflowRunNowTool => ["pgbus_workflow_run_now", :run_workflow_step_now]
  }.each do |tool, (name, action)|
    describe name do
      it "calls DataSource##{action} with actor mcp and the reason, and answers with the run after it" do
        allow(data_source).to receive(action).and_return(ok: true)

        out = call(tool)

        expect(tool.tool_name).to eq(name)
        expect(data_source).to have_received(action).with("wf-1", **who)
        expect(out).to include("ok" => true, "reason" => nil)
        expect(out["workflow"]).to include("status" => "ready")
      end
    end
  end

  describe "pgbus_workflow_resume_at" do
    let(:tool) { Pgbus::MCP::Tools::WorkflowResumeAtTool }

    it "names the step to run" do
      allow(data_source).to receive(:resume_workflow_at).and_return(ok: true)

      out = call(tool, step: " capture ")

      expect(tool.input_schema.to_h[:required]).to include("step")
      expect(data_source).to have_received(:resume_workflow_at).with("wf-1", step: "capture", **who)
      expect(out).to include("ok" => true)
    end

    it "passes an unknown step's refusal through" do
      allow(data_source).to receive(:resume_workflow_at)
        .and_return(ok: false, code: :invalid, reason: "CheckoutWorkflow has no step nope")

      expect(call(tool, step: "nope")).to include("ok" => false, "code" => "invalid",
                                                  "reason" => "CheckoutWorkflow has no step nope")
    end

    it "needs a step" do
      allow(data_source).to receive(:resume_workflow_at)

      expect(call(tool, step: "  ")).to include("ok" => false, "reason" => "step must name a step")
      expect(data_source).not_to have_received(:resume_workflow_at)
    end
  end

  describe "pgbus_workflow_signal" do
    let(:tool) { Pgbus::MCP::Tools::WorkflowSignalTool }
    let(:status) { "waiting" }

    before { allow(data_source).to receive(:signal_workflow).and_return(ok: true, duplicate: false) }

    it "sends the name, the payload with string keys, the idempotency key, the actor and the reason" do
      out = call(tool, name: " shipped ", payload: { carrier: "DHL", parcels: [{ id: 1 }] }, idempotency_key: "evt-9")

      expect(data_source).to have_received(:signal_workflow)
        .with("wf-1", name: "shipped", payload: { "carrier" => "DHL", "parcels" => [{ "id" => 1 }] },
                      idempotency_key: "evt-9", **who)
      expect(out).to include("ok" => true, "duplicate" => false)
    end

    it "sends an empty payload and no idempotency key when none is given" do
      call(tool, name: "shipped")

      expect(data_source).to have_received(:signal_workflow)
        .with("wf-1", name: "shipped", payload: {}, idempotency_key: nil, **who)
    end

    it "answers a redelivery as a duplicate" do
      allow(data_source).to receive(:signal_workflow).and_return(ok: true, duplicate: true)

      expect(call(tool, name: "shipped", idempotency_key: "evt-9")).to include("ok" => true, "duplicate" => true)
    end

    it "refuses a payload over workflow_max_payload_size without sending it" do
      limit = Pgbus.configuration.workflow_max_payload_size

      out = call(tool, name: "shipped", payload: { blob: "x" * limit })

      expect(out).to include("ok" => false)
      expect(out["reason"]).to match(/payload is \d+ bytes, over workflow_max_payload_size \(#{limit}\)/)
      expect(data_source).not_to have_received(:signal_workflow)
    end

    it "refuses a payload that is not an object" do
      expect(call(tool, name: "shipped", payload: [1, 2])).to include("ok" => false,
                                                                      "reason" => "payload must be a JSON object")
      expect(data_source).not_to have_received(:signal_workflow)
    end

    it "needs a signal name" do
      expect(call(tool, name: " ")).to include("ok" => false, "reason" => "name must name a signal")
      expect(data_source).not_to have_received(:signal_workflow)
    end

    it "caps the name and the idempotency key at 255 characters (both are indexed)" do
      expect(call(tool, name: "x" * 256)).to include("ok" => false, "reason" => "name must be at most 255 characters")
      expect(call(tool, name: "shipped", idempotency_key: "k" * 256))
        .to include("ok" => false, "reason" => "idempotency_key must be at most 255 characters")
      expect(data_source).not_to have_received(:signal_workflow)
      expect(tool.input_schema.to_h[:properties]).to include(name: hash_including(maxLength: 255),
                                                             idempotency_key: hash_including(maxLength: 255))
    end

    it "keeps the expected_status guard and the available_actions check" do
      expect(call(tool, name: "shipped", expected_status: "paused"))
        .to include("ok" => false, "reason" => "workflow wf-1 is waiting, not paused")
      allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(before_detail.merge(available_actions: []))
      expect(call(tool, name: "shipped")).to include("ok" => false, "reason" => "cannot signal a waiting workflow")
      expect(data_source).not_to have_received(:signal_workflow)
    end

    it "takes name, payload and idempotency_key in its schema" do
      schema = tool.input_schema.to_h

      expect(schema[:required]).to contain_exactly("workflow_id", "reason", "name")
      expect(schema[:properties]).to include(payload: hash_including(type: "object"),
                                             idempotency_key: hash_including(type: "string"))
    end
  end

  it "describes each tool with the transition its verb makes" do
    {
      Pgbus::MCP::Tools::WorkflowResumeTool => :resume, Pgbus::MCP::Tools::WorkflowCancelTool => :cancel,
      Pgbus::MCP::Tools::WorkflowSkipStepTool => :skip, Pgbus::MCP::Tools::WorkflowResumeAtTool => :resume_at,
      Pgbus::MCP::Tools::WorkflowRunNowTool => :run_now, Pgbus::MCP::Tools::WorkflowSignalTool => :signal
    }.each do |tool, verb|
      transition = Pgbus::Workflow::States.fetch(verb)
      expect(tool.description).to include(transition.effect, transition.from.join(", "), "mcp_write_tools")
    end
  end
end
