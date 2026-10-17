# frozen_string_literal: true

require "json"
require_relative "spec_helper"

# The write tools' shared contract (issue #550): destructive annotations, a
# required reason, the expected_status guard, the available_actions check,
# the actor, and the answer with the run after the action.
RSpec.describe Pgbus::MCP::WriteTool do
  subject(:tool) do
    Class.new(described_class) do
      tool_name "spec_workflow_resume"
      input_schema(**workflow_input_schema)

      def self.call(workflow_id:, reason:, expected_status: nil, server_context: nil)
        perform(:resume, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                         server_context: server_context) do |data_source, **who|
          data_source.resume_workflow(workflow_id, **who)
        end
      end
    end
  end

  let(:data_source) { instance_double(Pgbus::Web::DataSource, reset_cache!: nil) }
  let(:context) { { data_source: data_source, allow_payloads: true } }
  let(:paused) do
    { workflow_id: "wf-1", workflow_class: "CheckoutWorkflow", status: "paused", current_step: "capture",
      error_message: "card 4242", arguments: { "card" => "4242" }, available_actions: %i[resume resume_at cancel signal],
      steps: [{ step_name: "capture", status: "failed", output: { "token" => "tok_1" }, error_message: "card 4242",
                error_backtrace: "app/x.rb:1", error_causes: [] }],
      signals: [{ name: "shipped", payload: { "address" => "Main St 1" } }],
      events: [], timeline: { rows: [] } }
  end
  let(:ready) { paused.merge(status: "ready", available_actions: %i[cancel run_now reschedule signal], error_message: nil) }

  before do
    allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(paused, ready)
    allow(data_source).to receive(:resume_workflow).and_return(ok: true)
  end

  def answer(**args)
    response = tool.call(workflow_id: "wf-1", reason: "gateway fixed", server_context: context, **args)
    expect(response.error?).to be(false)
    JSON.parse(response.content.first[:text])
  end

  describe "annotations" do
    it "marks every write tool destructive and not read-only, closed-world" do
      expect(tool.annotations_value.to_h).to include(readOnlyHint: false, destructiveHint: true, idempotentHint: false,
                                                     openWorldHint: false)
    end

    it "keeps them when a tool declares itself idempotent" do
      idempotent = Class.new(described_class) do
        tool_name "spec_workflow_cancel"
        idempotent!
      end

      expect(idempotent.annotations_value.to_h).to include(readOnlyHint: false, destructiveHint: true, idempotentHint: true)
      expect(tool.annotations_value.to_h).to include(idempotentHint: false)
    end

    it "is not a read tool" do
      expect(tool).not_to be <= Pgbus::MCP::BaseTool
    end
  end

  describe "the input schema" do
    let(:schema) { tool.input_schema.to_h }

    it "requires workflow_id and a reason of 1-500 characters, and takes expected_status from the workflow states" do
      expect(schema[:required]).to contain_exactly("workflow_id", "reason")
      expect(schema[:properties][:reason]).to include(type: "string", minLength: 1, maxLength: 500)
      expect(schema[:properties][:expected_status][:enum]).to eq(Pgbus::Workflow::States::WORKFLOW)
    end
  end

  describe "the reason" do
    it "is required to say something" do
      out = answer(reason: "   ")

      expect(out).to include("ok" => false, "reason" => "reason must be 1-500 characters after trimming whitespace")
      expect(data_source).not_to have_received(:resume_workflow)
    end

    it "is at most 500 characters" do
      expect(answer(reason: "x" * 501)).to include("ok" => false)
      expect(data_source).not_to have_received(:resume_workflow)
    end

    it "is recorded trimmed, with actor mcp" do
      answer(reason: "  gateway fixed \n")

      expect(data_source).to have_received(:resume_workflow).with("wf-1", actor: "mcp", reason: "gateway fixed")
    end
  end

  it "answers ok: false for an unknown run, and does nothing" do
    allow(data_source).to receive(:workflow_detail).with("nope").and_return(nil)

    out = JSON.parse(tool.call(workflow_id: "nope", reason: "x", server_context: context).content.first[:text])

    expect(out).to eq("ok" => false, "reason" => "workflow nope not found", "workflow" => nil)
    expect(data_source).not_to have_received(:resume_workflow)
  end

  describe "expected_status" do
    it "acts when the run is still in that status" do
      expect(answer(expected_status: "paused")).to include("ok" => true)
    end

    it "answers ok: false with the run as it is now when it moved on" do
      allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(ready)

      out = answer(expected_status: "paused")

      expect(out).to include("ok" => false, "reason" => "workflow wf-1 is ready, not paused")
      expect(out["workflow"]).to include("status" => "ready")
      expect(data_source).not_to have_received(:resume_workflow)
    end
  end

  it "answers ok: false with the allowed actions when the run's status does not allow the verb" do
    allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(ready)

    out = answer

    expect(out).to include("ok" => false, "reason" => "cannot resume a ready workflow",
                           "available_actions" => %w[cancel run_now reschedule signal])
    expect(data_source).not_to have_received(:resume_workflow)
  end

  describe "the answer" do
    it "carries the action's outcome and the run after it" do
      out = answer

      expect(out).to include("ok" => true, "reason" => nil)
      expect(out["workflow"]).to include("workflow_id" => "wf-1", "status" => "ready")
      expect(out["workflow"]).not_to have_key("timeline")
    end

    it "passes a refused action's code and status through" do
      allow(data_source).to receive(:resume_workflow)
        .and_return(ok: false, code: :not_allowed, reason: "cannot resume a ready workflow", status: "ready")

      expect(answer).to include("ok" => false, "code" => "not_allowed", "reason" => "cannot resume a ready workflow",
                                "status" => "ready")
    end

    it "redacts arguments, outputs, signal payloads and error text even when the server allows payloads" do
      allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(paused, paused)

      workflow = answer["workflow"]

      expect(workflow).to include("arguments" => "[redacted]", "error_message" => "[redacted]")
      expect(workflow["steps"].sole).to include("output" => "[redacted]", "error_message" => "[redacted]",
                                                "error_backtrace" => "[redacted]")
      expect(workflow["signals"].sole).to include("payload" => "[redacted]")
    end
  end

  describe "the actor" do
    def context_with_client(info)
      client_context = { data_source: data_source }
      double("MCP::ServerContext", client_info: info).tap do |ctx|
        allow(ctx).to receive(:[]) { |key| client_context[key] }
      end
    end

    def actor_for(info)
      actors = []
      allow(data_source).to receive(:workflow_detail).with("wf-1").and_return(paused)
      allow(data_source).to receive(:resume_workflow) { |_id, actor:, **| actors << actor and { ok: true } }
      tool.call(workflow_id: "wf-1", reason: "fix", server_context: context_with_client(info))
      actors.sole
    end

    it "names the MCP client when the transport knows it" do
      expect(actor_for({ name: "claude-code", version: "2.1" })).to eq("mcp:claude-code")
    end

    it "takes a string-keyed client name too" do
      expect(actor_for({ "name" => "cursor" })).to eq("mcp:cursor")
    end

    it "strips control characters from the client name and caps its length" do
      expect(actor_for({ name: "evil\nname\e[31m#{"x" * 200}" })).to eq("mcp:evilname[31m#{"x" * 88}")
    end

    it "is plain mcp without a client name" do
      expect(actor_for(nil)).to eq("mcp")
      expect(actor_for({ name: "  " })).to eq("mcp")
    end
  end
end
