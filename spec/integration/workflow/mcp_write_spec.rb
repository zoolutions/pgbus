# frozen_string_literal: true

require_relative "support"
Pgbus::MCP.load!

# The MCP workflow write tools (issue #550) against a real database, called
# over the protocol (tools/call on a server built with mcp_write_tools on):
# the action lands on the run's audit trail with actor "mcp" and the reason,
# and the run goes on.
RSpec.describe "MCP workflow write tools (integration)", :integration do
  include WorkflowIntegration

  let(:gateway) { { up: false } }
  let(:server) { Pgbus::MCP::Server.build(data_source: Pgbus::Web::DataSource.new) }

  before do
    Pgbus.configuration.mcp_write_tools = true
    state = gateway
    stub_const("GatewayDown", Class.new(StandardError))
    stub_const("CheckoutWorkflow", Class.new(Pgbus::Workflow) do
      step(:capture) { raise GatewayDown, "gateway down" unless state[:up] }
      step(:receipt) { nil }
    end)
    stub_const("ShippingWorkflow", Class.new(Pgbus::Workflow) do
      step(:ship, wait_for: :shipped) { output(carrier: received_signal.payload[:carrier]) }
    end)
    stub_const("TwoGateWorkflow", Class.new(Pgbus::Workflow) do
      step(:packed, wait_for: :packed) { nil }
      step(:shipped, wait_for: :shipped) { nil }
    end)
  end

  after { Pgbus.configuration.mcp_write_tools = false }

  def call_tool(name, **arguments)
    request = { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: name, arguments: arguments } }
    response = JSON.parse(server.handle_json(JSON.generate(request)))
    expect(response).not_to have_key("error")
    JSON.parse(response.dig("result", "content").first["text"])
  end

  it "resumes a paused run: the event has actor mcp and the reason, and the run finishes" do
    wf = run_until(CheckoutWorkflow.start!)
    expect(wf.status).to eq("paused")
    gateway[:up] = true

    out = call_tool("pgbus_workflow_resume", workflow_id: wf.workflow_id, reason: "gateway is back",
                                             expected_status: "paused")

    expect(out).to include("ok" => true, "reason" => nil)
    expect(out["workflow"]).to include("workflow_id" => wf.workflow_id, "status" => "ready")
    resumed = wf.events.find { |event| event.event == "resumed" }
    expect([resumed.actor, resumed.reason]).to eq(["mcp", "gateway is back"])
    expect(out["workflow"]["events"].first).to include("event" => "step_scheduled", "actor" => "mcp")

    run_until(wf, statuses: %w[finished])
    expect(steps_of(wf).map { |step| [step.step_name, step.status] })
      .to eq([%w[capture failed], %w[capture completed], %w[receipt completed]])
  end

  it "does nothing on a stale read: the run moved on, the answer says where it is" do
    wf = run_until(CheckoutWorkflow.start!)
    events = wf.events.count

    out = call_tool("pgbus_workflow_cancel", workflow_id: wf.workflow_id, reason: "duplicate order",
                                             expected_status: "waiting")

    expect(out).to include("ok" => false, "reason" => "workflow #{wf.workflow_id} is paused, not waiting")
    expect(wf.reload.status).to eq("paused")
    expect(wf.events.count).to eq(events)
  end

  it "refuses a verb the run's status does not allow, with the allowed list" do
    wf = run_until(CheckoutWorkflow.start!)

    out = call_tool("pgbus_workflow_run_now", workflow_id: wf.workflow_id, reason: "hurry")

    expect(out).to include("ok" => false, "reason" => "cannot run now a paused workflow")
    expect(out["available_actions"]).to include("resume", "cancel")
    expect(wf.reload.status).to eq("paused")
  end

  it "signals a waiting run once per idempotency key, with the reason on the signal_received event" do
    wf = run_until(ShippingWorkflow.start!, statuses: %w[waiting])
    args = { workflow_id: wf.workflow_id, reason: "carrier webhook lost", name: "shipped",
             payload: { carrier: "DHL" }, idempotency_key: "evt-9" }

    expect(call_tool("pgbus_workflow_signal", **args)).to include("ok" => true, "duplicate" => false)
    expect(call_tool("pgbus_workflow_signal", **args)).to include("ok" => true, "duplicate" => true)

    received = wf.events.select { |event| event.event == "signal_received" }
    expect(received.map { |event| [event.actor, event.reason] })
      .to eq([["mcp", "carrier webhook lost"], ["mcp", "carrier webhook lost"]])
    expect(Pgbus::WorkflowSignal.where(workflow_id: wf.workflow_id).count).to eq(1)
    run_until(wf, statuses: %w[finished])
    expect(wf.output_of(:ship)).to include("carrier" => "DHL")
  end

  it "delivers every signal sent with an empty idempotency key: blank is no key, not a shared one" do
    wf = run_until(TwoGateWorkflow.start!, statuses: %w[waiting])

    first = call_tool("pgbus_workflow_signal", workflow_id: wf.workflow_id, reason: "packed", name: "packed",
                                               idempotency_key: "")
    expect(first).to include("ok" => true, "duplicate" => false)
    run_until(wf, statuses: %w[waiting])
    expect(steps_of(wf).last).to have_attributes(step_name: "shipped", status: "waiting")

    second = call_tool("pgbus_workflow_signal", workflow_id: wf.workflow_id, reason: "shipped", name: "shipped",
                                                idempotency_key: "")
    expect(second).to include("ok" => true, "duplicate" => false)
    run_until(wf, statuses: %w[finished])
    expect(Pgbus::WorkflowSignal.where(workflow_id: wf.workflow_id).pluck(:name, :idempotency_key))
      .to contain_exactly(["packed", nil], ["shipped", nil])
  end
end
