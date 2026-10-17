# frozen_string_literal: true

require "spec_helper"
require "async"

RSpec.describe Pgbus::Workflow::Current do
  let(:context) { { workflow_id: "wf-1", workflow_class: "SignupWorkflow", step_name: "welcome", attempt: 2 } }

  it "is empty outside a step" do
    expect(described_class.workflow_id).to be_nil
    expect(described_class.log_tag).to eq("")
  end

  it "exposes the running step inside the block and restores what was there after" do
    seen = described_class.with(**context) do
      [described_class.workflow_id, described_class.workflow_class, described_class.step_name, described_class.attempt,
       described_class.log_tag]
    end

    expect(seen).to eq(["wf-1", "SignupWorkflow", "welcome", 2, "workflow=wf-1 step=welcome attempt=2"])
    expect(described_class.context).to be_nil
  end

  it "restores the outer context after a nested step, also when the block raises" do
    described_class.with(**context) do
      expect do
        described_class.with(**context, workflow_id: "wf-2") { raise ArgumentError, "boom" }
      end.to raise_error(ArgumentError)

      expect(described_class.workflow_id).to eq("wf-1")
    end
    expect(described_class.workflow_id).to be_nil
  end

  it "puts the step on Pgbus's log context" do
    tags = described_class.with(**context) { Pgbus::LogFormatter.current_context.dup }

    expect(tags).to eq(workflow: "wf-1", step: "welcome", attempt: 2)
    expect(Pgbus::LogFormatter.current_context).to be_empty
  end

  it "is fiber-local, so a sibling fiber never sees it" do
    sibling = nil
    described_class.with(**context) do
      Fiber.new { sibling = described_class.workflow_id }.resume
    end

    expect(sibling).to be_nil
  end
end
