# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #572: what woken? and signal_ready? read before a wait_for: step's
# gate, with the models mocked. spec/integration/workflow/signal_guards_spec.rb
# runs it against a real database.
RSpec.describe Pgbus::Workflow::SignalPeek do
  let(:record) { double("WorkflowEntry", workflow_id: "wf-1") }
  let(:step) do
    double("WorkflowStep", step_name: "ship", waiting_since: nil, signal_id: nil, timeout_at: nil)
  end
  let(:waits) { Pgbus::Workflow::StepDefinition.new(name: :ship, body: proc {}, wait_for: :shipped) }
  let(:plain) { Pgbus::Workflow::StepDefinition.new(name: :ship, body: proc {}) }
  let(:held) { double("WorkflowSignal", name: "shipped", status: "claimed") }
  let(:pending) { double("WorkflowSignal", name: "shipped", status: "pending") }

  before do
    allow(Pgbus::WorkflowSignal).to receive(:held_by_step).and_return(nil)
    allow(Pgbus::Workflow::Signals).to receive(:match).and_return(nil)
  end

  it "reads nothing for a step without wait_for:" do
    peek = described_class.new(record, step, plain)

    expect([peek.held, peek.ready]).to eq([nil, nil])
    expect(Pgbus::WorkflowSignal).not_to have_received(:held_by_step)
    expect(Pgbus::Workflow::Signals).not_to have_received(:match)
  end

  it "answers the held signal first, without matching, and looks it up once" do
    allow(Pgbus::WorkflowSignal).to receive(:held_by_step).with("wf-1", "ship").and_return(held)
    peek = described_class.new(record, step, waits)

    expect([peek.held, peek.ready, peek.ready]).to eq([held, held, held])
    expect(Pgbus::WorkflowSignal).to have_received(:held_by_step).once
    expect(Pgbus::Workflow::Signals).not_to have_received(:match)
  end

  it "answers the pending signal the matcher takes when the step holds none" do
    allow(Pgbus::Workflow::Signals).to receive(:match).with(record, waits.wait_for).and_return(pending)
    peek = described_class.new(record, step, waits)

    expect([peek.held, peek.ready]).to eq([nil, pending])
  end

  it "answers no signal for an attempt the sweep woke with on_timeout: :run" do
    now = Time.current
    allow(step).to receive_messages(waiting_since: now - 3600, timeout_at: now)
    allow(Pgbus::Workflow::Signals).to receive(:match).and_return(pending)
    peek = described_class.new(record, step, waits, now: now)

    expect(peek.timed_out?).to be(true)
    expect(peek.ready).to be_nil
    expect(Pgbus::Workflow::Signals).not_to have_received(:match)
  end
end
