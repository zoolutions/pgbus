# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #541, step 4: the signal gate inside one StepJob delivery, with the
# models mocked. spec/integration/workflow/signals_spec.rb runs it against a
# real database.
RSpec.describe "Pgbus::Workflow::StepExecution signal gate" do # rubocop:disable RSpec/DescribeClass
  let(:ran) { [] }
  let(:klass) do
    calls = ran
    Class.new(Pgbus::Workflow) do
      def self.name = "SpecGateWorkflow"

      step(:ship, wait_for: :shipped) { calls << [received_signal&.name, timed_out?] }
    end
  end
  let(:record) { double("WorkflowEntry", workflow_id: "wf-1", workflow_class: "SpecGateWorkflow", status: "ready") }
  let(:step) do
    double("WorkflowStep", id: 11, workflow_id: "wf-1", workflow_class: "SpecGateWorkflow", step_name: "ship", attempt: 1,
                           status: "scheduled", scheduled_at: Time.current - 1, msg_id: nil, assign_attributes: nil)
  end
  let(:notes) { Pgbus::Workflow::Runner::Notes.new }
  let(:applied) { [] }

  def perform = Pgbus::Workflow::StepExecution.new(11, delivery: { msg_id: 42, read_ct: 1 }).call

  before do
    allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(step)
    allow(Pgbus::Workflow::Runner).to receive(:locked).with("wf-1").and_yield(record, notes)
    allow(record).to receive(:workflow_klass) { klass }
    allow(Pgbus::WorkflowStep).to receive_messages(claim!: 1, consecutive_reattempts: 0)
    allow(Pgbus::Workflow::Runner).to receive(:transition!)
    allow(Pgbus::Workflow::Runner).to receive(:event!)
    allow(Pgbus::Deploy::Revision).to receive(:current).and_return("rev-1")
    allow(Pgbus::Workflow::StepOutcome).to receive(:apply) { |_record, _step, outcome, **options| applied << [outcome, options] }
    allow(Pgbus::WorkflowStep).to receive(:where).with(id: 11, status: "in_progress").and_return(double(exists?: true))
  end

  it "runs the body with the signal the gate took" do
    allow(Pgbus::Workflow::Signals).to receive(:gate) do |_record, _step, _definition, workflow|
      workflow.send(:enter_wait, signal: double("WorkflowSignal", name: "shipped"), timed_out: false)
      true
    end

    perform

    expect(ran).to eq([["shipped", false]])
    expect(applied.map { |outcome, _| outcome.action }).to eq([:complete])
  end

  it "ends the delivery without running the body when the step parks" do
    allow(Pgbus::Workflow::Signals).to receive(:gate).and_return(false)

    perform

    expect(ran).to be_empty
    expect(applied).to be_empty
  end

  it "hands a matcher that raises to the exception policies, like a guard" do
    allow(Pgbus::Workflow::Signals).to receive(:gate).and_raise(ArgumentError, "matcher blew up")

    perform

    expect(ran).to be_empty
    outcome, options = applied.last
    expect(outcome.action).to eq(:pause)
    expect(options[:error]).to be_a(ArgumentError)
  end

  it "skips the gate for a step that does not wait" do
    calls = ran
    klass.step(:plain) { calls << :plain }
    allow(step).to receive(:step_name).and_return("plain")
    allow(Pgbus::Workflow::Signals).to receive(:gate)

    perform

    expect(ran).to eq([:plain])
    expect(Pgbus::Workflow::Signals).not_to have_received(:gate)
  end
end
