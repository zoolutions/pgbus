# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Workflow::StepOutcome do
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      def self.name = "SpecOutcomeWorkflow"

      step(:charge) { nil }
      step(:receipt, wait: 30) { nil }
    end
  end
  let(:status) { "performing" }
  let(:record) do
    double("WorkflowEntry", workflow_id: "wf-1", workflow_class: "SpecOutcomeWorkflow", workflow_klass: klass).tap do |r|
      allow(r).to receive(:status) { status }
    end
  end
  let(:step) do
    double("WorkflowStep", id: 4, workflow_id: "wf-1", step_name: "charge", attempt: 1, worker: "host:1",
                           stall_at: Time.current, started_at: Time.current, scheduled_at: Time.current - 2)
  end
  let(:notes) { Pgbus::Workflow::Runner::Notes.new }
  let(:events) { [] }
  let(:outcome) { Pgbus::Workflow::FlowControl::Outcome }
  let(:transitions) { [] }

  def apply(result, **) = described_class.apply(record, step, result, notes: notes, klass: klass, **)

  before do
    allow(Pgbus::WorkflowStep).to receive(:finish!).and_return(1)
    allow(Pgbus::WorkflowStep).to receive(:cancel_open!)
    allow(Pgbus::Workflow::Runner).to receive(:transition!) { |_record, verb, **| transitions << verb }
    allow(Pgbus::Workflow::Runner).to receive(:event!) { |_record, event, **| events << event }
    allow(Pgbus::Workflow::Runner).to receive(:schedule!)
  end

  it "completes the step and schedules the next one with its wait" do
    expect(apply(outcome.complete, output: { "x" => 1 })).to be(true)

    expect(Pgbus::WorkflowStep).to have_received(:finish!)
      .with(4, hash_including(status: "completed", outcome: "completed", output: { "x" => 1 }))
    expect(Pgbus::Workflow::Runner).to have_received(:schedule!).with(record, "receipt", at: be_within(2).of(30.seconds.from_now),
                                                                                         actor: "system")
    expect(transitions).to eq([:advance])
    expect(events).to eq(%w[step_completed])
  end

  it "finishes the run after the last step, and on finished!" do
    allow(step).to receive(:step_name).and_return("receipt")
    apply(outcome.complete)
    allow(step).to receive(:step_name).and_return("charge")
    apply(outcome.finish)

    expect(transitions).to eq(%i[finish finish])
    expect(Pgbus::WorkflowStep).to have_received(:finish!).with(4, hash_including(outcome: "finished_early"))
    expect(events).to eq(%w[step_completed finished step_completed finished])
  end

  it "records a failed step, keeps the error on the run, and pauses" do
    error = RuntimeError.new("boom")
    apply(outcome.pause, error: error)

    expect(Pgbus::WorkflowStep).to have_received(:finish!).with(4, hash_including(status: "failed", error_class: "RuntimeError"))
    expect(Pgbus::Workflow::Runner).to have_received(:transition!)
      .with(record, :step_pause, hash_including(error_class: "RuntimeError", paused_at: a_kind_of(Time)))
    expect(events).to eq(%w[step_failed paused])
  end

  it "schedules the next attempt of a reattempted step" do
    apply(outcome.reattempt(wait: 15))

    expect(Pgbus::Workflow::Runner).to have_received(:schedule!).with(record, "charge", at: be_within(2).of(15.seconds.from_now),
                                                                                        actor: "system")
    expect(transitions).to eq([:reattempt])
    expect(events).to eq(%w[step_reattempted])
  end

  it "cancels the run and whatever else is open" do
    apply(outcome.cancel)

    expect(transitions).to eq([:step_cancel])
    expect(Pgbus::WorkflowStep).to have_received(:cancel_open!).with("wf-1", now: a_kind_of(Time))
    expect(events).to eq(%w[cancelled])
  end

  context "when an outside pause! got there first" do
    let(:status) { "paused" }

    it "records the outcome, schedules the next step and holds the run paused" do
      apply(outcome.complete)
      apply(outcome.pause)

      expect(transitions).to eq(%i[hold hold])
      expect(Pgbus::Workflow::Runner).to have_received(:schedule!).once
    end
  end

  it "ignores a late completion: no transition, a late_completion_ignored event, false" do
    allow(Pgbus::WorkflowStep).to receive(:finish!).and_return(0)

    expect(apply(outcome.complete, output: { "x" => 1 })).to be(false)

    expect(Pgbus::Workflow::Runner).not_to have_received(:transition!)
    expect(events).to eq(%w[late_completion_ignored])
  end

  it "still reports the error of a late attempt that raised" do
    allow(Pgbus::WorkflowStep).to receive(:finish!).and_return(0)
    allow(Pgbus::ErrorReporter).to receive(:report)
    error = RuntimeError.new("late boom")

    apply(outcome.pause, error: error)
    notes.flush

    expect(Pgbus::ErrorReporter).to have_received(:report)
      .with(error, hash_including(workflow_id: "wf-1", step_name: "charge", outcome: "late_completion_ignored"))
  end

  it "notes the step event, and the error report only when asked" do
    allow(Pgbus::Instrumentation).to receive(:instrument)
    allow(Pgbus::ErrorReporter).to receive(:report)

    apply(outcome.pause, error: RuntimeError.new("quiet"), report: false, duration: 0.5)
    notes.flush

    expect(Pgbus::Instrumentation).to have_received(:instrument)
      .with("pgbus.workflow.step", hash_including(outcome: "paused", status: "failed", duration: 0.5, pickup_latency: be_within(1).of(2)))
    expect(Pgbus::ErrorReporter).not_to have_received(:report)
  end

  describe ".stall!" do
    it "records stall_detected and applies workflow_stall_action with StepStalled" do
      config = instance_double(Pgbus::Configuration, workflow_stall_action: :reattempt, workflow_error_backtrace_lines: 50)

      described_class.stall!(record, step, detection: "redelivery", notes: notes, config: config)

      expect(events.first).to eq("stall_detected")
      expect(Pgbus::WorkflowStep).to have_received(:finish!)
        .with(4, hash_including(status: "failed", outcome: "reattempted", error_class: "Pgbus::Workflow::StepStalled"))
      expect(transitions).to eq([:reattempt])
    end

    it "maps each stall action to its outcome" do
      expect(described_class.stall_outcome(:pause).action).to eq(:pause)
      expect(described_class.stall_outcome(:cancel).action).to eq(:cancel)
      expect(described_class.stall_outcome(:reattempt).action).to eq(:reattempt)
    end
  end
end
