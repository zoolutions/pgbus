# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #540, step 4: one StepJob delivery, with the models mocked. The same
# paths run against a real database in spec/integration/workflow/.
RSpec.describe Pgbus::Workflow::StepJob do
  let(:ran) { [] }
  let(:klass) do
    calls = ran
    Class.new(Pgbus::Workflow) do
      def self.name = "SpecStepWorkflow"

      step(:charge) { calls << [:charge, Pgbus::Workflow::Current.step_name] }
    end
  end
  let(:record) { double("WorkflowEntry", workflow_id: "wf-1", workflow_class: "SpecStepWorkflow", status: "ready") }
  let(:step) do
    double("WorkflowStep", id: 11, workflow_id: "wf-1", workflow_class: "SpecStepWorkflow", step_name: "charge", attempt: 1,
                           status: "scheduled", scheduled_at: Time.current - 1, msg_id: nil, assign_attributes: nil)
  end
  let(:notes) { Pgbus::Workflow::Runner::Notes.new }
  let(:delivery) { { msg_id: 42, read_ct: 1, queue_name: "default" } }
  let(:applied) { [] }

  def perform(delivery: self.delivery)
    Pgbus::Workflow::StepExecution.new(11, delivery: delivery).call
  end

  def outcomes = applied.map { |outcome, _options| outcome.action }

  before do
    allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(step)
    allow(Pgbus::Workflow::Runner).to receive(:locked).with("wf-1").and_yield(record, notes)
    allow(record).to receive(:workflow_klass) { klass }
    allow(Pgbus::WorkflowStep).to receive_messages(claim!: 1, consecutive_reattempts: 0)
    allow(Pgbus::Workflow::Runner).to receive(:transition!)
    allow(Pgbus::Workflow::Runner).to receive(:event!)
    allow(Pgbus::Deploy::Revision).to receive(:current).and_return("rev-1")
    allow(Pgbus::Workflow::StepOutcome).to receive(:apply) { |_record, _step, outcome, **options| applied << [outcome, options] }
    allow(Pgbus::Workflow::StepOutcome).to receive(:stall!)
  end

  it "runs perform(step_id) through a StepExecution" do
    execution = instance_double(Pgbus::Workflow::StepExecution, call: nil)
    allow(Pgbus::Workflow::StepExecution).to receive(:new).with(11).and_return(execution)

    described_class.new(11).perform_now

    expect(execution).to have_received(:call)
  end

  it "never opts out of the visibility heartbeat, which crash detection depends on" do
    ActiveJob::Base.include(Pgbus::VisibilityHeartbeat::JobMixin) unless ActiveJob::Base < Pgbus::VisibilityHeartbeat::JobMixin

    expect(described_class.pgbus_visibility_heartbeat_enabled).not_to be(false)
  end

  it "has no retry_on or discard_on: a step's failures belong to the exception policies" do
    expect(described_class.rescue_handlers).to be_empty
  end

  describe "the claim" do
    it "claims the row, moves the run to performing, runs the body and completes the step" do
      perform

      expect(Pgbus::WorkflowStep).to have_received(:claim!)
        .with(11, hash_including(msg_id: 42, worker: a_string_matching(/:\d+\z/), app_revision: "rev-1"))
      expect(Pgbus::Workflow::Runner).to have_received(:transition!).with(record, :claim, current_step: "charge")
      expect(Pgbus::Workflow::Runner).to have_received(:event!).with(record, "step_started", hash_including(actor: "system"))
      expect(ran).to eq([[:charge, "charge"]])
      expect(outcomes).to eq([:complete])
    end

    it "sets stall_at from the step's timeout, else workflow_stall_threshold" do
      allow(Pgbus.configuration).to receive(:workflow_stall_threshold).and_return(3600)
      perform
      klass.step(:quick, timeout: 30) { nil }
      allow(step).to receive(:step_name).and_return("quick")
      perform

      stall_ats = []
      expect(Pgbus::WorkflowStep).to have_received(:claim!).twice do |_id, now:, stall_at:, **|
        stall_ats << (stall_at - now).round
      end
      expect(stall_ats).to eq([3600, 30])
    end

    it "does not run the body when another delivery claimed the row first" do
      allow(Pgbus::WorkflowStep).to receive(:claim!).and_return(0)

      perform

      expect(ran).to be_empty
      expect(Pgbus::Workflow::Runner).not_to have_received(:transition!)
    end

    it "is a no-op for an early message (scheduled_at still ahead) or a run that is not ready" do
      allow(step).to receive(:scheduled_at).and_return(Time.current + 60)
      perform
      allow(step).to receive(:scheduled_at).and_return(Time.current)
      allow(record).to receive(:status).and_return("paused")
      perform

      expect(Pgbus::WorkflowStep).not_to have_received(:claim!)
      expect(ran).to be_empty
    end

    it "returns when the step row or its run is gone" do
      allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(nil)
      perform

      allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(step)
      allow(Pgbus::Workflow::Runner).to receive(:locked).with("wf-1").and_yield(nil, notes)
      perform

      expect(ran).to be_empty
      expect(Pgbus::WorkflowStep).not_to have_received(:claim!)
    end
  end

  describe "the guards" do
    it "cancels without running the body when cancel_if is true" do
      klass.cancel_if { true }

      perform

      expect(ran).to be_empty
      expect(outcomes).to eq([:cancel])
    end

    it "skips without running the body when skip_if is true" do
      klass.step(:skipped, skip_if: -> { true }) { raise "never runs" }
      allow(step).to receive(:step_name).and_return("skipped")

      perform

      expect(outcomes).to eq([:skip])
    end

    it "pauses with StepNotDefined when the row names a step the class does not define" do
      allow(step).to receive(:step_name).and_return("renamed")

      perform

      outcome, options = applied.first
      expect(outcome.action).to eq(:pause)
      expect(options[:error]).to be_a(Pgbus::Workflow::StepNotDefined)
      expect(options[:error].message).to include("removed_step :renamed")
    end

    it "pauses with StepNotDefined when the workflow class is gone" do
      allow(record).to receive(:workflow_klass).and_return(nil)

      perform

      expect(applied.first.last[:error]).to be_a(Pgbus::Workflow::StepNotDefined)
    end

    it "skips a step a removed_step gravestone covers" do
      klass.removed_step :old
      allow(step).to receive(:step_name).and_return("old")

      perform

      expect(outcomes).to eq([:skip])
    end

    it "sends a guard that raises through the exception policies" do
      klass.cancel_if { raise ArgumentError, "subject lookup failed" }

      perform

      outcome, options = applied.first
      expect(outcome.action).to eq(:pause)
      expect(options[:error]).to be_a(ArgumentError)
      expect(ran).to be_empty
    end
  end

  describe "the body's outcome" do
    {
      "pause!" => [:pause, nil], "cancel!" => [:cancel, nil], "skip!" => [:skip, nil],
      "finished!" => [:finish, nil], "reattempt!(wait: 30)" => [:reattempt, 30]
    }.each do |verb, (action, wait)|
      it "applies #{verb} as a #{action} outcome" do
        klass.step(:verb) { instance_eval(verb) }
        allow(step).to receive(:step_name).and_return("verb")

        perform

        expect(applied.first.first).to have_attributes(action: action, wait: wait)
      end
    end

    it "records the step's output with its outcome" do
      klass.step(:with_output) { output(charge_id: "ch_1") }
      allow(step).to receive(:step_name).and_return("with_output")

      perform

      expect(applied.first.last[:output]).to include("charge_id" => "ch_1")
    end

    it "resolves an exception through the policies and passes their report decision on" do
      klass.on_exception ArgumentError, action: :skip!, report: :never
      klass.step(:raises) { raise ArgumentError, "bad" }
      allow(step).to receive(:step_name).and_return("raises")

      perform

      outcome, options = applied.first
      expect(outcome.action).to eq(:skip)
      expect(options).to include(error: an_instance_of(ArgumentError), report: false)
    end

    it "pauses and reports an unhandled exception by default, without raising into the job" do
      klass.step(:raises) { raise "boom" }
      allow(step).to receive(:step_name).and_return("raises")

      expect { perform }.not_to raise_error

      expect(applied.first.first.action).to eq(:pause)
      expect(applied.first.last).to include(report: true)
    end

    it "decides an imperative policy before taking the run's lock again, so user code never holds it" do
      locks = 0
      allow(Pgbus::Workflow::Runner).to receive(:locked).with("wf-1") do |&block|
        locks += 1
        block.call(record, notes)
      end
      seen_at = nil
      klass.on_exception(RuntimeError) { |_e| (seen_at = locks) && skip! }
      klass.step(:raises) { raise "boom" }
      allow(step).to receive(:step_name).and_return("raises")

      perform

      expect(seen_at).to eq(1)
      expect(locks).to eq(2)
      expect(outcomes).to eq([:skip])
    end

    it "counts consecutive reattempts for max_reattempts from the step rows" do
      allow(Pgbus::WorkflowStep).to receive(:consecutive_reattempts).with("wf-1", "raises").and_return(3)
      klass.on_exception RuntimeError, action: :reattempt!, max_reattempts: 3, terminal_action: :cancel!
      klass.step(:raises) { raise "again" }
      allow(step).to receive(:step_name).and_return("raises")

      perform

      expect(outcomes).to eq([:cancel])
    end

    it "sets Pgbus::Workflow::Current inside the body and restores it, also when the body raises" do
      seen = nil
      klass.step(:current) do
        seen = [Pgbus::Workflow::Current.workflow_id, Pgbus::Workflow::Current.attempt]
        raise "boom"
      end
      allow(step).to receive(:step_name).and_return("current")

      perform

      expect(seen).to eq(["wf-1", 1])
      expect(Pgbus::Workflow::Current.context).to be_nil
    end
  end

  describe "crash detection on redelivery" do
    before do
      allow(step).to receive_messages(status: "in_progress", msg_id: 42)
      allow(record).to receive(:status).and_return("performing")
    end

    it "applies the stall action when the claiming message comes back with the heartbeat on" do
      perform(delivery: { msg_id: 42, read_ct: 2, queue_name: "default" })

      expect(Pgbus::Workflow::StepOutcome).to have_received(:stall!).with(record, step, hash_including(detection: "redelivery"))
      expect(ran).to be_empty
    end

    {
      "another message" => [{ msg_id: 99, read_ct: 2, queue_name: "default" }, true],
      "a first delivery" => [{ msg_id: 42, read_ct: 1, queue_name: "default" }, true],
      "the heartbeat off" => [{ msg_id: 42, read_ct: 2, queue_name: "default" }, false],
      "no delivery (perform_now)" => [nil, true]
    }.each do |label, (redelivery, heartbeat)|
      it "returns without touching the row for #{label}" do
        allow(Pgbus.configuration).to receive(:visibility_heartbeat).and_return(heartbeat)

        perform(delivery: redelivery)

        expect(Pgbus::Workflow::StepOutcome).not_to have_received(:stall!)
        expect(ran).to be_empty
      end
    end
  end
end
