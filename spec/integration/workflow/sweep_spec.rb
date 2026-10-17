# frozen_string_literal: true

require_relative "support"

# Issue #540, step 6: the dispatcher's workflow sweep and cleanup.
RSpec.describe Pgbus::Workflow::Sweep, :integration do
  include WorkflowIntegration

  let(:config) { Pgbus.configuration }

  before do
    stub_const("SweptWorkflow", Class.new(Pgbus::Workflow) do
      step(:work) { nil }
      step(:quick, timeout: 30) { nil }
    end)
  end

  # A run whose first step was claimed at +started+ by a worker that is gone.
  def stalled_run(started: 2.hours.ago, step_name: "work")
    wf = SweptWorkflow.start!
    step = steps_of(wf).first
    Pgbus::WorkflowEntry.where(workflow_id: wf.workflow_id).update_all(status: "performing")
    timeout = SweptWorkflow.workflow_steps.fetch(step_name).timeout || config.workflow_stall_threshold
    step.update_columns(step_name: step_name, status: "in_progress", started_at: started, stall_at: started + timeout,
                        worker: "gone-host:1")
    wf.reload
  end

  describe "stalled in_progress steps" do
    it "pauses with StepStalled by default" do
      wf = stalled_run

      expect(described_class.repair(config: config)).to include(stalled: 1)

      expect(wf.reload.status).to eq("paused")
      expect(steps_of(wf).first).to have_attributes(status: "failed", outcome: "paused", error_class: "Pgbus::Workflow::StepStalled")
      expect(wf.events.find { |e| e.event == "stall_detected" }.details).to include("detection" => "sweep")
      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(Pgbus::Workflow::StepStalled), anything)
    end

    it "reattempts the step with workflow_stall_action :reattempt" do
      allow(config).to receive(:workflow_stall_action).and_return(:reattempt)
      wf = stalled_run

      described_class.repair(config: config)

      expect(wf.reload.status).to eq("ready")
      expect(steps_of(wf).map { |s| [s.attempt, s.status] }).to eq([[1, "failed"], [2, "scheduled"]])
    end

    it "cancels the run with workflow_stall_action :cancel" do
      allow(config).to receive(:workflow_stall_action).and_return(:cancel)
      wf = stalled_run

      described_class.repair(config: config)

      expect(wf.reload.status).to eq("cancelled")
    end

    it "honours a step's own timeout: through stall_at, without loading the class" do
      quick = stalled_run(started: 45.seconds.ago, step_name: "quick")
      slow = stalled_run(started: 45.seconds.ago)

      described_class.repair(config: config)

      expect(quick.reload.status).to eq("paused")
      expect(slow.reload.status).to eq("performing")
    end
  end

  describe "overdue scheduled steps" do
    it "re-sends a step once per threshold, by bumping enqueued_at" do
      wf = SweptWorkflow.start!
      step = steps_of(wf).first
      Pgbus.client.purge_queue(WorkflowIntegration::QUEUE)
      step.update_columns(scheduled_at: 2.hours.ago, enqueued_at: 2.hours.ago)

      expect(described_class.repair(config: config)).to include(resent: 1)
      expect(queue_depth).to eq(1)
      expect(step.reload.enqueued_at).to be > 1.minute.ago

      expect(described_class.repair(config: config)).to include(resent: 0)
      expect(queue_depth).to eq(1)
    end

    it "does not take a step delayed by job_options wait: for a lost one" do
      stub_const("DelayedWorkflow", Class.new(Pgbus::Workflow) { step(:later, job_options: { wait: 3 * 3600 }) { nil } })
      wf = DelayedWorkflow.start!
      expect(steps_of(wf).first.scheduled_at).to be > 2.hours.from_now

      steps_of(wf).first.update_columns(enqueued_at: 2.hours.ago)
      expect(described_class.repair(config: config)).to include(resent: 0)
    end

    it "leaves the scheduled step of a paused run alone" do
      wf = SweptWorkflow.start!
      wf.pause!
      steps_of(wf).first.update_columns(scheduled_at: 2.hours.ago, enqueued_at: 2.hours.ago)

      expect(described_class.repair(config: config)).to include(resent: 0)
    end
  end

  describe "the gauge" do
    it "reports runs by class and status, and by class and stuck reason" do
      stalled_run.tap { described_class.repair(config: config) }
      SweptWorkflow.start!
      events = []
      subscription = ActiveSupport::Notifications.subscribe("pgbus.workflow.gauge") { |*args| events << args.last }

      described_class.repair(config: config)

      gauge = events.last
      expect(gauge[:statuses]).to include({ workflow_class: "SweptWorkflow", status: "paused", count: 1 },
                                          { workflow_class: "SweptWorkflow", status: "ready", count: 1 },
                                          { workflow_class: "SweptWorkflow", status: "finished", count: 0 })
      expect(gauge[:stuck]).to include({ workflow_class: "SweptWorkflow", reason: "stalled", count: 1 },
                                       { workflow_class: "SweptWorkflow", reason: "paused_error", count: 0 })
    ensure
      ActiveSupport::Notifications.unsubscribe(subscription) if subscription
    end

    it "tells each stuck reason apart" do
      error_run = run_until(Class.new(Pgbus::Workflow) { step(:x) { raise "boom" } }.tap { |k| stub_const("ErrorWorkflow", k) }.start!)
      manual_run = run_until(Class.new(Pgbus::Workflow) { step(:x) { pause! } }.tap { |k| stub_const("ManualWorkflow", k) }.start!)
      overdue = SweptWorkflow.start!
      steps_of(overdue).first.update_columns(scheduled_at: 2.hours.ago)

      counts = Pgbus::WorkflowEntry.stuck_reason_counts(threshold: config.workflow_stall_threshold)

      expect(counts).to eq([error_run.workflow_class, "paused_error"] => 1, [manual_run.workflow_class, "paused_manual"] => 1,
                           %w[SweptWorkflow overdue] => 1)
    end

    # Issue #541, step 5.
    it "counts waiting_overdue for a long wait without a wait_timeout:, never a wait that has one" do
      stub_const("ParkedWorkflow", Class.new(Pgbus::Workflow) do
        step(:forever, wait_for: :never) { nil }
        step(:bounded, wait_for: :never, wait_timeout: 30.days) { nil }
      end)
      old = run_until(ParkedWorkflow.start!, statuses: %w[waiting])
      steps_of(old).last.update_columns(waiting_since: 8.days.ago)
      fresh = run_until(ParkedWorkflow.start!, statuses: %w[waiting])
      bounded = run_until(ParkedWorkflow.start!, statuses: %w[waiting])
      Pgbus::WorkflowStep.where(workflow_id: bounded.workflow_id)
                         .update_all(step_name: "bounded", waiting_since: 8.days.ago, timeout_at: 22.days.from_now)

      counts = Pgbus::WorkflowEntry.stuck_reason_counts(threshold: config.workflow_stall_threshold,
                                                        waiting_threshold: config.workflow_waiting_visibility_threshold)
      expect(counts).to eq(%w[ParkedWorkflow waiting_overdue] => 1)
      expect(fresh.reload.status).to eq("waiting")
    end

    it "never treats a waiting step as stalled, and leaves it out of the waiting_overdue count before the schema exists" do
      stub_const("ParkedWorkflow", Class.new(Pgbus::Workflow) { step(:forever, wait_for: :never) { nil } })
      wf = run_until(ParkedWorkflow.start!, statuses: %w[waiting])
      steps_of(wf).last.update_columns(waiting_since: 30.days.ago, started_at: 30.days.ago)

      expect(described_class.repair(config: config)).to include(stalled: 0, resent: 0, timed_out: 0)
      expect(wf.reload.status).to eq("waiting")
      expect(Pgbus::WorkflowEntry.stuck_reason_counts(threshold: config.workflow_stall_threshold)).to be_empty
    end
  end

  describe ".cleanup" do
    it "deletes only finished and cancelled runs older than the retention, with their steps and events" do
      old_done = SweptWorkflow.start!.tap(&:cancel!)
      Pgbus::WorkflowEntry.where(workflow_id: old_done.workflow_id).update_all(updated_at: 40.days.ago)
      recent_done = SweptWorkflow.start!.tap(&:cancel!)
      old_ongoing = SweptWorkflow.start!
      Pgbus::WorkflowEntry.where(workflow_id: old_ongoing.workflow_id).update_all(updated_at: 40.days.ago)

      expect(described_class.cleanup(retention: 30 * 86_400, batch_size: 1)).to eq(1)

      expect(Pgbus::WorkflowEntry.pluck(:workflow_id)).to contain_exactly(recent_done.workflow_id, old_ongoing.workflow_id)
      expect(Pgbus::WorkflowStep.where(workflow_id: old_done.workflow_id)).not_to exist
      expect(Pgbus::WorkflowEvent.where(workflow_id: old_done.workflow_id)).not_to exist
    end

    it "keeps everything when the retention is nil" do
      SweptWorkflow.start!.cancel!

      expect(described_class.cleanup(retention: nil)).to eq(0)
    end
  end
end
