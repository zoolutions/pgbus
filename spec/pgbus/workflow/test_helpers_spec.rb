# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_support/core_ext/numeric/time"

RSpec.describe Pgbus::Workflow::TestHelpers do
  include described_class

  let(:statuses) { [] }
  let(:workflow) do
    double("workflow", workflow_id: "wf-1", inspect: "#<SpecWorkflow wf-1>").tap do |wf|
      allow(wf).to receive(:reload) { wf }
      allow(wf).to receive(:status) { statuses.first }
    end
  end
  let(:scheduled) { double("WorkflowStep", id: 7, step_name: "welcome", scheduled_at: 2.days.from_now) }

  before do
    allow(described_class).to receive(:perform) { statuses.shift }
  end

  describe "#perform_next_step" do
    before { statuses.replace(%w[ready]) }

    it "refuses a run that is not ready, before touching its step" do
      statuses.replace(%w[paused])
      allow(described_class).to receive(:scheduled_step).and_return(scheduled)

      expect { perform_next_step(workflow) }.to raise_error(described_class::Error, /is paused, not ready: resume!/)
      statuses.replace(%w[finished])
      expect { perform_next_step(workflow) }.to raise_error(described_class::Error, /finished, not ready: .*rerun!/)
      expect(described_class).not_to have_received(:perform)
    end

    it "runs the scheduled step and returns the reloaded workflow" do
      allow(described_class).to receive(:scheduled_step).with(workflow).and_return(scheduled)

      expect(perform_next_step(workflow)).to be(workflow)
      expect(described_class).to have_received(:perform).with(scheduled, workflow)
    end

    it "raises when nothing is scheduled" do
      allow(described_class).to receive(:scheduled_step).and_return(nil)

      expect { perform_next_step(workflow) }.to raise_error(described_class::Error, /no scheduled step/)
    end
  end

  describe "#run_step_inline" do
    before do
      statuses.replace(%w[ready])
      allow(described_class).to receive(:scheduled_step).and_return(scheduled)
    end

    it "runs the named step when it is the one scheduled" do
      run_step_inline(workflow, :welcome)

      expect(described_class).to have_received(:perform).with(scheduled, workflow)
    end

    it "raises, naming the scheduled step, when another one is next" do
      expect { run_step_inline(workflow, :complete) }
        .to raise_error(described_class::Error, /not about to run complete \(scheduled: welcome\)/)
    end
  end

  describe "#speedrun_workflow" do
    before { allow(described_class).to receive(:scheduled_step).and_return(scheduled) }

    it "runs steps until the run is finished" do
      statuses.replace(%w[ready ready finished])

      expect(speedrun_workflow(workflow)).to be(workflow)
      expect(described_class).to have_received(:perform).twice
    end

    it "stops at paused and at cancelled" do
      statuses.replace(%w[ready paused])
      speedrun_workflow(workflow)

      statuses.replace(%w[cancelled])
      speedrun_workflow(workflow)

      expect(described_class).to have_received(:perform).once
    end

    it "raises StepWaiting on waiting, naming the signal or the batch the step waits for" do
      statuses.replace(%w[waiting waiting])
      parked = double(step_name: "ship", batch_id: nil, wait_for: "shipped", timeout_at: nil)
      allow(described_class).to receive(:waiting_step).with(workflow).and_return(parked)

      expect { speedrun_workflow(workflow) }
        .to raise_error(Pgbus::Workflow::StepWaiting, /waiting on step ship for signal shipped: deliver_signal/)

      fanned_out = double(step_name: "notify", batch_id: "b-1", wait_for: nil)
      allow(described_class).to receive(:waiting_step).with(workflow).and_return(fanned_out)
      expect { speedrun_workflow(workflow) }
        .to raise_error(Pgbus::Workflow::StepWaiting, /notify's batch b-1: complete_fan_out/)
    end

    it "returns a run that finishes on the last allowed step" do
      statuses.replace(%w[ready ready finished])

      expect(speedrun_workflow(workflow, max_steps: 2)).to be(workflow)
    end

    it "raises when the run never settles" do
      allow(workflow).to receive(:status).and_return("ready")

      expect { speedrun_workflow(workflow, max_steps: 3) }.to raise_error(described_class::Error, /still ready after 3 steps/)
    end
  end

  describe "#deliver_signal" do
    it "signals the run and runs the step that signal scheduled" do
      statuses.replace(%w[ready])
      allow(workflow).to receive(:signal!)
      allow(described_class).to receive(:scheduled_step).and_return(scheduled)

      deliver_signal(workflow, :shipped, payload: { at: "today" })

      expect(workflow).to have_received(:signal!).with(:shipped, payload: { at: "today" }, idempotency_key: nil,
                                                                 actor: "test_helpers")
      expect(described_class).to have_received(:perform).with(scheduled, workflow)
    end
  end

  describe "#complete_fan_out" do
    let(:waiting) { double("WorkflowStep", id: 9, batch_id: "b-1") }

    it "completes every member of the waiting step's batch and runs its BatchFinishedJob inline" do
      allow(described_class).to receive(:waiting_step).with(workflow).and_return(waiting)
      allow(Pgbus::Batch).to receive(:executions_migrated?).and_return(true)
      relation = double(pluck: %w[j-1 j-2])
      allow(Pgbus::BatchExecution).to receive(:where).with(batch_id: "b-1").and_return(double(order: relation))
      allow(Pgbus::Batch).to receive(:job_completed)
      job = instance_double(Pgbus::Workflow::BatchFinishedJob, perform_now: nil)
      allow(Pgbus::Workflow::BatchFinishedJob).to receive(:new).with(9).and_return(job)

      expect(complete_fan_out(workflow)).to be(workflow)
      expect(Pgbus::Batch).to have_received(:job_completed).with("b-1", job_id: "j-1")
      expect(Pgbus::Batch).to have_received(:job_completed).with("b-1", job_id: "j-2")
      expect(job).to have_received(:perform_now)
    end

    it "counts the members complete through the counters before the executions table exists" do
      allow(described_class).to receive(:waiting_step).with(workflow).and_return(waiting)
      allow(Pgbus::Batch).to receive(:executions_migrated?).and_return(false)
      allow(Pgbus::Batch).to receive(:find).with("b-1").and_return(instance_double(Pgbus::Batch, pending_jobs: 2))
      allow(Pgbus::Batch).to receive(:job_completed)
      job = instance_double(Pgbus::Workflow::BatchFinishedJob, perform_now: nil)
      allow(Pgbus::Workflow::BatchFinishedJob).to receive(:new).with(9).and_return(job)

      complete_fan_out(workflow)

      expect(Pgbus::Batch).to have_received(:job_completed).with("b-1").twice
    end

    it "raises when the run is not waiting on a batch" do
      allow(described_class).to receive(:waiting_step).and_return(nil)

      expect { complete_fan_out(workflow) }.to raise_error(described_class::Error, /not waiting on a fan_out batch/)
    end
  end

  describe "#expire_wait" do
    it "raises when no waiting step has a wait_timeout:" do
      allow(described_class).to receive(:waiting_step).and_return(double(timeout_at: nil))

      expect { expire_wait(workflow) }.to raise_error(described_class::Error, /no waiting step with a wait_timeout:/)
    end
  end

  describe ".perform" do
    before do
      allow(described_class).to receive(:perform).and_call_original
      allow(workflow).to receive(:run_now!)
    end

    it "pulls a waiting step forward through run_now!, on the audit trail, and runs it through the StepJob" do
      job = instance_double(Pgbus::Workflow::StepJob, perform_now: nil)
      allow(Pgbus::Workflow::StepJob).to receive(:new).with(7).and_return(job)

      described_class.perform(scheduled, workflow)

      expect(workflow).to have_received(:run_now!).with(actor: "test_helpers", reason: a_kind_of(String))
      expect(job).to have_received(:perform_now)
    end

    it "leaves a step that is already due as it is" do
      due = double("WorkflowStep", id: 8, scheduled_at: 1.minute.ago)
      allow(Pgbus::Workflow::StepJob).to receive(:new).with(8).and_return(instance_double(Pgbus::Workflow::StepJob, perform_now: nil))

      described_class.perform(due, workflow)

      expect(workflow).not_to have_received(:run_now!)
    end
  end
end
