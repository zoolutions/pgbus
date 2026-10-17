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
      expect(described_class).to have_received(:perform).with(scheduled)
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

      expect(described_class).to have_received(:perform).with(scheduled)
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

    it "raises on waiting" do
      statuses.replace(%w[waiting])

      expect { speedrun_workflow(workflow) }.to raise_error(described_class::Error, /is waiting/)
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

  describe ".perform" do
    before { allow(described_class).to receive(:perform).and_call_original }

    it "pulls a waiting step forward to now and runs it through the StepJob" do
      job = instance_double(Pgbus::Workflow::StepJob, perform_now: nil)
      allow(Pgbus::Workflow::StepJob).to receive(:new).with(7).and_return(job)
      allow(scheduled).to receive(:update_columns)

      described_class.perform(scheduled)

      expect(scheduled).to have_received(:update_columns).with(scheduled_at: a_kind_of(Time))
      expect(job).to have_received(:perform_now)
    end

    it "leaves a step that is already due as it is" do
      due = double("WorkflowStep", id: 8, scheduled_at: 1.minute.ago)
      allow(due).to receive(:update_columns)
      allow(Pgbus::Workflow::StepJob).to receive(:new).with(8).and_return(instance_double(Pgbus::Workflow::StepJob, perform_now: nil))

      described_class.perform(due)

      expect(due).not_to have_received(:update_columns)
    end
  end
end
