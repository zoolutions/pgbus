# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #541, step 2, with Pgbus.batch mocked. spec/integration/workflow/fan_out_spec.rb
# runs the same paths against a real database.
RSpec.describe Pgbus::Workflow::FanOut do
  let(:member_job) do
    Class.new(ActiveJob::Base) do
      def self.name = "FanOutSpecMemberJob"
      def perform(*); end
    end
  end
  let(:klass) do
    job = member_job
    Class.new(Pgbus::Workflow) do
      def self.name = "FanOutSpecWorkflow"

      fan_out :notify, job_options: { queue: :vendors } do |b|
        skip! if arguments[:skip]
        b.on_success(job) if arguments[:callback]
        b.add(job.new(1), job.new(2))
        b.add(job.new(3))
      end
    end
  end
  let(:arguments) { {} }
  let(:record) do
    double("WorkflowEntry", workflow_id: "wf-1", workflow_class: "FanOutSpecWorkflow", subject_gid: nil,
                            arguments: ActiveJob::Arguments.serialize([arguments]).first)
  end
  let(:workflow) do
    klass.new(record).tap do |wf|
      wf.send(:enter_step, Pgbus::Workflow::StepContext::Attempt.new(step_id: 11, step_name: "notify", attempt: 1))
    end
  end
  let(:definition) { klass.workflow_steps.fetch(:notify) }
  let(:batch_builder) { instance_double(Pgbus::Batch::Builder, on_complete: nil, add: nil) }
  let(:batch) { instance_double(Pgbus::Batch, batch_id: "batch-1", total_jobs: 3) }

  def run_body = catch(Pgbus::Workflow::FlowControl::THROW_TAG) { definition.invoke(workflow) }

  before do
    allow(batch).to receive(:reload).and_return(batch)
    allow(Pgbus).to receive(:batch) do |**_options, &block|
      block.call(batch_builder)
      batch
    end
  end

  describe ".run" do
    it "opens one batch with the run's description and properties, on_complete resuming the step on its queue" do
      outcome = run_body

      expect(Pgbus).to have_received(:batch)
        .with(description: "FanOutSpecWorkflow#notify wf-1", properties: { workflow_id: "wf-1", step_id: 11 })
      expect(batch_builder).to have_received(:on_complete) do |callback|
        expect(callback).to be_a(Pgbus::Workflow::BatchFinishedJob)
        expect(callback.arguments).to eq([11])
        expect(callback.queue_name).to eq("vendors")
      end
      expect(outcome).to have_attributes(action: :fan_out, details: { batch_id: "batch-1", size: 3 })
    end

    it "forwards every b.add, in order, to the batch" do
      added = []
      allow(batch_builder).to receive(:add) { |*jobs| added << jobs.map { |job| job.arguments.first } }

      run_body

      expect(added).to eq([[1, 2], [3]])
    end

    it "opens no batch when the body ends the step with a flow-control verb" do
      arguments[:skip] = true

      expect(run_body.action).to eq(:skip)
      expect(Pgbus).not_to have_received(:batch)
    end

    it "raises on a callback the body sets, before any batch exists" do
      arguments[:callback] = true

      expect { run_body }.to raise_error(ArgumentError, /on_success: a fan_out step owns its batch's callbacks/)
      expect(Pgbus).not_to have_received(:batch)
    end
  end

  describe Pgbus::Workflow::FanOut::Builder do
    subject(:builder) { described_class.new }

    it "takes job instances only, and counts them" do
      builder.add(member_job.new(1), [member_job.new(2)])

      expect(builder.size).to eq(2)
      expect { builder.add(member_job) }.to raise_error(ArgumentError, /a batch takes job instances/)
    end

    it "rejects every callback setter" do
      %i[on_complete on_success on_failure on_finish].each do |slot|
        expect { builder.public_send(slot, member_job) }.to raise_error(ArgumentError, /#{slot}: a fan_out step owns/)
      end
    end
  end

  describe "the schema check at start!" do
    before { allow(Pgbus::Workflow::Sweep).to receive(:waiting_ready?).and_return(true) }

    it "raises BatchCallbacksUnmigrated for a fan_out class before the batch callback-job columns exist" do
      allow(Pgbus::Batch).to receive(:callback_jobs_migrated?).and_return(false)

      expect { Pgbus::Workflow::Waiting.check_schema!(klass) }
        .to raise_error(Pgbus::Workflow::BatchCallbacksUnmigrated, /pgbus:add_batch_callback_jobs/)
    end

    it "raises WaitingStepsUnmigrated for a waiting class before the waiting-step schema exists" do
      allow(Pgbus::Workflow::Sweep).to receive(:waiting_ready?).and_return(false)
      waiting = Class.new(Pgbus::Workflow) do
        def self.name = "SignalSpecWorkflow"

        step(:wait, wait_for: :go) { nil }
      end

      expect { Pgbus::Workflow::Waiting.check_schema!(waiting) }
        .to raise_error(Pgbus::Workflow::WaitingStepsUnmigrated, /pgbus:add_workflow_signals/)
    end

    it "checks nothing for a class without waiting steps" do
      plain = Class.new(Pgbus::Workflow) { step(:a) { nil } }
      allow(Pgbus::Batch).to receive(:callback_jobs_migrated?)

      expect { Pgbus::Workflow::Waiting.check_schema!(plain) }.not_to raise_error
      expect(Pgbus::Workflow::Sweep).not_to have_received(:waiting_ready?)
      expect(Pgbus::Batch).not_to have_received(:callback_jobs_migrated?)
    end

    it "runs from start! before anything is written" do
      allow(Pgbus::Batch).to receive(:callback_jobs_migrated?).and_return(false)
      allow(Pgbus::WorkflowEntry).to receive(:insert_run!)

      expect { klass.start! }.to raise_error(Pgbus::Workflow::BatchCallbacksUnmigrated)
      expect(Pgbus::WorkflowEntry).not_to have_received(:insert_run!)
    end
  end
end
