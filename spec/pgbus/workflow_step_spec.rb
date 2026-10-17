# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "active_support/core_ext/numeric/time"

# Issue #540, step 6a, the parts that need no database: when the after-commit
# callback fires and what it sends. spec/integration/workflow/step_callback_spec.rb
# covers the commit and rollback timing against a real database.
RSpec.describe Pgbus::WorkflowStep do
  let(:row) { described_class.allocate }
  let(:written) { [] }

  # Readers defined straight on an allocated row: an RSpec stub would ask
  # ActiveRecord for the attribute methods, which needs a database.
  def stub_row(**attrs)
    defaults = { status: "scheduled", previously_new_record?: false, saved_change_to_status?: false,
                 saved_change_to_enqueued_at?: false, workflow_class: "SpecCallbackWorkflow", step_name: "charge",
                 scheduled_at: Time.current, id: 9, workflow_id: "wf-1", attempt: 1 }
    defaults.merge(attrs).each { |name, value| row.define_singleton_method(name) { value } }
    writes = written
    row.define_singleton_method(:update_columns) { |columns| writes << columns }
    row
  end

  describe "#needs_enqueue?" do
    it "is true for a new scheduled row, a change to scheduled and a bumped enqueued_at" do
      expect(stub_row(previously_new_record?: true)).to be_needs_enqueue
      expect(stub_row(saved_change_to_status?: true)).to be_needs_enqueue
      expect(stub_row(saved_change_to_enqueued_at?: true)).to be_needs_enqueue
    end

    it "is false for any other save of a scheduled row, and for any row that is not scheduled" do
      expect(stub_row).not_to be_needs_enqueue
      expect(stub_row(status: "in_progress", previously_new_record?: true)).not_to be_needs_enqueue
      expect(stub_row(status: "completed", saved_change_to_status?: true)).not_to be_needs_enqueue
    end
  end

  describe "the StepJob options" do
    before do
      stub_const("SpecCallbackWorkflow", Class.new(Pgbus::Workflow) do
        set_step_job_options queue: :workflows, priority: 3
        step(:charge, job_options: { priority: 1 }) { nil }
        step(:slow, job_options: { wait: 600 }) { nil }
      end)
    end

    it "takes the class's options, the step's own winning, and sends at scheduled_at" do
      at = 1.hour.from_now
      stub_row(scheduled_at: at)

      expect(row.send(:step_job_set_options)).to eq(queue: "workflows", priority: 1, wait_until: at)
    end

    it "lets a job_options wait delay the step past scheduled_at, never before it" do
      stub_row(step_name: "slow")

      expect(row.send(:step_job_set_options)[:wait_until]).to be_within(5).of(600.seconds.from_now)
    end

    it "sends a due step without wait_until, on config.workflow_queue when nothing else is set" do
      stub_row(workflow_class: "GoneWorkflow", scheduled_at: 1.minute.ago)

      expect(row.send(:step_job_set_options)).to eq(queue: Pgbus.configuration.workflow_queue)
    end
  end

  describe "#enqueue_step_job" do
    it "sends the StepJob and stamps enqueued_at and job_id" do
      stub_row(workflow_class: "GoneWorkflow")
      job = instance_double(Pgbus::Workflow::StepJob, job_id: "job-1")
      configured = double("configured job", perform_later: job)
      allow(Pgbus::Workflow::StepJob).to receive(:set).and_return(configured)

      row.send(:enqueue_step_job)

      expect(configured).to have_received(:perform_later).with(9)
      expect(written).to match([{ enqueued_at: a_kind_of(Time), job_id: "job-1" }])
    end

    it "reports a failed send and does not raise" do
      stub_row(workflow_class: "GoneWorkflow")
      allow(Pgbus::Workflow::StepJob).to receive(:set).and_raise(StandardError, "pgmq down")
      allow(Pgbus::ErrorReporter).to receive(:report)

      expect { row.send(:enqueue_step_job) }.not_to raise_error
      expect(Pgbus::ErrorReporter).to have_received(:report)
        .with(an_instance_of(StandardError), hash_including(action: "workflow_step_enqueue", step_name: "charge"))
      expect(written).to be_empty
    end
  end
end
