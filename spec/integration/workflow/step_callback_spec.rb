# frozen_string_literal: true

require_relative "support"

# Issue #540, step 6a: WorkflowStep's after-commit callback is the one place a
# StepJob is enqueued.
RSpec.describe Pgbus::WorkflowStep, :integration do
  include WorkflowIntegration

  let(:sent) { [] }

  before do
    stub_const("CallbackWorkflow", Class.new(Pgbus::Workflow) do
      set_step_job_options queue: :default, priority: 1
      step(:one) { nil }
      step(:two) { nil }
    end)
    Pgbus::WorkflowEntry.create!(workflow_id: "wf-cb", workflow_class: "CallbackWorkflow", definition_digest: "x")
    calls = sent
    allow(Pgbus::Workflow::StepJob).to receive(:set).and_wrap_original do |original, **options|
      calls << options
      original.call(**options)
    end
  end

  def new_row(status: "scheduled", scheduled_at: Time.current, attempt: 1)
    described_class.create!(workflow_id: "wf-cb", workflow_class: "CallbackWorkflow", step_name: "one", attempt: attempt,
                            status: status, scheduled_at: scheduled_at, created_at: Time.current)
  end

  it "fires on create with status scheduled, setting enqueued_at and job_id" do
    row = new_row

    expect(sent.size).to eq(1)
    expect(row.reload.enqueued_at).not_to be_nil
    expect(row.job_id).to match(/\A\h{8}-/)
    expect(queue_depth).to eq(1)
  end

  it "sends with wait_until equal to scheduled_at, on the class's queue and priority" do
    at = 1.hour.from_now.change(usec: 0)
    new_row(scheduled_at: at)

    expect(sent.last).to eq(queue: "default", priority: 1, wait_until: at)
  end

  it "fires on a status change to scheduled and on a bumped enqueued_at, and on no other save" do
    row = new_row(status: "in_progress")
    expect(sent).to be_empty

    row.update!(worker: "host:1")
    expect(sent).to be_empty

    row.update!(status: "scheduled")
    expect(sent.size).to eq(1)

    row.update!(enqueued_at: 1.second.from_now)
    expect(sent.size).to eq(2)

    row.update!(worker: "host:2")
    expect(sent.size).to eq(2)
  end

  it "sends nothing inside a transaction until it commits, and nothing at all on a rollback" do
    Pgbus::WorkflowEntry.transaction do
      new_row
      expect(sent).to be_empty
    end
    expect(sent.size).to eq(1)

    Pgbus::WorkflowEntry.transaction do
      new_row(attempt: 2)
      raise ActiveRecord::Rollback
    end
    expect(sent.size).to eq(1)
  end

  it "reports a failed send without raising, and leaves the row for the sweep" do
    allow(Pgbus::Workflow::StepJob).to receive(:set).and_raise(StandardError, "pgmq down")

    row = new_row

    expect(row.reload.enqueued_at).to be_nil
    expect(Pgbus::ErrorReporter).to have_received(:report)
      .with(an_instance_of(StandardError), hash_including(action: "workflow_step_enqueue", step_name: "one"))
  end
end
