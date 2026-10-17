# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #541, step 3, with the models mocked. The real batch callback runs in
# spec/integration/workflow/fan_out_spec.rb.
RSpec.describe Pgbus::Workflow::BatchFinishedJob do
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      def self.name = "FinishedSpecWorkflow"

      fan_out(:notify, on_batch_failure: :cancel!) { |b| b }
    end
  end
  let(:record) { double("WorkflowEntry", workflow_id: "wf-1", workflow_klass: klass) }
  let(:notes) { Pgbus::Workflow::Runner::Notes.new }
  let(:step) { double("WorkflowStep", id: 11, workflow_id: "wf-1", step_name: "notify", status: "waiting", batch_id: "b-1") }
  let(:entry) { double("BatchEntry", batch_id: "b-1", status: "finished") }
  let(:batch) { instance_double(Pgbus::Batch, failed_jobs: 0, total_jobs: 12) }

  before do
    allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(step)
    allow(Pgbus::Workflow::Runner).to receive(:locked).with("wf-1").and_yield(record, notes)
    allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "b-1").and_return(entry)
    allow(Pgbus::Batch).to receive(:find).with("b-1").and_return(batch)
    allow(Pgbus::Workflow::Runner).to receive(:event!)
    allow(Pgbus::Workflow::Waiting).to receive(:resolve!)
  end

  it "goes to config.workflow_queue by default and has no retry_on or discard_on" do
    allow(Pgbus.configuration).to receive(:workflow_queue).and_return("workflows")

    expect(described_class.new(11).queue_name).to eq("workflows")
    expect(described_class.rescue_handlers).to be_empty
  end

  it "completes the waiting step when no member was dead-lettered, recording fan_out_finished" do
    described_class.new(11).perform_now

    expect(Pgbus::Workflow::Waiting).to have_received(:resolve!)
      .with(record, step, Pgbus::Workflow::FlowControl::Outcome.complete, notes: notes)
    expect(Pgbus::Workflow::Runner).to have_received(:event!)
      .with(record, "fan_out_finished", actor: "system", step: step, details: { batch_id: "b-1", failed_jobs: 0, total_jobs: 12 })
  end

  it "records FanOutFailed and applies on_batch_failure when members were dead-lettered" do
    allow(batch).to receive(:failed_jobs).and_return(3)

    described_class.new(11).perform_now

    expect(Pgbus::Workflow::Waiting).to have_received(:resolve!) do |_record, _step, outcome, error:, **|
      expect(outcome.action).to eq(:cancel)
      expect(error).to be_a(Pgbus::Workflow::FanOutFailed)
      expect(error.message).to eq("3 of 12 jobs dead-lettered (batch b-1)")
    end
  end

  it "leaves a step that is no longer waiting alone (skipped, cancelled, or resolved by its park)" do
    %w[skipped cancelled in_progress completed].each do |status|
      allow(step).to receive(:status).and_return(status)

      described_class.new(11).perform_now
    end

    expect(Pgbus::Workflow::Waiting).not_to have_received(:resolve!)
  end

  it "does nothing for a step whose run is gone" do
    allow(Pgbus::WorkflowStep).to receive(:find_by).with(id: 11).and_return(nil)

    expect { described_class.new(11).perform_now }.not_to raise_error
    expect(Pgbus::Workflow::Runner).not_to have_received(:locked)
  end
end
