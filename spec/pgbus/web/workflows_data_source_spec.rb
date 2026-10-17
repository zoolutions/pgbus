# frozen_string_literal: true

require "spec_helper"

# The workflow reads and actions of Web::DataSource with the models mocked
# (issue #542): the guards, the error paths and the outcome mapping. The SQL
# itself runs against a real database in
# spec/integration/workflow/dashboard_data_source_spec.rb.
RSpec.describe Pgbus::Web::DataSource::WorkflowList do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: double("Pgbus::Client")) }

  let(:actor) { "dashboard:alice" }

  before do
    allow(Pgbus::Workflow::Sweep).to receive_messages(tables_ready?: true, waiting_ready?: true)
    allow(Pgbus.logger).to receive(:error)
  end

  describe "an install without the workflow tables" do
    before { allow(Pgbus::Workflow::Sweep).to receive(:tables_ready?).and_return(false) }

    it "reads as empty without logging an error" do
      expect(data_source.workflows).to eq([])
      expect(data_source.workflows_count).to eq(Pgbus::Web::DataSource::ListCounts::EMPTY_COUNT)
      expect(data_source.workflow_detail("wf-1")).to be_nil
      expect([data_source.ongoing_workflows_count, data_source.paused_workflows_count,
              data_source.workflow_attention_count]).to eq([0, 0, 0])
      expect(data_source.workflow_attention_groups).to eq([])
      expect(Pgbus.logger).not_to have_received(:error)
    end
  end

  describe "query validation" do
    it "raises on an unknown status or filter, before touching the database" do
      expect { data_source.workflows(status: "stuck") }.to raise_error(ArgumentError, /unknown workflow status/)
      expect { data_source.workflows_count(colour: "red") }.to raise_error(ArgumentError, /unknown workflow filter/)
    end
  end

  describe "a failing query" do
    before do
      allow(Pgbus::WorkflowEntry).to receive_messages(all: Pgbus::WorkflowEntry.none)
      allow(Pgbus::WorkflowEntry).to receive(:where).and_raise(ActiveRecord::StatementInvalid, "db down")
      allow(Pgbus::WorkflowEntry).to receive(:find_by).and_raise(ActiveRecord::StatementInvalid, "db down")
      allow(Pgbus::WorkflowEntry).to receive(:stuck_reason_counts).and_raise(ActiveRecord::StatementInvalid, "db down")
      allow(Pgbus::WorkflowEntry).to receive(:attention_groups).and_raise(ActiveRecord::StatementInvalid, "db down")
    end

    it "degrades every read to its empty shape and logs" do
      expect(data_source.workflows(status: "paused")).to eq([])
      expect(data_source.workflows_count(status: "paused")).to eq(Pgbus::Web::DataSource::ListCounts::EMPTY_COUNT)
      expect(data_source.workflow_detail("wf-1")).to be_nil
      expect(data_source.paused_workflows_count).to eq(0)
      expect(data_source.workflow_attention_count).to eq(0)
      expect(data_source.workflow_attention_groups).to eq([])
      expect(Pgbus.logger).to have_received(:error).at_least(6).times
    end
  end

  describe "the attention thresholds" do
    it "leaves waiting_overdue out before the waiting-step schema exists" do
      allow(Pgbus::Workflow::Sweep).to receive(:waiting_ready?).and_return(false)
      allow(Pgbus::WorkflowEntry).to receive(:stuck_reason_counts).and_return({})

      data_source.workflow_attention_count

      expect(Pgbus::WorkflowEntry).to have_received(:stuck_reason_counts)
        .with(threshold: Pgbus.configuration.workflow_stall_threshold, waiting_threshold: nil)
    end
  end

  describe "actions" do
    let(:workflow) { instance_double(Pgbus::Workflow, workflow_id: "wf-1") }

    before { allow(Pgbus::Workflow).to receive(:find).with("wf-1").and_return(workflow) }

    it "passes the actor and reason through to the run's verb" do
      allow(workflow).to receive(:cancel!)

      expect(data_source.cancel_workflow("wf-1", actor: actor, reason: "duplicate order")).to eq(ok: true)
      expect(workflow).to have_received(:cancel!).with(actor: actor, reason: "duplicate order")
    end

    it "reports a run that is gone" do
      allow(Pgbus::Workflow).to receive(:find).with("gone").and_return(nil)

      expect(data_source.resume_workflow("gone", actor: actor)).to include(ok: false, code: :not_found)
    end

    it "reports a run deleted between the read and the lock" do
      allow(workflow).to receive(:skip!).and_raise(ActiveRecord::RecordNotFound, "workflow wf-1 is gone")

      expect(data_source.skip_workflow_step("wf-1", actor: actor)).to eq(ok: false, code: :not_found,
                                                                         reason: "workflow wf-1 is gone")
    end

    it "reports a payload over the cap as invalid" do
      allow(workflow).to receive(:signal!).and_raise(Pgbus::Workflow::PayloadTooLarge, "payload of signal x is too big")

      expect(data_source.signal_workflow("wf-1", name: "x", payload: {}, idempotency_key: "k", actor: actor))
        .to include(ok: false, code: :invalid, reason: "payload of signal x is too big")
    end

    it "logs and reports anything unexpected" do
      allow(workflow).to receive(:run_now!).and_raise(IOError, "socket closed")

      expect(data_source.run_workflow_step_now("wf-1", actor: actor)).to eq(ok: false, code: :failed,
                                                                            reason: "IOError: socket closed")
      expect(Pgbus.logger).to have_received(:error)
    end

    it "reports a bulk call whose group cannot be read" do
      allow(Pgbus::WorkflowEntry).to receive(:attention_ids).and_raise(ActiveRecord::StatementInvalid, "db down")

      result = data_source.bulk_cancel_workflows(group: { workflow_class: "X" }, actor: actor)

      expect(result).to include(ok: false, code: :failed, done: 0, skipped: 0)
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
