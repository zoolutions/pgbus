# frozen_string_literal: true

require "spec_helper"

# Issue #540, step 6, with the models mocked. spec/integration/workflow/sweep_spec.rb
# runs the stall, re-send, gauge and retention paths against a real database.
RSpec.describe Pgbus::Workflow::Sweep do
  let(:config) { Pgbus::Configuration.new }

  before { described_class.reset_tables_ready! }
  after { described_class.reset_tables_ready! }

  describe ".tables_ready?" do
    it "skips the sweep and the cleanup until the workflow tables exist" do
      [Pgbus::WorkflowEntry, Pgbus::WorkflowStep, Pgbus::WorkflowEvent].each do |model|
        allow(model).to receive(:table_exists?).and_return(model != Pgbus::WorkflowEvent)
      end

      expect(described_class.repair(config: config)).to be_nil
      expect(described_class.cleanup(retention: 60)).to eq(0)
    end

    it "keeps a true answer for the life of the process and treats a failing probe as false" do
      allow(Pgbus::WorkflowEntry).to receive(:table_exists?).and_raise(StandardError, "db down")
      expect(described_class.tables_ready?).to be(false)

      [Pgbus::WorkflowEntry, Pgbus::WorkflowStep, Pgbus::WorkflowEvent].each do |model|
        allow(model).to receive(:table_exists?).and_return(true)
      end
      expect(described_class.tables_ready?).to be(true)
      allow(Pgbus::WorkflowEntry).to receive(:table_exists?).and_return(false)
      expect(described_class.tables_ready?).to be(true)
    end
  end

  describe ".repair" do
    let(:gauges) { [] }

    before do
      allow(described_class).to receive(:tables_ready?).and_return(true)
      allow(Pgbus::WorkflowStep).to receive_messages(stalled: double("stalled", order: double(limit: double(pluck: []))),
                                                     overdue: double("overdue",
                                                                     order: double(limit: double(to_a: []))))
      allow(Pgbus::WorkflowEntry).to receive_messages(
        status_counts: { %w[SignupWorkflow paused] => 2 },
        stuck_reason_counts: { %w[SignupWorkflow stalled] => 1 }
      )
      allow(Pgbus::Instrumentation).to receive(:instrument) { |name, payload| gauges << payload if name == "pgbus.workflow.gauge" }
    end

    it "emits the gauge zero-filled across every status and stuck reason" do
      expect(described_class.repair(config: config)).to eq(stalled: 0, resent: 0)

      statuses = gauges.last[:statuses].select { |row| row[:workflow_class] == "SignupWorkflow" }
      stuck = gauges.last[:stuck].select { |row| row[:workflow_class] == "SignupWorkflow" }
      expected_statuses = Pgbus::Workflow::States::WORKFLOW.map { |state| [state, state == "paused" ? 2 : 0] }
      expect(statuses.map { |row| [row[:status], row[:count]] }).to eq(expected_statuses)
      expect(stuck.map { |row| [row[:reason], row[:count]] })
        .to eq(Pgbus::Workflow::States::STUCK_REASONS.map { |r| [r, r == "stalled" ? 1 : 0] })
    end

    it "queries stalled steps by stall_at and overdue steps by workflow_stall_threshold" do
      config.workflow_stall_threshold = 600

      described_class.repair(config: config)

      expect(Pgbus::WorkflowStep).to have_received(:stalled).with(now: a_kind_of(Time))
      expect(Pgbus::WorkflowStep).to have_received(:overdue).with(threshold: 600, now: a_kind_of(Time))
    end
  end

  describe ".cleanup" do
    it "deletes stale runs in batches until none are left" do
      allow(described_class).to receive(:tables_ready?).and_return(true)
      batches = [[1, 2], [3], []]
      stale = double("stale")
      allow(stale).to receive_message_chain(:order, :limit, :pluck) { batches.shift } # rubocop:disable RSpec/MessageChain
      allow(Pgbus::WorkflowEntry).to receive(:stale).and_return(stale)
      allow(Pgbus::WorkflowEntry).to receive(:where) { |id:| double("rows", delete_all: id.size) }

      expect(described_class.cleanup(retention: 86_400, batch_size: 2)).to eq(3)
    end
  end
end
