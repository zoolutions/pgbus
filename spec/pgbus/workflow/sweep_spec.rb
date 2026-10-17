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

  describe ".waiting_ready?" do
    let(:connection) { double("connection") }

    before { allow(Pgbus::WorkflowStep).to receive(:connection).and_return(connection) }

    it "asks the database (not the schema cache) for the signals table and the step columns, and keeps a true answer" do
      allow(connection).to receive(:table_exists?).with("pgbus_workflow_signals").and_return(true)
      allow(connection).to receive(:column_exists?).with("pgbus_workflow_steps", :timeout_at).and_return(false)
      expect(described_class.waiting_ready?).to be(false)

      allow(connection).to receive(:column_exists?).and_return(true)
      expect(described_class.waiting_ready?).to be(true)
      allow(connection).to receive(:table_exists?).and_raise(StandardError, "db down")
      expect(described_class.waiting_ready?).to be(true)
    end

    it "treats a failing probe as false" do
      allow(connection).to receive(:table_exists?).and_raise(StandardError, "db down")

      expect(described_class.waiting_ready?).to be(false)
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
      allow(described_class).to receive(:waiting_ready?).and_return(false)

      expect(described_class.repair(config: config)).to eq(stalled: 0, resent: 0, timed_out: 0, fan_outs: 0)

      statuses = gauges.last[:statuses].select { |row| row[:workflow_class] == "SignupWorkflow" }
      stuck = gauges.last[:stuck].select { |row| row[:workflow_class] == "SignupWorkflow" }
      expected_statuses = Pgbus::Workflow::States::WORKFLOW.map { |state| [state, state == "paused" ? 2 : 0] }
      expect(statuses.map { |row| [row[:status], row[:count]] }).to eq(expected_statuses)
      expect(stuck.map { |row| [row[:reason], row[:count]] })
        .to eq(Pgbus::Workflow::States::STUCK_REASONS.map { |r| [r, r == "stalled" ? 1 : 0] })
    end

    it "times out waits and counts waiting_overdue once the waiting-step schema exists" do
      allow(described_class).to receive(:waiting_ready?).and_return(true)
      allow(Pgbus::Workflow::WaitTimeout).to receive(:expire_due).and_return(2)
      allow(Pgbus::Workflow::FanOut).to receive(:resolve_lost).and_return(1)
      config.workflow_waiting_visibility_threshold = 3600

      expect(described_class.repair(config: config)).to eq(stalled: 0, resent: 0, timed_out: 2, fan_outs: 1)
      expect(Pgbus::Workflow::FanOut).to have_received(:resolve_lost)
        .with(now: a_kind_of(Time), grace: config.workflow_stall_threshold)
      expect(Pgbus::WorkflowEntry).to have_received(:stuck_reason_counts)
        .with(threshold: config.workflow_stall_threshold, now: a_kind_of(Time), waiting_threshold: 3600)
    end

    it "leaves waits and waiting_overdue alone before the waiting-step schema exists" do
      allow(described_class).to receive(:waiting_ready?).and_return(false)
      allow(Pgbus::Workflow::WaitTimeout).to receive(:expire_due)

      described_class.repair(config: config)

      expect(Pgbus::Workflow::WaitTimeout).not_to have_received(:expire_due)
      expect(Pgbus::WorkflowEntry).to have_received(:stuck_reason_counts).with(hash_including(waiting_threshold: nil))
    end

    it "queries stalled steps by stall_at and overdue steps by workflow_stall_threshold" do
      allow(described_class).to receive(:waiting_ready?).and_return(false)
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
      now = Time.current
      limited = double("limited")
      allow(limited).to receive(:pluck).with(:id) { batches.shift }
      ordered = double("ordered")
      allow(ordered).to receive(:limit).with(2).and_return(limited)
      stale = double("stale")
      allow(stale).to receive(:order).with(:id).and_return(ordered)
      allow(Pgbus::WorkflowEntry).to receive(:stale).with(before: now - 86_400).and_return(stale)
      allow(Pgbus::WorkflowEntry).to receive(:where) { |id:| double("rows", delete_all: id.size) }

      expect(described_class.cleanup(retention: 86_400, now: now, batch_size: 2)).to eq(3)
    end
  end
end
