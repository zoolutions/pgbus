# frozen_string_literal: true

require "spec_helper"

# Instrumentation and error reports noted during a workflow transition go out
# only once the transition has committed: right away on its own transaction,
# or when the app's enclosing transaction commits (never on its rollback).
RSpec.describe Pgbus::Workflow::Runner::Notes do
  subject(:notes) { described_class.new }

  let(:connection) { double("connection") }

  before do
    allow(Pgbus::Instrumentation).to receive(:instrument)
    allow(Pgbus::ErrorReporter).to receive(:report)
    notes.instrument("pgbus.workflow_paused", workflow_id: "wf-1")
    notes.report(RuntimeError.new("boom"), workflow_id: "wf-1")
  end

  it "sends at once when no transaction is open any more" do
    allow(connection).to receive(:transaction_open?).and_return(false)

    notes.flush_after_commit(connection)

    expect(Pgbus::Instrumentation).to have_received(:instrument).with("pgbus.workflow_paused", workflow_id: "wf-1")
    expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(RuntimeError), workflow_id: "wf-1")
  end

  context "with an enclosing transaction still open" do
    let(:records) { [] }

    before do
      allow(connection).to receive(:transaction_open?).and_return(true)
      allow(connection).to receive(:add_transaction_record) { |record, *| records << record }
      notes.flush_after_commit(connection)
    end

    it "waits for the commit" do
      expect(Pgbus::Instrumentation).not_to have_received(:instrument)

      records.each(&:before_committed!)
      records.each { |record| record.committed!(should_run_callbacks: true) }

      expect(Pgbus::Instrumentation).to have_received(:instrument).once
      expect(Pgbus::ErrorReporter).to have_received(:report).once
    end

    it "sends nothing on a rollback" do
      expect(records.first.trigger_transactional_callbacks?).to be(true)
      records.each { |record| record.rolledback!(force_restore_state: true, should_run_callbacks: true) }

      expect(Pgbus::Instrumentation).not_to have_received(:instrument)
      expect(Pgbus::ErrorReporter).not_to have_received(:report)
    end
  end
end
