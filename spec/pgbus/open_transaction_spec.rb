# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::OpenTransaction do
  let(:pool) { double("pool") }

  before do
    stub_const("ActiveRecord::Base", Class.new)
    allow(ActiveRecord::Base).to receive(:connection_pool).and_return(pool)
  end

  def connection_with(transaction)
    double("connection", current_transaction: transaction, add_transaction_record: nil)
  end

  describe ".current" do
    it "is nil when the thread leases no connection" do
      allow(pool).to receive(:active_connection?).and_return(nil)

      expect(described_class.current).to be_nil
    end

    it "is nil when the leased connection has no open transaction" do
      allow(pool).to receive(:active_connection?).and_return(connection_with(double(open?: false)))

      expect(described_class.current).to be_nil
    end

    it "is the open transaction" do
      transaction = double(open?: true)
      allow(pool).to receive(:active_connection?).and_return(connection_with(transaction))

      expect(described_class.current).to be(transaction)
    end

    it "is nil when the probe raises" do
      allow(pool).to receive(:active_connection?).and_raise(StandardError, "not connected")

      expect(described_class.current).to be_nil
    end
  end

  describe ".defer" do
    it "returns nil and registers nothing without an open transaction" do
      allow(pool).to receive(:active_connection?).and_return(nil)

      expect(described_class.defer { raise "never" }).to be_nil
    end

    it "registers the block as an after_commit callback of a joinable transaction" do
      callbacks = []
      transaction = double(open?: true, joinable?: true)
      allow(transaction).to receive(:after_commit) { |&block| callbacks << block }
      allow(pool).to receive(:active_connection?).and_return(connection_with(transaction))
      ran = false

      expect(described_class.defer { ran = true }).to be(transaction)
      expect(ran).to be(false)

      callbacks.each(&:call)
      expect(ran).to be(true)
    end

    # The fixture transaction of an app's transactional tests never commits.
    it "does not defer on a transaction that is not joinable" do
      transaction = double(open?: true, joinable?: false)
      allow(pool).to receive(:active_connection?).and_return(connection_with(transaction))

      expect(described_class.defer { nil }).to be_nil
    end

    # Rails 7.1: no Transaction#after_commit, so a record-shaped hook.
    it "adds a commit hook record when the transaction has no after_commit" do
      transaction = double(open?: true, joinable?: true)
      connection = connection_with(transaction)
      allow(pool).to receive(:active_connection?).and_return(connection)
      ran = false

      expect(described_class.defer { ran = true }).to be(transaction)

      expect(connection).to have_received(:add_transaction_record) do |hook|
        hook.before_committed!
        hook.committed!(should_run_callbacks: false)
        expect(ran).to be(false)
        hook.rolledback!(force_restore_state: true, should_run_callbacks: true)
        expect(ran).to be(false)
        expect(hook.trigger_transactional_callbacks?).to be(true)
        hook.committed!(should_run_callbacks: true)
      end
      expect(ran).to be(true)
    end
  end
end
