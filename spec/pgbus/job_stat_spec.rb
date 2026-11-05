# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::JobStat do
  before do
    # The bookkeeping leases through connection_pool.with_connection (issue #649).
    stub_bus_record_pool
    allow(described_class).to receive_messages(create!: nil, table_exists?: true)
  end

  describe ".record!" do
    # Issue #649: the executor writes the stat after the job's own lease went
    # back, so record! must take its own and return it.
    it "probes and inserts inside one connection_pool.with_connection" do
      pool = stub_bus_record_pool
      allow(described_class).to receive(:latency_columns?).and_return(false)

      described_class.record!(job_class: "TestJob", queue_name: "default", status: "success", duration_ms: 1)

      expect(pool.leases).to eq(1)
      expect(described_class).to have_received(:create!)
    end

    context "without latency columns" do
      before { allow(described_class).to receive(:latency_columns?).and_return(false) }

      it "creates a stat record without latency fields" do
        described_class.record!(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 1
        )

        expect(described_class).to have_received(:create!).with(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42
        )
      end
    end

    context "with latency columns" do
      before { allow(described_class).to receive(:latency_columns?).and_return(true) }

      it "creates a stat record with latency fields" do
        described_class.record!(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 2
        )

        expect(described_class).to have_received(:create!).with(
          job_class: "TestJob",
          queue_name: "default",
          status: "success",
          duration_ms: 42,
          enqueue_latency_ms: 150,
          retry_count: 2
        )
      end
    end

    it "skips when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!(job_class: "X", queue_name: "q", status: "s", duration_ms: 0)

      expect(described_class).not_to have_received(:create!)
    end

    it "rescues errors gracefully" do
      allow(described_class).to receive(:create!).and_raise(StandardError, "db error")

      expect do
        described_class.record!(job_class: "X", queue_name: "q", status: "s", duration_ms: 0)
      end.not_to raise_error
    end
  end

  describe ".table_exists?" do
    before do
      # Undo the outer `before` that stubs table_exists? so we exercise
      # the real memoization logic.
      allow(described_class).to receive(:table_exists?).and_call_original
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    after do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    it "returns false on connection error without poisoning the memo" do
      allow(described_class).to receive(:connection_pool).and_raise(StandardError, "no db")

      expect(described_class.table_exists?).to be false
      # The failed probe must NOT set @table_exists — otherwise a
      # transient PG blip during boot permanently disables job stats
      # for the process lifetime.
      expect(described_class.instance_variable_defined?(:@table_exists)).to be false
    end

    it "memoizes a successful probe" do
      fake_connection = instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter)
      allow(fake_connection).to receive(:table_exists?).with(described_class.table_name).and_return(true)
      pool = instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool)
      allow(pool).to receive(:with_connection).and_yield(fake_connection)
      allow(described_class).to receive(:connection_pool).and_return(pool)

      expect(described_class.table_exists?).to be true
      # Second call hits the memo, not the connection.
      expect(described_class.table_exists?).to be true
      expect(fake_connection).to have_received(:table_exists?).once
    end
  end

  describe ".latency_columns?" do
    before { described_class.remove_instance_variable(:@latency_columns) if described_class.instance_variable_defined?(:@latency_columns) }

    after { described_class.remove_instance_variable(:@latency_columns) if described_class.instance_variable_defined?(:@latency_columns) }

    it "returns false when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)
      expect(described_class.latency_columns?).to be false
    end

    it "returns false on column_names error without poisoning the memo" do
      allow(described_class).to receive(:table_exists?).and_return(true)
      allow(described_class).to receive(:column_names).and_raise(StandardError, "no db")

      expect(described_class.latency_columns?).to be false
      # A transient error must not lock latency_columns? to false
      # for the process lifetime.
      expect(described_class.instance_variable_defined?(:@latency_columns)).to be false
    end

    it "returns false when table_exists? transiently fails without poisoning the memo" do
      # table_exists? returns false due to its own rescue path (transient PG
      # error), not because the table genuinely doesn't exist. This must NOT
      # memoize @latency_columns = false — the table may exist on the next
      # call once the connection recovers.
      allow(described_class).to receive(:table_exists?).and_return(false)

      expect(described_class.latency_columns?).to be false
      expect(described_class.instance_variable_defined?(:@latency_columns)).to be false
    end

    it "memoizes a successful probe" do
      allow(described_class).to receive_messages(
        table_exists?: true,
        column_names: %w[id job_class enqueue_latency_ms]
      )

      expect(described_class.latency_columns?).to be true
      expect(described_class.latency_columns?).to be true
      expect(described_class).to have_received(:column_names).once
    end
  end

  describe ".latency_trend" do
    it "returns empty array when latency columns are not available" do
      allow(described_class).to receive(:latency_columns?).and_return(false)
      expect(described_class.latency_trend).to eq([])
    end
  end

  describe ".avg_latency_by_queue" do
    it "returns empty array when latency columns are not available" do
      allow(described_class).to receive(:latency_columns?).and_return(false)
      expect(described_class.avg_latency_by_queue).to eq([])
    end
  end
end
