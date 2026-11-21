# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #683: a scope flushes early every coalesce_chunk_size held jobs, and
# Pgbus.batch opens a scope of its own for its block, holding its members and
# sending them before the batch starts.
RSpec.describe Pgbus::ActiveJob::Coalesce, "chunks and batch scopes" do
  let(:adapter) { Pgbus::ActiveJob::Adapter.new }
  let(:mock_client) { build_mock_client }
  let(:next_id) { [100] }
  let(:batches) { [] }

  let(:plain_class) do
    the_adapter = adapter
    Class.new(ActiveJob::Base) do
      include Pgbus::ActiveJob::BatchId

      self.queue_adapter = the_adapter
      def self.name = "CoalesceBatchSpec::PlainJob"
      def perform(*) = nil
    end
  end

  def take_id
    next_id[0] += 1
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CoalesceBatchSpec::PlainJob", plain_class)
    stub_bus_record_pool
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:send_message) { take_id }
    allow(mock_client).to receive(:send_batch) do |_queue, payloads, **|
      batches << payloads
      payloads.map { take_id }
    end
    allow(Pgbus::Batch).to receive(:track_enqueue)
    allow(Pgbus::Batch).to receive(:backfill_executions)
    allow(Pgbus::Batch).to receive(:hold_enqueue)
    allow(Pgbus::Batch).to receive(:release_enqueue_hold)
  end

  after do
    Thread.current[:pgbus_coalesce] = nil
    Thread.current[:pgbus_batch_id] = nil
  end

  describe "chunking" do
    around do |example|
      original = Pgbus.configuration.coalesce_chunk_size
      Pgbus.configuration.coalesce_chunk_size = 2
      example.run
    ensure
      Pgbus.configuration.coalesce_chunk_size = original
    end

    it "flushes every coalesce_chunk_size held jobs and once more at the end" do
      sizes_in_block = nil

      Pgbus.coalesce do
        5.times { |i| plain_class.perform_later(i) }
        sizes_in_block = batches.map(&:size)
      end

      expect(sizes_in_block).to eq([2, 2])
      expect(batches.map(&:size)).to eq([2, 2, 1])
    end

    it "applies on_failure once, at the end, to the jobs of every chunk that failed" do
      calls = 0
      allow(mock_client).to receive(:send_batch) do |_queue, payloads, **|
        calls += 1
        raise IOError, "chunk lost" if calls == 2

        payloads.map { take_id }
      end
      allow(mock_client).to receive(:send_message).and_raise(IOError, "still lost")
      failures = []

      Pgbus.coalesce(on_failure: ->(jobs, _error) { failures << jobs.size }) do
        5.times { |i| plain_class.perform_later(i) }

        expect(failures).to be_empty
      end

      expect(failures).to eq([2])
    end
  end

  describe "a batch scope" do
    it "holds the members of its batch, tagged at push" do
      described_class.open(batch_id: "b-1") do
        Thread.current[:pgbus_batch_id] = "b-1"
        2.times { |i| plain_class.perform_later(i) }

        expect(batches).to be_empty
      end

      expect(batches.size).to eq(1)
      expect(batches.first.map { |p| p[Pgbus::Batch::METADATA_KEY] }).to eq(%w[b-1 b-1])
      expect(Pgbus::Batch).to have_received(:track_enqueue).once
      expect(mock_client).not_to have_received(:send_message)
    end

    it "takes one hold on its batch before the first held member and gives it back after the flush" do
      order = []
      allow(Pgbus::Batch).to receive(:hold_enqueue) { order << :hold }
      allow(Pgbus::Batch).to receive(:track_enqueue) { order << :count }
      allow(Pgbus::Batch).to receive(:release_enqueue_hold) { order << :release }

      described_class.open(batch_id: "b-1") do
        Thread.current[:pgbus_batch_id] = "b-1"
        3.times { |i| plain_class.perform_later(i) }
      end

      expect(order).to eq(%i[hold count release])
    end

    it "takes no hold when it holds nothing" do
      described_class.open(batch_id: "b-1") { nil }

      expect(Pgbus::Batch).not_to have_received(:hold_enqueue)
      expect(Pgbus::Batch).not_to have_received(:release_enqueue_hold)
    end

    it "reports a hold it cannot give back" do
      allow(Pgbus::Batch).to receive(:release_enqueue_hold).and_raise(IOError, "db gone")
      allow(Pgbus::ErrorReporter).to receive(:report)

      described_class.open(batch_id: "b-1") do
        Thread.current[:pgbus_batch_id] = "b-1"
        plain_class.perform_later(1)
      end

      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(IOError), { pgbus_coalesce_hold: "b-1" })
    end

    it "sends a member of another batch at once" do
      described_class.open(batch_id: "b-1") do
        Thread.current[:pgbus_batch_id] = "b-2"
        plain_class.perform_later(1)

        expect(mock_client).to have_received(:send_message).once
      end
    end

    it "does not join an outer scope, and restores it afterwards" do
      outer = nil
      inner = nil
      Pgbus.coalesce do
        outer = described_class.current
        described_class.open(batch_id: "b-1") do
          inner = described_class.current
          Thread.current[:pgbus_batch_id] = "b-1"
          plain_class.perform_later(1)
          Thread.current[:pgbus_batch_id] = nil
        end

        expect(described_class.current).to be(outer)
        expect(batches.size).to eq(1)
        plain_class.perform_later(2)
      end

      expect(inner).not_to be(outer)
      expect(batches.size).to eq(2)
    end

    it "is not joined by a plain scope opened inside it" do
      described_class.open(batch_id: "b-1") do
        scope = described_class.current
        Pgbus.coalesce { expect(described_class.current).to be(scope) }
      end
    end
  end

  describe "Pgbus.batch" do
    it "rejects an unknown on_enqueue_failure" do
      expect { Pgbus::Batch.new(on_enqueue_failure: :ignore) }.to raise_error(ArgumentError, /on_failure/)
    end
  end
end
