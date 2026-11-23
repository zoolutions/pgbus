# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #683: a scope flushes early every coalesce_chunk_size held jobs, and
# Pgbus.batch opens a scope of its own for its block, holding its members and
# sending them before the batch starts.
RSpec.describe Pgbus::ActiveJob::Coalesce do
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

    it "keeps one running total, not a result per chunk" do
      scope = nil
      Pgbus.coalesce do
        scope = described_class.current
        5.times { |i| plain_class.perform_later(i) }

        expect(scope.instance_variable_get(:@totals).buffered).to eq(4)
      end
    end

    it "keeps no delivered job once its chunk was sent, only the chunk still held" do
      scope = nil
      Pgbus.coalesce do
        scope = described_class.current
        5.times { |i| plain_class.perform_later(i) }

        expect(scope.send(:retained_entries).size).to eq(1)
      end
    end

    it "sends a block of exactly coalesce_chunk_size jobs as one flush, at its end" do
      Pgbus.coalesce do
        2.times { |i| plain_class.perform_later(i) }

        expect(batches).to be_empty
      end

      expect(batches.map(&:size)).to eq([2])
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

    # Issue #689: coalesce_default applies to a batch's own scope too, so a
    # fan-out job declares pgbus_coalesce true to be sent in bulk.
    context "with coalesce_default = false" do
      around do |example|
        original = Pgbus.configuration.coalesce_default
        Pgbus.configuration.coalesce_default = false
        example.run
      ensure
        Pgbus.configuration.coalesce_default = original
      end

      it "sends a member that declares nothing at once, counted in as usual" do
        described_class.open(batch_id: "b-1") do
          Thread.current[:pgbus_batch_id] = "b-1"
          plain_class.perform_later(1)

          expect(mock_client).to have_received(:send_message).once
        end

        expect(batches).to be_empty
      end

      it "holds a member whose class declares pgbus_coalesce true" do
        fan_out = Class.new(plain_class) do
          include Pgbus::ActiveJob::Coalesce::JobMixin

          pgbus_coalesce true
        end

        described_class.open(batch_id: "b-1") do
          Thread.current[:pgbus_batch_id] = "b-1"
          2.times { |i| fan_out.perform_later(i) }

          expect(batches).to be_empty
        end

        expect(batches.size).to eq(1)
        expect(mock_client).not_to have_received(:send_message)
      end
    end

    it "counts each chunk in before sending it, and closes with a final count even when empty" do
      calls = []
      count_in = ->(payloads, final:) { calls << [payloads.size, final, batches.size] }
      original = Pgbus.configuration.coalesce_chunk_size
      Pgbus.configuration.coalesce_chunk_size = 2
      allow_any_instance_of(described_class).to receive(:hold_for_transaction?).and_return(false)

      described_class.open(batch_id: "b-1", count_in: count_in) do
        Thread.current[:pgbus_batch_id] = "b-1"
        3.times { |i| plain_class.perform_later(i) }
      end

      expect(calls).to eq([[2, false, 0], [1, true, 1]])
      expect(Pgbus::Batch).not_to have_received(:track_enqueue)
    ensure
      Pgbus.configuration.coalesce_chunk_size = original
    end

    it "keeps holding a full chunk while a transaction is open on the batch's connection" do
      calls = []
      count_in = ->(payloads, final:) { calls << [payloads.size, final] }
      original = Pgbus.configuration.coalesce_chunk_size
      Pgbus.configuration.coalesce_chunk_size = 1
      allow_any_instance_of(described_class).to receive(:hold_for_transaction?).and_return(true)

      described_class.open(batch_id: "b-1", count_in: count_in) do
        Thread.current[:pgbus_batch_id] = "b-1"
        3.times { |i| plain_class.perform_later(i) }
      end

      expect(calls).to eq([[3, true]])
    ensure
      Pgbus.configuration.coalesce_chunk_size = original
    end

    it "sends nothing of a chunk it could not count in, and fails its jobs" do
      count_in = ->(_payloads, final:) { raise Pgbus::Batch::AlreadyFinished, "reaped" if final }
      failed = []

      described_class.open(batch_id: "b-1", count_in: count_in, on_failure: ->(jobs, error) { failed << [jobs.size, error.cause] }) do
        Thread.current[:pgbus_batch_id] = "b-1"
        plain_class.perform_later(1)
      end

      expect(batches).to be_empty
      expect(failed.size).to eq(1)
      expect(failed.first[0]).to eq(1)
      expect(failed.first[1]).to be_a(Pgbus::Batch::AlreadyFinished)
    end

    it "raises a final count that fails with nothing held" do
      count_in = ->(_payloads, final:) { raise Pgbus::Batch::AlreadyFinished, "reaped" if final }

      expect { described_class.open(batch_id: "b-1", count_in: count_in) { nil } }
        .to raise_error(Pgbus::Batch::AlreadyFinished)
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

    it "leaves no scope current while it flushes" do
      seen = []
      allow(mock_client).to receive(:send_batch) do |_queue, payloads, **|
        seen << described_class.current
        payloads.map { take_id }
      end

      Pgbus.coalesce do
        described_class.open(batch_id: "b-1") do
          Thread.current[:pgbus_batch_id] = "b-1"
          plain_class.perform_later(1)
          Thread.current[:pgbus_batch_id] = nil
        end
      end

      expect(seen).to eq([nil])
    end

    it "is joined by a plain scope opened inside it" do
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
