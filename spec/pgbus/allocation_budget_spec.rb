# frozen_string_literal: true

require "spec_helper"
require "memory_profiler"

RSpec.describe Pgbus::Client do
  include PgmqDoubles

  let(:mock_pgmq) { build_mock_pgmq }
  let(:all_queues_created) do
    Concurrent::Map.new.tap do |m|
      full = Pgbus.configuration.queue_name("default")
      m[full] = true
    end
  end
  let(:client) do
    described_class.allocate.tap do |c|
      c.instance_variable_set(:@pgmq, mock_pgmq)
      c.instance_variable_set(:@config, Pgbus.configuration)
      c.instance_variable_set(:@pgmq_mutex, Mutex.new)
      c.instance_variable_set(:@queues_created, all_queues_created)
      c.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
      c.instance_variable_set(:@schema_ensured, true)
      c.instance_variable_set(:@connection_health, Pgbus::Client::ConnectionHealth.new)
      # The queue's native-statement check already ran and found no native
      # support (issue #605), so the mocked pgmq-ruby call below is the one
      # measured, as before.
      c.instance_variable_set(:@native_statements, Concurrent::Map.new.tap do |m|
        m[Pgbus.configuration.queue_name("default")] = false
      end)
      # Model the production read hot path: a dedicated connection on Linux with
      # libpq >= 12, where Client#initialize sets @libpq_read_bounds_effective
      # true so reads skip the Ruby Timeout fallback and go through the cheap
      # libpq-bounded path. The Timeout fallback is a rare non-Linux branch, not
      # what the per-call allocation budget is meant to protect.
      c.instance_variable_set(:@shared_connection, false)
      c.instance_variable_set(:@libpq_read_bounds_effective, true)
    end
  end

  let(:small_payload) do
    { "job_class" => "TestJob", "job_id" => "abc-123", "queue_name" => "default", "arguments" => [1] }
  end

  def count_allocations(&block)
    report = MemoryProfiler.report(&block)
    report.total_allocated
  end

  describe "#send_message" do
    it "allocates fewer than 50 objects per call" do
      5.times { client.send_message("default", small_payload) }

      allocs = count_allocations { client.send_message("default", small_payload) }

      expect(allocs).to be < 50
    end
  end

  describe "#send_batch" do
    let(:batch) { Array.new(10) { small_payload } }

    it "allocates fewer than 25 objects per item in a batch of 10" do
      5.times { client.send_batch("default", batch) }

      allocs = count_allocations { client.send_batch("default", batch) }
      per_item = allocs / 10.0

      expect(per_item).to be < 25
    end
  end

  describe "#read_batch" do
    it "allocates fewer than 30 objects per call" do
      5.times { client.read_batch("default", qty: 5) }

      allocs = count_allocations { client.read_batch("default", qty: 5) }

      expect(allocs).to be < 30
    end
  end

  # The native statements (issue #605) on a prepared connection, under the
  # same budgets as the pgmq-ruby calls above. Plain objects, not doubles, so
  # the count is the client's own.
  describe "the native statements" do
    let(:native_pgmq) do
      result = Class.new do
        def initialize(rows) = (@rows = rows)
        def map(&) = @rows.map(&)
        def [](index) = @rows[index]
        def ntuples = @rows.size
      end
      rows = [{ "msg_id" => "1", "read_ct" => "1", "enqueued_at" => "2026-10-29 10:00:00+00",
                "last_read_at" => "2026-10-29 10:00:01+00", "vt" => "2026-10-29 10:00:31+00",
                "message" => "{}", "headers" => nil }].freeze
      conn = Class.new do
        define_method(:transaction_status) { PG::PQTRANS_IDLE }
        define_method(:exec_prepared) { |_name, _params| result.new(rows) }
      end.new
      Class.new do
        define_method(:with_connection) { |&blk| blk.call(conn) }
      end.new
    end
    let(:native_client) do
      client.tap do |c|
        c.instance_variable_set(:@pgmq, native_pgmq)
        full = Pgbus.configuration.queue_name("default")
        c.instance_variable_set(:@native_statements, Concurrent::Map.new.tap do |m|
          m[full] = Pgbus::Client::NativeStatements.statements_for("q_#{full}", "a_#{full}")
        end)
      end
    end

    before do
      require "pg"
      # pgmq-ruby's PGMQ::Message: a Data built from every column of the row.
      stub_const("PGMQ::Message", Class.new(Data.define(:msg_id, :read_ct, :enqueued_at, :last_read_at, :vt,
                                                        :message, :headers, :queue_name)) do
        def self.new(row, **)
          super(msg_id: row["msg_id"], read_ct: row["read_ct"], enqueued_at: row["enqueued_at"],
                last_read_at: row["last_read_at"], vt: row["vt"], message: row["message"],
                headers: row["headers"], queue_name: row["queue_name"])
        end
      end)
    end

    it "send_message allocates fewer than 50 objects per call" do
      5.times { native_client.send_message("default", small_payload) }

      expect(count_allocations { native_client.send_message("default", small_payload) }).to be < 50
    end

    it "read_batch allocates fewer than 30 objects per call" do
      5.times { native_client.read_batch("default", qty: 5) }

      expect(count_allocations { native_client.read_batch("default", qty: 5) }).to be < 30
    end

    it "archive_message allocates fewer than 30 objects per call" do
      5.times { native_client.archive_message("default", 1) }

      expect(count_allocations { native_client.archive_message("default", 1) }).to be < 30
    end
  end

  describe "JSON serialization round-trip" do
    it "allocates fewer than 20 objects for generate + parse" do
      json = JSON.generate(small_payload)
      5.times { JSON.parse(json) }

      allocs = count_allocations do
        s = JSON.generate(small_payload)
        JSON.parse(s)
      end

      expect(allocs).to be < 20
    end
  end

  describe "no retained objects (leak detection)" do
    let(:plain_pgmq) do
      Class.new do
        def create(...) = nil
        def enable_notify_insert(...) = nil
        def produce(...) = 1
        def read(...) = nil
        def read_batch(*, **) = []
        def delete(...) = true
        def archive(...) = true
        def set_vt(...) = nil
        def close(...) = nil
        def transaction(...) = yield(self)
      end.new
    end

    let(:plain_client) do
      described_class.allocate.tap do |c|
        c.instance_variable_set(:@pgmq, plain_pgmq)
        c.instance_variable_set(:@config, Pgbus.configuration)
        c.instance_variable_set(:@pgmq_mutex, Mutex.new)
        c.instance_variable_set(:@queues_created, all_queues_created)
        c.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
        c.instance_variable_set(:@schema_ensured, true)
        c.instance_variable_set(:@native_statements, Concurrent::Map.new.tap do |m|
          m[Pgbus.configuration.queue_name("default")] = false
        end)
      end
    end

    # Ruby 3.3's MemoryProfiler reports spurious retained objects from
    # gem-internal lazy initialization (JSON parser caches, connection_pool
    # singletons) that survive GC across test ordering. Not a Pgbus leak —
    # passes on 3.4 and 4.0 where GC is more aggressive.
    ruby_three_three = RUBY_VERSION.start_with?("3.3")

    it "retains zero objects across 100 send_message cycles",
       skip: ruby_three_three && "MemoryProfiler unreliable on Ruby 3.3" do
      50.times { plain_client.send_message("default", small_payload) }
      GC.start

      report = MemoryProfiler.report do
        100.times { plain_client.send_message("default", small_payload) }
      end

      expect(report.total_retained).to eq(0)
    end
  end
end
