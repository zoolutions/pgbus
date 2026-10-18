# frozen_string_literal: true

require "spec_helper"
require "pg"
require "monitor"
require "pgbus/streams/presence"
# Loaded with the real ActiveRecord::Base, before the examples stub it.
require "pgbus/bus_record"

# Every presence statement runs under the ActiveRecord adapter's lock (issue
# #566). `raw_connection` releases that lock as soon as it returns the
# PG::Connection, so a statement run on it afterwards is unguarded: on a
# pinned connection (transactional tests) another thread could put its own
# statement on the same libpq connection mid-flight. A real Monitor stands in
# for the adapter's (a ThreadMonitor once the pool is pinned to a thread, a
# Monitor when pinned to a fiber, a NullLock otherwise);
# spec/integration/streams/presence_adapter_lock_spec.rb drives a real pinned
# pool.
RSpec.describe Pgbus::Streams::Presence do
  subject(:presence) { described_class.new(stream) }

  let(:stream) { double("Pgbus::Streams::Stream", name: "room:42") }
  let(:raw_connection) { double("PG::Connection") }
  let(:adapter_lock) { Monitor.new }
  # Like AbstractAdapter#raw_connection: with_raw_connection takes the lock
  # to hand the connection out and releases it on return.
  let(:ar_connection) do
    double("ActiveRecord::ConnectionAdapters::PostgreSQLAdapter", lock: adapter_lock).tap do |adapter|
      allow(adapter).to receive(:raw_connection) { adapter_lock.synchronize { raw_connection } }
    end
  end
  let(:seat_row) do
    { "member_id" => "7", "metadata" => "{}", "joined_at" => "t", "last_seen_at" => "t",
      "connection_id" => "c1", "seat" => "s1", "state" => "{}", "n" => "1", "stream_name" => "room:42" }
  end

  before do
    allow(Pgbus.configuration).to receive(:connects_to).and_return(nil)
    stub_const("ActiveRecord::Base", Class.new)
    allow(ActiveRecord::Base).to receive(:connection).and_return(ar_connection)
    allow(described_class).to receive(:seats_supported?).and_return(true)
  end

  describe "every statement" do
    {
      "join" => ->(presence) { presence.join(member_id: "7") },
      "join with connection_id:" => ->(presence) { presence.join(member_id: "7", connection_id: "c1", seat: "s1") },
      "leave" => ->(presence) { presence.leave(member_id: "7") },
      "leave with connection_id:" => ->(presence) { presence.leave(connection_id: "c1") },
      "touch" => ->(presence) { presence.touch(member_id: "7") },
      "touch with connection_id:" => ->(presence) { presence.touch(connection_id: %w[c1]) },
      "set_state" => ->(presence) { presence.set_state(connection_id: "c1", state: { a: 1 }) },
      "members" => lambda(&:members),
      "count" => lambda(&:count),
      "sweep!" => ->(presence) { presence.sweep!(older_than: Time.now) },
      "expire_seats!" => ->(_) { described_class.expire_seats!(ttl_for: ->(_) { 30 }, min_ttl: 30) }
    }.each do |name, call|
      it "runs #{name}'s SQL while holding the adapter's lock" do
        held = []
        allow(raw_connection).to receive(:exec_params) do
          held << adapter_lock.mon_owned?
          [seat_row]
        end

        call.call(presence)

        expect(held).not_to be_empty
        expect(held).to all(be(true))
      end
    end

    it "runs the seat-schema probe while holding the adapter's lock" do
      allow(described_class).to receive(:seats_supported?).and_call_original
      described_class.reset_schema_cache!
      held = []
      allow(raw_connection).to receive(:exec_params) do
        held << adapter_lock.mon_owned?
        [{ "present" => "1" }]
      end

      expect(described_class.seats_supported?).to be(true)
      expect(held).to eq([true])
    ensure
      described_class.reset_schema_cache!
    end
  end

  context "with connects_to (the pgbus tables in their own database)" do
    let(:bus_lock) { Monitor.new }
    let(:bus_raw_connection) { double("PG::Connection (pgbus database)") }
    let(:bus_adapter) do
      double("ActiveRecord::ConnectionAdapters::PostgreSQLAdapter (pgbus)", lock: bus_lock).tap do |adapter|
        allow(adapter).to receive(:raw_connection) { bus_lock.synchronize { bus_raw_connection } }
      end
    end

    before do
      allow(Pgbus.configuration).to receive(:connects_to).and_return({ database: { writing: :pgbus } })
      stub_const("Pgbus::BusRecord", Class.new)
      allow(Pgbus::BusRecord).to receive(:connection).and_return(bus_adapter)
    end

    it "runs the statement on Pgbus::BusRecord's connection while holding that adapter's lock" do
      held = []
      allow(bus_raw_connection).to receive(:exec_params) do
        held << bus_lock.mon_owned?
        [{ "n" => "2" }]
      end
      allow(raw_connection).to receive(:exec_params)

      expect(presence.count).to eq(2)
      expect(held).to eq([true])
      expect(raw_connection).not_to have_received(:exec_params)
    end
  end

  describe "across threads" do
    let(:threads) { [] }

    after { threads.each(&:kill) }

    def spawn(&)
      Thread.new(&).tap { |thread| threads << thread }
    end

    def sweep
      described_class.expire_seats!(ttl_for: ->(_) { 30 }, min_ttl: 30)
    end

    def wait_until_blocked(thread)
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      Thread.pass until thread.status != "run" || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    end

    it "does not start a sweep's statement while another thread holds the adapter's lock" do
      allow(raw_connection).to receive(:exec_params).and_return([])
      sweeper = nil

      adapter_lock.synchronize do
        sweeper = spawn { sweep }

        expect(sweeper.join(0.3)).to be_nil
        expect(raw_connection).not_to have_received(:exec_params)
      end

      expect(sweeper.join(2)&.value).to eq([])
      expect(raw_connection).to have_received(:exec_params).once
    end

    it "re-enters the lock on a thread that already holds it (a request inside a transaction)" do
      allow(raw_connection).to receive(:exec_params).and_return([seat_row])

      adapter_lock.synchronize do
        expect(presence.join(member_id: "7", connection_id: "c1", seat: "s1")["connection_id"]).to eq("c1")
        expect(presence.set_state(connection_id: "c1", state: { a: 1 })["seat"]).to eq("s1")
      end
    end

    context "with a cold seat-schema cache" do
      before do
        allow(described_class).to receive(:seats_supported?).and_call_original
        described_class.reset_schema_cache!
        allow(raw_connection).to receive(:exec_params) do |sql, _binds|
          sql.include?("information_schema") ? [{ "present" => "1" }] : []
        end
      end

      after { described_class.reset_schema_cache! }

      # within_new_transaction holds the adapter's lock for the whole block,
      # so a pinned request inside a transaction takes the adapter lock first
      # and the schema mutex second (seats? -> seats_supported?). A sweep that
      # probed the schema while holding the mutex would take them the other
      # way round.
      it "never waits for the adapter's lock while holding the schema mutex" do
        in_transaction = Queue.new
        go = Queue.new
        request = spawn do
          adapter_lock.synchronize do
            in_transaction << true
            go.pop
            presence.members
          end
        end
        in_transaction.pop
        sweeper = spawn { sweep }
        wait_until_blocked(sweeper)
        go << true

        expect(request.join(2)&.value).to eq([])
        expect(sweeper.join(2)&.value).to eq([])
      end
    end
  end

  describe "a seat-schema probe racing another" do
    before do
      allow(described_class).to receive(:seats_supported?).and_call_original
      described_class.reset_schema_cache!
    end

    after { described_class.reset_schema_cache! }

    it "keeps a true answer the other probe stored while this one ran" do
      probes = 0
      allow(raw_connection).to receive(:exec_params) do
        probes += 1
        next [{ "present" => "1" }] if probes == 2

        # The other probe runs and stores its answer while this one is in flight.
        expect(described_class.seats_supported?).to be(true)
        []
      end

      expect(described_class.seats_supported?).to be(true)
      expect(probes).to eq(2)
    end

    # A request that hit the pre-seat conflict target resets the cache while
    # a sweep's probe, started before the migration, is still in flight. The
    # sweep's stale "no seats" must not become the fresh cached answer, or the
    # destructive member-keyed statements would drop `connection_id IS NULL`.
    it "does not store an answer from a probe that a reset overtook" do
      answers = [[], [{ "present" => "1" }]]
      allow(raw_connection).to receive(:exec_params) do
        answer = answers.shift
        described_class.reset_schema_cache! if answers.size == 1
        answer
      end

      expect(described_class.seats_supported?).to be(false)
      expect(described_class.seats_supported?).to be(true)
      expect(answers).to be_empty
    end
  end
end
