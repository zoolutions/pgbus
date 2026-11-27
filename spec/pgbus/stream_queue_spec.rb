# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::StreamQueue do
  # The registry reaches the database only through connection_pool.with_connection
  # (issue #694), so every example gets the no-database pool stub.
  let(:connection) { instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter) }
  let!(:pool) { stub_bus_record_pool(connection) }

  before { described_class.reset_cache! }

  # record! runs on broadcast threads while the dispatcher iterates the set it
  # got from all_names (issue #527): the memo is a frozen Set, replaced whole.
  describe ".all_names memo" do
    before do
      allow(described_class).to receive_messages(table_exists?: true)
      allow(described_class).to receive(:load_names).and_return(Set["pgbus_chat_1"])
      allow(connection).to receive(:quote_table_name) { |name| %("#{name}") }
      allow(connection).to receive(:quote) { |value| "'#{value}'" }
      allow(connection).to receive(:execute)
      allow(connection).to receive(:transaction_open?).and_return(false)
    end

    it "hands out a frozen set" do
      expect(described_class.all_names).to be_frozen
    end

    it "does not change the set a caller holds when record! adds a name" do
      held = described_class.all_names

      described_class.record!("pgbus_chat_2")

      expect(held).to eq(Set["pgbus_chat_1"])
      expect(described_class.all_names).to eq(Set["pgbus_chat_1", "pgbus_chat_2"])
    end

    # A cold load's snapshot can predate a concurrent record! or reset_cache!.
    # The load is held mid-query while the other call runs, so publishing
    # that snapshot afterwards would lose the other call's change every run.
    # The database has the recorded name by the next load.
    context "when a cold load is in flight" do
      let(:loading) { Queue.new }
      let(:release) { Queue.new }

      before do
        loads = 0
        allow(described_class).to receive(:load_names) do
          loads += 1
          next Set["pgbus_chat_1", "pgbus_chat_2"] unless loads == 1

          loading << true
          release.pop
          Set["pgbus_chat_1"]
        end
      end

      def during_cold_load(&other_call)
        reader = Thread.new { described_class.all_names }
        loading.pop
        other = Thread.new(&other_call)
        Thread.pass until other.stop?
        release << true
        [reader, other].each(&:join)
      end

      it "keeps a name record! adds meanwhile" do
        during_cold_load { described_class.record!("pgbus_chat_2") }

        expect(described_class.all_names).to include("pgbus_chat_2")
      end

      it "does not undo a reset_cache! made meanwhile" do
        during_cold_load { described_class.reset_cache! }

        expect(described_class.instance_variable_get(:@all_names)).to be_nil
      end
    end

    # record! takes a connection before the memo's lock; a load holding the
    # lock while it waits for a connection would invert that order and could
    # deadlock on an exhausted pool.
    it "runs the load without holding the memo's lock" do
      lock = described_class.instance_variable_get(:@names_lock)
      held_during_load = nil
      allow(described_class).to receive(:load_names) do
        held_during_load = lock.owned?
        Set["pgbus_chat_1"]
      end

      described_class.all_names

      expect(held_during_load).to be(false)
    end
  end

  describe ".record!" do
    before do
      described_class.instance_variable_set(:@record_failure_warned, false)
      allow(described_class).to receive_messages(table_exists?: true)
      allow(connection).to receive(:quote_table_name) { |name| %("#{name}") }
      allow(connection).to receive(:quote) { |value| "'#{value}'" }
      allow(connection).to receive(:execute)
      allow(connection).to receive(:transaction_open?).and_return(false)
    end

    # Issue #694: record! runs on the coalescer flush thread, which no Rails
    # executor wraps, so a bare `connection` there was never returned.
    it "runs the block at once after a write outside any transaction" do
      durable = false
      described_class.record!("pgbus_chat_42") { durable = true }
      expect(durable).to be(true)
    end

    it "does not run the block when the write fails" do
      allow(connection).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "db down")
      durable = false
      described_class.record!("pgbus_chat_42") { durable = true }
      expect(durable).to be(false)
    end

    it "writes inside one connection_pool.with_connection" do
      expect(described_class.record!("pgbus_chat_42")).to be(true)

      expect(pool.leases).to eq(1)
      expect(connection).to have_received(:execute).once
    end

    # Issue #695: inside the caller's open transaction nothing is written
    # yet. The row is inserted after that commit (never on its rollback), so a
    # rollback cannot take back a registration taken for done, and no other
    # thread waits on the row's lock until the commit.
    context "when the caller has a transaction open on the connection" do
      let(:hooks) { [] }

      let(:transaction) { double("transaction", joinable?: true) }

      before do
        allow(connection).to receive_messages(transaction_open?: true, current_transaction: transaction)
        allow(Pgbus::OpenTransaction).to receive(:after_commit).with(connection) { |_conn, &hook| hooks << hook }
      end

      # The fixture transaction of transactional tests never commits: the row
      # is written into it at once, as OpenTransaction.defer runs work at once.
      it "writes at once into a transaction that is not joinable" do
        allow(transaction).to receive(:joinable?).and_return(false)
        durable = false

        expect(described_class.record!("pgbus_chat_42") { durable = true }).to be(true)
        expect(connection).to have_received(:execute)
        expect(durable).to be(true)
        expect(hooks).to be_empty
      end

      it "returns :pending and writes nothing yet" do
        expect(described_class.record!("pgbus_chat_42")).to eq(:pending)
        expect(connection).not_to have_received(:execute)
      end

      it "writes after the commit, then adds the name to a warm memo and runs the block" do
        relation = instance_double(ActiveRecord::Relation, pluck: [])
        allow(described_class).to receive(:all).and_return(relation)
        described_class.all_names
        durable = false

        described_class.record!("pgbus_chat_42") { durable = true }
        expect([durable, described_class.stream?("pgbus_chat_42")]).to eq([false, false])

        allow(connection).to receive(:transaction_open?).and_return(false)
        hooks.each(&:call)
        expect(connection).to have_received(:execute).with(a_string_matching(/INSERT INTO "pgbus_stream_queues"/))
        expect([durable, described_class.stream?("pgbus_chat_42")]).to eq([true, true])
      end

      it "reports a failed deferred write instead of raising into the committing caller" do
        allow(connection).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "db down")
        durable = false
        described_class.record!("pgbus_chat_42") { durable = true }

        expect { hooks.each(&:call) }.not_to raise_error
        expect(durable).to be(false)
      end
    end

    it "inserts the physical queue name with ON CONFLICT DO NOTHING (idempotent)" do
      described_class.record!("pgbus_chat_42")

      expect(connection).to have_received(:execute).with(
        %(INSERT INTO "pgbus_stream_queues" (queue_name) ) +
        %(VALUES ('pgbus_chat_42') ON CONFLICT (queue_name) DO NOTHING)
      )
    end

    it "does not resolve the unique index through Rails upsert machinery (issue #401)" do
      # upsert(unique_by:) resolves the index via the pool schema cache, which
      # latches a wrong negative data_source_exists? probe for the process
      # lifetime. The gem owns the index — nothing to resolve.
      allow(described_class).to receive(:upsert)

      described_class.record!("pgbus_chat_42")

      expect(described_class).not_to have_received(:upsert)
    end

    it "skips when the table does not exist (unmigrated install)" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!("pgbus_chat_42")

      expect(connection).not_to have_received(:execute)
    end

    it "swallows errors so a registry blip cannot kill a broadcast" do
      allow(connection).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "db down")

      expect { described_class.record!("pgbus_chat_42") }.not_to raise_error
    end

    it "returns true on a successful insert and false on failure or missing table" do
      expect(described_class.record!("pgbus_chat_42")).to be(true)

      allow(connection).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "db down")
      expect(described_class.record!("pgbus_chat_42")).to be(false)

      allow(described_class).to receive(:table_exists?).and_return(false)
      expect(described_class.record!("pgbus_chat_42")).to be(false)
    end

    context "when logging failures" do
      let(:logger) { instance_double(Logger, warn: nil, debug: nil) }

      before { allow(Pgbus).to receive(:logger).and_return(logger) }

      it "logs a transient database error at DEBUG, never WARN" do
        allow(connection).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "db down")

        3.times { described_class.record!("pgbus_chat_42") }

        expect(logger).to have_received(:debug).exactly(3).times
        expect(logger).not_to have_received(:warn)
      end

      it "logs a non-database failure (bug signal) at WARN once per process, DEBUG thereafter" do
        allow(connection).to receive(:execute)
          .and_raise(ArgumentError, "No unique index found for queue_name")

        3.times { described_class.record!("pgbus_chat_42") }
        described_class.record!("pgbus_other_stream")

        expect(logger).to have_received(:warn).once
        expect(logger).to have_received(:debug).exactly(3).times
      end

      it "returns false on a non-database failure" do
        allow(connection).to receive(:execute).and_raise(ArgumentError, "boom")

        expect(described_class.record!("pgbus_chat_42")).to be(false)
      end
    end

    it "adds the name to an already-warm cache without a re-query" do
      relation = instance_double(ActiveRecord::Relation, pluck: [])
      allow(described_class).to receive(:all).and_return(relation)
      described_class.all_names # warm the cache first

      described_class.record!("pgbus_chat_42")

      expect(described_class.stream?("pgbus_chat_42")).to be(true)
      expect(described_class).to have_received(:all).once
    end

    it "falls through to a real load on a cold cache, rather than fabricating a one-entry set" do
      # @all_names has never been loaded in this process. Seeding it here
      # with just the recorded name would silently hide every other
      # already-registered stream (see the regression test below) — so
      # record! must leave a cold cache cold and let the next read query.
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.record!("pgbus_chat_42")

      expect(described_class.stream?("pgbus_chat_42")).to be(true)
    end

    it "still finds other already-registered streams after a first-touch record! (regression)" do
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_room_1])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.record!("pgbus_chat_42")

      expect(described_class.stream?("pgbus_room_1")).to be(true)
    end
  end

  describe ".all_names" do
    it "returns the set of registered physical queue names" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42 pgbus_room_1])
      allow(described_class).to receive(:all).and_return(relation)

      expect(described_class.all_names).to eq(Set.new(%w[pgbus_chat_42 pgbus_room_1]))
      expect(pool.leases).to eq(1)
    end

    it "returns an empty set when the table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      expect(described_class.all_names).to eq(Set.new)
    end

    it "memoizes so repeated dispatcher loops do not re-query" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.all_names
      described_class.all_names

      expect(described_class).to have_received(:all).once
    end

    it "re-queries after reset_cache!" do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)

      described_class.all_names
      described_class.reset_cache!
      described_class.all_names

      expect(described_class).to have_received(:all).twice
    end
  end

  describe ".stream?" do
    before do
      allow(described_class).to receive_messages(table_exists?: true)
      relation = instance_double(ActiveRecord::Relation, pluck: %w[pgbus_chat_42])
      allow(described_class).to receive(:all).and_return(relation)
    end

    it "is true for a registered stream queue" do
      expect(described_class.stream?("pgbus_chat_42")).to be(true)
    end

    it "is false for a job queue" do
      expect(described_class.stream?("pgbus_default")).to be(false)
    end
  end

  describe ".table_exists?" do
    after { described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists) }

    it "returns false (and does not raise) when the connection is unavailable" do
      allow(pool).to receive(:with_connection).and_raise(StandardError, "no db")

      expect(described_class.table_exists?).to be(false)
    end

    it "probes inside connection_pool.with_connection, never a bare lease (issue #694)" do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
      allow(connection).to receive(:table_exists?).with("pgbus_stream_queues").and_return(true)

      expect(described_class.table_exists?).to be(true)
      expect(pool.leases).to eq(1)
    end
  end

  # Issue #366: dormant pre-registry stream queues never call ensure_stream_queue
  # again, so they stay unregistered. The archive msg_id index created by
  # ensure_stream_queue is a fingerprint no job queue has.
  describe ".fingerprint_matched_names (issue #366)" do
    it "returns physical queue names that carry the stream archive msg_id index" do
      allow(connection).to receive(:select_values)
        .with(a_string_matching(/pg_indexes/))
        .and_return(%w[pgbus_chat_42 pgbus_checkout_9])

      expect(described_class.fingerprint_matched_names)
        .to eq(Set.new(%w[pgbus_chat_42 pgbus_checkout_9]))
      expect(pool.leases).to eq(1)
    end

    it "returns an empty set when the query fails (unreadable / no pgmq schema)" do
      allow(connection).to receive(:select_values).and_raise(StandardError, "no pgmq")

      expect(described_class.fingerprint_matched_names).to eq(Set.new)
    end

    it "does not raise when the connection is unavailable" do
      allow(pool).to receive(:with_connection).and_raise(StandardError, "no db")

      expect(described_class.fingerprint_matched_names).to eq(Set.new)
    end
  end

  describe ".known_names (issue #366)" do
    it "unions registry names with fingerprint-matched names" do
      allow(described_class).to receive_messages(
        all_names: Set.new(%w[pgbus_chat_1]),
        fingerprint_matched_names: Set.new(%w[pgbus_chat_1 pgbus_dormant_99])
      )

      expect(described_class.known_names)
        .to eq(Set.new(%w[pgbus_chat_1 pgbus_dormant_99]))
    end
  end

  describe ".backfill! (issue #366)" do
    before do
      allow(described_class).to receive_messages(table_exists?: true)
    end

    it "registers fingerprint-matched queues that are missing from the registry" do
      allow(described_class).to receive_messages(
        all_names: Set.new(%w[pgbus_chat_1]),
        fingerprint_matched_names: Set.new(%w[pgbus_chat_1 pgbus_dormant_99 pgbus_checkout_3])
      )
      recorded = []
      allow(described_class).to receive(:record!) do |name|
        recorded << name
        true
      end

      count = described_class.backfill!

      expect(count).to eq(2)
      expect(recorded).to contain_exactly("pgbus_dormant_99", "pgbus_checkout_3")
    end

    it "counts only successful writes when some upserts fail" do
      allow(described_class).to receive_messages(
        all_names: Set.new,
        fingerprint_matched_names: Set.new(%w[pgbus_ok pgbus_fail])
      )
      allow(described_class).to receive(:record!) do |name|
        name == "pgbus_ok"
      end
      allow(described_class).to receive(:reset_cache!)

      expect(described_class.backfill!).to eq(1)
      expect(described_class).to have_received(:reset_cache!)
    end

    it "does not report a successful backfill when every write fails" do
      allow(described_class).to receive_messages(
        all_names: Set.new,
        fingerprint_matched_names: Set.new(%w[pgbus_fail])
      )
      allow(described_class).to receive(:record!).and_return(false)
      allow(described_class).to receive(:reset_cache!)

      expect(described_class.backfill!).to eq(0)
      expect(described_class).not_to have_received(:reset_cache!)
    end

    it "is a no-op when every fingerprint match is already registered" do
      allow(described_class).to receive_messages(
        all_names: Set.new(%w[pgbus_chat_1]),
        fingerprint_matched_names: Set.new(%w[pgbus_chat_1])
      )
      allow(described_class).to receive(:record!)

      expect(described_class.backfill!).to eq(0)
      expect(described_class).not_to have_received(:record!)
    end

    it "returns 0 without writing when the registry table is absent" do
      allow(described_class).to receive_messages(table_exists?: false)
      allow(described_class).to receive(:record!)

      expect(described_class.backfill!).to eq(0)
      expect(described_class).not_to have_received(:record!)
    end

    it "resets the name cache after a successful write so subsequent all_names sees the rows" do
      allow(described_class).to receive_messages(
        all_names: Set.new,
        fingerprint_matched_names: Set.new(%w[pgbus_dormant_99])
      )
      allow(described_class).to receive(:record!).and_return(true)
      allow(described_class).to receive(:reset_cache!)

      described_class.backfill!

      expect(described_class).to have_received(:reset_cache!)
    end
  end
end
