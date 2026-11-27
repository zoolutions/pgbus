# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::StreamStat do
  describe ".record!" do
    let!(:pool) { stub_bus_record_pool }

    before do
      allow(described_class).to receive_messages(create!: nil, table_exists?: true)
    end

    # Issue #694: the dispatcher records off-request, where no Rails executor
    # returns a lease, so the write takes its own and gives it back.
    it "creates inside one connection_pool.with_connection" do
      described_class.record!(stream_name: "chat_42", event_type: "broadcast", duration_ms: 1)

      expect(pool.leases).to eq(1)
      expect(described_class).to have_received(:create!)
    end

    it "creates a broadcast stat with fanout" do
      described_class.record!(
        stream_name: "chat_42",
        event_type: "broadcast",
        duration_ms: 17,
        fanout: 12
      )

      expect(described_class).to have_received(:create!).with(
        stream_name: "chat_42",
        event_type: "broadcast",
        duration_ms: 17,
        fanout: 12
      )
    end

    it "creates a connect stat without fanout" do
      described_class.record!(
        stream_name: "chat_42",
        event_type: "connect",
        duration_ms: 3
      )

      expect(described_class).to have_received(:create!).with(
        stream_name: "chat_42",
        event_type: "connect",
        duration_ms: 3,
        fanout: nil
      )
    end

    it "skips when table does not exist" do
      allow(described_class).to receive(:table_exists?).and_return(false)

      described_class.record!(stream_name: "chat", event_type: "broadcast", duration_ms: 0)

      expect(described_class).not_to have_received(:create!)
    end

    it "swallows errors" do
      allow(described_class).to receive(:create!).and_raise(StandardError, "db down")

      expect do
        described_class.record!(stream_name: "chat", event_type: "broadcast", duration_ms: 0)
      end.not_to raise_error
    end

    it "coerces duration_ms to integer" do
      described_class.record!(
        stream_name: "chat",
        event_type: "broadcast",
        duration_ms: 4.7
      )

      expect(described_class).to have_received(:create!).with(
        hash_including(duration_ms: 4)
      )
    end
  end

  # The dispatcher prunes off-request (issue #694): the delete takes its own
  # lease and returns it.
  describe ".cleanup!" do
    it "deletes inside one connection_pool.with_connection" do
      pool = stub_bus_record_pool
      relation = instance_double(ActiveRecord::Relation, delete_all: 3)
      allow(described_class).to receive(:where).and_return(relation)

      expect(described_class.cleanup!(older_than: Time.now)).to eq(3)
      expect(pool.leases).to eq(1)
    end
  end

  describe "scope names" do
    # Regression for issue #92 — `turbo-rails` auto-includes
    # Turbo::Broadcastable into every ActiveRecord::Base descendant,
    # which defines a class method `broadcasts`. AR's
    # dangerous_class_method? check then rejects any subsequent
    # `scope :broadcasts` on an AR model, crashing eager_load.
    #
    # We must not define scopes whose names collide with turbo-rails'
    # Broadcastable DSL. The equivalent filters live under
    # broadcast_events / connect_events / disconnect_events.

    # Inject a fake `broadcasts` class method onto ActiveRecord::Base
    # (what Turbo::Broadcastable does via on_load(:active_record))
    # and remove it after the example so later specs are not polluted.
    around do |example|
      fake_turbo = Module.new do
        def broadcasts(*); end
      end
      ActiveRecord::Base.singleton_class.include(fake_turbo)
      example.run
    ensure
      fake_turbo.send(:remove_method, :broadcasts)
    end

    it "reloads StreamStat cleanly when turbo-rails has injected `broadcasts`" do
      # Sanity check: AR's dangerous_class_method? guard fires for
      # any subsequent model declaring `scope :broadcasts`.
      expect do
        Class.new(ActiveRecord::Base) do
          self.table_name = "pgbus_stream_stats"
          scope :broadcasts, -> { where(event_type: "broadcast") }
        end
      end.to raise_error(ArgumentError, /already defined a class method/)

      # With the turbo shim installed, reloading StreamStat must
      # not raise. `load` re-evaluates the file the same way
      # Zeitwerk's eager_load pass would. Silence the benign
      # "already initialized constant" warning re-evaluation emits.
      stream_stat_path = File.expand_path(
        "../../app/models/pgbus/stream_stat.rb",
        __dir__
      )
      expect do
        original_verbose = $VERBOSE
        $VERBOSE = nil
        begin
          load stream_stat_path
        ensure
          $VERBOSE = original_verbose
        end
      end.not_to raise_error
    end

    it "exposes broadcast_events / connect_events / disconnect_events scopes" do
      expect(described_class).to respond_to(:broadcast_events)
      expect(described_class).to respond_to(:connect_events)
      expect(described_class).to respond_to(:disconnect_events)
    end
  end

  describe ".table_exists?" do
    before do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    after do
      described_class.remove_instance_variable(:@table_exists) if described_class.instance_variable_defined?(:@table_exists)
    end

    it "returns false on connection error without poisoning the memo" do
      pool = stub_bus_record_pool
      allow(pool).to receive(:with_connection).and_raise(StandardError, "no db")

      expect(described_class.table_exists?).to be false
      # The failed probe must NOT set @table_exists — otherwise a
      # transient PG blip during boot permanently disables stream
      # stats for the process lifetime.
      expect(described_class.instance_variable_defined?(:@table_exists)).to be false
    end

    it "memoizes a successful probe" do
      fake_connection = instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter)
      allow(fake_connection).to receive(:table_exists?).with(described_class.table_name).and_return(true)
      pool = stub_bus_record_pool(fake_connection)

      expect(described_class.table_exists?).to be true
      # Second call hits the memo, not the connection.
      expect(described_class.table_exists?).to be true
      expect(fake_connection).to have_received(:table_exists?).once
      # The probe leases through with_connection (issue #694).
      expect(pool.leases).to eq(1)
    end
  end
end
