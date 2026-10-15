# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Client do
  describe "pgmq-ruby 0.7.0 features" do
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      c = described_class.new(config)
      c.instance_variable_set(:@schema_ensured, true)
      allow(c).to receive(:tune_autovacuum)
      allow(c).to receive(:notify_trigger_current?).and_return(false)
      c
    end

    before do
      # Stub the class method that loads pgmq so the faked PGMQ::Client stands;
      # a clean per-example stub, unlike stubbing global Kernel#require.
      allow(described_class).to receive(:load_pgmq_gem!)
      stub_const("PGMQ::Client", Class.new do
        def initialize(*args, **kwargs); end
      end)
    end

    let(:config) do
      Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
      end
    end
    let(:mock_pgmq) { build_mock_pgmq }

    describe "#wait_for_notify" do
      # The wait runs on a dedicated connection of its own (issue #527), never
      # on a pool connection or under the shared-path @pgmq_mutex.
      let(:listen_conn) do
        double("PG::Connection", exec: nil, close: nil).tap do |conn|
          allow(conn).to receive(:quote_ident) { |name| %("#{name}") }
          allow(conn).to receive(:wait_for_notify).and_return("pgmq.q_pgbus_test_default.INSERT")
        end
      end

      before do
        allow(Pgbus::DedicatedConnection).to receive(:connect).and_return(listen_conn)
        allow(mock_pgmq).to receive(:wait_for_notify).and_return("pgmq.q_pgbus_test_default.INSERT")
      end

      it "listens on the queue's insert channel over a dedicated connection" do
        client.wait_for_notify("default", timeout: 5)

        expect(Pgbus::DedicatedConnection).to have_received(:connect).with(config.worker_notify_connection_options)
        expect(listen_conn).to have_received(:exec).with('LISTEN "pgmq.q_pgbus_test_default.INSERT"')
        expect(listen_conn).to have_received(:wait_for_notify).with(5)
        expect(mock_pgmq).not_to have_received(:wait_for_notify)
      end

      it "returns the channel name" do
        expect(client.wait_for_notify("default", timeout: 5)).to eq("pgmq.q_pgbus_test_default.INSERT")
      end

      it "passes nil timeout for indefinite wait" do
        client.wait_for_notify("default")

        expect(listen_conn).to have_received(:wait_for_notify).with(nil)
      end

      it "yields the notification to the block" do
        allow(listen_conn).to receive(:wait_for_notify).and_yield("ch", 42, "payload")
        yielded = nil

        client.wait_for_notify("default", timeout: 1) { |ch, pid, pl| yielded = [ch, pid, pl] }

        expect(yielded).to eq(["ch", 42, "payload"])
      end

      # The retry the pooled wait had (with_stale_connection_retry): a
      # connection found dead before it was used is replaced and the wait
      # starts again. A connection lost mid-wait is not retried, as before.
      context "when the dedicated connection is stale" do
        let(:stale_conn) do
          double("PG::Connection", exec: nil, close: nil).tap do |conn|
            allow(conn).to receive(:quote_ident) { |name| %("#{name}") }
            allow(conn).to receive(:wait_for_notify).and_raise(PG::ConnectionBad, "no connection to the server")
          end
        end

        before do
          require "pg"
          allow(Pgbus::DedicatedConnection).to receive(:connect).and_return(stale_conn, listen_conn)
          allow(client).to receive(:sleep)
        end

        it "waits again on a fresh connection" do
          expect(client.wait_for_notify("default", timeout: 5)).to eq("pgmq.q_pgbus_test_default.INSERT")
          expect(Pgbus::DedicatedConnection).to have_received(:connect).twice
          expect(stale_conn).to have_received(:close)
        end

        it "does not retry a connection lost mid-wait" do
          allow(stale_conn).to receive(:wait_for_notify)
            .and_raise(PG::ConnectionBad, "server closed the connection unexpectedly")

          expect { client.wait_for_notify("default", timeout: 5) }.to raise_error(PG::ConnectionBad)
          expect(Pgbus::DedicatedConnection).to have_received(:connect).once
        end
      end

      it "closes the dedicated connection, also when the wait raises" do
        allow(listen_conn).to receive(:wait_for_notify).and_raise(IOError, "gone")

        expect { client.wait_for_notify("default", timeout: 1) }.to raise_error(IOError)
        expect(listen_conn).to have_received(:close)
      end

      context "with the shared-connection Proc path" do
        let(:waiting) { Queue.new }
        let(:release) { Queue.new }

        before do
          config.worker_notify_database_url = "postgres://localhost/pgbus_test"
          allow(config).to receive(:connection_options).and_return(-> { double("raw PG::Connection") })
          blocking_wait = lambda do |*|
            waiting << true
            release.pop
          end
          allow(listen_conn).to receive(:wait_for_notify, &blocking_wait)
          allow(mock_pgmq).to receive(:wait_for_notify, &blocking_wait)
        end

        it "needs a connection of its own when there is no dedicated URL" do
          config.worker_notify_database_url = nil
          allow(Pgbus::DedicatedConnection).to receive(:connect).and_call_original

          expect { client.wait_for_notify("default", timeout: 1) }.to raise_error(Pgbus::ConfigurationError)
          expect(mock_pgmq).not_to have_received(:wait_for_notify)
        end

        it "does not hold the PGMQ mutex while it waits" do
          waiter = Thread.new { client.wait_for_notify("default", timeout: 5) }
          waiting.pop
          locked = client.instance_variable_get(:@pgmq_mutex).locked?
          release << true
          waiter.join

          expect(locked).to be(false)
        end
      end
    end

    describe "#read_grouped" do
      let(:messages) { [build_message_double(msg_id: 1), build_message_double(msg_id: 2)] }

      before do
        allow(mock_pgmq).to receive(:read_grouped).and_return(messages)
      end

      it "delegates to pgmq-ruby read_grouped with full queue name" do
        client.read_grouped("default", qty: 10)

        expect(mock_pgmq).to have_received(:read_grouped).with(
          "pgbus_test_default", vt: config.visibility_timeout, qty: 10
        )
      end

      it "returns messages from pgmq" do
        result = client.read_grouped("default", qty: 5)
        expect(result).to eq(messages)
      end

      it "allows custom visibility timeout" do
        client.read_grouped("default", qty: 5, vt: 60)
        expect(mock_pgmq).to have_received(:read_grouped).with("pgbus_test_default", vt: 60, qty: 5)
      end
    end

    describe "#read_grouped_rr" do
      let(:messages) { [build_message_double(msg_id: 1)] }

      before do
        allow(mock_pgmq).to receive(:read_grouped_rr).and_return(messages)
      end

      it "delegates to pgmq-ruby read_grouped_rr with full queue name" do
        client.read_grouped_rr("default", qty: 10)

        expect(mock_pgmq).to have_received(:read_grouped_rr).with(
          "pgbus_test_default", vt: config.visibility_timeout, qty: 10
        )
      end
    end

    describe "#read_grouped_head" do
      let(:messages) { [build_message_double(msg_id: 1)] }

      before do
        allow(mock_pgmq).to receive(:read_grouped_head).and_return(messages)
      end

      it "delegates to pgmq-ruby read_grouped_head" do
        client.read_grouped_head("default", qty: 5)

        expect(mock_pgmq).to have_received(:read_grouped_head).with(
          "pgbus_test_default", vt: config.visibility_timeout, qty: 5
        )
      end
    end

    describe "#create_fifo_index" do
      before do
        allow(mock_pgmq).to receive(:create_fifo_index).and_return(nil)
      end

      it "delegates to pgmq-ruby with the full queue name" do
        client.create_fifo_index("default")
        expect(mock_pgmq).to have_received(:create_fifo_index).with("pgbus_test_default")
      end
    end

    describe "#create_fifo_indexes_all" do
      before do
        allow(mock_pgmq).to receive(:create_fifo_indexes_all).and_return(nil)
      end

      it "delegates to pgmq-ruby" do
        client.create_fifo_indexes_all
        expect(mock_pgmq).to have_received(:create_fifo_indexes_all)
      end
    end

    describe "#update_notify_insert" do
      before do
        allow(mock_pgmq).to receive(:update_notify_insert).and_return(nil)
      end

      it "delegates with full queue name and throttle interval" do
        client.update_notify_insert("default", throttle_interval_ms: 500)

        expect(mock_pgmq).to have_received(:update_notify_insert).with(
          "pgbus_test_default", throttle_interval_ms: 500
        )
      end
    end

    describe "#list_notify_insert_throttles" do
      let(:throttle_data) do
        [double("NotifyThrottle", queue_name: "pgbus_test_default", throttle_interval_ms: 250, last_notified_at: nil)]
      end

      before do
        allow(mock_pgmq).to receive(:list_notify_insert_throttles).and_return(throttle_data)
      end

      it "delegates to pgmq-ruby" do
        result = client.list_notify_insert_throttles
        expect(result).to eq(throttle_data)
      end
    end

    describe "#convert_archive_partitioned" do
      before do
        allow(mock_pgmq).to receive(:convert_archive_partitioned).and_return(nil)
      end

      it "delegates with full queue name and options" do
        client.convert_archive_partitioned(
          "default",
          partition_interval: "daily",
          retention_interval: "30 days"
        )

        expect(mock_pgmq).to have_received(:convert_archive_partitioned).with(
          "pgbus_test_default",
          partition_interval: "daily",
          retention_interval: "30 days",
          leading_partition: 10
        )
      end

      it "uses default options when not specified" do
        client.convert_archive_partitioned("default")

        expect(mock_pgmq).to have_received(:convert_archive_partitioned).with(
          "pgbus_test_default",
          partition_interval: "10000",
          retention_interval: "100000",
          leading_partition: 10
        )
      end
    end

    describe "FIFO index auto-creation during queue bootstrap" do
      before do
        allow(mock_pgmq).to receive(:create_fifo_index).and_return(nil)
      end

      context "when group_mode is :fifo" do
        before { config.group_mode = :fifo }

        it "creates a FIFO index when ensuring a queue" do
          client.ensure_queue("default")
          expect(mock_pgmq).to have_received(:create_fifo_index).with("pgbus_test_default")
        end
      end

      context "when group_mode is :round_robin" do
        before { config.group_mode = :round_robin }

        it "creates a FIFO index when ensuring a queue" do
          client.ensure_queue("default")
          expect(mock_pgmq).to have_received(:create_fifo_index).with("pgbus_test_default")
        end
      end

      context "when group_mode is nil (default)" do
        it "does not create a FIFO index" do
          client.ensure_queue("default")
          expect(mock_pgmq).not_to have_received(:create_fifo_index)
        end
      end
    end

    describe "Time delay in send_message" do
      let(:scheduled_at) { Time.utc(2026, 6, 14, 12, 0, 0) }

      it "passes Time objects through to pgmq produce" do
        client.send_message("default", { "type" => "test" }, delay: scheduled_at)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default",
          anything,
          headers: nil,
          delay: scheduled_at
        )
      end

      it "passes integer delay as before" do
        client.send_message("default", { "type" => "test" }, delay: 60)

        expect(mock_pgmq).to have_received(:produce).with(
          "pgbus_test_default",
          anything,
          headers: nil,
          delay: 60
        )
      end
    end
  end

  describe "#tune_autovacuum (delegates to pgmq-ruby v0.7+)" do
    subject(:client) do
      allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
      c = described_class.new(config)
      c.instance_variable_set(:@schema_ensured, true)
      c
    end

    before do
      # Stub the class method that loads pgmq so the faked PGMQ::Client stands;
      # a clean per-example stub, unlike stubbing global Kernel#require.
      allow(described_class).to receive(:load_pgmq_gem!)
      stub_const("PGMQ::Client", Class.new do
        def initialize(*args, **kwargs); end
      end)
    end

    let(:config) do
      Pgbus::Configuration.new.tap do |c|
        c.database_url = "postgres://localhost/pgbus_test"
        c.queue_prefix = "pgbus_test"
      end
    end
    let(:mock_pgmq) { build_mock_pgmq }

    it "delegates to pgmq-ruby tune_autovacuum with the physical queue name" do
      physical_queue = config.queue_name("jobs")
      client.send(:tune_autovacuum, physical_queue)
      expect(mock_pgmq).to have_received(:tune_autovacuum).with(physical_queue)
    end

    it "swallows tuning failures (best-effort, never blocks queue use)" do
      allow(mock_pgmq).to receive(:tune_autovacuum).and_raise(StandardError, "boom")
      physical_queue = config.queue_name("jobs")
      expect { client.send(:tune_autovacuum, physical_queue) }.not_to raise_error
    end
  end
end
