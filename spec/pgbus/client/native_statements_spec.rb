# frozen_string_literal: true

require "spec_helper"
require "pg"
require "connection_pool"

RSpec.describe Pgbus::Client::NativeStatements do
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = Pgbus::Client.new(config, schema_ensured: true)
    allow(c).to receive(:tune_autovacuum)
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    c
  end

  let(:message_class) do
    # pgmq-ruby's PGMQ::Message shape: a Data built from a result row.
    Class.new(Data.define(:msg_id, :read_ct, :enqueued_at, :last_read_at, :vt, :message, :headers, :queue_name)) do
      def self.new(row) = super(**members.to_h { |m| [m, row[m.to_s]] })
    end
  end
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.visibility_timeout = 45
    end
  end
  let(:queue) { "pgbus_test_default" }
  let(:mock_pgmq) { build_mock_pgmq }
  let(:raw_conn) { double("PG::Connection", backend_pid: 101, transaction_status: PG::PQTRANS_IDLE) }
  let(:all_columns) do
    %W[q_#{queue} a_#{queue}].flat_map do |table|
      described_class::REQUIRED_COLUMNS.map { |c| { "table_name" => table, "column_name" => c } }
    end
  end
  let(:statements) { described_class.statements_for("q_#{queue}", "a_#{queue}") }
  let(:read_row) do
    { "msg_id" => "7", "read_ct" => "1", "enqueued_at" => "2026-10-29 10:00:00+00", "last_read_at" => "2026-10-29 10:00:01+00",
      "vt" => "2026-10-29 10:00:46+00", "message" => "{}", "headers" => nil }
  end

  def pg_result(rows)
    double("PG::Result", to_a: rows, ntuples: rows.size).tap do |r|
      allow(r).to receive(:[]) { |i| rows[i] }
      allow(r).to receive(:map) { |&blk| rows.map(&blk) }
    end
  end

  def pg_error(klass)
    klass.new("boom")
  end

  before do
    described_class.reset!
    real_pgmq_connection_error
    allow(Pgbus::Client).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new { def initialize(*, **); end })
    stub_const("PGMQ::Message", message_class)
    allow(mock_pgmq).to receive(:with_connection).and_yield(raw_conn)
    allow(raw_conn).to receive(:exec_params)
      .with(described_class::COLUMNS_SQL, ["q_#{queue}", "a_#{queue}"]).and_return(pg_result(all_columns))
    allow(raw_conn).to receive(:exec_prepared).and_return(pg_result([]))
    allow(raw_conn).to receive(:prepare)
  end

  after { described_class.reset! }

  describe ".statements_for" do
    # Pinned so a drift from pgmq 1.13.1's function bodies is a visible diff.
    it "builds pgmq.read's statement against the queue table" do
      expect(statements[:read].sql).to eq(
        'WITH cte AS (SELECT msg_id FROM pgmq."q_pgbus_test_default" WHERE vt <= clock_timestamp() ' \
        "ORDER BY msg_id ASC LIMIT $1::integer FOR UPDATE SKIP LOCKED) " \
        'UPDATE pgmq."q_pgbus_test_default" m SET last_read_at = clock_timestamp(), ' \
        "vt = clock_timestamp() + make_interval(secs => $2::integer), read_ct = read_ct + 1 " \
        "FROM cte WHERE m.msg_id = cte.msg_id " \
        "RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers"
      )
    end

    it "builds pgmq.archive's move into the archive table, single and batch" do
      move = "RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers) " \
             'INSERT INTO pgmq."a_pgbus_test_default" (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers) ' \
             "SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived RETURNING msg_id"
      expect(statements[:archive].sql).to eq(
        "WITH archived AS (DELETE FROM pgmq.\"q_pgbus_test_default\" WHERE msg_id = $1::bigint #{move}"
      )
      expect(statements[:archive_batch].sql).to eq(
        "WITH archived AS (DELETE FROM pgmq.\"q_pgbus_test_default\" WHERE msg_id = ANY($1::bigint[]) #{move}"
      )
    end

    it "builds pgmq.delete, single and batch" do
      expect(statements[:delete].sql).to eq('DELETE FROM pgmq."q_pgbus_test_default" WHERE msg_id = $1::bigint RETURNING msg_id')
      expect(statements[:delete_batch].sql)
        .to eq('DELETE FROM pgmq."q_pgbus_test_default" WHERE msg_id = ANY($1::bigint[]) RETURNING msg_id')
    end

    it "builds pgmq.send and pgmq._send_batch as INSERTs, so the table's NOTIFY trigger still fires" do
      expect(statements[:send].sql).to eq(
        'INSERT INTO pgmq."q_pgbus_test_default" (vt, message, headers) ' \
        "VALUES (clock_timestamp() + make_interval(secs => $3::integer), $1::jsonb, $2::jsonb) RETURNING msg_id"
      )
      expect(statements[:send_batch].sql).to eq(
        'INSERT INTO pgmq."q_pgbus_test_default" (vt, message, headers) ' \
        "SELECT d.vt, unnest($1::jsonb[]), unnest(coalesce($2::jsonb[], ARRAY[]::jsonb[])) " \
        "FROM (SELECT clock_timestamp() + make_interval(secs => $3::integer) AS vt) d RETURNING msg_id"
      )
    end

    it "names each statement pgbus_<op>_<12 hex>, inside Postgres's 63-byte identifier limit" do
      statements.each do |op, statement|
        expect(statement.name).to match(/\Apgbus_#{op}_\h{12}\z/)
        expect(statement.name.bytesize).to be <= 63
      end
      expect(statements.values.map(&:name).uniq.size).to eq(7)
    end

    it "names another queue's statements differently" do
      other = described_class.statements_for("q_pgbus_test_other", "a_pgbus_test_other")
      expect(other[:read].name).not_to eq(statements[:read].name)
    end

    it "quotes the table name as an identifier" do
      odd = described_class.statements_for('q_a"b', 'a_a"b')
      expect(odd[:delete].sql).to start_with('DELETE FROM pgmq."q_a""b"')
    end
  end

  describe "#read_batch" do
    before { allow(raw_conn).to receive(:exec_prepared).with(statements[:read].name, [5, 45]).and_return(pg_result([read_row])) }

    it "runs the prepared read with [qty, vt] and returns Messages with String fields" do
      messages = client.read_batch("default", qty: 5)

      expect(messages.map(&:class)).to eq([message_class])
      expect(messages.first.to_h).to eq(read_row.transform_keys(&:to_sym).merge(queue_name: nil))
      expect(mock_pgmq).not_to have_received(:read_batch)
    end

    it "honours an explicit vt" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:read].name, [2, 10]).and_return(pg_result([]))

      expect(client.read_batch("default", qty: 2, vt: 10)).to eq([])
    end

    it "checks the queue's columns once per process" do
      2.times { client.read_batch("default", qty: 5) }

      expect(raw_conn).to have_received(:exec_params).with(described_class::COLUMNS_SQL, anything).once
    end

    it "prepares on first use, then runs it again" do
      calls = 0
      allow(raw_conn).to receive(:exec_prepared).with(statements[:read].name, [5, 45]) do
        calls += 1
        raise pg_error(PG::InvalidSqlStatementName) if calls == 1

        pg_result([read_row])
      end

      expect(client.read_batch("default", qty: 5).size).to eq(1)
      expect(raw_conn).to have_received(:prepare).with(statements[:read].name, statements[:read].sql).once
    end

    it "runs the statement when another call prepared it in between" do
      calls = 0
      allow(raw_conn).to receive(:exec_prepared).with(statements[:read].name, [5, 45]) do
        calls += 1
        raise pg_error(PG::InvalidSqlStatementName) if calls == 1

        pg_result([read_row])
      end
      allow(raw_conn).to receive(:prepare).and_raise(pg_error(PG::DuplicatePstatement))

      expect(client.read_batch("default", qty: 5).size).to eq(1)
    end

    it "sends the SQL unprepared inside an open transaction, so a missing statement cannot abort it" do
      allow(raw_conn).to receive(:transaction_status).and_return(PG::PQTRANS_INTRANS)
      allow(raw_conn).to receive(:exec_params).with(statements[:read].sql, [5, 45]).and_return(pg_result([read_row]))

      expect(client.read_batch("default", qty: 5).size).to eq(1)
      expect(raw_conn).not_to have_received(:exec_prepared)
      expect(raw_conn).not_to have_received(:prepare)
    end
  end

  describe "the switch to unprepared SQL behind a transaction-mode pooler" do
    let(:lost) { pg_error(PG::InvalidSqlStatementName) }

    # exec_prepared misses once per op, then succeeds on the retry after prepare.
    def miss_once_per_call
      seen = Hash.new(0)
      allow(raw_conn).to receive(:exec_prepared) do |name, _params|
        seen[name] += 1
        raise lost if seen[name].odd?

        pg_result([])
      end
    end

    it "does not count a first prepare on each backend" do
      miss_once_per_call
      pids = [101, 102, 103, 104].each
      allow(raw_conn).to receive(:backend_pid) { pids.next }

      4.times { client.delete_message("default", 1) }

      expect(described_class).not_to be_unprepared
    end

    it "switches after #{described_class::LOST_STATEMENT_LIMIT} statements disappear in a row from a backend that had them" do
      miss_once_per_call
      allow(Pgbus.logger).to receive(:warn)

      # First call prepares (not counted); the next three find it gone.
      4.times { client.delete_message("default", 1) }

      expect(described_class).to be_unprepared
      expect(Pgbus.logger).to have_received(:warn).once
    end

    it "resets the count on a statement that runs prepared" do
      miss_once_per_call
      client.delete_message("default", 1)
      2.times { client.delete_message("default", 1) }
      allow(raw_conn).to receive(:exec_prepared).and_return(pg_result([]))
      client.delete_message("default", 1)
      miss_once_per_call
      2.times { client.delete_message("default", 1) }

      expect(described_class).not_to be_unprepared
    end

    it "switches at once when a statement is gone right after its own prepare" do
      allow(raw_conn).to receive(:exec_prepared).and_raise(lost)
      allow(raw_conn).to receive(:exec_params).with(statements[:delete].sql, [1]).and_return(pg_result([{ "msg_id" => "1" }]))
      allow(Pgbus.logger).to receive(:warn)

      expect(client.delete_message("default", 1)).to be(true)
      expect(described_class).to be_unprepared
    end

    it "sends every later statement unprepared" do
      described_class.unprepare!("test")
      allow(raw_conn).to receive(:exec_params).with(statements[:archive].sql, [3]).and_return(pg_result([{ "msg_id" => "3" }]))

      expect(client.archive_message("default", 3)).to be(true)
      expect(raw_conn).not_to have_received(:exec_prepared)
    end
  end

  describe "return shapes" do
    it "archive_message is true when the row moved and false when it was already gone" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:archive].name, [9]).and_return(pg_result([{ "msg_id" => "9" }]))
      allow(raw_conn).to receive(:exec_prepared).with(statements[:archive].name, [10]).and_return(pg_result([]))

      expect(client.archive_message("default", 9)).to be(true)
      expect(client.archive_message("default", 10)).to be(false)
    end

    it "archive_batch returns the ids it moved, as Strings, with the ids array-encoded" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:archive_batch].name, ["{1,2,3}"])
                                                .and_return(pg_result([{ "msg_id" => "1" }, { "msg_id" => "3" }]))

      expect(client.archive_batch("default", [1, 2, 3])).to eq(%w[1 3])
    end

    it "delete_message and delete_batch mirror archive" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:delete].name, [4]).and_return(pg_result([]))
      allow(raw_conn).to receive(:exec_prepared).with(statements[:delete_batch].name, ["{4,5}"])
                                                .and_return(pg_result([{ "msg_id" => "5" }]))

      expect(client.delete_message("default", 4)).to be(false)
      expect(client.delete_batch("default", [4, 5])).to eq(%w[5])
    end

    it "accepts an already physical queue name" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:delete].name, [4]).and_return(pg_result([{ "msg_id" => "4" }]))

      expect(client.delete_message(queue, 4, prefixed: false)).to be(true)
    end

    it "send_message returns the new id as a String, passing message, headers and delay" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:send].name, ['{"a":1}', '{"h":2}', 5])
                                                .and_return(pg_result([{ "msg_id" => "11" }]))

      expect(client.send_message("default", { "a" => 1 }, headers: { "h" => 2 }, delay: 5)).to eq("11")
    end

    it "send_message passes NULL headers when there are none" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:send].name, ['{"a":1}', nil, 0])
                                                .and_return(pg_result([{ "msg_id" => "12" }]))

      expect(client.send_message("default", { "a" => 1 })).to eq("12")
    end

    it "send_batch array-encodes payloads and headers and returns the ids" do
      allow(raw_conn).to receive(:exec_prepared)
        .with(statements[:send_batch].name, ['{"{\"a\":1}","{\"a\":2}"}', '{"{\"h\":1}",NULL}', 0])
        .and_return(pg_result([{ "msg_id" => "1" }, { "msg_id" => "2" }]))

      expect(client.send_batch("default", [{ "a" => 1 }, { "a" => 2 }], headers: [{ "h" => 1 }, nil])).to eq(%w[1 2])
    end

    it "send_batch passes NULL headers when there are none" do
      allow(raw_conn).to receive(:exec_prepared)
        .with(statements[:send_batch].name, ['{"{\"a\":1}"}', nil, 3]).and_return(pg_result([{ "msg_id" => "1" }]))

      expect(client.send_batch("default", [{ "a" => 1 }], delay: 3)).to eq(%w[1])
    end
  end

  describe "the pgmq function path" do
    it "is used everywhere when native_statements is false" do
      config.native_statements = false

      client.read_batch("default", qty: 5)
      client.archive_message("default", 1)
      client.send_message("default", { "a" => 1 })

      expect(mock_pgmq).to have_received(:read_batch).with(queue, vt: 45, qty: 5)
      expect(mock_pgmq).to have_received(:archive).with(queue, 1)
      expect(mock_pgmq).to have_received(:produce)
      expect(raw_conn).not_to have_received(:exec_params)
      expect(raw_conn).not_to have_received(:exec_prepared)
    end

    it "is used, with one info log, for a queue whose table lacks a column" do
      allow(raw_conn).to receive(:exec_params).with(described_class::COLUMNS_SQL, anything)
                                              .and_return(pg_result(all_columns.reject { |r| r["column_name"] == "headers" }))
      allow(Pgbus.logger).to receive(:info)

      2.times { client.archive_message("default", 1) }

      expect(mock_pgmq).to have_received(:archive).twice
      expect(raw_conn).not_to have_received(:exec_prepared)
      expect(Pgbus.logger).to have_received(:info).once
      expect(raw_conn).to have_received(:exec_params).with(described_class::COLUMNS_SQL, anything).once
    end

    it "is used, and the check repeated, while the queue table does not exist yet" do
      allow(raw_conn).to receive(:exec_params).with(described_class::COLUMNS_SQL, anything).and_return(pg_result([]))

      2.times { client.read_batch("default", qty: 1) }

      expect(mock_pgmq).to have_received(:read_batch).twice
      expect(raw_conn).to have_received(:exec_params).with(described_class::COLUMNS_SQL, anything).twice
    end

    it "is used for a Time delay, an empty batch and a headers/payload length mismatch" do
      at = Time.now + 60
      client.send_message("default", { "a" => 1 }, delay: at)
      client.send_batch("default", [])
      client.send_batch("default", [{ "a" => 1 }], headers: [{}, {}])
      client.archive_batch("default", [])
      client.delete_batch("default", [])

      expect(mock_pgmq).to have_received(:produce).with(queue, '{"a":1}', headers: nil, delay: at)
      expect(mock_pgmq).to have_received(:produce_batch).twice
      expect(mock_pgmq).to have_received(:archive_batch).with(queue, [])
      expect(mock_pgmq).to have_received(:delete_batch).with(queue, [])
      expect(raw_conn).not_to have_received(:exec_prepared)
    end
  end

  describe "#native_ready?" do
    it "checks the queue's columns on first call, so a queue created by another process is covered" do
      expect(client.native_ready?(queue)).to be(true)
      expect(raw_conn).to have_received(:exec_params).with(described_class::COLUMNS_SQL, ["q_#{queue}", "a_#{queue}"])
    end

    it "is false without a query when native_statements is false" do
      config.native_statements = false

      expect(client.native_ready?(queue)).to be(false)
      expect(raw_conn).not_to have_received(:exec_params)
    end

    it "does not check at queue creation" do
      client.ensure_queue("default")

      expect(raw_conn).not_to have_received(:exec_params).with(described_class::COLUMNS_SQL, anything)
    end
  end

  # The native call runs inside @pgmq.with_connection, so a dropped connection
  # surfaces exactly as on the function path: pgmq-ruby's Connection rescues
  # PG::Error, retries a lost connection once and raises
  # PGMQ::Errors::ConnectionError — the class every guard keys on. These
  # examples run pgmq-ruby's real PGMQ::Connection over the stubbed raw_conn.
  describe "error classes" do
    let(:connection_error) { real_pgmq_connection_error }
    let(:real_connection) do
      load(Gem.find_files("pgmq/connection.rb").first) unless defined?(PGMQ::Connection)
      PGMQ::Connection.new(-> { raw_conn }, pool_size: 1, pool_timeout: 1)
    end
    let(:dropped) { PG::ConnectionBad.new("server closed the connection unexpectedly") }

    before do
      connection_error
      allow(raw_conn).to receive_messages(finished?: false, status: PG::CONNECTION_OK, reset: nil)
      allow(mock_pgmq).to receive(:with_connection) { |&blk| real_connection.with_connection(&blk) }
      client.native_ready?(queue)
      allow(client).to receive(:sleep)
    end

    it "retries a dropped connection once inside pgmq-ruby, as the function path does" do
      calls = 0
      allow(raw_conn).to receive(:exec_prepared).with(statements[:archive].name, [9]) do
        calls += 1
        raise dropped if calls == 1

        pg_result([{ "msg_id" => "9" }])
      end

      expect(client.archive_message("default", 9)).to be(true)
      expect(calls).to eq(2)
    end

    it "raises PGMQ::Errors::ConnectionError, which with_stale_connection_retry retries" do
      calls = 0
      allow(raw_conn).to receive(:exec_prepared).with(statements[:send].name, anything) do
        calls += 1
        raise PG::ConnectionBad, "connection is closed" if calls <= 2

        pg_result([{ "msg_id" => "1" }])
      end

      expect(client.send_message("default", { "a" => 1 })).to eq("1")
      expect(calls).to eq(3) # pgmq-ruby's own retry, then one stale retry
      expect(client).to have_received(:sleep).once
    end

    it "reaches the executor's archive retry as the class it rescues" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:archive].name, [9]).and_raise(dropped)

      expect { client.archive_message("default", 9) }.to raise_error(connection_error, /server closed the connection/)

      executor = Pgbus::ActiveJob::Executor.new(client: client, config: config)
      allow(Pgbus.logger).to receive(:warn)
      expect { executor.send(:archive_from, "default", 9) }.to raise_error(connection_error)
      expect(Pgbus.logger).to have_received(:warn).with(no_args).once
    end

    it "counts toward guarded_read's breaker like a function-path failure" do
      allow(raw_conn).to receive(:exec_prepared).with(statements[:read].name, anything).and_raise(dropped)

      Pgbus::Client::ConnectionHealth::OPEN_THRESHOLD.times do
        expect { client.read_batch("default", qty: 1) }.to raise_error(connection_error)
      end

      expect(client.connection_health).to be_open
      expect { client.read_batch("default", qty: 1) }.to raise_error(Pgbus::ConnectionCircuitOpenError)
    end
  end
end
