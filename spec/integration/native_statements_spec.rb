# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #605: the native statements against a real pgmq install, checked
# against pgmq's own functions (client.pgmq.*) on the same queue.
RSpec.describe "Native statements (integration)", :integration do
  let(:config) { Pgbus.configuration }
  # One pooled connection, so every operation and every pg_prepared_statements
  # read below are on the same session: prepared statements and their plan
  # counters live per session.
  let(:single_connection_config) { config.dup.tap { |c| c.pool_size = 1 } }
  let(:client) { Pgbus::Client.new(single_connection_config) }
  let(:pgmq) { client.pgmq }
  let(:queue) { "native" }
  let(:full) { config.queue_name(queue) }

  def prepared_statement_names
    pgmq.with_connection { |conn| conn.exec("SELECT name FROM pg_prepared_statements").map { |r| r["name"] } }
  end

  def row(table, msg_id)
    ActiveRecord::Base.connection.select_one(<<~SQL)
      SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers,
             EXTRACT(EPOCH FROM (vt - clock_timestamp())) AS vt_in
      FROM pgmq.#{table}_#{full} WHERE msg_id = #{Integer(msg_id)}
    SQL
  end

  before do
    Pgbus::Client::NativeStatements.reset!
    client.ensure_queue(queue)
  end

  after do
    client.close
    Pgbus::Client::NativeStatements.reset!
  end

  it "detects a queue pgmq created as native-ready" do
    expect(client.native_ready?(full)).to be(true)
  end

  it "prepares each statement once on the pooled connection" do
    client.send_message(queue, { "a" => 1 })
    client.send_message(queue, { "a" => 2 })

    expect(prepared_statement_names.grep(/\Apgbus_send_\h{12}\z/).size).to eq(1)
  end

  describe "send" do
    it "stores what pgmq.send stores and returns the id as a String" do
      native_id = client.send_message(queue, { "a" => 1 }, headers: { "h" => "x" })
      function_id = pgmq.produce(full, '{"a": 1}', headers: '{"h": "x"}')

      expect(native_id).to be_a(String)
      native = row("q", native_id)
      function = row("q", function_id)
      expect(native.values_at("read_ct", "last_read_at", "message", "headers"))
        .to eq(function.values_at("read_ct", "last_read_at", "message", "headers"))
      expect(JSON.parse(native["headers"])).to eq("h" => "x")
    end

    it "keeps a delayed message invisible until its delay" do
      id = client.send_message(queue, { "a" => 1 }, delay: 30)

      expect(row("q", id)["vt_in"].to_f).to be_within(2).of(30)
      expect(pgmq.read_batch(full, vt: 30, qty: 10)).to be_empty
    end

    it "fires the queue's NOTIFY insert trigger" do
      config.listen_notify = true
      Pgbus.reset_client!
      Pgbus.client.ensure_queue("native_notify")
      channel = "pgmq.q_#{config.queue_name("native_notify")}.INSERT"
      listener = build_pg_listen_connection
      listener.exec("LISTEN #{listener.quote_ident(channel)}")

      Pgbus.client.send_message("native_notify", { "a" => 1 })

      heard = listener.wait_for_notify(5) { |ch, *| ch }
      expect(heard).to eq(channel)
    ensure
      config.listen_notify = false
      listener&.close
    end

    it "batch-sends with and without headers, ids in payload order" do
      with = client.send_batch(queue, [{ "n" => 1 }, { "n" => 2 }], headers: [{ "h" => 1 }, nil])
      without = client.send_batch(queue, [{ "n" => 3 }], delay: 30)

      expect(with.map(&:class).uniq).to eq([String])
      expect(with.map(&:to_i)).to eq(with.map(&:to_i).sort)
      expect([with, without].flatten.map { |id| JSON.parse(row("q", id)["message"])["n"] }).to eq([1, 2, 3])
      expect(with.map { |id| row("q", id)["headers"] }).to eq(['{"h": 1}', nil])
      expect(row("q", without.first)["vt_in"].to_f).to be_within(2).of(30)
    end
  end

  describe "read" do
    it "claims what pgmq.send wrote: read_ct, vt and last_read_at advance as pgmq.read's do" do
      id = pgmq.produce(full, '{"a": 1}', headers: '{"h": 1}')

      messages = client.read_batch(queue, qty: 5, vt: 30)

      expect(messages.map(&:class)).to eq([PGMQ::Message])
      message = messages.first
      expect([message.msg_id, message.read_ct, message.message, message.headers]).to eq([id, "1", '{"a": 1}', '{"h": 1}'])
      expect(message.last_read_at).not_to be_nil
      expect(row("q", id)["vt_in"].to_f).to be_within(2).of(30)
      expect(client.read_batch(queue, qty: 5, vt: 30)).to be_empty
    end

    it "is read by pgmq.read after a native send, and back, with read_ct counting both" do
      id = client.send_message(queue, { "a" => 1 })

      expect(pgmq.read_batch(full, vt: 0, qty: 1).map(&:read_ct)).to eq(["1"])
      expect(client.read_batch(queue, qty: 1, vt: 0).map { |m| [m.msg_id, m.read_ct] }).to eq([[id, "2"]])
      expect(pgmq.read_batch(full, vt: 0, qty: 1).map(&:read_ct)).to eq(["3"])
    end

    # With LIMIT $1 the generic plan hash-joins a full scan, so Postgres would
    # plan every read afresh; the literal-LIMIT read settles on the generic plan.
    it "runs a read on its cached generic plan once Postgres has seen it a few times" do
      10.times { client.read_batch(queue, qty: 3) }

      plans = pgmq.with_connection do |conn|
        conn.exec("SELECT generic_plans FROM pg_prepared_statements WHERE name LIKE 'pgbus_read_%'").map { |r| r["generic_plans"].to_i }
      end
      expect(plans.sum).to be_positive
    end

    it "reads more than the literal-LIMIT reads cover through LIMIT $1" do
      qty = Pgbus::Client::NativeStatements::LITERAL_READ_LIMIT + 1
      ids = Array.new(2) { client.send_message(queue, { "a" => 1 }) }

      expect(client.read_batch(queue, qty: qty).map(&:msg_id)).to eq(ids)
    end

    it "reads oldest first, up to qty" do
      ids = Array.new(3) { |i| client.send_message(queue, { "i" => i }) }

      expect(client.read_batch(queue, qty: 2).map(&:msg_id)).to eq(ids.first(2))
    end
  end

  describe "archive" do
    it "moves every column into the archive table, as pgmq.archive does" do
      native_id = client.send_message(queue, { "a" => 1 }, headers: { "h" => 1 })
      function_id = client.send_message(queue, { "a" => 1 }, headers: { "h" => 1 })
      client.read_batch(queue, qty: 2, vt: 0)
      queued = row("q", native_id)

      expect(client.archive_message(queue, native_id)).to be(true)
      expect(pgmq.archive(full, function_id)).to be(true)

      archived = row("a", native_id)
      expect(row("q", native_id)).to be_nil
      expect(archived.except("vt_in")).to eq(queued.except("vt_in"))
      expect(archived.except("msg_id", "enqueued_at", "vt", "last_read_at", "vt_in"))
        .to eq(row("a", function_id).except("msg_id", "enqueued_at", "vt", "last_read_at", "vt_in"))
    end

    it "returns false for an id that is already archived" do
      id = client.send_message(queue, { "a" => 1 })
      client.archive_message(queue, id)

      expect(client.archive_message(queue, id)).to be(false)
    end

    it "batch-archives and returns only the ids it moved" do
      ids = Array.new(2) { client.send_message(queue, { "a" => 1 }) }
      client.archive_message(queue, ids.first)

      expect(client.archive_batch(queue, ids + ["999999999"])).to eq([ids.last])
    end
  end

  describe "delete" do
    it "deletes one, false when gone, and a batch returning the ids it removed" do
      ids = Array.new(3) { client.send_message(queue, { "a" => 1 }) }

      expect(client.delete_message(queue, ids[0])).to be(true)
      expect(client.delete_message(queue, ids[0])).to be(false)
      expect(client.delete_batch(queue, ids)).to match_array(ids.drop(1))
      expect(row("q", ids[2])).to be_nil
    end
  end

  it "runs unprepared inside an open transaction, leaving the transaction usable" do
    fresh = Pgbus::Client.new(single_connection_config)
    begin
      fresh.ensure_queue(queue)
      inner = nil
      id = fresh.pgmq.with_connection do |conn|
        conn.transaction do
          # connection_pool hands a thread the connection it already holds,
          # so the send below runs on conn, inside its open transaction.
          fresh.pgmq.with_connection { |c| inner = c }
          expect(inner).to equal(conn)
          sent = fresh.send_message(queue, { "a" => 1 })
          conn.exec("SELECT 1")
          sent
        end
      end

      expect(row("q", id)).not_to be_nil
      expect(fresh.pgmq.with_connection { |c| c.exec("SELECT name FROM pg_prepared_statements").ntuples }).to eq(0)
    ensure
      fresh.close
    end
  end

  it "re-prepares on the new backend after the connection is dropped" do
    id = client.send_message(queue, { "a" => 1 })
    old_pid = pgmq.with_connection(&:backend_pid)
    ActiveRecord::Base.connection.execute("SELECT pg_terminate_backend(#{Integer(old_pid)})")

    expect(client.archive_message(queue, id)).to be(true)
    expect(pgmq.with_connection(&:backend_pid)).not_to eq(old_pid)
    expect(Pgbus::Client::NativeStatements).not_to be_unprepared
  end

  context "with a queue table that lacks the headers column" do
    let(:legacy) { config.queue_name("native_legacy") }

    before do
      conn = ActiveRecord::Base.connection
      %w[q a].each do |prefix|
        conn.execute("DROP TABLE IF EXISTS pgmq.#{prefix}_#{legacy}")
        conn.execute(<<~SQL)
          CREATE TABLE pgmq.#{prefix}_#{legacy} (
            msg_id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY, read_ct INT NOT NULL DEFAULT 0,
            enqueued_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_read_at TIMESTAMPTZ,
            vt TIMESTAMPTZ NOT NULL, message JSONB
          )
        SQL
      end
      conn.execute("INSERT INTO pgmq.q_#{legacy} (vt, message) VALUES (now(), '{}')")
    end

    after do
      %w[q a].each { |prefix| ActiveRecord::Base.connection.execute("DROP TABLE IF EXISTS pgmq.#{prefix}_#{legacy}") }
    end

    it "stays on the pgmq functions, which still work on it" do
      expect(client.native_ready?(legacy)).to be(false)

      expect(client.delete_message(legacy, 1, prefixed: false)).to be(true)
      expect(prepared_statement_names.grep(/\Apgbus_delete_/)).to be_empty
    end
  end

  context "with native_statements = false" do
    around do |example|
      config.native_statements = false
      Pgbus.reset_client!
      example.run
    ensure
      config.native_statements = true
    end

    it "runs the whole lifecycle on the pgmq functions" do
      id = client.send_message(queue, { "a" => 1 }, headers: { "h" => 1 })
      client.send_batch(queue, [{ "a" => 2 }])
      read = client.read_batch(queue, qty: 5, vt: 30)

      expect(read.map(&:msg_id)).to include(id)
      expect(client.archive_message(queue, id)).to be(true)
      expect(client.delete_batch(queue, read.map(&:msg_id))).to eq(read.map(&:msg_id) - [id])
      expect(prepared_statement_names.grep(/\Apgbus_/)).to be_empty
    end
  end
end
