# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #619: config.completion_synchronous_commit = false against a real
# Postgres. One pooled connection, so the session state read after a call
# (synchronous_commit, pipeline and transaction status, prepared statements)
# is the connection the completion ran on.
RSpec.describe "Async completion commit (integration)", :integration do
  let(:config) { Pgbus.configuration }
  let(:async_config) do
    config.dup.tap do |c|
      c.pool_size = 1
      c.completion_synchronous_commit = false
    end
  end
  let(:client) { Pgbus::Client.new(async_config) }
  let(:sync_client) { Pgbus::Client.new(config.dup.tap { |c| c.pool_size = 1 }) }
  let(:pgmq) { client.pgmq }
  let(:queue) { "async_commit" }
  let(:full) { config.queue_name(queue) }

  def on_connection(&) = pgmq.with_connection(&)

  # The archive table outlives an example: read only the ids it sent.
  def archived(ids) = present("a", ids)
  def queued(ids) = present("q", ids)

  def present(table, ids)
    ActiveRecord::Base.connection.select_values(
      "SELECT msg_id FROM pgmq.#{table}_#{full} WHERE msg_id IN (#{ids.map { |id| Integer(id) }.join(", ")}) ORDER BY msg_id"
    ).map(&:to_s)
  end

  def send_messages(count) = Array.new(count) { |i| client.send_message(queue, { "n" => i }) }

  before do
    Pgbus::Client::NativeStatements.reset!
    client.ensure_queue(queue)
  end

  after do
    client.close
    sync_client.close
    Pgbus::Client::NativeStatements.reset!
  end

  it "archives and deletes, one and in batches, returning what the synchronous commit returns" do
    ids = send_messages(6)

    expect(client.archive_message(queue, ids[0])).to be(true)
    expect(client.delete_message(queue, ids[1])).to be(true)
    expect(client.archive_batch(queue, ids[2, 2])).to eq(ids[2, 2])
    expect(client.delete_batch(queue, ids[4, 2])).to eq(ids[4, 2])

    expect(archived(ids)).to eq([ids[0], *ids[2, 2]])
    expect(queued(ids)).to be_empty
  end

  it "reports an already-gone message the way the synchronous commit does" do
    ids = send_messages(2)
    client.archive_message(queue, ids[0])
    client.delete_message(queue, ids[1])

    expect([client.archive_message(queue, ids[0]), client.delete_message(queue, ids[1])])
      .to eq([sync_client.archive_message(queue, ids[0]), sync_client.delete_message(queue, ids[1])])
    expect([client.archive_message(queue, ids[0]), client.archive_batch(queue, ids), client.delete_batch(queue, ids)])
      .to eq([false, [], []])
  end

  it "runs the completion as the prepared statement" do
    client.archive_message(queue, send_messages(1).first)

    names = on_connection { |conn| conn.exec("SELECT name FROM pg_prepared_statements").map { |r| r["name"] } }
    expect(names.grep(/\Apgbus_archive_\h{12}\z/).size).to eq(1)
  end

  it "leaves the pooled connection with synchronous_commit on, out of pipeline mode and idle" do
    client.archive_message(queue, send_messages(1).first)

    state = on_connection do |conn|
      [conn.exec("SHOW synchronous_commit").getvalue(0, 0), conn.pipeline_status, conn.transaction_status]
    end
    expect(state).to eq(["on", PG::PQ_PIPELINE_OFF, PG::PQTRANS_IDLE])
  end

  it "leaves a completion inside a caller's transaction to that transaction" do
    id = send_messages(1).first
    on_connection { |conn| conn.exec("BEGIN") }

    expect(client.archive_message(queue, id)).to be(true)
    on_connection { |conn| conn.exec("ROLLBACK") }

    expect(queued([id])).to eq([id])
    expect(archived([id])).to be_empty
  end

  it "raises a failed completion, rolls its transaction back and leaves the connection usable" do
    id = send_messages(1).first
    locker = PG.connect(PGBUS_DATABASE_URL)
    locker.exec("BEGIN")
    locker.exec("SELECT 1 FROM pgmq.q_#{full} WHERE msg_id = #{Integer(id)} FOR UPDATE")
    on_connection { |conn| conn.exec("SET lock_timeout = '50ms'") }

    expect { client.archive_message(queue, id) }.to raise_error(/lock timeout/)

    locker.exec("ROLLBACK")
    state = on_connection { |conn| [conn.pipeline_status, conn.transaction_status] }
    expect(state).to eq([PG::PQ_PIPELINE_OFF, PG::PQTRANS_IDLE])
    expect(client.archive_message(queue, id)).to be(true)
  ensure
    locker&.close
  end

  it "prepares and runs a statement the session lost, then pipelines it again" do
    ids = send_messages(2)
    client.archive_message(queue, ids[0])
    on_connection { |conn| conn.exec("DEALLOCATE ALL") }

    expect(client.archive_message(queue, ids[1])).to be(true)
    expect(archived(ids)).to eq(ids)
    state = on_connection { |conn| [conn.pipeline_status, conn.transaction_status] }
    expect(state).to eq([PG::PQ_PIPELINE_OFF, PG::PQTRANS_IDLE])
  end
end
