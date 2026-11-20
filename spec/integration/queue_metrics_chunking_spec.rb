# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #677: Web::DataSource#queues_with_metrics reads every queue's metrics
# in chunks, one scan per queue table, each chunk in its own savepoint, and a
# failed chunk marks only its own queues unavailable.
RSpec.describe "Queue metrics chunking (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:conn) { ActiveRecord::Base.connection }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue_name) { client.config.queue_name("metrics_677") }

  # The ghost row is written to pgmq.meta by hand on purpose: a queue whose
  # table is gone is the broken state under test. Its name is outside
  # pgbus_%, so the suite's per-example purge never walks it.
  let(:ghost) { "zz_ghost_677" }

  def metrics_of(name) = data_source.queues_with_metrics.find { |q| q[:name] == name }

  # Errors scoped to the queues an example made, so a stray broken queue left
  # in a shared database cannot fail it.
  def errors_naming(names) = data_source.queue_metrics_errors.select { |e| e[:queues].intersect?(names) }

  def statement_names(&block)
    names = []
    callback = ->(*, payload) { names << payload[:name] }
    ActiveSupport::Notifications.subscribed(callback, "sql.active_record", &block)
    names
  end

  before do
    client.ensure_queue("metrics_677")
    client.purge_queue("metrics_677")
  end

  after do
    conn.execute("DELETE FROM pgmq.meta WHERE queue_name = #{conn.quote(ghost)}")
    allow(Pgbus.configuration).to receive(:web_queue_metrics_chunk_size).and_call_original
  end

  it "reads the same values the single-queue detail does, for visible, parked and read messages" do
    client.send_message("metrics_677", { "visible" => true })
    client.send_message("metrics_677", { "read" => true })
    client.read_batch("metrics_677", qty: 1, vt: 0)
    client.send_message("metrics_677", { "parked" => true }, delay: 3600)

    batched = metrics_of(queue_name)
    detail = data_source.queue_detail(queue_name)

    expect(batched.except(:paused)).to eq(detail)
    # total_messages is the msg_id sequence, which no purge resets: the eq
    # above pins it against the detail read.
    expect(batched).to include(queue_length: 3, queue_visible_length: 2, parked_length: 1,
                               max_read_ct: 1, visible_unread_length: 1)
    expect(batched).not_to have_key(:metrics_unavailable)
  end

  it "reports every queue across chunk boundaries" do
    allow(Pgbus.configuration).to receive(:web_queue_metrics_chunk_size).and_return(1)
    names = conn.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")

    expect(data_source.queues_with_metrics.map { |q| q[:name] }).to eq(names)
    expect(metrics_of(queue_name)).not_to have_key(:metrics_unavailable)
    expect(errors_naming([queue_name])).to eq([])
  end

  context "with a queue whose table is gone" do
    before do
      conn.execute("INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged) VALUES (#{conn.quote(ghost)}, false, false)")
      allow(Pgbus.configuration).to receive(:web_queue_metrics_chunk_size).and_return(1)
      client.send_message("metrics_677", { "a" => 1 })
    end

    it "marks only that queue unavailable and names the error" do
      expect(metrics_of(ghost)).to include(metrics_unavailable: true, queue_length: nil)
      expect(metrics_of(queue_name)).to include(queue_length: 1)
      expect(metrics_of(queue_name)).not_to have_key(:metrics_unavailable)
      expect(errors_naming([ghost, queue_name])).to contain_exactly(
        a_hash_including(queues: [ghost], error: a_string_including("ActiveRecord::StatementInvalid", "q_#{ghost}"))
      )
    end

    it "leaves the caller's open transaction usable" do
      after_read = nil
      ActiveRecord::Base.transaction do
        data_source.queues_with_metrics
        after_read = conn.select_value("SELECT 42")
      end

      expect(after_read).to eq(42)
      expect(metrics_of(ghost)).to include(metrics_unavailable: true)
    end
  end

  it "runs no metrics statement for the job list or a dead-letter lookup" do
    names = statement_names do
      data_source.job_rows
      data_source.dlq_message_detail(1)
    end

    expect(names).to include("Pgbus Job List")
    expect(names).not_to include("Pgbus Batched Queue Metrics")
  end

  describe "1 000 queues under a 5 s statement_timeout" do
    let(:prefix) { "qm677" }
    let(:count) { 1_000 }

    let(:physical_prefix) { client.config.queue_name(prefix) }

    before do
      conn.execute("SET client_min_messages = warning")
      count.times do |i|
        name = format("#{prefix}_%04d", i)
        client.ensure_queue(name)
        client.send_message(name, {})
      end
    end

    after do
      conn.execute("RESET statement_timeout")
      # Matched in Ruby, not with LIKE: the prefix's own underscores would be wildcards.
      conn.select_values("SELECT queue_name FROM pgmq.meta").each do |name|
        client.drop_queue(name, prefixed: false) if name.start_with?("#{physical_prefix}_")
      end
      conn.execute("RESET client_min_messages")
    end

    it "reports every queue with its metrics" do
      conn.execute("SET statement_timeout = '5s'")

      queues = data_source.queues_with_metrics.select { |q| q[:name].start_with?("#{physical_prefix}_") }

      expect(errors_naming(queues.map { |q| q[:name] })).to eq([])
      expect(queues.size).to eq(count)
      expect(queues.count { |q| q[:metrics_unavailable] }).to eq(0)
      expect(queues.map { |q| q[:queue_length] }.uniq).to eq([1])
    end
  end
end
