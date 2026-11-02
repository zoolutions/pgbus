# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

RSpec.describe "StatBuffer integration", :integration do
  before do
    conn = ActiveRecord::Base.connection

    unless conn.table_exists?("pgbus_job_stats")
      conn.execute(<<~SQL)
        CREATE TABLE pgbus_job_stats (
          id BIGSERIAL PRIMARY KEY,
          job_class VARCHAR NOT NULL,
          queue_name VARCHAR NOT NULL,
          status VARCHAR NOT NULL,
          duration_ms INTEGER NOT NULL DEFAULT 0,
          enqueue_latency_ms BIGINT,
          retry_count INTEGER DEFAULT 0,
          created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
      SQL
    end

    # Ensure latency columns exist (may be missing if only base migration ran)
    unless conn.column_exists?(:pgbus_job_stats, :enqueue_latency_ms)
      conn.execute("ALTER TABLE pgbus_job_stats ADD COLUMN enqueue_latency_ms BIGINT")
    end
    unless conn.column_exists?(:pgbus_job_stats, :retry_count)
      conn.execute("ALTER TABLE pgbus_job_stats ADD COLUMN retry_count INTEGER DEFAULT 0")
    end

    conn.execute("DELETE FROM pgbus_job_stats")

    # Clear memoized flags and schema cache so they're re-evaluated against the real DB
    Pgbus::JobStat.remove_instance_variable(:@table_exists) if Pgbus::JobStat.instance_variable_defined?(:@table_exists)
    Pgbus::JobStat.remove_instance_variable(:@latency_columns) if Pgbus::JobStat.instance_variable_defined?(:@latency_columns)
    Pgbus::JobStat.reset_column_information
  end

  let(:stat_attrs) do
    {
      job_class: "TestJob",
      queue_name: "default",
      status: "success",
      duration_ms: 42,
      enqueue_latency_ms: 150,
      retry_count: 0
    }
  end

  describe "StatBuffer#flush inserts rows into pgbus_job_stats" do
    it "persists buffered stats to the real database" do
      buffer = Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60)

      3.times { buffer.push(stat_attrs) }
      buffer.flush

      count = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pgbus_job_stats")
      expect(count).to eq(3)
    end

    it "sets created_at via database default" do
      buffer = Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60)

      buffer.push(stat_attrs)
      buffer.flush

      row = ActiveRecord::Base.connection.select_one("SELECT * FROM pgbus_job_stats LIMIT 1")
      expect(row["job_class"]).to eq("TestJob")
      expect(row["duration_ms"]).to eq(42)
      expect(row["enqueue_latency_ms"]).to eq(150)
      expect(row["created_at"]).not_to be_nil
    end

    it "auto-flushes at flush_size" do
      buffer = Pgbus::StatBuffer.new(flush_size: 2, flush_interval: 60)

      2.times { buffer.push(stat_attrs) }

      count = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pgbus_job_stats")
      expect(count).to eq(2)
      expect(buffer.size).to eq(0)
    end

    it "stores a nil latency as NULL and a quote in a job class verbatim" do
      buffer = Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60)

      buffer.push(stat_attrs.merge(job_class: "O'Brien::Job", enqueue_latency_ms: nil, retry_count: 2))
      buffer.push(stat_attrs.merge(status: "failed", duration_ms: 7))
      buffer.flush

      rows = ActiveRecord::Base.connection.select_rows(
        "SELECT job_class, queue_name, status, duration_ms, enqueue_latency_ms, retry_count FROM pgbus_job_stats ORDER BY id"
      )
      expect(rows).to eq([["O'Brien::Job", "default", "success", 42, nil, 2],
                          ["TestJob", "default", "failed", 7, 150, 0]])
    end

    it "falls back to the base columns when the latency migration has not run" do
      conn = ActiveRecord::Base.connection
      conn.execute("ALTER TABLE pgbus_job_stats DROP COLUMN enqueue_latency_ms, DROP COLUMN retry_count")
      buffer = Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60)

      2.times { buffer.push(stat_attrs) }
      buffer.flush

      expect(conn.select_rows("SELECT job_class, status, duration_ms FROM pgbus_job_stats"))
        .to eq([%w[TestJob success] + [42], %w[TestJob success] + [42]])
    ensure
      conn.execute("ALTER TABLE pgbus_job_stats ADD COLUMN IF NOT EXISTS enqueue_latency_ms BIGINT, " \
                   "ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0")
    end

    it "flushes remaining entries on stop" do
      buffer = Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60)

      buffer.push(stat_attrs)
      buffer.stop

      count = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pgbus_job_stats")
      expect(count).to eq(1)
    end
  end

  describe "Executor records stats via buffer" do
    let(:client) { Pgbus.client.tap { |c| c.ensure_queue("default") } }
    let(:config) { Pgbus.configuration }
    let(:buffer) { Pgbus::StatBuffer.new(flush_size: 100, flush_interval: 60) }
    let(:executor) { Pgbus::ActiveJob::Executor.new(client: client, config: config, stat_buffer: buffer) }

    before do
      stub_const("PlainBenchJob", Class.new(ActiveJob::Base) { def perform(*); end })
    end

    around do |example|
      original = config.stats_enabled
      config.stats_enabled = true
      example.run
    ensure
      config.stats_enabled = original
    end

    it "stats appear in the database after buffer flush" do
      payload = { "job_class" => "PlainBenchJob", "arguments" => [], "queue_name" => "default" }
      client.send_message("default", payload)
      msg = client.read_batch("default", qty: 1).first

      executor.execute(msg, "default")

      expect(buffer.size).to eq(1)
      buffer.flush

      count = ActiveRecord::Base.connection.select_value("SELECT COUNT(*) FROM pgbus_job_stats")
      expect(count).to eq(1)

      row = ActiveRecord::Base.connection.select_one("SELECT * FROM pgbus_job_stats LIMIT 1")
      expect(row["status"]).to eq("success")
      expect(row["duration_ms"]).to be > 0
    end
  end
end
