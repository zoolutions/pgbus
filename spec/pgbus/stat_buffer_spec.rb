# frozen_string_literal: true

require "spec_helper"
require "pg"

RSpec.describe Pgbus::StatBuffer do
  subject(:buffer) { described_class.new(flush_size: 3, flush_interval: 0.1) }

  let(:stat_attrs) do
    {
      job_class: "TestJob",
      queue_name: "default",
      status: "success",
      duration_ms: 42,
      enqueue_latency_ms: 10,
      retry_count: 0
    }
  end

  # A connection double that quotes like PostgreSQL (standard_conforming_strings
  # on) and records every statement the buffer sends.
  let(:statements) { [] }
  let(:conn) do
    instance_double(ActiveRecord::ConnectionAdapters::AbstractAdapter).tap do |c|
      allow(c).to receive(:quote) { |value| value.nil? ? "NULL" : "'#{value.to_s.gsub("'", "''")}'" }
      allow(c).to receive(:execute) { |sql, *| statements << sql }
    end
  end
  let(:pool) { instance_double(ActiveRecord::ConnectionAdapters::ConnectionPool) }

  before do
    stub_const("Pgbus::JobStat", Class.new)
    allow(Pgbus::JobStat).to receive_messages(table_exists?: true, latency_columns?: true,
                                              quoted_table_name: '"pgbus_job_stats"', connection_pool: pool)
    allow(pool).to receive(:with_connection).and_yield(conn)
  end

  describe "#initialize" do
    it "defaults to the module flush thresholds" do
      default = described_class.new
      expect(default.flush_size).to eq(described_class::DEFAULT_FLUSH_SIZE)
      expect(default.flush_interval).to eq(described_class::DEFAULT_FLUSH_INTERVAL)
    end

    it "honors custom flush_size and flush_interval" do
      custom = described_class.new(flush_size: 500, flush_interval: 2)
      expect(custom.flush_size).to eq(500)
      expect(custom.flush_interval).to eq(2)
    end

    it "auto-flushes at the custom flush_size, not the default" do
      custom = described_class.new(flush_size: 2, flush_interval: 60)

      2.times { custom.push(stat_attrs) }

      expect(statements.size).to eq(1)
      expect(custom.size).to eq(0)
    end
  end

  describe "#push" do
    it "accumulates entries in the buffer" do
      buffer.push(stat_attrs)
      expect(buffer.size).to eq(1)
    end

    it "auto-flushes when flush_size is reached" do
      3.times { buffer.push(stat_attrs) }

      expect(statements.size).to eq(1)
      expect(buffer.size).to eq(0)
    end

    it "does not flush before reaching flush_size" do
      2.times { buffer.push(stat_attrs) }

      expect(statements).to be_empty
      expect(buffer.size).to eq(2)
    end
  end

  describe "#flush" do
    it "writes buffered entries to the database in one multi-row INSERT" do
      2.times { buffer.push(stat_attrs) }
      buffer.flush

      expect(statements).to eq([
                                 'INSERT INTO "pgbus_job_stats" ' \
                                 "(job_class, queue_name, status, duration_ms, enqueue_latency_ms, retry_count) " \
                                 "VALUES ('TestJob', 'default', 'success', 42, 10, 0), " \
                                 "('TestJob', 'default', 'success', 42, 10, 0)"
                               ])
      expect(buffer.size).to eq(0)
    end

    it "runs the statement on a pooled connection of JobStat's database" do
      buffer.push(stat_attrs)
      buffer.flush

      expect(pool).to have_received(:with_connection).once
    end

    it "writes a nil latency or retry count as NULL" do
      buffer.push(stat_attrs.merge(enqueue_latency_ms: nil, retry_count: nil))
      buffer.flush

      expect(statements.first).to end_with("VALUES ('TestJob', 'default', 'success', 42, NULL, NULL)")
    end

    it "quotes every string through the connection, once per distinct value" do
      buffer.push(stat_attrs.merge(job_class: "O'Brien::Job"))
      buffer.push(stat_attrs.merge(job_class: "O'Brien::Job", status: "failed"))
      buffer.flush

      expect(statements.first).to include("('O''Brien::Job', 'default', 'success', 42, 10, 0)",
                                          "('O''Brien::Job', 'default', 'failed', 42, 10, 0)")
      expect(conn).to have_received(:quote).with("O'Brien::Job").once
      expect(conn).to have_received(:quote).with("default").once
    end

    it "writes nothing and logs when an integer column holds a non-integer" do
      allow(Pgbus.logger).to receive(:warn)
      buffer.push(stat_attrs.merge(duration_ms: "1); DROP TABLE pgbus_job_stats; --"))
      buffer.flush

      expect(statements).to be_empty
      expect(Pgbus.logger).to have_received(:warn) { |&block| expect(block.call).to include("Stat buffer flush failed") }
    end

    it "is a no-op when buffer is empty" do
      buffer.flush

      expect(statements).to be_empty
    end

    it "writes nothing when the stats table does not exist" do
      allow(Pgbus::JobStat).to receive(:table_exists?).and_return(false)
      buffer.push(stat_attrs)
      buffer.flush

      expect(statements).to be_empty
      expect(buffer.size).to eq(0)
    end

    it "rescues database errors without raising" do
      allow(conn).to receive(:execute).and_raise(StandardError, "db error")

      buffer.push(stat_attrs)
      expect { buffer.flush }.not_to raise_error
    end

    it "logs a statement error that is not about the latency columns, without a retry" do
      allow(Pgbus.logger).to receive(:warn)
      allow(conn).to receive(:execute).and_raise(ActiveRecord::StatementInvalid, "connection lost")

      buffer.push(stat_attrs)
      buffer.flush

      expect(conn).to have_received(:execute).once
      expect(Pgbus.logger).to have_received(:warn) { |&block| expect(block.call).to include("connection lost") }
    end
  end

  describe "#flush_if_due" do
    it "flushes when interval has elapsed" do
      buffer.push(stat_attrs)
      sleep(0.15) # exceed 0.1s flush_interval
      buffer.flush_if_due

      expect(statements.size).to eq(1)
    end

    it "does not flush before interval elapses" do
      buffer.push(stat_attrs)
      buffer.flush_if_due

      expect(statements).to be_empty
    end
  end

  describe "#stop" do
    it "flushes remaining entries" do
      buffer.push(stat_attrs)
      buffer.stop

      expect(statements.size).to eq(1)
      expect(buffer.size).to eq(0)
    end
  end

  describe "always includes all columns" do
    it "includes latency fields regardless of latency_columns? memoization" do
      allow(Pgbus::JobStat).to receive(:latency_columns?).and_return(false)

      buffer.push(stat_attrs)
      buffer.flush

      expect(statements.first).to include("(job_class, queue_name, status, duration_ms, enqueue_latency_ms, retry_count)",
                                          "('TestJob', 'default', 'success', 42, 10, 0)")
    end
  end

  describe "fallback when latency columns missing in DB" do
    # What ActiveRecord raises for PostgreSQL's undefined_column (42703): the
    # PG error is the cause, its text (in the server's lc_messages) the message.
    def undefined_column(text)
      raise PG::UndefinedColumn, text
    rescue PG::UndefinedColumn
      raise ActiveRecord::StatementInvalid, "PG::UndefinedColumn: #{text}"
    end

    let(:missing_column_text) { 'ERROR:  column "enqueue_latency_ms" of relation "pgbus_job_stats" does not exist' }

    before do
      allow(Pgbus.logger).to receive(:warn)
      allow(conn).to receive(:execute) do |sql, *|
        statements << sql
        undefined_column(missing_column_text) if sql.include?("enqueue_latency_ms")
      end
    end

    it "retries with base columns on an undefined latency column" do
      buffer.push(stat_attrs)
      buffer.flush

      expect(statements.size).to eq(2)
      expect(statements[0]).to include("enqueue_latency_ms")
      expect(statements[1]).to eq('INSERT INTO "pgbus_job_stats" (job_class, queue_name, status, duration_ms) ' \
                                  "VALUES ('TestJob', 'default', 'success', 42)")
    end

    it "retries when retry_count is the missing column" do
      allow(conn).to receive(:execute) do |sql, *|
        statements << sql
        undefined_column('ERROR:  column "retry_count" of relation "pgbus_job_stats" does not exist') if
          sql.include?("retry_count")
      end

      buffer.push(stat_attrs)
      buffer.flush

      expect(statements.size).to eq(2)
    end

    context "with a server whose lc_messages is not English" do
      let(:missing_column_text) { "FEHLER:  Spalte »enqueue_latency_ms« von Relation »pgbus_job_stats« existiert nicht" }

      it "still retries with base columns" do
        buffer.push(stat_attrs)
        buffer.flush

        expect(statements.size).to eq(2)
        expect(statements[1]).not_to include("enqueue_latency_ms")
      end
    end

    it "does not retry an error whose statement excerpt merely contains the latency columns" do
      allow(conn).to receive(:execute).and_raise(
        ActiveRecord::StatementInvalid,
        "PG::NumericValueOutOfRange: ERROR:  integer out of range\n" \
        "LINE 1: ...ration_ms, enqueue_latency_ms, retry_count) VALUES ('TestJob', ..."
      )

      buffer.push(stat_attrs)
      buffer.flush

      expect(conn).to have_received(:execute).once
      expect(Pgbus.logger).to have_received(:warn) { |&block| expect(block.call).to include("integer out of range") }
    end

    it "does not retry an undefined column that is not a latency column" do
      allow(conn).to receive(:execute) do |sql, *|
        statements << sql
        undefined_column(%(ERROR:  column "status" of relation "pgbus_job_stats" does not exist\n) +
                         "LINE 1: ...stats (job_class, queue_name, status, duration_ms, enqueue_latency_ms, retry_count)...")
      end

      buffer.push(stat_attrs)
      buffer.flush

      expect(statements.size).to eq(1)
    end

    it "warns about the missing columns once, not on every flush" do
      2.times do
        buffer.push(stat_attrs)
        buffer.flush
      end

      expect(statements.size).to eq(4)
      expect(Pgbus.logger).to have_received(:warn).once
    end

    it "logs a failed base-column retry without raising" do
      warnings = []
      allow(Pgbus.logger).to receive(:warn) { |&block| warnings << block.call }
      allow(conn).to receive(:execute) do |sql, *|
        statements << sql
        undefined_column('ERROR:  column "retry_count" of relation "pgbus_job_stats" does not exist')
      end

      buffer.push(stat_attrs)
      expect { buffer.flush }.not_to raise_error
      expect(statements.size).to eq(2)
      expect(warnings.last).to include("Stat buffer fallback flush failed", "retry_count")
    end
  end
end
