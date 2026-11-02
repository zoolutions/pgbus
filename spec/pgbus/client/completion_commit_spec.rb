# frozen_string_literal: true

require "spec_helper"
require "pg"

# Issue #619: config.completion_synchronous_commit = false commits the four
# completion statements with SET LOCAL synchronous_commit TO off, pipelined.
RSpec.describe Pgbus::Client::AsyncCommit do
  subject(:client) do
    allow(PGMQ::Client).to receive(:new).and_return(mock_pgmq)
    c = Pgbus::Client.new(config, schema_ensured: true)
    allow(c).to receive(:tune_autovacuum)
    allow(c).to receive(:notify_trigger_current?).and_return(false)
    c
  end

  # A PG::Connection stand-in: records every call in order and answers the
  # pipeline with the results a real libpq pipeline returns (each query's
  # result, then nil, then the sync).
  let(:fake_connection_class) do
    Class.new do
      attr_reader :calls, :pipeline_status, :status
      attr_accessor :transaction_status, :completion_result, :exec_prepared_result
      attr_writer :failing

      def initialize(result_class)
        @result_class = result_class
        @calls = []
        @transaction_status = PG::PQTRANS_IDLE
        @pipeline_status = PG::PQ_PIPELINE_OFF
        @queued = []
        @failing = nil
        @status = PG::CONNECTION_OK
        @lose_connection = false
      end

      # The next pipeline's first get_result finds the socket closed, as a
      # backend killed mid-call does.
      def lose_connection! = (@lose_connection = true)

      # The next pipeline's first get_result raises a non-PG error (a
      # Timeout, an interrupt) with the connection still up and results unread.
      def interrupt_read! = (@interrupt_read = true)

      # The next pipeline answers no sync: libpq runs out of results.
      def drop_sync! = (@drop_sync = true)

      def finished? = false

      # PG::Connection#reset: a new session, out of pipeline mode.
      def reset
        @calls << [:reset]
        @status = PG::CONNECTION_OK
        @pipeline_status = PG::PQ_PIPELINE_OFF
      end

      def backend_pid = 101

      def exec_params(sql, params)
        @calls << [:exec_params, sql, params]
        return columns_result if sql == Pgbus::Client::NativeStatements::COLUMNS_SQL

        @exec_prepared_result
      end

      def exec_prepared(name, params)
        @calls << [:exec_prepared, name, params]
        @exec_prepared_result
      end

      def prepare(name, sql)
        @calls << [:prepare, name, sql]
      end

      def exec(sql)
        @calls << [:exec, sql]
        @transaction_status = PG::PQTRANS_IDLE if sql == "ROLLBACK"
      end

      def enter_pipeline_mode
        @calls << [:enter_pipeline_mode]
        @pipeline_status = PG::PQ_PIPELINE_ON
      end

      # libpq refuses to leave pipeline mode with results left unread.
      def exit_pipeline_mode
        @calls << [:exit_pipeline_mode]
        raise PG::Error, "cannot exit pipeline mode with uncollected results" if @replies&.any?

        @pipeline_status = PG::PQ_PIPELINE_OFF
      end

      def send_query_params(sql, params)
        @calls << [:send_query_params, sql, params]
        @queued << sql
      end

      def send_query_prepared(name, params)
        @calls << [:send_query_prepared, name, params]
        @queued << :completion
      end

      def pipeline_sync
        @calls << [:pipeline_sync]
        @replies = build_replies
      end

      def get_result # rubocop:disable Naming/AccessorMethodName -- PG::Connection#get_result's name
        if @interrupt_read
          @interrupt_read = false
          raise Interrupt
        end
        if @lose_connection
          @lose_connection = false
          @status = PG::CONNECTION_BAD
          raise PG::ConnectionBad, "server closed the connection unexpectedly"
        end

        @replies.shift
      end

      def columns_result
        rows = %w[q_pgbus_test_default a_pgbus_test_default].flat_map do |table|
          Pgbus::Client::NativeStatements::REQUIRED_COLUMNS.map { |c| { "table_name" => table, "column_name" => c } }
        end
        @result_class.new(PG::PGRES_TUPLES_OK, rows)
      end

      private

      # The queued queries' results: the completion statement (sent prepared,
      # or as SQL under the unprepared fallback) answers completion_result;
      # an error at @failing aborts the rest, and leaves the explicit BEGIN
      # open (INERROR) as the server does; a failed COMMIT ends it.
      def build_replies
        aborted = false
        replies = @queued.flat_map do |query|
          result = if aborted
                     @result_class.new(PG::PGRES_PIPELINE_ABORTED, [])
                   elsif @failing && @failing[0] == query
                     aborted = true
                     @transaction_status = query == "COMMIT" ? PG::PQTRANS_IDLE : PG::PQTRANS_INERROR
                     @result_class.new(PG::PGRES_FATAL_ERROR, [], error: @failing[1])
                   elsif query == :completion || query.start_with?("WITH", "DELETE")
                     @completion_result
                   else
                     @result_class.new(PG::PGRES_COMMAND_OK, [])
                   end
          [result, nil]
        end
        @queued = []
        return replies + [nil] if @drop_sync

        replies << @result_class.new(PG::PGRES_PIPELINE_SYNC, [])
      end
    end
  end

  let(:result_class) do
    Class.new do
      attr_reader :result_status

      def initialize(status, rows, error: nil)
        @result_status = status
        @rows = rows
        @error = error
      end

      def ntuples = @rows.size
      def [](index) = @rows[index]
      def map(&) = @rows.map(&)
      def to_a = @rows

      def check
        raise @error if @error

        self
      end
    end
  end

  let(:conn) { fake_connection_class.new(result_class) }
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.database_url = "postgres://localhost/pgbus_test"
      c.queue_prefix = "pgbus_test"
      c.completion_synchronous_commit = false
    end
  end
  let(:queue) { "pgbus_test_default" }
  let(:mock_pgmq) { build_mock_pgmq }
  let(:statements) { Pgbus::Client::NativeStatements.statements_for("q_#{queue}", "a_#{queue}") }

  def rows(*ids) = result_class.new(PG::PGRES_TUPLES_OK, ids.map { |id| { "msg_id" => id.to_s } })

  def pipeline_calls(statement_call)
    [
      [:enter_pipeline_mode],
      [:send_query_params, "BEGIN", []],
      [:send_query_params, "SET LOCAL synchronous_commit TO off", []],
      statement_call,
      [:send_query_params, "COMMIT", []],
      [:pipeline_sync],
      [:exit_pipeline_mode]
    ]
  end

  def calls_after_detection
    conn.calls.reject { |call| call[1] == Pgbus::Client::NativeStatements::COLUMNS_SQL }
  end

  before do
    Pgbus::Client::NativeStatements.reset!
    real_pgmq_connection_error
    allow(Pgbus::Client).to receive(:load_pgmq_gem!)
    stub_const("PGMQ::Client", Class.new { def initialize(*, **); end })
    allow(mock_pgmq).to receive(:with_connection).and_yield(conn)
    conn.completion_result = rows(9)
    conn.exec_prepared_result = rows(9)
  end

  after { Pgbus::Client::NativeStatements.reset! }

  {
    archive_message: [:archive, [9], 9, true],
    delete_message: [:delete, [9], 9, true],
    archive_batch: [:archive_batch, [[9, 10]], "{9,10}", %w[9]],
    delete_batch: [:delete_batch, [[9, 10]], "{9,10}", %w[9]]
  }.each do |method, (op, args, param, expected)|
    it "pipelines #{op} as BEGIN, SET LOCAL, the prepared statement, COMMIT, one sync" do
      result = client.public_send(method, "default", *args, job_completion: true)

      expect(result).to eq(expected)
      expect(calls_after_detection).to eq(pipeline_calls([:send_query_prepared, statements[op].name, [param]]))
    end
  end

  it "returns false for a completion whose row is already gone, the shape the executor reads" do
    conn.completion_result = rows

    expect(client.archive_message("default", 9, job_completion: true)).to be(false)
    expect(client.delete_batch("default", [9], job_completion: true)).to eq([])
  end

  {
    archive_message: [:archive, [9], [9], true],
    delete_message: [:delete, [9], [9], true],
    archive_batch: [:archive_batch, [[9, 10]], ["{9,10}"], %w[9]],
    delete_batch: [:delete_batch, [[9, 10]], ["{9,10}"], %w[9]]
  }.each do |method, (op, args, params, expected)|
    it "keeps #{method} durable for a caller that is not completing a job (the event consumer, the dashboard)" do
      expect(client.public_send(method, "default", *args)).to eq(expected)
      expect(calls_after_detection).to eq([[:exec_prepared, statements[op].name, params]])
    end
  end

  it "never pipelines a send, a batch send or a read" do
    conn.exec_prepared_result = rows(1)

    client.send_message("default", { "a" => 1 }, delay: 0)
    client.send_batch("default", [{ "a" => 1 }], delay: 0)
    conn.exec_prepared_result = result_class.new(PG::PGRES_TUPLES_OK, [])
    client.read_batch("default", qty: 1)

    expect(conn.calls.map(&:first)).not_to include(:enter_pipeline_mode, :send_query_params, :send_query_prepared)
  end

  it "leaves a completion inside a caller's transaction to that transaction" do
    conn.transaction_status = PG::PQTRANS_INTRANS

    expect(client.archive_message("default", 9, job_completion: true)).to be(true)
    expect(calls_after_detection).to eq([[:exec_params, statements[:archive].sql, [9]]])
  end

  it "pipelines the SQL unprepared once the process has switched to unprepared statements" do
    Pgbus::Client::NativeStatements.unprepare!("test")

    expect(client.delete_message("default", 9, job_completion: true)).to be(true)
    expect(calls_after_detection).to eq(pipeline_calls([:send_query_params, statements[:delete].sql, [9]]))
  end

  it "raises the statement's error, rolls the open transaction back and leaves pipeline mode" do
    conn.failing = [:completion, PG::LockNotAvailable.new("lock timeout")]

    expect { client.archive_message("default", 9, job_completion: true) }.to raise_error(PG::LockNotAvailable, /lock timeout/)
    expect(conn.pipeline_status).to eq(PG::PQ_PIPELINE_OFF)
    expect(conn.transaction_status).to eq(PG::PQTRANS_IDLE)
    expect(calls_after_detection.last(2)).to eq([[:exit_pipeline_mode], [:exec, "ROLLBACK"]])
  end

  it "raises a failing COMMIT's error too, with nothing left to roll back" do
    conn.failing = ["COMMIT", PG::TRSerializationFailure.new("could not serialize")]

    expect { client.delete_message("default", 9, job_completion: true) }.to raise_error(PG::TRSerializationFailure, /could not serialize/)
    expect(conn.pipeline_status).to eq(PG::PQ_PIPELINE_OFF)
    expect(conn.calls).not_to include([:exec, "ROLLBACK"])
  end

  it "raises instead of waiting for a sync libpq will not deliver" do
    conn.drop_sync!

    expect { client.archive_message("default", 9, job_completion: true) }
      .to raise_error(PG::ConnectionBad, /pipeline ended before its sync/)
  end

  it "resets a connection that cannot leave pipeline mode, so the pool never hands it out in it" do
    conn.interrupt_read!
    allow(Pgbus.logger).to receive(:warn)

    expect { client.archive_message("default", 9, job_completion: true) }.to raise_error(Interrupt)
    expect(conn.calls.last(2)).to eq([[:exit_pipeline_mode], [:reset]])
    expect(conn.pipeline_status).to eq(PG::PQ_PIPELINE_OFF)
    expect(Pgbus.logger).to have_received(:warn)
  end

  it "falls back to prepare_and_exec outside the pipeline when the statement is not prepared on this session" do
    conn.failing = [:completion, PG::InvalidSqlStatementName.new("prepared statement does not exist")]

    expect(client.archive_message("default", 9, job_completion: true)).to be(true)
    expect(calls_after_detection.last(4)).to eq(
      [
        [:exit_pipeline_mode],
        [:exec, "ROLLBACK"],
        [:prepare, statements[:archive].name, statements[:archive].sql],
        [:exec_prepared, statements[:archive].name, [9]]
      ]
    )
  end

  it "pipelines again on the next call after a fallback" do
    conn.failing = [:completion, PG::InvalidSqlStatementName.new("prepared statement does not exist")]
    client.archive_message("default", 9, job_completion: true)
    conn.failing = nil
    conn.calls.clear

    client.archive_message("default", 9, job_completion: true)

    expect(conn.calls).to eq(pipeline_calls([:send_query_prepared, statements[:archive].name, [9]]))
  end

  # run_native's re-run and short-row checks hold through the pipeline: a lost
  # connection is re-run by pgmq-ruby, and a re-run that finds the id gone may
  # follow a commit whose reply was lost (issue #607 review).
  context "when the connection is lost mid-pipeline" do
    before(:all) { real_pgmq_connection_class } # rubocop:disable RSpec/BeforeAfterAll

    let(:real_connection) { PGMQ::Connection.new(-> { conn }, pool_size: 1, pool_timeout: 1) }

    before do
      allow(mock_pgmq).to receive(:with_connection) { |&blk| real_connection.with_connection(&blk) }
      client.native_ready?(queue)
      allow(client).to receive(:sleep)
      conn.lose_connection!
    end

    it "leaves the dead connection for pgmq-ruby to reset, and returns the re-run when it finds the id" do
      expect(client.archive_message("default", 9, job_completion: true)).to be(true)

      first_attempt = conn.calls.take_while { |call| call != [:reset] }
      expect(first_attempt).not_to include([:exit_pipeline_mode])
      expect(conn.calls).to include([:reset])
      expect(conn.pipeline_status).to eq(PG::PQ_PIPELINE_OFF)
    end

    it "raises COMPLETION_NOT_RESENT when the re-run finds the id gone: the first may have committed" do
      conn.completion_result = rows

      expect { client.delete_message("default", 9, job_completion: true) }
        .to raise_error(PGMQ::Errors::ConnectionError, /it may have committed: server closed the connection/)
    end
  end

  context "with completion_synchronous_commit true (the default)" do
    before { config.completion_synchronous_commit = true }

    it "runs the completion as one exec_prepared, as before" do
      expect(client.archive_message("default", 9, job_completion: true)).to be(true)
      expect(calls_after_detection).to eq([[:exec_prepared, statements[:archive].name, [9]]])
    end
  end

  context "with native_statements false" do
    before { config.native_statements = false }

    it "has no effect: the completion goes through pgmq-ruby" do
      allow(mock_pgmq).to receive(:archive).and_return(true)

      expect(client.archive_message("default", 9, job_completion: true)).to be(true)
      expect(conn.calls).to be_empty
    end
  end
end
