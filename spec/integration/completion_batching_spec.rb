# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# Issue #606: with config.completion_batching a worker archives through its
# Process::Completer (archive_batch per flush) instead of one pgmq.archive per
# job. Every message is archived once, the duplicate guard still holds, and a
# worker killed with jobs waiting on a flush loses nothing: they re-run.
#
# The Timeout.timeout below bounds a Process.waitpid2 on a forked CHILD; no
# pooled PG::Connection is in the timed block.
RSpec.describe "Completion batching (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:queue) { "completion_batching" }
  let(:physical) { config.queue_name(queue) }
  let(:noop_job) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "CompletionBatchingSpec::NoopJob"
      def perform(*) = nil
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CompletionBatchingSpec", Module.new)
    stub_const("CompletionBatchingSpec::NoopJob", noop_job)
    config.completion_batching = true
    client.ensure_queue(queue)
    client.purge_queue(queue)
    sql("DELETE FROM pgmq.a_#{physical}")
  end

  after do
    config.completion_batching = false
    Pgbus.stopping = false
    Pgbus::VisibilityHeartbeat.reset!
  end

  def sql(query) = ActiveRecord::Base.connection.select_value(query)

  def queued = sql("SELECT count(*) FROM pgmq.q_#{physical}").to_i

  def wait_until(seconds = 10)
    deadline = Time.now + seconds
    sleep 0.01 until yield || Time.now > deadline
    yield
  end

  # Runs a worker on its own thread until it processed `jobs`, then drains it.
  def drain(jobs, threads: 4)
    worker = Pgbus::Process::Worker.new(queues: [queue], threads: threads)
    allow(worker).to receive_messages(setup_signals: nil, restore_signals: nil)
    runner = Thread.new { worker.run } # rubocop:disable ThreadSafety/NewThread -- joined below
    finished = wait_until(30) { worker.stats[:jobs_processed] + worker.stats[:jobs_failed] >= jobs }
    worker.graceful_shutdown
    expect(runner.join(10)).to eq(runner)
    expect(finished).to be(true)
    worker
  ensure
    runner&.kill
  end

  def ensure_job_stats_table
    conn = ActiveRecord::Base.connection
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pgbus_job_stats (
        id BIGSERIAL PRIMARY KEY, job_class VARCHAR NOT NULL, queue_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL, duration_ms INTEGER NOT NULL DEFAULT 0, enqueue_latency_ms BIGINT,
        retry_count INTEGER DEFAULT 0, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      )
    SQL
    conn.execute("DELETE FROM pgbus_job_stats WHERE queue_name = '#{queue}'")
    %i[@table_exists @latency_columns].each do |ivar|
      Pgbus::JobStat.remove_instance_variable(ivar) if Pgbus::JobStat.instance_variable_defined?(ivar)
    end
    Pgbus::JobStat.reset_column_information
  end

  it "drains 200 jobs on 4 threads with archive_batch, each message archived exactly once" do
    ensure_job_stats_table
    ids = client.send_batch(queue, Array.new(200) { noop_job.new.serialize }).map(&:to_i)
    allow(client).to receive(:archive_batch).and_call_original
    allow(client).to receive(:archive_message).and_call_original

    worker = drain(200)

    expect(worker.stats).to include(jobs_processed: 200, jobs_failed: 0)
    expect(queued).to eq(0)
    archived = ActiveRecord::Base.connection.select_values("SELECT msg_id FROM pgmq.a_#{physical}").map(&:to_i)
    expect(archived).to match_array(ids)
    expect(client).to have_received(:archive_batch).at_least(:once)
    expect(client).not_to have_received(:archive_message)
    expect(sql("SELECT count(*) FROM pgbus_job_stats WHERE queue_name = '#{queue}' AND status = 'success'").to_i)
      .to eq(200)
  end

  # rails/solid_queue#761: the execution that loses the archive race owns no
  # completion signal.
  it "returns :duplicate and signals nothing when another client archived the message first" do
    client.send_message(queue, noop_job.new.serialize)
    message = client.read_batch(queue, qty: 1, vt: 30).first
    client.archive_message(queue, message.msg_id.to_i)
    allow(Pgbus::Concurrency).to receive(:extract_key).and_return("CompletionBatchingSpec-1")
    allow(Pgbus::Concurrency::Semaphore).to receive(:signal)
    completer = Pgbus::Process::Completer.new(client: client, batch_size: 100)
    executor = Pgbus::ActiveJob::Executor.new(client: client, completer: completer)

    expect(executor.execute(message, queue)).to eq(:duplicate)
    expect(Pgbus::Concurrency::Semaphore).not_to have_received(:signal)
  ensure
    completer&.stop
  end

  describe "a worker killed with jobs waiting on a flush" do
    def connection_url(pool:)
      parsed = URI.parse(PGBUS_DATABASE_URL)
      params = URI.decode_www_form(parsed.query || "").to_h.merge("pool" => pool.to_s, "gssencmode" => "disable")
      parsed.query = URI.encode_www_form(params)
      parsed.to_s
    end

    def fork_with_fresh_connections(&)
      ActiveRecord::Base.connection_handler.clear_all_connections!
      Pgbus.reset_client!
      pid = fork(&)
      ActiveRecord::Base.establish_connection(connection_url(pool: 20))
      Pgbus.reset_client!
      pid
    end

    # In the child: every job reports "P" once it performed, and the first
    # flush reports "F" and then hangs before it archives anything, so every
    # job of the drain is waiting on the completer when the parent kills it.
    def run_doomed_worker(writer)
      ActiveRecord::Base.establish_connection(connection_url(pool: 5))
      Pgbus.reset_client!
      CompletionBatchingSpec::NoopJob.define_method(:perform) do |*|
        writer.write("P")
        writer.flush
      end
      Pgbus.client.define_singleton_method(:archive_batch) do |*|
        writer.write("F")
        writer.flush
        sleep 60
      end
      Pgbus::Process::Worker.new(queues: [queue], threads: 4).run
    ensure
      exit!(1)
    end

    def read_reports(reader, until_performed:)
      reports = +""
      deadline = Time.now + 15
      until (reports.count("P") >= until_performed && reports.include?("F")) || Time.now > deadline
        reports << reader.read_nonblock(64) if reader.wait_readable(0.1)
      end
      reports
    end

    it "loses nothing: the waiting jobs stay queued and run again after their visibility timeout" do
      ids = client.send_batch(queue, Array.new(4) { noop_job.new.serialize }).map(&:to_i)
      reader, writer = IO.pipe
      pid = fork_with_fresh_connections do
        reader.close
        run_doomed_worker(writer)
      end
      writer.close

      reports = read_reports(reader, until_performed: 4)
      expect(reports.count("P")).to eq(4)
      expect(reports).to include("F")
      Process.kill("KILL", pid)
      Timeout.timeout(10) { Process.waitpid2(pid) } # rubocop:disable Pgbus/NoRubyTimeout -- bounds waitpid2 on a child, not a PG connection

      expect(queued).to eq(4)
      expect(sql("SELECT count(*) FROM pgmq.a_#{physical}").to_i).to eq(0)
      # The visibility timeout running out, without waiting 30 s for it.
      # The fork reset the memoized client; this is the new one.
      ids.each { |id| Pgbus.client.set_visibility_timeout(queue, id, vt: 0) }

      worker = drain(4)
      expect(worker.stats).to include(jobs_processed: 4, jobs_failed: 0)
      expect(queued).to eq(0)
      expect(sql("SELECT count(*) FROM pgmq.a_#{physical}").to_i).to eq(4)
    ensure
      reader&.close
      begin
        Process.kill("KILL", pid) if pid
        Process.waitpid(pid) if pid
      rescue Errno::ESRCH, Errno::ECHILD
        nil
      end
    end
  end
end
