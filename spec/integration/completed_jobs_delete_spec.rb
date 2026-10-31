# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #607: with config.completed_jobs = :delete a successful job's message
# is removed with pgmq.delete (delete_batch through the completer) instead of
# being archived. A drained queue leaves no q_ and no a_ rows; failures still
# back off in place and dead letters still land in the DLQ.
RSpec.describe "completed_jobs: :delete (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:queue) { "completed_jobs_delete" }
  let(:physical) { config.queue_name(queue) }
  let(:noop_job) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "CompletedJobsDeleteSpec::NoopJob"
      def perform(*) = nil
    end
  end
  let(:failing_job) do
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :inline
      def self.name = "CompletedJobsDeleteSpec::FailingJob"
      def perform(*) = raise("boom")
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("CompletedJobsDeleteSpec", Module.new)
    stub_const("CompletedJobsDeleteSpec::NoopJob", noop_job)
    stub_const("CompletedJobsDeleteSpec::FailingJob", failing_job)
    config.completed_jobs = :delete
    client.ensure_queue(queue)
    client.ensure_dead_letter_queue(queue)
    client.purge_queue(queue)
    sql("DELETE FROM pgmq.q_#{physical}_dlq")
    sql("DELETE FROM pgmq.a_#{physical}")
    ensure_job_stats_table
  end

  after do
    config.completed_jobs = :archive
    config.completion_batching = false
    Pgbus.stopping = false
    Pgbus::VisibilityHeartbeat.reset!
  end

  def sql(query) = ActiveRecord::Base.connection.select_value(query)

  def count(table) = sql("SELECT count(*) FROM pgmq.#{table}").to_i

  def successes = sql("SELECT count(*) FROM pgbus_job_stats WHERE queue_name = '#{queue}' AND status = 'success'").to_i

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
    # A failed expectation above, or a run that raised, must not leave the
    # worker's pool draining the queue under the next example. Status false is
    # a run that returned: its shutdown already ran.
    worker&.immediate_shutdown unless runner&.status == false
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
    # The table now exists with the latency column, whatever an earlier
    # example memoised.
    Pgbus::JobStat.reset_column_information
    allow(Pgbus::JobStat).to receive_messages(table_exists?: true, latency_columns?: true)
  end

  it "drains 200 jobs with delete_message: no queue rows, no archive rows, every success recorded" do
    client.send_batch(queue, Array.new(200) { noop_job.new.serialize })
    allow(client).to receive(:delete_message).and_call_original
    allow(client).to receive(:archive_message).and_call_original

    worker = drain(200)

    expect(worker.stats).to include(jobs_processed: 200, jobs_failed: 0)
    expect(count("q_#{physical}")).to eq(0)
    expect(count("a_#{physical}")).to eq(0)
    expect(client).to have_received(:delete_message).exactly(200).times
    expect(client).not_to have_received(:archive_message)
    expect(successes).to eq(200)
  end

  it "drains through the completer with delete_batch when completion_batching is on" do
    config.completion_batching = true
    client.send_batch(queue, Array.new(200) { noop_job.new.serialize })
    allow(client).to receive(:delete_batch).and_call_original
    allow(client).to receive(:archive_batch).and_call_original

    worker = drain(200)

    expect(worker.stats).to include(jobs_processed: 200, jobs_failed: 0)
    expect(count("q_#{physical}")).to eq(0)
    expect(count("a_#{physical}")).to eq(0)
    expect(client).to have_received(:delete_batch).at_least(:once)
    expect(client).not_to have_received(:archive_batch)
    expect(successes).to eq(200)
  end

  it "returns :duplicate when the message was already deleted" do
    client.send_message(queue, noop_job.new.serialize)
    message = client.read_batch(queue, qty: 1, vt: 30).first
    client.delete_message(queue, message.msg_id.to_i)

    expect(Pgbus::ActiveJob::Executor.new(client: client).execute(message, queue)).to eq(:duplicate)
  end

  it "leaves a failed job in its queue, invisible until its backoff runs out" do
    msg_id = client.send_message(queue, failing_job.new.serialize).to_i
    message = client.read_batch(queue, qty: 1, vt: 30).first

    expect(Pgbus::ActiveJob::Executor.new(client: client).execute(message, queue)).to eq(:failed)

    expect(sql("SELECT read_ct FROM pgmq.q_#{physical} WHERE msg_id = #{msg_id}").to_i).to eq(1)
    expect(sql("SELECT vt > now() FROM pgmq.q_#{physical} WHERE msg_id = #{msg_id}")).to be(true)
    expect(count("a_#{physical}")).to eq(0)
  end

  # A worker that deleted a batch job's message and died before its batch
  # signal: the stall sweep finishes the batch from the queue and DLQ probes
  # alone, never asking an archive :delete does not write.
  it "lets Batch::Sweep count a batch job whose message was deleted before its signal" do
    job_queue = queue
    batch_job = Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(job_queue)
      def self.name = "CompletedJobsDeleteSpec::BatchJob"
      def perform(*) = nil
    end
    stub_const("CompletedJobsDeleteSpec::BatchJob", batch_job)
    batch = Pgbus::Batch.new
    batch.enqueue { batch_job.perform_later }
    message = client.read_batch(queue, qty: 1, vt: 30).first
    expect(client.delete_message(queue, message.msg_id.to_i)).to be(true)
    allow(client).to receive(:message_archived?).and_call_original
    allow(Pgbus.logger).to receive(:warn).and_call_original

    # A cutoff a second ahead, so the row just inserted is stale without a
    # same-microsecond tie against created_at.
    Pgbus::Batch::Sweep.run(stalled_for: -1, client: client)

    expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).completed_jobs).to eq(1)
    expect(Pgbus::BatchExecution.where(batch_id: batch.batch_id).count).to eq(0)
    expect(client).not_to have_received(:message_archived?)
    expect(Pgbus.logger).not_to have_received(:warn)
  end

  it "still moves a job past max_retries to the dead letter queue" do
    client.send_message(queue, failing_job.new.serialize)
    message = client.read_batch(queue, qty: 1, vt: 30).first
    executor = Pgbus::ActiveJob::Executor.new(client: client, config: config)
    allow(config).to receive(:max_retries).and_return(0)

    expect(executor.execute(message, queue)).to eq(:dead_lettered)

    expect(count("q_#{physical}")).to eq(0)
    expect(count("q_#{physical}_dlq")).to eq(1)
  end
end
