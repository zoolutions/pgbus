# frozen_string_literal: true

require "logger"
require "uri"
require "active_record"
require "active_job"
require "pgbus"
require_relative "worker_profile_harness"

# Drives one matrix cell of the worker profiling bench (issue #484) inside the
# current process. Pgbus is configured GLOBALLY (Pgbus.configure) because the
# worker reads Pgbus.client and the executor defaults to Pgbus.client /
# Pgbus.configuration; a local Configuration object would be ignored. That is
# also why the bench runs each cell in its own subprocess: one global config,
# one JIT setting, one database endpoint per process.
module WorkerProfileRunner
  QUEUE = "default"
  ENQUEUE_SLICE = 500

  class NoopJob < ActiveJob::Base
    self.queue_adapter = :inline
    def perform(*); end
  end

  module_function

  def setup!(database_url, threads:)
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    ActiveRecord::Base.establish_connection(with_pool(database_url, threads + 8))

    Pgbus.configure do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_wprof"
      c.default_queue = QUEUE
      c.logger = Logger.new(IO::NULL)
      c.pgmq_schema_mode = :embedded
      c.listen_notify = false
      c.stats_enabled = true
    end

    bootstrap_tables(ActiveRecord::Base.connection)
    Pgbus.client.ensure_queue(QUEUE)
    reset!
  end

  def reset!
    Pgbus.client.purge_queue(QUEUE)
    conn = ActiveRecord::Base.connection
    conn.execute("DELETE FROM pgbus_failed_events WHERE queue_name LIKE 'pgbus_wprof%'")
    conn.execute("DELETE FROM pgbus_job_stats WHERE queue_name LIKE 'pgbus_wprof%'")
  end

  # Enqueues `jobs` plain jobs, then drains them with a real Worker.
  # Returns {wall_s:, cpu_s:, gc_s:, jobs:} measured from worker start to the
  # last job's completion. With profile_path, the drain runs under vernier and
  # the result also carries :shares.
  def drain(jobs:, threads:, profile_path: nil)
    reset!
    enqueue(jobs)
    Pgbus.stopping = false
    worker = Pgbus::Process::Worker.new(queues: [QUEUE], threads: threads)

    measurement = nil
    nil
    run = lambda do
      measurement = measure do
        runner = Thread.new { worker.run }
        runner.name = WorkerProfileHarness::LOOP_THREAD_NAME
        # sleep, not Thread.pass: a spinning watcher holds the GVL and would
        # show up as gvl_wait on every thread it is measuring.
        # A crash rescued in Worker#process_message counts as failed but not
        # processed, so wait on either: the check below reports it.
        sleep(0.002) until drained?(worker, jobs)
        worker.graceful_shutdown
        runner.join
      end
    end

    if profile_path
      require "vernier"
      vernier_result = Vernier.trace(interval: 500, allocation_interval: 0) { run.call }
      vernier_result.write(out: profile_path)
      measurement[:pool_shares] = WorkerProfileHarness.pool_shares(vernier_result)
      measurement[:loop_shares] = WorkerProfileHarness.loop_shares(vernier_result)
      measurement[:cpu_shares] = WorkerProfileHarness.cpu_shares(vernier_result)
    else
      run.call
    end

    failed = worker.stats[:jobs_failed]
    raise "#{failed} jobs failed during the drain — the bench measured errors, not work" if failed.positive?

    measurement.merge(jobs: jobs)
  end

  def drained?(worker, jobs)
    stats = worker.stats
    stats[:jobs_failed].positive? || stats[:jobs_processed] >= jobs
  end

  def measure
    wall0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    cpu0 = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
    gc0 = GC.stat(:time)
    yield
    {
      wall_s: Process.clock_gettime(Process::CLOCK_MONOTONIC) - wall0,
      cpu_s: Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu0,
      gc_s: (GC.stat(:time) - gc0) / 1000.0
    }
  end

  def enqueue(jobs)
    payload = NoopJob.new(1).serialize
    jobs.times.each_slice(ENQUEUE_SLICE) do |slice|
      Pgbus.client.send_batch(QUEUE, Array.new(slice.size) { payload })
    end
  end

  def with_pool(url, pool)
    parsed = URI.parse(url)
    params = URI.decode_www_form(parsed.query || "").to_h
    params["pool"] = pool.to_s
    parsed.query = URI.encode_www_form(params)
    parsed.to_s
  end

  def bootstrap_tables(conn)
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pgbus_failed_events (
        id BIGSERIAL PRIMARY KEY, queue_name VARCHAR NOT NULL, msg_id BIGINT, payload JSONB, headers JSONB,
        error_class VARCHAR, error_message TEXT, backtrace TEXT, retry_count INTEGER DEFAULT 0,
        failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_failed_events_queue_msg ON pgbus_failed_events (queue_name, msg_id);
      CREATE TABLE IF NOT EXISTS pgbus_job_stats (
        id BIGSERIAL PRIMARY KEY, job_class VARCHAR NOT NULL, queue_name VARCHAR NOT NULL,
        status VARCHAR NOT NULL, duration_ms INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, enqueue_latency_ms BIGINT, retry_count INTEGER DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS pgbus_processes (
        id BIGSERIAL PRIMARY KEY, kind VARCHAR NOT NULL, hostname VARCHAR, pid INTEGER, metadata JSONB DEFAULT '{}',
        last_heartbeat_at TIMESTAMP, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    SQL
  end
end
