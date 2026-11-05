# frozen_string_literal: true

require "logger"
require "uri"
require "active_record"
require "active_job"
require "pgbus"
require "active_job/queue_adapters/pgbus_adapter"
require "active_record/connection_adapters/postgresql_adapter"
require "concurrent"
require_relative "worker_profile_harness"
require_relative "bench_rails_app"

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

  # WP_BENCH_JOB_SHAPE's jobs (issue #649) go out through the pgbus adapter,
  # so each enqueue takes the lock, the slot or the batch row the executor
  # hands back. A key per job (queue + index): nothing is rejected or parked.
  # Keys and the batch description start with wprof-, and reset! deletes only
  # those, so a shared database keeps every other lock, slot and batch.
  class UniqueJob < ActiveJob::Base
    include Pgbus::Uniqueness

    self.queue_adapter = :pgbus
    ensures_uniqueness strategy: :until_executed, key: ->(queue, i) { "wprof-u:#{queue}:#{i}" }
    def perform(*); end
  end

  class LimitedJob < ActiveJob::Base
    include Pgbus::Concurrency

    self.queue_adapter = :pgbus
    limits_concurrency to: 1_000_000, key: ->(queue, i) { "wprof-c:#{queue}:#{i}" }
    def perform(*); end
  end

  class BatchJob < ActiveJob::Base
    self.queue_adapter = :pgbus
    def perform(*); end
  end

  SHAPED_JOBS = { "unique" => UniqueJob, "limited" => LimitedJob, "batch" => BatchJob }.freeze
  BATCH_DESCRIPTION = "wprof-bench"
  SHAPE_TABLES = %w[pgbus_uniqueness_keys pgbus_semaphores pgbus_blocked_executions pgbus_batches
                    pgbus_batch_executions].freeze

  # PostgreSQLAdapter#reset! calls: the pool's reaper resets every connection
  # it takes back from a dead owner, and nothing instruments it.
  RESETS = Concurrent::AtomicFixnum.new(0)
  ActiveRecord::ConnectionAdapters::PostgreSQLAdapter.prepend(Module.new do
    def reset!
      RESETS.increment
      super
    end
  end)

  module_function

  def setup!(database_url, threads:, pool_size: nil)
    configure!(database_url, threads: threads, pool_size: pool_size)
    bootstrap_tables(ActiveRecord::Base.connection)
    check_shape_tables!(ActiveRecord::Base.connection)
    Pgbus.client.ensure_queue(QUEUE)
    reset!
  end

  # The process's configuration, with no table bootstrap and no purge: the
  # ActiveRecord connection, Pgbus.configure, with WP_BENCH_RAILS_APP=1 the
  # minimal Rails app's boot (issue #618), then ActiveJob::Base.logger for
  # WP_BENCH_AJ_LOGS. A fleet drain child (issue #602) configures this way,
  # because reset! would purge the queue its siblings are draining. A nil pool_size keeps pgbus's default;
  # ar_pool caps the ActiveRecord pool (a fleet child budgets it exactly).
  def configure!(database_url, threads:, pool_size: nil, ar_pool: threads + 8)
    aj_logs = WorkerProfileHarness.aj_logs
    # Issue #649: async runs each job on a fiber, so leases are per fiber.
    ActiveSupport::IsolatedExecutionState.isolation_level = :fiber if async?
    ActiveRecord::Base.establish_connection(with_pool(database_url, ar_pool))

    Pgbus.configure do |c|
      c.database_url = database_url
      c.queue_prefix = "pgbus_wprof"
      c.default_queue = QUEUE
      c.logger = Logger.new(IO::NULL)
      c.pgmq_schema_mode = :embedded
      c.listen_notify = false
      c.stats_enabled = true
      c.pool_size = pool_size if pool_size
      # Issue #606: set only when asked, so the bench still runs on a branch
      # whose Configuration predates the setting.
      c.completion_batching = true if WorkerProfileHarness.completion_batching?
      # Issue #607, likewise.
      completed_jobs = WorkerProfileHarness.completed_jobs
      c.completed_jobs = completed_jobs if completed_jobs
      # Issue #619, likewise.
      synchronous = WorkerProfileHarness.completion_commit
      c.completion_synchronous_commit = synchronous unless synchronous.nil?
      # Issue #618, likewise.
      c.quiet_active_job_logs = true if aj_logs == "quiet"
    end
    # Issue #618: after Pgbus.configure, so the app's boot leaves it alone.
    BenchRailsApp.boot!(with_pool(database_url, ar_pool)) if WorkerProfileHarness.rails_app?
    # After the boot: ActiveJob's railtie sets ActiveJob::Base.logger to Rails.logger.
    ActiveJob::Base.logger = active_job_logger(aj_logs)
  end

  # default: the bench's logger since #484 (a plain Logger to IO::NULL at
  # debug: every line formatted, none written). warn: the throughput
  # checklist's recipe, ActiveJob's own TaggedLogging logger at warn.
  def active_job_logger(aj_logs)
    return Logger.new(IO::NULL) unless aj_logs == "warn"

    ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new(IO::NULL, level: :warn))
  end

  def reset!
    Pgbus.client.purge_queue(QUEUE)
    conn = ActiveRecord::Base.connection
    conn.execute("DELETE FROM pgbus_failed_events WHERE queue_name LIKE 'pgbus_wprof%'")
    conn.execute("DELETE FROM pgbus_job_stats WHERE queue_name LIKE 'pgbus_wprof%'")
    reset_shape_rows(conn) unless WorkerProfileHarness.job_shape == "plain"
    # The setup's own lease, so the drain's pool stat counts the worker's alone.
    ActiveRecord::Base.connection_pool.release_connection
  end

  def reset_shape_rows(conn)
    conn.execute(<<~SQL)
      DELETE FROM pgbus_uniqueness_keys WHERE lock_key LIKE 'wprof-u:%';
      DELETE FROM pgbus_semaphores WHERE key LIKE 'wprof-c:%';
      DELETE FROM pgbus_batch_executions WHERE batch_id IN
        (SELECT batch_id FROM pgbus_batches WHERE description = '#{BATCH_DESCRIPTION}');
      DELETE FROM pgbus_batches WHERE description = '#{BATCH_DESCRIPTION}';
    SQL
  end

  def check_shape_tables!(conn)
    return if WorkerProfileHarness.job_shape == "plain"

    missing = SHAPE_TABLES.reject { |table| conn.table_exists?(table) }
    return if missing.empty?

    abort "WP_BENCH_JOB_SHAPE=#{WorkerProfileHarness.job_shape} needs #{missing.join(", ")}: point " \
          "PGBUS_DATABASE_URL at a database the install generator migrated (the integration database)"
  end

  def async? = WorkerProfileHarness.execution_mode == "async"

  # Enqueues `jobs` plain jobs, then drains them with a real Worker.
  # Returns {wall_s:, cpu_s:, gc_s:, jobs:} measured from worker start to the
  # last job's completion. With profile_path, the drain runs under vernier and
  # the result also carries :shares. A nil read_ahead is not passed at all, so
  # the bench also runs against a Worker that predates the keyword (#486).
  def drain(jobs:, threads:, read_ahead: nil, profile_path: nil)
    reset!
    enqueue(jobs)
    Pgbus.stopping = false
    options = { queues: [QUEUE], threads: threads }
    options[:read_ahead] = read_ahead unless read_ahead.nil?
    options[:execution_mode] = :async if async?
    worker = Pgbus::Process::Worker.new(**options)

    # A crash rescued in Worker#process_message counts as failed but not
    # processed, so wait on either: the check below reports it.
    RESETS.value = 0
    ar_pool = nil
    measurement = drive(worker, profile_path) do
      drained?(worker, jobs).tap { |done| ar_pool = ar_pool_stat if done }
    end
    ar_pool["shutdown_resets"] = RESETS.value - ar_pool["resets"]

    failed = worker.stats[:jobs_failed]
    raise "#{failed} jobs failed during the drain — the bench measured errors, not work" if failed.positive?

    measurement.merge(jobs: jobs, ar_pool: ar_pool)
  end

  # The ActiveRecord pool the moment the drain finished, before the worker's
  # shutdown reclaims anything (issue #649): busy is a connection a live
  # thread or fiber still holds, dead one whose owner ended without returning
  # it.
  def ar_pool_stat
    stat = ActiveRecord::Base.connection_pool.stat
    { "busy" => stat[:busy], "dead" => stat[:dead], "connections" => stat[:connections], "resets" => RESETS.value }
  end

  # Runs `process` (a Worker or a Consumer) on a named loop thread until the
  # block says the drain is done, then shuts it down gracefully. Shared with
  # ConsumerProfileRunner so both loops are measured the same way.
  def drive(process, profile_path)
    measurement = nil
    run = lambda do
      measurement = measure do
        # The worker loop under measurement, on its own thread on purpose;
        # graceful_shutdown ends it and it is joined right after.
        runner = Thread.new { process.run } # rubocop:disable ThreadSafety/NewThread
        runner.name = WorkerProfileHarness::LOOP_THREAD_NAME
        # sleep, not Thread.pass: a spinning watcher holds the GVL and would
        # show up as gvl_wait on every thread it is measuring.
        sleep(0.002) until yield
        process.graceful_shutdown
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
      measurement[:rails_sub_shares] = WorkerProfileHarness.rails_sub_shares(vernier_result)
    else
      run.call
    end
    measurement
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

  def enqueue(jobs, queue: QUEUE, shape: WorkerProfileHarness.job_shape)
    return enqueue_shaped(jobs, queue, shape) unless shape == "plain"

    payload = NoopJob.new(1).serialize
    jobs.times.each_slice(ENQUEUE_SLICE) do |slice|
      Pgbus.client.send_batch(queue, Array.new(slice.size) { payload })
    end
  end

  # unique and limited go out one by one (the adapter sends a job with a
  # lock or a slot on its own); batch puts each slice in one batch.
  def enqueue_shaped(jobs, queue, shape)
    job_class = SHAPED_JOBS.fetch(shape)
    jobs.times.each_slice(ENQUEUE_SLICE) do |slice|
      shaped = slice.map { |i| job_class.new(queue, i).set(queue: queue) }
      if shape == "batch"
        Pgbus.batch(description: BATCH_DESCRIPTION) { |b| b.add(*shaped) }
      else
        ::ActiveJob.perform_all_later(shaped)
      end
    end
    # The enqueue's own lease (the adapter's semaphore transaction, the batch
    # rows), so the drain's pool stat counts the worker's alone.
    ActiveRecord::Base.connection_pool.release_connection
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
