# frozen_string_literal: true

require "concurrent"

# Execution-mode connection-consumption harness (issue #323).
#
# Runs the SAME offered workload through the threads and async execution pools
# and measures how many real Postgres connections each mode actually holds, so
# we can guide users on DB-pool sizing per execution_mode.
#
# THE HONEST FINDING this harness exists to prove/disprove: pgbus holds a pgmq
# connection only for the read/archive SQL round-trip, NOT the job body
# (executor.rb runs `perform_now` with zero pgmq connections). So async does NOT
# "save connections" by yielding mid-checkout — a fiber holds a connection for
# exactly one SQL round-trip, same as a thread. Async's only connection
# advantage is the fraction of wall time spent OUTSIDE `with_connection`
# (io_light). For DB-bound work, async is connection-bound just like threads and
# a tiny pool STARVES (surfacing as pool_timeouts). The io_profile cells prove
# which regime you're in.
#
# Pure, require-able module (no side effects on load, mirroring bench_support).
# The fairness/struct/measurement LOGIC is unit-tested with injected fakes (no
# DB); the actual numbers come from a real DB via run_cell in the bench script.
module ExecutionModeHarness
  # A single measured (mode × pool_size × concurrency × io_profile) cell.
  Result = Data.define(
    :mode,           # :threads | :async
    :pool_size,      # configured pgmq pool_size (the connection ceiling)
    :concurrency,    # offered concurrency = execution-pool capacity (threads / fibers)
    :io_profile,     # IoProfile
    :job_count,      # total jobs offered in the measured phase (SAME for both modes in a fair pair)
    :peak_busy,      # max(size - available) over samples — the headline metric
    :steady_busy,    # median(size - available) over samples
    :throughput,     # jobs/s over the measured phase
    :p50, :p95, :p99, # per-job wall latency, ms
    :pool_timeouts,  # count of pgmq pool-timeout errors (tallied, never swallowed)
    :other_errors,   # count of any other error (must be 0 in a clean row)
    :completed,      # jobs finished without error
    :wall_seconds
  ) do
    def under_provisioned? = pool_size < concurrency

    def self.headers
      %w[mode pool conc io_profile jobs peak_busy steady_busy thr/s p50 p95 p99 timeouts errs]
    end

    def to_row
      [mode, pool_size, concurrency, io_profile.label, job_count,
       peak_busy, steady_busy, throughput.round(1),
       p50.round(1), p95.round(1), p99.round(1), pool_timeouts, other_errors]
    end
  end

  # Two portions of a job's wall time:
  #   db_seconds    — INSIDE a pooled checkout (SELECT pg_sleep) → holds 1 conn, blocks the fiber
  #   yield_seconds — OUTSIDE any checkout (app I/O) → holds 0 pgmq conns; a fiber can share its slot here
  # db_seconds is kept >= 10ms (above the 5ms sampler interval) so peak_busy
  # can't undercount the checkout window.
  IoProfile = Data.define(:label, :db_seconds, :yield_seconds) do
    def self.io_light = new(label: "io_light", db_seconds: 0.010, yield_seconds: 0.040)
    def self.db_bound = new(label: "db_bound", db_seconds: 0.030, yield_seconds: 0.005)
  end

  # Raw result of driving `job_count` jobs through a pool.
  Outcome = Data.define(:latencies, :elapsed, :finished, :completed, :pool_timeouts, :other_errors)

  # Substring pgmq stamps into a ConnectionPool::TimeoutError message. Replicated
  # here (rather than calling Client#pool_timeout_error?, which is private) so the
  # harness tally can't drift from production classification.
  POOL_TIMEOUT_MARKER = "connection pool timeout"

  # Samples the pool's own counters (client.pool_stats → {size:, available:}).
  # busy = size - available is the EXACT live checkout count, race-free (an
  # in-memory read of the object under test) and process-scoped (immune to
  # foreign backends on a shared Postgres). This is why we sample the pool, not
  # pg_stat_activity.
  class PoolSampler
    def initialize(client, interval_s: 0.005)
      @client = client
      @interval = interval_s
      @busy = Concurrent::Array.new
    end

    attr_reader :interval

    # Records one stat sample. A degraded read ({}) is skipped, NOT counted as
    # busy: 0 (which would fabricate an idle sample). Public so the primitive is
    # unit-testable without a background thread; the harness's sampling thread
    # calls it every `interval` seconds during the measured phase.
    def record(stats)
      return if stats.nil? || stats.empty?

      @busy << (stats[:size] - stats[:available])
      nil
    end

    def summary
      xs = @busy.to_a
      { peak_busy: xs.max || 0, steady_busy: median(xs) }
    end

    private

    def median(values)
      return 0 if values.empty?

      sorted = values.sort
      mid = sorted.length / 2
      sorted.length.odd? ? sorted[mid] : ((sorted[mid - 1] + sorted[mid]) / 2.0).round
    end
  end

  module_function

  # Runs ONE cell against a real client and returns a Result. `sampler` is
  # injectable so specs can drive it deterministically without a DB.
  def run_cell(mode:, pool_size:, concurrency:, io_profile:, job_count:, client:,
               sampler: nil, warmup_jobs: nil, clock: Process)
    assert_dedicated_path!(client)
    sampler ||= PoolSampler.new(client)
    warmup_jobs ||= pool_size * 2
    pool = Pgbus::ExecutionPools.build(mode: mode, capacity: concurrency)
    sampler_thread = nil
    begin
      warmup!(pool: pool, client: client, io_profile: io_profile, warmup_jobs: warmup_jobs)
      sampler_thread = start_sampling(sampler, client)
      outcome = drive(pool: pool, client: client, io_profile: io_profile, job_count: job_count, clock: clock)
      summarize(mode: mode, pool_size: pool_size, concurrency: concurrency,
                io_profile: io_profile, job_count: job_count, sampler: sampler, outcome: outcome)
    ensure
      # Stop the sampler FIRST (else its thread polls pool_stats forever if
      # drive/summarize raised), then tear down the pool.
      stop_sampling(sampler_thread) if sampler_thread
      pool.shutdown
      pool.wait_for_termination(15)
    end
  end

  # Fairness invariant: two Results are only comparable if offered load matches.
  # The direct anti-rigging guard against unequal-load comparisons.
  def assert_fair_pair!(lhs, rhs)
    return if lhs.job_count == rhs.job_count && lhs.io_profile == rhs.io_profile &&
              lhs.concurrency == rhs.concurrency

    raise ArgumentError,
          "unfair comparison: offered load differs " \
          "(#{lhs.mode}=#{lhs.job_count}/#{lhs.concurrency}/#{lhs.io_profile.label} vs " \
          "#{rhs.mode}=#{rhs.job_count}/#{rhs.concurrency}/#{rhs.io_profile.label})"
  end

  def assert_dedicated_path!(client)
    return unless client.shared_connection?

    raise ArgumentError,
          "execution-mode bench requires the dedicated-connection path " \
          "(set config.database_url = PGBUS_DATABASE_URL); the shared-AR path forces " \
          "pool_size:1 and makes the threads-vs-async question meaningless"
  end

  # Tally a job error without swallowing it: pool timeouts (the under-provision
  # signal) vs everything else. Nothing is dropped — sum of tallies == errors.
  def classify_error!(error, timeouts, others)
    if error.message.to_s.downcase.include?(POOL_TIMEOUT_MARKER)
      timeouts.increment
    else
      others.increment
      Pgbus.logger.debug { "[em-harness] job error #{error.class}: #{error.message}" } if defined?(Pgbus.logger)
    end
    nil
  end

  # THE fair unit of work — identical bytes for threads & async, real pooled
  # checkout. yield_seconds is app I/O OUTSIDE any checkout (a fiber can share
  # its slot here); db_seconds runs INSIDE a real pooled checkout via the public
  # reader.
  #
  # A plain Kernel#sleep is correct in BOTH modes: under Async's fiber scheduler
  # it's intercepted (the kernel_sleep hook) and yields to the reactor, so other
  # fibers keep running; under threads it just blocks the thread. (Async::Task#sleep
  # is deprecated in favor of Kernel#sleep in async 2.x precisely because of this.)
  def run_job(client, io_profile)
    sleep(io_profile.yield_seconds) if io_profile.yield_seconds.positive?
    return unless io_profile.db_seconds.positive?

    client.pgmq.with_connection do |conn|
      conn.exec_params("SELECT pg_sleep($1)", [io_profile.db_seconds])
    end
  end

  # Posts EXACTLY job_count units through a BOUNDED in-flight window equal to the
  # pool capacity, so at most `capacity` jobs run concurrently in BOTH modes —
  # the fair definition of "offered concurrency = capacity". This also normalizes
  # a real behavioral difference: ThreadPool queues unlimited posts, but
  # AsyncPool#post RAISES once `capacity` units are in-flight (async_pool.rb
  # reserve_capacity!). Submitting behind a Concurrent::Semaphore sized to the
  # capacity keeps both pools at the same concurrency and never overflows async.
  # Records per-job wall latency, tallies errors (never swallows).
  def drive(pool:, client:, io_profile:, job_count:, clock: Process)
    latencies = Concurrent::Array.new
    timeouts  = Concurrent::AtomicFixnum.new(0)
    others    = Concurrent::AtomicFixnum.new(0)
    completed = Concurrent::AtomicFixnum.new(0)
    done      = Concurrent::CountDownLatch.new(job_count)
    slots     = Concurrent::Semaphore.new(pool.capacity)
    started   = clock.clock_gettime(Process::CLOCK_MONOTONIC)

    job_count.times do
      slots.acquire # block the submitter until a slot frees — bounds in-flight
      pool.post do
        t0 = clock.clock_gettime(Process::CLOCK_MONOTONIC)
        run_job(client, io_profile)
        latencies << ((clock.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0)
        completed.increment
      rescue StandardError => e
        classify_error!(e, timeouts, others)
      ensure
        slots.release
        done.count_down
      end
    end

    finished = done.wait(deadline_for(job_count, io_profile))
    elapsed  = clock.clock_gettime(Process::CLOCK_MONOTONIC) - started
    Outcome.new(latencies: latencies.to_a, elapsed: elapsed, finished: finished,
                completed: completed.value, pool_timeouts: timeouts.value, other_errors: others.value)
  end

  def summarize(mode:, pool_size:, concurrency:, io_profile:, job_count:, sampler:, outcome:)
    sorted = outcome.latencies.sort
    busy = sampler.summary
    Result.new(
      mode: mode, pool_size: pool_size, concurrency: concurrency, io_profile: io_profile,
      job_count: job_count, peak_busy: busy[:peak_busy], steady_busy: busy[:steady_busy],
      throughput: outcome.elapsed.positive? ? (outcome.completed / outcome.elapsed) : 0.0,
      p50: percentile(sorted, 0.50), p95: percentile(sorted, 0.95), p99: percentile(sorted, 0.99),
      pool_timeouts: outcome.pool_timeouts, other_errors: outcome.other_errors,
      completed: outcome.completed, wall_seconds: outcome.elapsed
    )
  end

  # Sorted-array percentile (pure; unit-tested against [1..100]).
  def percentile(sorted, fraction)
    return 0.0 if sorted.empty?

    idx = [(sorted.length * fraction).to_i, sorted.length - 1].min
    sorted[idx]
  end

  def deadline_for(job_count, io_profile)
    per = io_profile.db_seconds + io_profile.yield_seconds
    [(job_count * per) + 30, 180].min
  end

  # Drives warmup_jobs concurrently so connection_pool opens its working set
  # (lazy: first checkout pays TCP/TLS/auth). Warmup latencies are discarded.
  def warmup!(pool:, client:, io_profile:, warmup_jobs:)
    done  = Concurrent::CountDownLatch.new(warmup_jobs)
    slots = Concurrent::Semaphore.new(pool.capacity) # bound in-flight (async rejects overflow)
    warmup_jobs.times do
      slots.acquire
      pool.post do
        run_job(client, io_profile)
      rescue StandardError
        nil
      ensure
        slots.release
        done.count_down
      end
    end
    done.wait(60)
  end

  # Runs the sampler on its own thread for the measured phase.
  def start_sampling(sampler, client)
    stop = Concurrent::AtomicBoolean.new(false)
    # The sampler thread: the caller sets `stop` and joins it after the
    # measured phase.
    thread = Thread.new do # rubocop:disable ThreadSafety/NewThread
      until stop.true?
        sampler.record(client.pool_stats)
        sleep(sampler.interval)
      end
    end
    [thread, stop]
  end

  def stop_sampling(handle)
    thread, stop = handle
    stop.make_true
    thread&.join(2)
  end
end
