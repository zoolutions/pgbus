# frozen_string_literal: true

require "json"
require "rbconfig"
require_relative "worker_profile_runner"
require_relative "fleet_profile_harness"

# Drives the fleet cell of the worker profiling bench (issue #602): N worker
# processes draining ONE queue on one Postgres, the production shape
# (`processes: N`). Pure arithmetic and rendering live in FleetProfileHarness.
#
# The parent (the cell subprocess) bootstraps and fills the queue with
# jobs_per_process × N jobs, then spawns N drain children (the bench file again,
# with WPROF_DRAIN_CHILD set). Each child configures pgbus without purging,
# warms up on a private queue, builds its Worker, prints READY and blocks on
# stdin. Once every child is ready the parent resets pg_stat_statements for this
# database, stamps go_at and releases them all at once, so no child drains alone
# while a sibling is still booting. Each child drives its Worker until the
# shared queue is empty, and reports how many jobs it ran and when (wall clock,
# CLOCK_REALTIME: the one clock every process on the machine shares).
module FleetProfileRunner
  QUEUE = WorkerProfileRunner::QUEUE
  READY = "WPROF_CHILD_READY"
  CHILD_RESULT = "WPROF_CHILD_RESULT "
  WARMUP_JOBS = 500
  # The empty-queue check runs a count(*) on the queue table, so a child calls
  # it at most this often, and only once its own worker is idle.
  EMPTY_POLL_S = 0.05
  EMPTY_POLLS = 3

  STATEMENTS_SQL = <<~SQL
    SELECT query, calls, total_exec_time
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
      AND toplevel
      AND (query ILIKE '%pgmq.%' OR query ILIKE '%pgbus_%')
      AND query NOT ILIKE '%pgmq.metrics%'
    ORDER BY total_exec_time DESC
  SQL

  # What this database's active client backends (autovacuum excluded) are doing, sampled by the parent
  # while the children drain, as type:event ("LWLock:WALWrite"); "CPU" is
  # active with no wait event.
  WAITS_SQL = <<~SQL
    SELECT COALESCE(wait_event_type || ':' || wait_event, 'CPU') AS wait, count(*) AS n
    FROM pg_stat_activity
    WHERE datname = current_database() AND backend_type = 'client backend' AND state = 'active'
      AND pid <> pg_backend_pid()
    GROUP BY 1
  SQL
  WAIT_SAMPLE_S = 0.01

  module_function

  # The job threads, the worker's loop thread and the heartbeat ticker. pgbus's
  # resolved_pool_size would size it from configured workers, which the bench
  # never sets.
  def pool_size(threads) = threads + 2

  def realtime = Process.clock_gettime(Process::CLOCK_REALTIME)

  # Parent side. Returns the fleet cell's fields (string keys), or
  # {"skip_reason" => …} when the connection budget does not fit.
  def run_cell(url, processes:, jobs_per_process:, threads:, bench_file:, yjit:)
    pool = pool_size(threads)
    WorkerProfileRunner.setup!(url, threads: threads, pool_size: pool)
    conn = ActiveRecord::Base.connection
    free = FleetProfileHarness.free_connections(
      max_connections: Integer(conn.select_value("SHOW max_connections")),
      reserved: Integer(conn.select_value("SHOW superuser_reserved_connections")),
      in_use: client_backends(conn)
    )
    reason = FleetProfileHarness.budget_skip_reason(processes: processes, pool_size: pool, free_connections: free)
    return { "skip_reason" => reason } if reason

    pg_unavailable = pg_stat_unavailable_reason(conn)
    total = jobs_per_process * processes
    WorkerProfileRunner.enqueue(total)

    children = Array.new(processes) { |i| spawn_child(i, url, bench_file: bench_file, yjit: yjit) }
    children.each { |child| await_ready(child) }
    reset_statements(conn) unless pg_unavailable
    go_at = realtime
    children.each do |child|
      child[:stdin].puts("go")
      child[:stdin].close
    end
    waits = sample_waits_until_exit(conn, children)
    results = children.map { |child| collect(child) }

    verify!(results, total)
    measured = FleetProfileHarness.throughput(results, go_at: go_at)
    cost = pg_unavailable ? {} : pg_cost(conn, total, measured[:wall_s])

    {
      "jobs" => total, "wall_s" => measured[:wall_s], "jobs_per_s" => measured[:jobs_per_s],
      "start_skew_ms" => measured[:start_skew_ms], "pg_unavailable_reason" => pg_unavailable,
      "pg_top" => cost.fetch(:top, []), "pg_waits" => waits, "pg_ms_per_job" => cost[:ms_per_job],
      "pg_statements_per_job" => cost[:statements_per_job], "pg_busy_backends" => cost[:busy_backends]
    }
  end

  # Child side (WPROF_DRAIN_CHILD=<index>). Never purges the shared queue.
  def run_drain_child(url, index:, threads:, read_ahead:, input: $stdin, out: $stdout)
    WorkerProfileRunner.configure!(url, threads: threads, pool_size: pool_size(threads))
    warm_up(index, threads: threads, read_ahead: read_ahead)

    Pgbus.stopping = false
    worker = Pgbus::Process::Worker.new(**worker_options(QUEUE, threads, read_ahead))
    out.puts(READY)
    out.flush
    input.gets

    state = { started_at: realtime, count: 0, polled_at: 0.0, zeros: 0 }
    state[:finished_at] = state[:started_at]
    WorkerProfileRunner.drive(worker, nil) { drained?(worker, state) }

    stats = worker.stats
    out.puts(CHILD_RESULT + JSON.generate(
      "processed" => stats[:jobs_processed], "failed" => stats[:jobs_failed],
      "started_at" => state[:started_at], "finished_at" => state[:finished_at]
    ))
  end

  def worker_options(queue, threads, read_ahead)
    options = { queues: [queue], threads: threads }
    options[:read_ahead] = read_ahead unless read_ahead.nil?
    options
  end

  # A private queue per child, so YJIT has compiled the job path before the
  # timed drain, as the worker cell's warmup drain does.
  def warm_up(index, threads:, read_ahead:)
    queue = "wprof_warm_#{index}"
    Pgbus.client.ensure_queue(queue)
    Pgbus.client.purge_queue(queue)
    WorkerProfileRunner.enqueue(WARMUP_JOBS, queue: queue)
    Pgbus.stopping = false
    worker = Pgbus::Process::Worker.new(**worker_options(queue, threads, read_ahead))
    WorkerProfileRunner.drive(worker, nil) do
      worker.stats[:jobs_failed].positive? || worker.stats[:jobs_processed] >= WARMUP_JOBS
    end
    failed = worker.stats[:jobs_failed]
    raise "#{failed} warmup jobs failed in drain child #{index}" if failed.positive?
  end

  # The drain is over for this child when its worker has nothing in flight and
  # the shared queue read empty EMPTY_POLLS times in a row. finished_at is the
  # moment the child last saw its processed count move.
  def drained?(worker, state)
    stats = worker.stats
    now = realtime
    if stats[:jobs_processed] != state[:count]
      state[:count] = stats[:jobs_processed]
      state[:finished_at] = now
      state[:zeros] = 0
      return false
    end
    return true if stats[:jobs_failed].positive?
    return false unless stats[:in_flight].zero? && now - state[:polled_at] >= EMPTY_POLL_S

    state[:polled_at] = now
    state[:zeros] = Pgbus.client.metrics(QUEUE).queue_length.to_i.zero? ? state[:zeros] + 1 : 0
    state[:zeros] >= EMPTY_POLLS
  end

  def spawn_child(index, url, bench_file:, yjit:)
    out_r, out_w = IO.pipe
    in_r, in_w = IO.pipe
    env = { "WPROF_DRAIN_CHILD" => index.to_s, "WPROF_CHILD_URL" => url, "RUBY_YJIT_ENABLE" => nil }
    args = [RbConfig.ruby]
    args << "--yjit" if yjit
    args << bench_file
    pid = Process.spawn(env, *args, in: in_r, out: out_w, err: out_w)
    out_w.close
    in_r.close
    { index: index, pid: pid, stdout: out_r, stdin: in_w, output: +"" }
  end

  def await_ready(child)
    while (line = child[:stdout].gets)
      child[:output] << line
      return if line.chomp == READY
    end
    Process.wait(child[:pid])
    abort "drain child #{child[:index]} exited before it was ready:\n#{child[:output]}"
  end

  # Polls pg_stat_activity every WAIT_SAMPLE_S until every child has exited.
  # The children print only READY and their result line, so their stdout pipes
  # cannot fill while the parent is not reading them.
  def sample_waits_until_exit(conn, children)
    waits = Hash.new(0)
    samples = 0
    pending = children.dup
    until pending.empty?
      conn.select_rows(WAITS_SQL).each { |wait, n| waits[wait] += Integer(n) }
      samples += 1
      pending.reject! { |child| child[:status] = Process.wait2(child[:pid], Process::WNOHANG)&.last }
      sleep(WAIT_SAMPLE_S)
    end
    FleetProfileHarness.wait_shares(waits, samples: samples)
  end

  def collect(child)
    child[:output] << child[:stdout].read
    child[:stdout].close
    status = child[:status]
    line = child[:output].lines.reverse.find { |l| l.start_with?(CHILD_RESULT) }
    abort "drain child #{child[:index]} failed (#{status}):\n#{child[:output]}" unless line

    JSON.parse(line.delete_prefix(CHILD_RESULT))
  end

  def verify!(results, total)
    failed = results.sum { |r| r["failed"] }
    raise "#{failed} jobs failed during the fleet drain — the bench measured errors, not work" if failed.positive?

    processed = results.sum { |r| r["processed"] }
    raise "fleet processed #{processed} of #{total} jobs" unless processed == total

    left = Pgbus.client.metrics(QUEUE).queue_length.to_i
    raise "#{left} jobs left in the queue after the fleet drain" if left.positive?
  end

  def client_backends(conn)
    Integer(conn.select_value("SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'"))
  end

  # nil when pg_stat_statements is loaded and installed in this database,
  # else why not.
  def pg_stat_unavailable_reason(conn)
    libs = conn.select_value("SHOW shared_preload_libraries").to_s.split(",").map(&:strip)
    return "add it to shared_preload_libraries" unless libs.include?("pg_stat_statements")

    conn.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    nil
  rescue ActiveRecord::StatementInvalid => e
    "CREATE EXTENSION failed: #{e.message.lines.first.strip}"
  end

  # Scoped to this database, so a sibling bench on the same cluster keeps its stats.
  def reset_statements(conn)
    conn.select_value(
      "SELECT pg_stat_statements_reset(0::oid, (SELECT oid FROM pg_database WHERE datname = current_database()), 0::bigint)"
    )
  end

  def pg_cost(conn, jobs, wall_s)
    FleetProfileHarness.summarize_statements(conn.select_all(STATEMENTS_SQL).to_a, jobs: jobs, wall_s: wall_s)
  end
end
