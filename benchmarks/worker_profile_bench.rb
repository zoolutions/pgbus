# frozen_string_literal: true

# Worker profiling bench (issue #484): where does a job's wall time go?
#
# Drives a REAL Pgbus::Process::Worker draining a real PGMQ queue of no-op
# ActiveJob jobs, and reports jobs/s plus the pool-thread time split (waiting
# on Postgres, waiting for work, GVL wait, pgbus / driver / Rails / other Ruby
# CPU). See benchmarks/support/worker_profile_harness.rb for the buckets.
#
# Issue #486 adds a consumer cell: the same harness over a real
# Pgbus::Process::Consumer draining an event-bus topic queue with a plain
# (not idempotent!) handler, so the transport is measured, not the dedup table.
#
# Issue #602 adds a fleet cell (opt-in: WP_BENCH_ROLE=fleet or all): N worker
# processes draining ONE queue on one Postgres, the production shape
# (`processes: N`). It reports jobs/s per process count and the Postgres cost
# per job from pg_stat_statements (scoped to this database; prints n/a when the
# extension is not in shared_preload_libraries). Each drain child sets
# pool_size = threads + 2 (job threads, the loop thread, the heartbeat ticker)
# explicitly, so N processes need N × (threads + 2) connections: a row whose
# N × pool_size + 10 exceeds the free connections (max_connections minus the
# superuser reserve and the client backends already connected) is skipped with
# the reason. WP_BENCH_PROCESSES defaults to 1,2,4; 8 × 14 = 112 does not fit a
# stock max_connections = 100, so 8 is opt-in. WP_BENCH_JOBS is per process.
# No vernier profile for the fleet cell: the worker cell already shows one.
#
# Matrix: {worker, consumer, fleet} x {local, proxied} x {YJIT on, YJIT off}. Each cell is its own
# subprocess (one global Pgbus config, one JIT setting per process). The
# proxied cells run only when PGBUS_BENCH_PROXY_URL points at Postgres behind
# toxiproxy with added latency, which is the production shape:
#
#   toxiproxy-server &
#   toxiproxy-cli create -l 127.0.0.1:15432 -u 127.0.0.1:5432 pg
#   toxiproxy-cli toxic add -t latency -a latency=1 pg      # +1 ms per round trip (downstream)
#   PGBUS_DATABASE_URL=postgres://localhost:5432/pgbus_test \
#   PGBUS_BENCH_PROXY_URL=postgres://127.0.0.1:15432/pgbus_test \
#   bundle exec rake bench:worker_profile
#
# Knobs: WP_BENCH_JOBS (timed drain, default 5000), WP_BENCH_PROFILE_JOBS
# (profiled drain, default 2000), WP_BENCH_THREADS (default 12),
# WP_BENCH_ROLE (worker, consumer, both, fleet or all; default both),
# WP_BENCH_PROCESSES (fleet only, default 1,2,4), WP_BENCH_READ_AHEAD
# (a non-negative Integer passed as read_ahead: to the Worker/Consumer; unset
# means the keyword is not passed at all).
# Raw vernier profiles land in tmp/benchmarks/ (open in profiler.firefox.com).
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.

require "json"
require "fileutils"
require "rbconfig"
require_relative "support/fleet_profile_harness"

RESULT_PREFIX = "WPROF_RESULT "
JOBS = Integer(ENV.fetch("WP_BENCH_JOBS", "5000"))
PROFILE_JOBS = Integer(ENV.fetch("WP_BENCH_PROFILE_JOBS", "2000"))
THREADS = Integer(ENV.fetch("WP_BENCH_THREADS", "12"))
WARMUP_JOBS = 500
READ_AHEAD = ENV["WP_BENCH_READ_AHEAD"]&.then { |v| Integer(v) }
ROLES = { "worker" => %w[worker], "consumer" => %w[consumer], "both" => %w[worker consumer],
          "fleet" => %w[fleet], "all" => %w[worker consumer fleet] }.freeze
ROLE = ENV.fetch("WP_BENCH_ROLE", "both")
{ "WP_BENCH_JOBS" => JOBS, "WP_BENCH_PROFILE_JOBS" => PROFILE_JOBS, "WP_BENCH_THREADS" => THREADS }.each do |name, value|
  abort "#{name} must be a positive integer (got #{value})" unless value.positive?
end
abort "WP_BENCH_READ_AHEAD must be a non-negative integer (got #{READ_AHEAD})" if READ_AHEAD&.negative?
abort "WP_BENCH_ROLE must be one of #{ROLES.keys.join(", ")} (got #{ROLE})" unless ROLES.key?(ROLE)
PROCESSES = begin
  FleetProfileHarness.parse_processes(ENV.fetch("WP_BENCH_PROCESSES", FleetProfileHarness::DEFAULT_PROCESSES))
rescue ArgumentError => e
  abort e.message
end

def runner_for(role)
  if role == "consumer"
    require_relative "support/consumer_profile_runner"
    ConsumerProfileRunner
  else
    require_relative "support/worker_profile_runner"
    WorkerProfileRunner
  end
end

def run_child(role, location, url)
  runner = runner_for(role)

  runner.setup!(url, threads: THREADS)
  runner.drain(jobs: WARMUP_JOBS, threads: THREADS, read_ahead: READ_AHEAD)
  timed = runner.drain(jobs: JOBS, threads: THREADS, read_ahead: READ_AHEAD)

  jit = WorkerProfileHarness.jit_label
  FileUtils.mkdir_p("tmp/benchmarks")
  profile_path = "tmp/benchmarks/#{role}_profile_#{location}_#{jit}.vernier.json"
  profiled = runner.drain(jobs: PROFILE_JOBS, threads: THREADS, read_ahead: READ_AHEAD, profile_path: profile_path)

  cell = WorkerProfileHarness::Cell.new(
    role: role, location: location, jit: jit, jobs: JOBS,
    wall_s: timed[:wall_s], cpu_s: timed[:cpu_s], gc_s: timed[:gc_s],
    jobs_per_s: JOBS / timed[:wall_s], pool_shares: profiled[:pool_shares], loop_shares: profiled[:loop_shares],
    cpu_shares: profiled[:cpu_shares], profile_path: profile_path
  )
  puts RESULT_PREFIX + JSON.generate(cell.to_h)
end

def run_fleet_child(location, url, processes)
  require_relative "support/fleet_profile_runner"
  jit = WorkerProfileHarness.jit_label
  # The drain children read WP_BENCH_THREADS / WP_BENCH_READ_AHEAD from the environment they inherit.
  fields = FleetProfileRunner.run_cell(url, processes: processes, jobs_per_process: JOBS, threads: THREADS,
                                            bench_file: __FILE__, yjit: jit == "yjit")
  cell = WorkerProfileHarness::Cell.new(role: "fleet", location: location, jit: jit, processes: processes)
  puts RESULT_PREFIX + JSON.generate(cell.to_h.merge(fields))
end

def spawn_cell(role, location, url, yjit:, processes: nil)
  env = { "WPROF_CHILD" => location, "WPROF_CHILD_ROLE" => role, "WPROF_CHILD_URL" => url, "RUBY_YJIT_ENABLE" => nil,
          "WPROF_CHILD_PROCESSES" => processes&.to_s }
  args = [RbConfig.ruby]
  args << "--yjit" if yjit
  args << __FILE__
  output = IO.popen(env, args, err: %i[child out], &:read)
  line = output.lines.reverse.find { |l| l.start_with?(RESULT_PREFIX) }
  abort "cell #{role}/#{location}/yjit=#{yjit}#{"/processes=#{processes}" if processes} failed:\n#{output}" unless line

  JSON.parse(line.delete_prefix(RESULT_PREFIX))
end

def pct(value) = format("%5.1f%%", value * 100)
def label(cell) = "#{cell["role"]} / #{cell["location"]} / #{cell["jit"]}"

BUCKETS = %w[db_wait pool_wait idle gvl_wait pgbus driver rails ruby].freeze
CPU_BUCKETS = %w[pgbus driver rails ruby].freeze

def print_throughput(cells)
  puts
  puts "| cell | jobs/s | ms/job | CPU/wall | GC/wall |"
  puts "|---|---:|---:|---:|---:|"
  cells.each do |c|
    ms_per_job = c["wall_s"] * 1000 / c["jobs"]
    puts "| #{label(c)} | #{c["jobs_per_s"].round} | #{format("%.3f", ms_per_job)} | " \
         "#{pct(c["cpu_s"] / c["wall_s"])} | #{pct(c["gc_s"] / c["wall_s"])} |"
  end
end

def print_split(cells, key, title, buckets = BUCKETS)
  puts
  puts "#{title}:"
  puts "| cell | #{buckets.join(" | ")} |"
  puts "|---|#{buckets.map { "---:" }.join("|")}|"
  cells.each do |c|
    puts "| #{label(c)} | #{buckets.map { |b| pct(c[key][b]) }.join(" | ")} |"
  end
end

def print_fleet_table(cells)
  puts
  puts "Fleet: N worker processes draining one queue (#{JOBS} jobs per process, #{THREADS} threads each, " \
       "pool_size #{THREADS + 2})"
  puts FleetProfileHarness.fleet_table(cells)
  puts
  puts "Top statements by total execution time (pg_stat_statements, this database, top-level):"
  puts FleetProfileHarness.statement_lines(cells)
  puts
  puts "jobs/s = total jobs / (last job finished in any child - the parent's start signal); every child"
  puts "warmed up on its own queue and waited at a start barrier. PG ms/job = Σ total_exec_time / jobs and"
  puts "statements/job = Σ calls / jobs over pgmq.* and pgbus_* statements (pgmq.metrics polls excluded);"
  puts "PG busy backends = Σ total_exec_time / wall, the average number of backends executing them."
end

def print_table(cells)
  fleet, profiled = cells.partition { |c| c["role"] == "fleet" }
  print_profile_table(profiled) if profiled.any?
  print_fleet_table(fleet) if fleet.any?
end

def print_profile_table(cells)
  print_throughput(cells)
  print_split(cells, "pool_shares", "Job-pool threads (where each executing job's time goes)")
  print_split(cells, "loop_shares", "Main loop (reads batches, hands them to the pool)")
  print_split(cells, "cpu_shares", "On-CPU composition, all threads (whose code ran while holding the GVL)", CPU_BUCKETS)
  puts
  puts "jobs/s and ms/job come from the unprofiled drain (#{JOBS} jobs, #{THREADS} threads, " \
       "read_ahead #{READ_AHEAD.nil? ? "not passed" : READ_AHEAD})."
  puts "The splits come from a separate vernier-profiled drain (#{PROFILE_JOBS} jobs)."
  puts "CPU/wall is process CPU time over wall time across all threads."
end

# A fleet drain child (issue #602) is checked first: it inherits nothing from a
# cell's environment, and must never purge the shared queue.
if (index = ENV.fetch("WPROF_DRAIN_CHILD", nil))
  require_relative "support/fleet_profile_runner"
  FleetProfileRunner.run_drain_child(ENV.fetch("WPROF_CHILD_URL"), index: Integer(index), threads: THREADS,
                                                                   read_ahead: READ_AHEAD)
  exit 0
end

if (location = ENV.fetch("WPROF_CHILD", nil))
  if ENV.fetch("WPROF_CHILD_ROLE") == "fleet"
    run_fleet_child(location, ENV.fetch("WPROF_CHILD_URL"), Integer(ENV.fetch("WPROF_CHILD_PROCESSES")))
  else
    run_child(ENV.fetch("WPROF_CHILD_ROLE"), location, ENV.fetch("WPROF_CHILD_URL"))
  end
  exit 0
end

database_url = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: PGBUS_DATABASE_URL=postgres://localhost:5432/pgbus_test " \
        "bundle exec rake bench:worker_profile"
end
proxy_url = ENV.fetch("PGBUS_BENCH_PROXY_URL", nil)

targets = [["local", database_url]]
targets << ["proxied", proxy_url] if proxy_url

puts "=" * 70
puts "Pgbus worker profile (real Worker / Consumer, real PostgreSQL)"
puts "  Ruby #{RUBY_VERSION}, #{THREADS} threads, #{JOBS} timed jobs, roles: #{ROLES.fetch(ROLE).join(", ")}"
puts "  read_ahead: #{READ_AHEAD.nil? ? "not passed" : READ_AHEAD}"
puts "  proxied cells: #{proxy_url ? "on" : "off (set PGBUS_BENCH_PROXY_URL)"}"
puts "  fleet processes: #{PROCESSES.join(", ")}" if ROLES.fetch(ROLE).include?("fleet")
puts "=" * 70

cells = ROLES.fetch(ROLE).flat_map do |role|
  targets.flat_map do |location, url|
    [true, false].flat_map do |yjit|
      next [spawn_cell(role, location, url, yjit: yjit)] unless role == "fleet"

      PROCESSES.map { |processes| spawn_cell(role, location, url, yjit: yjit, processes: processes) }
    end
  end
end
print_table(cells)
