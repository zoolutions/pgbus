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
# Matrix: {worker, consumer} x {local, proxied} x {YJIT on, YJIT off}. Each cell is its own
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
# WP_BENCH_ROLE (worker, consumer or both; default both), WP_BENCH_READ_AHEAD
# (a non-negative Integer passed as read_ahead: to the Worker/Consumer; unset
# means the keyword is not passed at all).
# Raw vernier profiles land in tmp/benchmarks/ (open in profiler.firefox.com).
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.

require "json"
require "fileutils"
require "rbconfig"

RESULT_PREFIX = "WPROF_RESULT "
JOBS = Integer(ENV.fetch("WP_BENCH_JOBS", "5000"))
PROFILE_JOBS = Integer(ENV.fetch("WP_BENCH_PROFILE_JOBS", "2000"))
THREADS = Integer(ENV.fetch("WP_BENCH_THREADS", "12"))
WARMUP_JOBS = 500
READ_AHEAD = ENV["WP_BENCH_READ_AHEAD"]&.then { |v| Integer(v) }
ROLES = { "worker" => %w[worker], "consumer" => %w[consumer], "both" => %w[worker consumer] }.freeze
ROLE = ENV.fetch("WP_BENCH_ROLE", "both")
{ "WP_BENCH_JOBS" => JOBS, "WP_BENCH_PROFILE_JOBS" => PROFILE_JOBS, "WP_BENCH_THREADS" => THREADS }.each do |name, value|
  abort "#{name} must be a positive integer (got #{value})" unless value.positive?
end
abort "WP_BENCH_READ_AHEAD must be a non-negative integer (got #{READ_AHEAD})" if READ_AHEAD&.negative?
abort "WP_BENCH_ROLE must be one of #{ROLES.keys.join(", ")} (got #{ROLE})" unless ROLES.key?(ROLE)

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

def spawn_cell(role, location, url, yjit:)
  env = { "WPROF_CHILD" => location, "WPROF_CHILD_ROLE" => role, "WPROF_CHILD_URL" => url, "RUBY_YJIT_ENABLE" => nil }
  args = [RbConfig.ruby]
  args << "--yjit" if yjit
  args << __FILE__
  output = IO.popen(env, args, err: %i[child out], &:read)
  line = output.lines.reverse.find { |l| l.start_with?(RESULT_PREFIX) }
  abort "cell #{role}/#{location}/yjit=#{yjit} failed:\n#{output}" unless line

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

def print_table(cells)
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

if (location = ENV.fetch("WPROF_CHILD", nil))
  run_child(ENV.fetch("WPROF_CHILD_ROLE"), location, ENV.fetch("WPROF_CHILD_URL"))
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
puts "=" * 70

cells = ROLES.fetch(ROLE).flat_map do |role|
  targets.flat_map do |location, url|
    [true, false].map { |yjit| spawn_cell(role, location, url, yjit: yjit) }
  end
end
print_table(cells)
