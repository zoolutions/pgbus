# frozen_string_literal: true

# Worker profiling bench (issue #484): where does a job's wall time go?
#
# Drives a REAL Pgbus::Process::Worker draining a real PGMQ queue of no-op
# ActiveJob jobs, and reports jobs/s plus the pool-thread time split (waiting
# on Postgres, waiting for work, GVL wait, pgbus / driver / Rails / other Ruby
# CPU). See benchmarks/support/worker_profile_harness.rb for the buckets.
#
# Matrix: {local, proxied} x {YJIT on, YJIT off}. Each cell is its own
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
# (profiled drain, default 2000), WP_BENCH_THREADS (default 12).
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

def run_child(location, url)
  require_relative "support/worker_profile_runner"

  WorkerProfileRunner.setup!(url, threads: THREADS)
  WorkerProfileRunner.drain(jobs: WARMUP_JOBS, threads: THREADS)
  timed = WorkerProfileRunner.drain(jobs: JOBS, threads: THREADS)

  jit = WorkerProfileHarness.jit_label
  FileUtils.mkdir_p("tmp/benchmarks")
  profile_path = "tmp/benchmarks/worker_profile_#{location}_#{jit}.vernier.json"
  profiled = WorkerProfileRunner.drain(jobs: PROFILE_JOBS, threads: THREADS, profile_path: profile_path)

  cell = WorkerProfileHarness::Cell.new(
    location: location, jit: jit, jobs: JOBS,
    wall_s: timed[:wall_s], cpu_s: timed[:cpu_s], gc_s: timed[:gc_s],
    jobs_per_s: JOBS / timed[:wall_s], pool_shares: profiled[:pool_shares], loop_shares: profiled[:loop_shares],
    cpu_shares: profiled[:cpu_shares], profile_path: profile_path
  )
  puts RESULT_PREFIX + JSON.generate(cell.to_h)
end

def spawn_cell(location, url, yjit:)
  env = { "WPROF_CHILD" => location, "WPROF_CHILD_URL" => url, "RUBY_YJIT_ENABLE" => nil }
  args = [RbConfig.ruby]
  args << "--yjit" if yjit
  args << __FILE__
  output = IO.popen(env, args, err: %i[child out], &:read)
  line = output.lines.reverse.find { |l| l.start_with?(RESULT_PREFIX) }
  abort "cell #{location}/yjit=#{yjit} failed:\n#{output}" unless line

  JSON.parse(line.delete_prefix(RESULT_PREFIX))
end

def pct(value) = format("%5.1f%%", value * 100)

BUCKETS = %w[db_wait pool_wait idle gvl_wait pgbus driver rails ruby].freeze
CPU_BUCKETS = %w[pgbus driver rails ruby].freeze

def print_throughput(cells)
  puts
  puts "| cell | jobs/s | ms/job | CPU/wall | GC/wall |"
  puts "|---|---:|---:|---:|---:|"
  cells.each do |c|
    ms_per_job = c["wall_s"] * 1000 / c["jobs"]
    puts "| #{c["location"]} / #{c["jit"]} | #{c["jobs_per_s"].round} | #{format("%.3f", ms_per_job)} | " \
         "#{pct(c["cpu_s"] / c["wall_s"])} | #{pct(c["gc_s"] / c["wall_s"])} |"
  end
end

def print_split(cells, key, title, buckets = BUCKETS)
  puts
  puts "#{title}:"
  puts "| cell | #{buckets.join(" | ")} |"
  puts "|---|#{buckets.map { "---:" }.join("|")}|"
  cells.each do |c|
    puts "| #{c["location"]} / #{c["jit"]} | #{buckets.map { |b| pct(c[key][b]) }.join(" | ")} |"
  end
end

def print_table(cells)
  print_throughput(cells)
  print_split(cells, "pool_shares", "Job-pool threads (where each executing job's time goes)")
  print_split(cells, "loop_shares", "Worker main loop (reads batches, hands them to the pool)")
  print_split(cells, "cpu_shares", "On-CPU composition, all threads (whose code ran while holding the GVL)", CPU_BUCKETS)
  puts
  puts "jobs/s and ms/job come from the unprofiled drain (#{JOBS} jobs, #{THREADS} threads)."
  puts "The splits come from a separate vernier-profiled drain (#{PROFILE_JOBS} jobs)."
  puts "CPU/wall is process CPU time over wall time across all threads."
end

if (location = ENV.fetch("WPROF_CHILD", nil))
  run_child(location, ENV.fetch("WPROF_CHILD_URL"))
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
puts "Pgbus worker profile (real Worker, real PostgreSQL)"
puts "  Ruby #{RUBY_VERSION}, #{THREADS} threads, #{JOBS} timed jobs"
puts "  proxied cells: #{proxy_url ? "on" : "off (set PGBUS_BENCH_PROXY_URL)"}"
puts "=" * 70

cells = targets.flat_map do |location, url|
  [true, false].map { |yjit| spawn_cell(location, url, yjit: yjit) }
end
print_table(cells)
