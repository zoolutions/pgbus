# frozen_string_literal: true

# Connection Pool Benchmark
# Measures peak active Postgres connections under varying concurrency
# for ThreadPool vs AsyncPool execution modes.
#
# NOTE: this bench opens a RAW PG.connect per task and offers UNEQUAL load per
# mode, so its "peak connections" reflects raw connect behavior, not how a
# shared pool is consumed. For the fair, pool-aware threads-vs-async
# connection-consumption comparison (equal offered load, real pool checkout,
# sizing guidance), use `benchmarks/execution_modes_bench.rb`
# (`rake bench:execution_modes`) — see docs/performance.md "Execution mode and
# DB pool sizing". This bench is kept for its raw peak-connection matrix.
#
# REQUIRES: PGBUS_DATABASE_URL environment variable
#
# Usage:
#   PGBUS_DATABASE_URL=postgres://user@localhost/pgbus_test ruby benchmarks/connection_pool_bench.rb

require "json"
require "securerandom"
require "concurrent"
require "pgbus"
require "pg"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost:5432/pgbus_test ruby benchmarks/connection_pool_bench.rb"
  exit 1
end

Pgbus.configure do |c|
  c.logger = Logger.new(IO::NULL)
  c.queue_prefix = "pgbus_connbench"
  c.default_queue = "default"
  c.stats_enabled = false
  c.database_url = DATABASE_URL
end

def monitor_connection(url)
  PG.connect(url)
end

def count_active_connections(conn, prefix = "pgbus_connbench")
  pattern = "%#{prefix}%"
  result = conn.exec_params(
    "SELECT count(*) FROM pg_stat_activity " \
    "WHERE application_name LIKE $1 OR query LIKE $1",
    [pattern]
  )
  result[0]["count"].to_i
rescue PG::Error => e
  warn "Connection count query failed: #{e.message}"
  0
end

def update_peak(peak, current)
  old = peak.value
  peak.compare_and_set(old, current) if current > old
end

def measure_peak_connections(mode:, capacity:, tasks:, monitor_conn:)
  pool = if mode == :threads
           Pgbus::ExecutionPools::ThreadPool.new(capacity: capacity)
         else
           Pgbus::ExecutionPools::AsyncPool.new(capacity: capacity)
         end

  peak = Concurrent::AtomicFixnum.new(0)
  done = Concurrent::CountDownLatch.new(tasks)

  # Monitor thread samples pg_stat_activity during the benchmark
  stop_monitoring = Concurrent::AtomicBoolean.new(false)
  # The bench's own sampler thread: stop_monitoring ends it and it is
  # joined before the results are read.
  monitor = Thread.new do # rubocop:disable ThreadSafety/NewThread
    while stop_monitoring.false?
      current = count_active_connections(monitor_conn)
      update_peak(peak, current)
      sleep 0.01
    end
  end

  tasks.times do
    pool.post do
      # Simulate a real job: query the database
      conn = PG.connect(DATABASE_URL)
      conn.exec("SELECT pg_sleep(0.01)")
      conn.close
      done.count_down
    rescue PG::Error
      done.count_down
    end
  end

  done.wait(30)
  stop_monitoring.make_true
  monitor.join(2)

  pool.shutdown
  pool.wait_for_termination(10)

  peak.value
end

puts "=" * 70
puts "Connection Pool Benchmark"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "=" * 70

monitor_conn = monitor_connection(DATABASE_URL)

configs = [
  { mode: :threads, capacity: 5, tasks: 20 },
  { mode: :threads, capacity: 10, tasks: 40 },
  { mode: :threads, capacity: 25, tasks: 100 },
  { mode: :async, capacity: 50, tasks: 200 },
  { mode: :async, capacity: 100, tasks: 400 }
]

header = ["Mode", "Capacity", "Tasks", "Peak Connections"]
puts format("\n%-12s  %-10s  %-8s  %-20s", *header)
puts "-" * 55

configs.each do |cfg|
  peak = measure_peak_connections(
    mode: cfg[:mode],
    capacity: cfg[:capacity],
    tasks: cfg[:tasks],
    monitor_conn: monitor_conn
  )
  puts format("%-12s  %-10d  %-8d  %-20d", cfg[:mode], cfg[:capacity], cfg[:tasks], peak)
  sleep 1 # let connections drain between runs
end

monitor_conn.close

puts "\nExpected: async mode should show significantly fewer peak connections"
puts "than thread mode, regardless of fiber capacity."
puts "\nDone."
