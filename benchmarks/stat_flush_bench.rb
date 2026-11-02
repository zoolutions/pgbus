# frozen_string_literal: true

# Job stats flush benchmark (issue #622) — real PostgreSQL.
#
#   PGBUS_DATABASE_URL=postgres://localhost:5432/pgbus_test bundle exec rake bench:one[stat_flush_bench]
#
# Times one StatBuffer flush of STAT_BENCH_ROWS rows (default 100, the
# stats_flush_size default) shaped like a worker's: three job classes, two
# queues, mostly successes, a nil enqueue latency now and then. Reports wall
# i/s (the INSERT round trip included), Ruby CPU per flush (what a GVL-bound
# worker pays) and objects allocated per flush. The table is emptied between
# measurements so its size stays flat.

require "json"
require "uri"
require "active_record"
require "pgbus"
require_relative "bench_support"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://localhost:5432/pgbus_test"
end
ROWS = Integer(ENV.fetch("STAT_BENCH_ROWS", "100"))
CPU_FLUSHES = 500

ActiveRecord::Base.establish_connection(DATABASE_URL)
Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.logger = Logger.new(IO::NULL)
  c.stats_enabled = true
end

conn = ActiveRecord::Base.connection
conn.execute(<<~SQL)
  CREATE TABLE IF NOT EXISTS pgbus_job_stats (
    id BIGSERIAL PRIMARY KEY, job_class VARCHAR NOT NULL, queue_name VARCHAR NOT NULL,
    status VARCHAR NOT NULL, duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, enqueue_latency_ms BIGINT, retry_count INTEGER DEFAULT 0
  )
SQL
# The migration's indexes (add_job_stats, add_job_stats_latency), so the
# INSERT's wall time includes their upkeep as on a real install.
conn.execute(<<~SQL)
  CREATE INDEX IF NOT EXISTS idx_pgbus_job_stats_time ON pgbus_job_stats (created_at);
  CREATE INDEX IF NOT EXISTS idx_pgbus_job_stats_class_time ON pgbus_job_stats (job_class, created_at);
  CREATE INDEX IF NOT EXISTS idx_pgbus_job_stats_status_time ON pgbus_job_stats (status, created_at);
  CREATE INDEX IF NOT EXISTS idx_pgbus_job_stats_queue_time ON pgbus_job_stats (queue_name, created_at);
SQL
missing = %w[enqueue_latency_ms retry_count].reject { |column| conn.column_exists?(:pgbus_job_stats, column) }
abort "[bench] pgbus_job_stats lacks #{missing.join(", ")}: the bench would time the base-column fallback" if missing.any?
conn.execute("TRUNCATE pgbus_job_stats")

classes = %w[OrderMailerJob ChargeCardJob Reports::NightlyJob].freeze
queues = %w[pgbus_default pgbus_mailers].freeze
ENTRIES = Array.new(ROWS) do |i|
  { job_class: classes[i % 3], queue_name: queues[i % 2], status: (i % 17).zero? ? "failed" : "success",
    duration_ms: 3 + (i % 40), enqueue_latency_ms: (i % 11).zero? ? nil : 120 + i, retry_count: i % 5 == 4 ? 1 : 0 }
    .freeze
end.freeze

buffer = Pgbus::StatBuffer.new(flush_size: ROWS + 1, flush_interval: 3600)
flush = lambda do
  ENTRIES.each { |entry| buffer.push(entry) }
  buffer.flush
end

flush.call
written = conn.select_value("SELECT COUNT(*) FROM pgbus_job_stats").to_i
abort "[bench] the flush wrote #{written} rows, not #{ROWS}: the bench would time a failing flush" unless written == ROWS
20.times { flush.call }
conn.execute("TRUNCATE pgbus_job_stats")

puts "=" * 60
puts "Pgbus StatBuffer flush (#{ROWS} rows per flush, real PostgreSQL)"
yjit = defined?(RubyVM::YJIT) && RubyVM::YJIT.enabled? ? "on" : "off"
puts "Ruby #{RUBY_VERSION}, YJIT #{yjit}, " \
     "ActiveRecord #{ActiveRecord::VERSION::STRING}"
puts "=" * 60

BenchSupport.header("Wall time per flush (push #{ROWS} + flush, INSERT round trip included)")
BenchSupport.ips(time: 5, warmup: 2) do |x|
  x.report("flush #{ROWS} rows") { flush.call }
end
conn.execute("TRUNCATE pgbus_job_stats")

BenchSupport.header("Ruby CPU per flush (#{CPU_FLUSHES} flushes)")
cpu_start = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID)
wall_start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
CPU_FLUSHES.times { flush.call }
cpu = Process.clock_gettime(Process::CLOCK_PROCESS_CPUTIME_ID) - cpu_start
wall = Process.clock_gettime(Process::CLOCK_MONOTONIC) - wall_start
puts format("  CPU %<cpu>.1f µs/flush (%<per_row>.2f µs/row), wall %<wall>.1f µs/flush",
            cpu: cpu / CPU_FLUSHES * 1e6, per_row: cpu / CPU_FLUSHES / ROWS * 1e6, wall: wall / CPU_FLUSHES * 1e6)
conn.execute("TRUNCATE pgbus_job_stats")

BenchSupport.header("Allocations per flush (100 flushes)")
report = BenchSupport.allocations("100 flushes") { 100.times { flush.call } }
puts format("  per flush: ~%<objects>d objects, ~%<bytes>d bytes, retained %<retained>d objects",
            objects: report.total_allocated / 100, bytes: report.total_allocated_memsize / 100,
            retained: report.total_retained / 100)
conn.execute("TRUNCATE pgbus_job_stats")
