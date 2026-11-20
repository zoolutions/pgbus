# frozen_string_literal: true

# Web::DataSource#queues_with_metrics against a real database (issue #677):
# how long one read of every queue's metrics takes at 100, 1 000 and 5 000
# queues, one message each. Before #677 the read was one statement whose cost
# grew with the queue count until a statement_timeout cancelled it.
#
#   PGBUS_DATABASE_URL=postgres://localhost/pgbus_bench bundle exec ruby benchmarks/queue_metrics_bench.rb
#   QUEUE_METRICS_BENCH_SIZES=100,1000 ...   # a subset
#
# Only queues named qmbench_* are created, and they are dropped before seeding
# and after the run.

require "active_record"
require "uri"
require "pgbus"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://localhost/pgbus_bench"
end

SIZES = ENV.fetch("QUEUE_METRICS_BENCH_SIZES", "100,1000,5000").split(",").map { |s| Integer(s) }
SAMPLES = 5
PREFIX = "qmbench"

ActiveRecord::Base.establish_connection(DATABASE_URL)
ActiveRecord::Base.logger = nil
Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = false
  c.stats_enabled = false
end

CONN = ActiveRecord::Base.connection
CONN.execute("SET client_min_messages = warning")
Pgbus.client.ensure_queue("default")

def bench_queues
  CONN.select_values("SELECT queue_name FROM pgmq.meta WHERE queue_name LIKE '#{PREFIX}\\_%' ORDER BY queue_name")
end

def drop_bench_queues!
  bench_queues.each { |name| CONN.select_value("SELECT pgmq.drop_queue(#{CONN.quote(name)})") }
end

def grow_to!(count)
  existing = bench_queues.size
  (existing...count).each do |i|
    name = format("#{PREFIX}_%05d", i)
    CONN.select_value("SELECT pgmq.create(#{CONN.quote(name)})")
    CONN.select_value("SELECT pgmq.send(#{CONN.quote(name)}, '{}'::jsonb)")
  end
end

def median_ms(&)
  times = Array.new(SAMPLES) do
    t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t) * 1000
  end
  times.sort[SAMPLES / 2]
end

begin
  drop_bench_queues!
  puts "queues_with_metrics, median of #{SAMPLES} (one message per queue)"
  puts "queues           ms     rows"
  SIZES.sort.each do |size|
    grow_to!(size)
    rows = 0
    ms = median_ms { rows = Pgbus::Web::DataSource.new.queues_with_metrics.size }
    puts format("%-8d %10.1f %8d", size, ms, rows)
  end
ensure
  drop_bench_queues!
end
