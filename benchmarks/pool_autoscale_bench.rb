# frozen_string_literal: true

# Streams-pool autoscaler benchmark (issue #323).
#
# The autoscaler runs as a periodic maintenance check (default every 5 min) on
# the streamer's idle LISTEN connection — a pghero capture_query_stats-style
# task, not a busy loop. This bench measures against a real Postgres:
#   (1) check cost — one headroom query + decision, so we can confirm it's cheap
#       enough to run on the Listener's idle window;
#   (2) grow-under-saturation — that a saturated streams pool grows into headroom
#       over a few checks, bounded by streams_pool_max.
#
# HONEST FRAMING: this measures the check's cost + that it reacts, NOT a per-op
# speedup. Whether autoscaling beats a well-sized static pool is a burstiness
# question (#324: a fixed small pool SERIALISES under a burst — it does not
# error). For steady load, a larger static streams_pool_size is simpler.
#
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:pool_autoscale

require "logger"
require "concurrent"
require "pgbus"
require "pg"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost/pgbus_test bundle exec rake bench:pool_autoscale"
  exit 1
end

MIN_FREE_BACKENDS = 20
STREAM = "autoscale_#{Process.pid}".freeze

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def free_backends
  conn = PG.connect(DATABASE_URL)
  total = conn.exec("SHOW max_connections").first["max_connections"].to_i
  used  = conn.exec("SELECT count(*) AS n FROM pg_stat_activity").first["n"].to_i
  total - used
ensure
  conn&.close
end

def build_client
  config = Pgbus::Configuration.new.tap do |c|
    c.database_url = DATABASE_URL
    c.queue_prefix = "pgbus_autoscale"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.streams_pool_size = 3
    c.streams_pool_max = 10
    c.streams_pool_timeout = 2
  end
  Pgbus::Client.new(config, schema_ensured: true)
end

puts "=" * 76
puts "Streams-pool autoscaler benchmark (issue #323)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "=" * 76

if free_backends < MIN_FREE_BACKENDS
  warn "Only #{free_backends} Postgres backends free (need >= #{MIN_FREE_BACKENDS}); free some and retry."
  exit 1
end

client = build_client
client.ensure_stream_queue(STREAM)
autoscaler = Pgbus::Streams::PoolAutoscaler.new(client: client, config: client.config, logger: client.config.logger)
maintenance = Pgbus::Streams::PoolAutoscaler::Maintenance.new(
  autoscaler: autoscaler, interval: 0, application_name_prefix: client.config.streams_application_name
)

# One check = the Listener would run maintenance.run(listen_conn); here a
# standalone connection stands in for the idle LISTEN connection.
probe_conn = PG.connect(DATABASE_URL)
def check(maintenance, conn) = maintenance.run(conn)

# --- (1) check cost: idle pool, measure headroom query + decision ---
5.times { check(maintenance, probe_conn) }
iters = 200
t0 = monotonic
iters.times { check(maintenance, probe_conn) }
per_check_ms = ((monotonic - t0) / iters) * 1000.0

# --- (2) grow-under-saturation: CHURN the pool (re-checkout each op so the load
# follows the pool across a hot-swap), run checks, watch it grow step by step ---
stop = Concurrent::AtomicBoolean.new(false)
# Load generators, on purpose: `stop` ends them and each is joined below.
churn = Array.new(12) do
  Thread.new do # rubocop:disable ThreadSafety/NewThread
    until stop.true?
      begin
        client.send(:streams_pool).with_connection { |c| c.exec("SELECT pg_sleep(0.05)") }
      rescue StandardError
        # pool timeout under contention is expected
      end
    end
  end
end
sleep 0.3 # let the churn saturate

sizes = []
10.times do
  check(maintenance, probe_conn)
  size = client.streams_pool_stats[:size]
  sizes << size
  break if size >= 10

  sleep 0.1 # let churn re-saturate the freshly-grown pool
end
stop.make_true
churn.each { |t| t.join(5) }

final = client.streams_pool_stats[:size]
swaps = client.streams_swap_stats.swap_count
probe_conn.close
client.close

puts
puts "Check cost (idle):"
puts format("  %.3f ms/check (headroom query + decision)", per_check_ms)
puts "  -> at the 5-minute default this is utterly negligible; it runs on the"
puts "     streamer's already-idle LISTEN connection (zero extra connections)."
puts
puts "Grow-under-saturation (3 -> cap 10, all baseline connections held):"
puts "  size trajectory : #{sizes.inspect}"
puts "  final size      : #{final} (started 3)"
puts "  swaps           : #{swaps}"
puts
if final > 3
  puts "RESULT: the autoscaler reacted — grew the streams pool into headroom under"
  puts "saturation, one step per check, capped at streams_pool_max. Per-op throughput"
  puts "is a workload question (#324): a fixed small pool serialises the same load; a"
  puts "larger static pool avoids both. Enable autoscale for BURSTY streams load."
else
  puts "RESULT: no growth (pool not saturated or no headroom). For steady load, size"
  puts "streams_pool_size statically — autoscale earns its keep only under bursts."
end
puts "Done."
