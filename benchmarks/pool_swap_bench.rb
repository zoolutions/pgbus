# frozen_string_literal: true

# Streams-pool hot-swap benchmark (issue #323 spike).
#
# Proves the swap primitive under live load and measures its cost: drive
# continuous produce + replay-read against a dedicated-path client while
# hot-swapping the streams pool (grow 5->12, then shrink 12->5) mid-stream.
#
# PASS gates (printed at the end):
#   (a) swap cost    — build ≈ 0 (lazy pool), cost = drain + close, O(open conns)
#   (b) zero loss    — landed-in-queue == successful produces
#   (c) zero leak    — pg_stat_activity after <= before
#   (d) no race      — zero errors, in particular no "Connection pool is closed"
#
# Requires PGBUS_DATABASE_URL. Run-and-report, never a CI gate.
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:pool_swap

require "logger"
require "concurrent"
require "pgbus"
require "pg"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  warn "PGBUS_DATABASE_URL not set. This benchmark requires a real PostgreSQL database."
  warn "Example: PGBUS_DATABASE_URL=postgres://user@localhost/pgbus_test bundle exec rake bench:pool_swap"
  exit 1
end

MIN_FREE_BACKENDS = 20
STREAM = "poolswap_#{Process.pid}".freeze
HOLD_SECONDS = 3.0
# Tag this bench's own backends so the leak check isn't fooled by neighbor apps
# sharing the local Postgres (only THIS process's pool connections count).
APP_NAME = "pgbus_pool_swap_bench_#{Process.pid}".freeze
# libpq reads application_name from the conninfo; append to the URL.
CLIENT_URL = "#{DATABASE_URL}#{DATABASE_URL.include?("?") ? "&" : "?"}application_name=#{APP_NAME}".freeze

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

def free_backends
  conn = PG.connect(DATABASE_URL)
  total = conn.exec("SHOW max_connections").first["max_connections"].to_i
  used  = conn.exec("SELECT count(*) AS n FROM pg_stat_activity").first["n"].to_i
  total - used
ensure
  conn&.close
end

# Count ONLY this bench's own pool backends (by application_name), so the leak
# check is immune to neighbor apps on the shared local Postgres.
def app_backends
  conn = PG.connect(DATABASE_URL)
  conn.exec_params("SELECT count(*) AS n FROM pg_stat_activity WHERE application_name = $1", [APP_NAME])
      .first["n"].to_i
ensure
  conn&.close
end

def landed_count(client)
  full = client.config.queue_name(STREAM)
  sanitized = Pgbus::QueueNameValidator.sanitize!(full)
  conn = PG.connect(DATABASE_URL)
  conn.exec("SELECT count(*) AS n FROM pgmq.q_#{sanitized}").first["n"].to_i
ensure
  conn&.close
end

def build_client
  config = Pgbus::Configuration.new.tap do |c|
    c.database_url = CLIENT_URL
    c.queue_prefix = "pgbus_poolswap"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.streams_pool_size = 5
    c.streams_pool_timeout = 5
  end
  Pgbus::Client.new(config, schema_ensured: true)
end

puts "=" * 76
puts "Streams-pool hot-swap benchmark (issue #323)"
puts "Database: #{DATABASE_URL.sub(%r{//[^@]+@}, "//***@")}"
puts "=" * 76

if free_backends < MIN_FREE_BACKENDS
  warn "Only #{free_backends} Postgres backends free (need >= #{MIN_FREE_BACKENDS}); free some and retry."
  exit 1
end

client = build_client
client.ensure_stream_queue(STREAM)

produced = Concurrent::AtomicFixnum.new(0)
errors   = Concurrent::Array.new
closed_pool_errors = Concurrent::AtomicFixnum.new(0)
stop = Concurrent::AtomicBoolean.new(false)

# Producers (4 threads): continuous durable broadcast.
# Load generators, on purpose: `stop` ends them and each is joined below.
producers = Array.new(4) do
  Thread.new do # rubocop:disable ThreadSafety/NewThread
    until stop.true?
      begin
        client.send_stream_message(STREAM, { "seq" => produced.value })
        produced.increment
      rescue StandardError => e
        errors << e.message
        closed_pool_errors.increment if e.message.include?("Connection pool is closed")
      end
    end
  end
end

# Reader (1 thread, matches the single dispatcher): continuous replay read.
cursor = Concurrent::AtomicFixnum.new(0)
reader = Thread.new do # rubocop:disable ThreadSafety/NewThread -- ended by `stop`, joined below with the producers
  until stop.true?
    begin
      envs = client.read_after(STREAM, after_id: cursor.value, limit: 200)
      cursor.value = envs.last.msg_id if envs.any?
    rescue StandardError => e
      errors << e.message
    end
  end
end

sleep 0.3 # warm up
before_backends = app_backends

t = monotonic
client.resize_streams_pool(12) # grow 5 -> 12
grow_ms = (monotonic - t) * 1000.0
grow_stats = client.streams_swap_stats

sleep HOLD_SECONDS

t = monotonic
client.resize_streams_pool(5) # shrink 12 -> 5
shrink_ms = (monotonic - t) * 1000.0
shrink_stats = client.streams_swap_stats

sleep 0.3
stop.make_true
(producers + [reader]).each { |th| th.join(10) }

sleep 0.3 # let old-pool connections finish closing
after_backends = app_backends
landed = landed_count(client)
final_size = client.streams_pool_stats[:size]
client.close

puts
puts "Swap cost:"
puts format("  grow  5->12 : total %.1f ms | drain+close %.1f ms | conns closed %d",
            grow_ms, grow_stats.last_drain_seconds * 1000.0, grow_stats.last_conns_closed)
puts format("  shrink 12->5 : total %.1f ms | drain+close %.1f ms | conns closed %d",
            shrink_ms, shrink_stats.last_drain_seconds * 1000.0, shrink_stats.last_conns_closed)
puts "  (build cost = total - drain; expected ~0 because connection_pool is lazy)"
puts
puts "Correctness:"
puts "  produced (successful) : #{produced.value}"
puts "  landed in queue       : #{landed}"
puts "  backends before/after : #{before_backends} / #{after_backends}"
puts "  final pool size       : #{final_size}"
puts "  errors                : #{errors.size} (pool-closed races: #{closed_pool_errors.value})"
puts

gates = {
  "(b) zero lost broadcasts" => landed == produced.value,
  "(c) zero leaked conns" => after_backends <= before_backends,
  "(d) no pool-closed race" => closed_pool_errors.value.zero?,
  "    no other errors" => errors.empty?,
  "    final size restored" => final_size == 5
}
gates.each { |name, ok| puts "  #{ok ? "PASS" : "FAIL"}  #{name}" }
puts
puts(gates.values.all? ? "ALL GATES PASS — swap is correct + cheap under load." : "GATE FAILURE — see above.")
puts "Done."
