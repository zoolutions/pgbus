# frozen_string_literal: true

# Streams benchmark — measures real fanout throughput, connect latency,
# and broadcast-to-delivery p99 against a real Puma server, real PGMQ,
# and real LISTEN/NOTIFY.
#
# Requires PGBUS_DATABASE_URL:
#   PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:streams
#
# What it measures (each section runs TWICE — once with
# streams_stats_enabled=false, once with =true — so the cost of opt-in
# stream event recording shows up in the same output):
#
#   1. Single-broadcast roundtrip: time from Stream#broadcast to the
#      SSE client receiving the message. This is the per-event latency
#      that wake-coalescing and fanout-batching changes affect directly.
#
#   2. Burst broadcast throughput: time to deliver N broadcasts to one
#      connected SSE client. This is where wake coalescing earns its
#      keep — without coalescing, N broadcasts = N read_after queries.
#
#   3. Fanout to many connections: time to deliver one broadcast to M
#      simultaneously connected SSE clients. This is where fanout
#      batching matters — Connection#enqueue called M times means M
#      io.write_nonblock calls.
#
#   4. Connect latency under thundering herd: time to open K SSE
#      connections concurrently. This is where Listener mutex
#      contention shows up if ensure_listening serializes badly.
#
# The A/B comparison at the end prints the stats=off baseline vs the
# stats=on cost so the opt-in default can be justified numerically.

require "benchmark/ips"
require "json"
require "uri"
require "securerandom"
require "logger"
require "active_record"
require "pgbus"

require_relative "../spec/support/puma_test_harness"
require_relative "../spec/support/sse_test_client"

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://user@host/db"
end

parsed_url = URI.parse(DATABASE_URL)
params = URI.decode_www_form(parsed_url.query || "").to_h
params["pool"] = "20"
parsed_url.query = URI.encode_www_form(params)
ActiveRecord::Base.establish_connection(parsed_url.to_s)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_sbench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = true
  c.stats_enabled = false
  c.streams_signed_name_secret = "a" * 64
  c.streams_listen_health_check_ms = 100
  c.streams_heartbeat_interval = 60
  c.streams_write_deadline_ms = 5_000
  c.streams_stats_enabled = false # overridden per-suite below
end

# Ensure pgbus_stream_stats exists for the stats=on pass. The benchmark
# is intentionally self-contained so it can run against any fresh
# development DB without requiring the generator to have been run first.
def ensure_stream_stats_table!
  ActiveRecord::Base.connection.execute(<<~SQL)
    CREATE TABLE IF NOT EXISTS pgbus_stream_stats (
      id           BIGSERIAL PRIMARY KEY,
      stream_name  TEXT NOT NULL,
      event_type   TEXT NOT NULL,
      duration_ms  INTEGER NOT NULL DEFAULT 0,
      fanout       INTEGER,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_pgbus_stream_stats_time
      ON pgbus_stream_stats (created_at);
    CREATE INDEX IF NOT EXISTS idx_pgbus_stream_stats_stream_time
      ON pgbus_stream_stats (stream_name, created_at);
    CREATE INDEX IF NOT EXISTS idx_pgbus_stream_stats_type_time
      ON pgbus_stream_stats (event_type, created_at);
  SQL
  # Clear any rows from prior benchmark runs so numbers aren't skewed
  # by a gigantic table on subsequent invocations.
  ActiveRecord::Base.connection.execute("TRUNCATE TABLE pgbus_stream_stats")
  # Reset the memoized table_exists? check on the model — the model
  # may have been loaded before this function ran and cached "false".
  return unless Pgbus::StreamStat.instance_variable_defined?(:@table_exists)

  Pgbus::StreamStat.remove_instance_variable(:@table_exists)
end

def build_pg_listen_connection
  require "pg"
  uri = URI.parse(DATABASE_URL)
  PG.connect(
    host: uri.host || "localhost",
    port: (uri.port || 5432).to_i,
    dbname: uri.path.delete_prefix("/"),
    user: uri.user || ENV.fetch("USER"),
    password: uri.password
  )
end

def boot_streamer
  streamer = Pgbus::Web::Streamer::Instance.new(
    client: Pgbus.client,
    config: Pgbus.configuration,
    pg_connection: build_pg_listen_connection,
    logger: Logger.new(IO::NULL)
  )
  streamer.start
  streamer
end

def boot_harness(streamer)
  app = Pgbus::Web::StreamApp.new(
    streamer: streamer,
    config: Pgbus.configuration,
    logger: Logger.new(IO::NULL)
  )
  SseTestSupport::PumaTestHarness.boot(rack_app: app)
end

def signed(name)
  Pgbus::Streams::SignedName.sign(name)
end

def with_clean_stream
  stream_name = "sb_#{SecureRandom.hex(4)}"
  Pgbus.client.ensure_stream_queue(stream_name)
  streamer = boot_streamer
  harness = boot_harness(streamer)
  yield(stream_name, streamer, harness)
ensure
  streamer&.shutdown!
  harness&.shutdown
end

def connect_client(harness, stream_name, since_id: 0)
  url = "#{harness.url("/#{signed(stream_name)}")}?since=#{since_id}"
  SseTestSupport::SseTestClient.connect(url: url, timeout: 5)
end

# ═══════════════════════════════════════════════════════════════════════
# The suite — returns a hash of measurements so the A/B comparison can
# print paired tables. Each section body is identical to the pre-stats
# version; only the wrapping structure changed.
# ═══════════════════════════════════════════════════════════════════════

def run_suite(stats_enabled:)
  Pgbus.configuration.streams_stats_enabled = stats_enabled
  label = stats_enabled ? "stats=on " : "stats=off"

  results = {
    roundtrip: nil,
    burst: {},
    fanout: {},
    connect: {}
  }

  puts "\n══════════════════════════════════════════════════════════════════════"
  puts "  SUITE: #{label}"
  puts "══════════════════════════════════════════════════════════════════════"

  # ───────────────────────────────────────────────────────────────────
  # 1. Single broadcast roundtrip
  # ───────────────────────────────────────────────────────────────────
  puts "\n--- 1. Single broadcast roundtrip latency ---"
  with_clean_stream do |stream_name, _streamer, harness|
    stream = Pgbus.stream(stream_name)
    client = connect_client(harness, stream_name, since_id: stream.current_msg_id)
    sleep 0.1

    3.times do
      stream.broadcast("<turbo-stream>warmup</turbo-stream>")
      client.wait_for_events(count: client.events.size + 1, timeout: 2)
    end

    samples = []
    50.times do
      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      target = client.events.size + 1
      stream.broadcast("<turbo-stream>x</turbo-stream>")
      client.wait_for_events(count: target, timeout: 2)
      samples << ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0)
    end

    samples.sort!
    results[:roundtrip] = {
      min: samples.min,
      median: samples[samples.size / 2],
      p95: samples[(samples.size * 0.95).to_i],
      max: samples.max
    }
    printf "    n=%d  min=%6.2fms  median=%6.2fms  p95=%6.2fms  max=%6.2fms\n",
           samples.size, results[:roundtrip][:min], results[:roundtrip][:median],
           results[:roundtrip][:p95], results[:roundtrip][:max]

    client.close
  end

  # ───────────────────────────────────────────────────────────────────
  # 2. Burst throughput
  # ───────────────────────────────────────────────────────────────────
  puts "\n--- 2. Burst broadcast throughput (N broadcasts → 1 client) ---"
  [10, 50, 200].each do |burst_size|
    with_clean_stream do |stream_name, _streamer, harness|
      stream = Pgbus.stream(stream_name)
      client = connect_client(harness, stream_name, since_id: stream.current_msg_id)
      sleep 0.1

      stream.broadcast("<turbo-stream>warm</turbo-stream>")
      client.wait_for_events(count: 1, timeout: 5)
      warm_baseline = client.events.size

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      burst_size.times { |i| stream.broadcast("<turbo-stream>burst-#{i}</turbo-stream>") }
      client.wait_for_events(count: warm_baseline + burst_size, timeout: 30)
      elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

      received = client.events.size - warm_baseline
      results[:burst][burst_size] = { total_ms: elapsed, per_msg_ms: elapsed / burst_size }
      printf "    burst=%3d  delivered=%3d  total=%7.2fms  per-msg=%5.2fms\n",
             burst_size, received, elapsed, elapsed / burst_size

      client.close
    end
  end

  # ───────────────────────────────────────────────────────────────────
  # 3. Fanout
  # ───────────────────────────────────────────────────────────────────
  puts "\n--- 3. Fanout (1 broadcast → M clients) ---"
  [5, 20, 50].each do |fanout|
    with_clean_stream do |stream_name, _streamer, harness|
      stream = Pgbus.stream(stream_name)
      clients = Array.new(fanout) { connect_client(harness, stream_name, since_id: stream.current_msg_id) }
      sleep 0.5

      stream.broadcast("<turbo-stream>warm</turbo-stream>")
      deadline = Time.now + 10
      sleep 0.005 until clients.all? { |c| c.events.size >= 1 } || Time.now > deadline
      warm_baseline = clients.map { |c| c.events.size }

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      stream.broadcast("<turbo-stream>fanout</turbo-stream>")
      deadline = Time.now + 5
      target = warm_baseline.first + 1
      sleep 0.005 until clients.all? { |c| c.events.size >= target } || Time.now > deadline
      elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

      delivered = clients.count { |c| c.events.size >= target }
      results[:fanout][fanout] = { total_ms: elapsed, per_client_ms: elapsed / fanout }
      printf "    M=%2d  delivered=%2d  total=%7.2fms  per-client=%5.2fms\n",
             fanout, delivered, elapsed, elapsed / fanout

      clients.each(&:close)
    end
  end

  # ───────────────────────────────────────────────────────────────────
  # 4. Concurrent connect latency
  # ───────────────────────────────────────────────────────────────────
  puts "\n--- 4. Concurrent connect latency (K clients connecting at once) ---"
  [5, 20, 50].each do |k|
    with_clean_stream do |stream_name, _streamer, harness|
      stream = Pgbus.stream(stream_name)
      since = stream.current_msg_id

      t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      threads = Array.new(k) do
        # K concurrent connects, on purpose; each is joined by #value below.
        Thread.new { connect_client(harness, stream_name, since_id: since) } # rubocop:disable ThreadSafety/NewThread
      end
      clients = threads.map(&:value)
      elapsed = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0

      results[:connect][k] = { total_ms: elapsed, per_connect_ms: elapsed / k }
      printf "    K=%2d  total=%7.2fms  per-connect=%5.2fms\n", k, elapsed, elapsed / k

      clients.each(&:close)
    end
  end

  results
end

# ═══════════════════════════════════════════════════════════════════════
# A/B comparison
# ═══════════════════════════════════════════════════════════════════════

def pct_delta(off_value, on_value)
  return "  n/a" if off_value.nil? || off_value.zero?

  delta = ((on_value - off_value) / off_value.to_f) * 100.0
  sign = delta >= 0 ? "+" : ""
  format("%s%5.1f%%", sign, delta)
end

def print_comparison(off_results, on_results)
  puts "\n"
  puts "═" * 70
  puts "  A/B comparison — stats=off (baseline) vs stats=on (overhead)"
  puts "═" * 70

  puts "\n1. Single broadcast roundtrip (ms)"
  printf "   %-10s  %8s  %8s  %8s  %8s\n", "", "min", "median", "p95", "max"
  rtt_off = off_results[:roundtrip]
  rtt_on  = on_results[:roundtrip]
  printf "   %-10s  %8.2f  %8.2f  %8.2f  %8.2f\n", "off",
         rtt_off[:min], rtt_off[:median], rtt_off[:p95], rtt_off[:max]
  printf "   %-10s  %8.2f  %8.2f  %8.2f  %8.2f\n", "on",
         rtt_on[:min], rtt_on[:median], rtt_on[:p95], rtt_on[:max]
  printf "   %-10s  %8s  %8s  %8s  %8s\n", "Δ",
         pct_delta(rtt_off[:min], rtt_on[:min]),
         pct_delta(rtt_off[:median], rtt_on[:median]),
         pct_delta(rtt_off[:p95], rtt_on[:p95]),
         pct_delta(rtt_off[:max], rtt_on[:max])

  puts "\n2. Burst throughput (per-msg ms)"
  printf "   %-10s  %8s  %8s  %8s\n", "", "N=10", "N=50", "N=200"
  burst_off = off_results[:burst]
  burst_on  = on_results[:burst]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "off",
         burst_off[10][:per_msg_ms], burst_off[50][:per_msg_ms], burst_off[200][:per_msg_ms]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "on",
         burst_on[10][:per_msg_ms], burst_on[50][:per_msg_ms], burst_on[200][:per_msg_ms]
  printf "   %-10s  %8s  %8s  %8s\n", "Δ",
         pct_delta(burst_off[10][:per_msg_ms], burst_on[10][:per_msg_ms]),
         pct_delta(burst_off[50][:per_msg_ms], burst_on[50][:per_msg_ms]),
         pct_delta(burst_off[200][:per_msg_ms], burst_on[200][:per_msg_ms])

  puts "\n3. Fanout (per-client ms)"
  printf "   %-10s  %8s  %8s  %8s\n", "", "M=5", "M=20", "M=50"
  fan_off = off_results[:fanout]
  fan_on  = on_results[:fanout]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "off",
         fan_off[5][:per_client_ms], fan_off[20][:per_client_ms], fan_off[50][:per_client_ms]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "on",
         fan_on[5][:per_client_ms], fan_on[20][:per_client_ms], fan_on[50][:per_client_ms]
  printf "   %-10s  %8s  %8s  %8s\n", "Δ",
         pct_delta(fan_off[5][:per_client_ms], fan_on[5][:per_client_ms]),
         pct_delta(fan_off[20][:per_client_ms], fan_on[20][:per_client_ms]),
         pct_delta(fan_off[50][:per_client_ms], fan_on[50][:per_client_ms])

  puts "\n4. Concurrent connect (per-connect ms)"
  printf "   %-10s  %8s  %8s  %8s\n", "", "K=5", "K=20", "K=50"
  con_off = off_results[:connect]
  con_on  = on_results[:connect]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "off",
         con_off[5][:per_connect_ms], con_off[20][:per_connect_ms], con_off[50][:per_connect_ms]
  printf "   %-10s  %8.2f  %8.2f  %8.2f\n", "on",
         con_on[5][:per_connect_ms], con_on[20][:per_connect_ms], con_on[50][:per_connect_ms]
  printf "   %-10s  %8s  %8s  %8s\n", "Δ",
         pct_delta(con_off[5][:per_connect_ms], con_on[5][:per_connect_ms]),
         pct_delta(con_off[20][:per_connect_ms], con_on[20][:per_connect_ms]),
         pct_delta(con_off[50][:per_connect_ms], con_on[50][:per_connect_ms])
end

# ═══════════════════════════════════════════════════════════════════════
# Run
# ═══════════════════════════════════════════════════════════════════════

puts "=" * 70
puts "Pgbus Streams Benchmarks (real Puma + real PGMQ + real SSE)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}"
puts "=" * 70

ensure_stream_stats_table!

off_results = run_suite(stats_enabled: false)
on_results = run_suite(stats_enabled: true)

print_comparison(off_results, on_results)

puts "\nDone."
