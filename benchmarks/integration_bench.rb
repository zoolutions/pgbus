# frozen_string_literal: true

# Integration benchmark — measures real PostgreSQL performance.
# Requires PGBUS_DATABASE_URL to be set:
#   PGBUS_DATABASE_URL=postgres://pgbus:pgbus@localhost:5432/pgbus_test bin/rake bench:integration

require "benchmark/ips"
require "memory_profiler"
require "json"
require "uri"
require "securerandom"
require "active_record"
require "active_job"
require "pgbus"

ActiveJob::Base.logger = Logger.new(IO::NULL)

DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL") do
  abort "PGBUS_DATABASE_URL not set. Example: postgres://pgbus:pgbus@localhost:5432/pgbus_test"
end

# Merge pool size safely via URI parsing to avoid breaking URLs with existing query params.
parsed_url = URI.parse(DATABASE_URL)
params = URI.decode_www_form(parsed_url.query || "").to_h
params["pool"] = "20"
parsed_url.query = URI.encode_www_form(params)
ActiveRecord::Base.establish_connection(parsed_url.to_s)

Pgbus.configure do |c|
  c.database_url = DATABASE_URL
  c.queue_prefix = "pgbus_ibench"
  c.default_queue = "default"
  c.logger = Logger.new(IO::NULL)
  c.pgmq_schema_mode = :embedded
  c.listen_notify = false
  c.stats_enabled = false
end

# Ensure schema + queues exist
Pgbus.client.ensure_queue("default")
Pgbus.client.ensure_dead_letter_queue("default")

# Self-bootstrap: create required tables if they don't exist.
# This avoids requiring users to run full migrations just to benchmark.
conn = ActiveRecord::Base.connection

conn.execute(<<~SQL) unless conn.table_exists?("pgbus_uniqueness_keys")
  CREATE TABLE pgbus_uniqueness_keys (
    lock_key VARCHAR NOT NULL,
    queue_name VARCHAR NOT NULL,
    msg_id BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
  CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_uniqueness_keys_key ON pgbus_uniqueness_keys (lock_key);
SQL

conn.execute(<<~SQL) unless conn.table_exists?("pgbus_semaphores")
  CREATE TABLE pgbus_semaphores (
    id BIGSERIAL PRIMARY KEY,
    key VARCHAR NOT NULL,
    value INTEGER NOT NULL DEFAULT 0,
    max_value INTEGER NOT NULL DEFAULT 1,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
  CREATE UNIQUE INDEX idx_pgbus_semaphores_key ON pgbus_semaphores (key);
SQL

conn.execute(<<~SQL) unless conn.table_exists?("pgbus_processes")
  CREATE TABLE pgbus_processes (
    id BIGSERIAL PRIMARY KEY,
    kind VARCHAR NOT NULL,
    hostname VARCHAR,
    pid INTEGER,
    metadata JSONB DEFAULT '{}',
    last_heartbeat_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
SQL

# ─── Job classes ───
# rubocop:disable Style/OneClassPerFile

class PlainBenchJob < ActiveJob::Base
  self.queue_adapter = :inline
  def perform(*); end
end

class UniqueUntilExecutedJob < ActiveJob::Base
  include Pgbus::Uniqueness

  self.queue_adapter = :inline
  ensures_uniqueness strategy: :until_executed, key: ->(arg) { "ue:#{arg}" }, on_conflict: :discard
  def perform(*); end
end

class UniqueWhileExecutingJob < ActiveJob::Base
  include Pgbus::Uniqueness

  self.queue_adapter = :inline
  ensures_uniqueness strategy: :while_executing, key: ->(arg) { "we:#{arg}" }, on_conflict: :discard
  def perform(*); end
end

# rubocop:enable Style/OneClassPerFile

# ─── Helpers ───

def purge_queue!
  Pgbus.client.purge_queue("default")
  Pgbus::UniquenessKey.delete_all
rescue StandardError => e
  warn "[bench] purge_queue! failed: #{e.class}: #{e.message}"
end

# Enqueue a message and read it back as a real PGMQ message object.
# This exercises the full DB path: INSERT → SELECT FOR UPDATE SKIP LOCKED.
def enqueue_and_read(client, payload)
  client.send_message("default", payload)
  client.read_batch("default", qty: 1, vt: 300).first
end

client = Pgbus.client
adapter = Pgbus::ActiveJob::Adapter.new
executor = Pgbus::ActiveJob::Executor.new(client: client, config: Pgbus.configuration)

puts "=" * 70
puts "Pgbus Integration Benchmarks (real PostgreSQL)"
puts "  DB: #{DATABASE_URL.sub(%r{//[^@]*@}, "//***@")}"
puts "=" * 70

# ═══════════════════════════════════════════════════════════════════════
# 1. Enqueue throughput
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Enqueue throughput (real DB) ---"
purge_queue!

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("plain enqueue") do
    client.send_message("default", { "job_class" => "PlainBenchJob", "arguments" => [1] })
  end

  x.report("enqueue + until_executed lock") do |times|
    times.times do |i|
      key = "bench-ue-#{i}-#{SecureRandom.hex(4)}"
      payload = {
        "job_class" => "UniqueUntilExecutedJob", "arguments" => [i],
        Pgbus::Uniqueness::METADATA_KEY => key,
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
      }
      Pgbus::UniquenessKey.acquire!(key, queue_name: "default", msg_id: 0)
      client.send_message("default", payload)
    end
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 2. Full adapter enqueue (serialize + uniqueness + send)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Adapter#enqueue (full path, real DB) ---"
purge_queue!

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("adapter: plain job") do
    adapter.enqueue(PlainBenchJob.new(rand(1_000_000)))
  end

  # until_executed: each call needs a unique key to avoid conflict
  x.report("adapter: until_executed") do
    adapter.enqueue(UniqueUntilExecutedJob.new(SecureRandom.hex(8)))
  end

  x.report("adapter: while_executing") do
    adapter.enqueue(UniqueWhileExecutingJob.new(SecureRandom.hex(8)))
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 3. Execute throughput (enqueue → read → deserialize → perform → archive)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Executor#execute (full path, real DB) ---"

# Build payloads
plain_payload = PlainBenchJob.new(42).serialize

ue_payload = UniqueUntilExecutedJob.new(42).serialize
ue_payload[Pgbus::Uniqueness::METADATA_KEY] = "bench-exec-ue"
ue_payload[Pgbus::Uniqueness::STRATEGY_KEY] = "until_executed"

we_payload = UniqueWhileExecutingJob.new(42).serialize
we_payload[Pgbus::Uniqueness::METADATA_KEY] = "bench-exec-we"
we_payload[Pgbus::Uniqueness::STRATEGY_KEY] = "while_executing"

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("execute: plain job") do
    msg = enqueue_and_read(client, plain_payload)
    executor.execute(msg, "default")
  end

  x.report("execute: until_executed") do
    Pgbus::UniquenessKey.acquire!("bench-exec-ue", queue_name: "default", msg_id: 0)
    msg = enqueue_and_read(client, ue_payload)
    executor.execute(msg, "default")
  end

  x.report("execute: while_executing") do
    Pgbus::UniquenessKey.release!("bench-exec-we")
    msg = enqueue_and_read(client, we_payload)
    executor.execute(msg, "default")
  end

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 4. Batch enqueue throughput
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Batch send_message (real DB) ---"
purge_queue!

small_batch = Array.new(10) { { "job_class" => "PlainBenchJob", "arguments" => [rand(1000)] } }
large_batch = Array.new(100) { { "job_class" => "PlainBenchJob", "arguments" => [rand(1000)] } }

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("send_batch(10)") { client.send_batch("default", small_batch) }
  x.report("send_batch(100)") { client.send_batch("default", large_batch) }

  x.compare!
end
purge_queue!

# ═══════════════════════════════════════════════════════════════════════
# 5. Lock operations (raw)
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- UniquenessKey operations (real DB) ---"
Pgbus::UniquenessKey.delete_all

Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("acquire + release") do |times|
    times.times do |i|
      key = "bench-lock-#{i}-#{SecureRandom.hex(4)}"
      Pgbus::UniquenessKey.acquire!(key, queue_name: "default", msg_id: i)
      Pgbus::UniquenessKey.release!(key)
    end
  end

  x.report("acquire (conflict)") do |times|
    Pgbus::UniquenessKey.acquire!("conflict-key", queue_name: "default", msg_id: 0)
    times.times do
      Pgbus::UniquenessKey.acquire!("conflict-key", queue_name: "default", msg_id: 1)
    end
    Pgbus::UniquenessKey.release!("conflict-key")
  end

  x.compare!
end
Pgbus::UniquenessKey.delete_all

# ═══════════════════════════════════════════════════════════════════════
# 6. Memory profile — full enqueue+execute cycle
# ═══════════════════════════════════════════════════════════════════════

puts "\n--- Memory: 500 enqueue+execute cycles (plain) ---"
purge_queue!
report = MemoryProfiler.report do
  500.times do
    msg = enqueue_and_read(client, plain_payload)
    executor.execute(msg, "default")
  end
end
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-cycle:       ~#{report.total_allocated_memsize / 500} bytes (~#{report.total_allocated / 500} objects)"

puts "\n--- Memory: 500 enqueue+execute cycles (until_executed) ---"
purge_queue!
report = MemoryProfiler.report do
  500.times do |i|
    key = "mem-ue-#{i}"
    payload = ue_payload.merge(Pgbus::Uniqueness::METADATA_KEY => key)
    Pgbus::UniquenessKey.acquire!(key, queue_name: "default", msg_id: 0)
    msg = enqueue_and_read(client, payload)
    executor.execute(msg, "default")
  end
end
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-cycle:       ~#{report.total_allocated_memsize / 500} bytes (~#{report.total_allocated / 500} objects)"

if report.total_retained.positive?
  puts "\nWARNING: retained objects detected — potential memory leak"
  report.retained_objects_by_location.first(5).each do |stat|
    puts "  #{stat[:data]}: #{stat[:count]} objects"
  end
end

purge_queue!
puts "\nDone."
