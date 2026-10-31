# frozen_string_literal: true

require_relative "bench_support"
require "json"
require "securerandom"
require "concurrent"
require "pgbus"
# Pgbus::Client.new loads pgmq-ruby; the stubbed client is allocated, never
# initialized, so load it the same way for PGMQ::Message.
Pgbus::Client.load_pgmq_gem!

# Suppress logging during benchmarks
Pgbus.configure do |c|
  c.logger = Logger.new(IO::NULL)
  c.queue_prefix = "pgbus_bench"
  c.default_queue = "default"
  c.stats_enabled = false # Don't record stats during benchmarks
end

# Stub PGMQ to isolate Pgbus overhead from database I/O.
# This measures the gem's own CPU and memory cost.
module BenchStubs
  def self.build_mock_pgmq
    MockPgmq.new
  end

  def self.build_mock_client
    pgmq = build_mock_pgmq
    Pgbus::Client.allocate.tap do |client|
      client.instance_variable_set(:@pgmq, pgmq)
      client.instance_variable_set(:@config, Pgbus.configuration)
      client.instance_variable_set(:@pgmq_mutex, Mutex.new)
      client.instance_variable_set(:@queues_created, Concurrent::Map.new.tap { |m| m["pgbus_bench_default"] = true })
      client.instance_variable_set(:@queue_strategy, Pgbus::QueueFactory.for(Pgbus.configuration))
      client.instance_variable_set(:@schema_ensured, true)
      client.instance_variable_set(:@shared_connection, false)
      # read_batch routes through guarded_read; without this the read section
      # of client_bench aborts on `undefined method 'run_guarded' for nil`.
      client.instance_variable_set(:@connection_health, Pgbus::Client::ConnectionHealth.new)
      client.instance_variable_set(:@libpq_read_bounds_effective, true)
      # The mock answers pgmq-ruby calls, not SQL: false is what a queue without
      # the native columns caches, so this times the function path.
      client.instance_variable_set(:@native_statements, Concurrent::Map.new.tap { |m| m["pgbus_bench_default"] = false })
    end
  end

  # The executor reads every field a PGMQ read returns (enqueued_at for the
  # job's delivery and its stats), so a bench message is the real class with
  # the values PGMQ hands over. A stand-in missing a field fails each job
  # before perform, and the bench then times the failure path.
  def self.build_message(msg_id:, message:, read_ct: 1, headers: nil)
    PGMQ::Message.new(
      { "msg_id" => msg_id, "read_ct" => read_ct, "enqueued_at" => Time.now.utc.strftime("%Y-%m-%d %H:%M:%S.%6N+00"),
        "last_read_at" => nil, "vt" => nil, "message" => message, "headers" => headers }
    )
  end

  # Abort before timing anything unless the job runs to :success.
  def self.expect_success!(executor, message, queue_name = "default")
    result = executor.execute(message, queue_name)
    return if result == :success

    abort "[bench] Executor#execute returned #{result.inspect}, not :success: the bench would time a failing job"
  end

  class MockPgmq
    def create(...) = nil
    def enable_notify_insert(...) = nil
    def produce(...) = 1
    def produce_batch(_, msgs, **) = (1..msgs.size).to_a
    def read(...) = nil
    def read_batch(*, **) = []
    def read_with_poll(...) = []
    def delete(...) = true # rubocop:disable Naming/PredicateMethod
    def archive(...) = true # rubocop:disable Naming/PredicateMethod
    def set_vt(...) = nil
    def metrics(...) = nil
    def metrics_all(...) = []
    def list_queues(...) = []
    def purge_queue(...) = nil
    def bind_topic(...) = nil
    def produce_topic(...) = nil
    def close(...) = nil
    def transaction(...) = yield(self)
  end
end

# Sample payloads of varying sizes
SMALL_PAYLOAD = { "job_class" => "SmallJob", "job_id" => SecureRandom.uuid,
                  "queue_name" => "default", "arguments" => [1] }.freeze
MEDIUM_PAYLOAD = SMALL_PAYLOAD.merge("arguments" => (1..100).to_a).freeze
LARGE_PAYLOAD = SMALL_PAYLOAD.merge("arguments" => [{ "data" => "x" * 10_000 }]).freeze
