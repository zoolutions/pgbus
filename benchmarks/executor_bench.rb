# frozen_string_literal: true

require_relative "bench_helper"
require "active_job"

# Silence ActiveJob's own logger — it's separate from Pgbus.logger
# and produces ~500k lines of "Performing/Performed" noise per run.
ActiveJob::Base.logger = Logger.new(IO::NULL)

puts "=" * 60
puts "Pgbus Executor Benchmarks (mocked PGMQ — measures gem overhead)"
puts "=" * 60

# Minimal ActiveJob for benchmarking
class BenchJob < ActiveJob::Base
  self.queue_adapter = :inline

  def perform(*)
    # no-op
  end
end

# Stub JobStat so it doesn't hit the DB
stub_job_stat = Class.new do
  def self.record!(**) = nil
end
Object.const_set(:PGBUS_BENCH_JOB_STAT_STUB, true) unless defined?(PGBUS_BENCH_JOB_STAT_STUB)

# Replace JobStat with stub for benchmarks
Pgbus.send(:remove_const, :JobStat) if defined?(Pgbus::JobStat)
Pgbus.const_set(:JobStat, stub_job_stat)

client = BenchStubs.build_mock_client
executor = Pgbus::ActiveJob::Executor.new(client: client, config: Pgbus.configuration)

# Build a realistic message
job = BenchJob.new(42)
job_payload = job.serialize
message_json = JSON.generate(job_payload)

msg = BenchStubs.build_message(msg_id: 1, message: message_json)
BenchStubs.expect_success!(executor, msg)

puts "\n--- Executor#execute throughput ---"
Benchmark.ips do |x|
  x.config(time: 5, warmup: 2)

  x.report("execute (small job)") do
    executor.execute(msg, "default")
  end

  x.compare!
end

puts "\n--- Executor#execute throughput (stats enabled) ---"
begin
  Pgbus.configuration.stats_enabled = true
  Benchmark.ips do |x|
    x.config(time: 5, warmup: 2)

    x.report("execute + stat recording") do
      executor.execute(msg, "default")
    end

    x.compare!
  end
ensure
  Pgbus.configuration.stats_enabled = false
end

puts "\n--- Memory: execute (1000 jobs) ---"
report = MemoryProfiler.report { 1000.times { executor.execute(msg, "default") } }
puts "Total allocated: #{report.total_allocated_memsize} bytes (#{report.total_allocated} objects)"
puts "Total retained:  #{report.total_retained_memsize} bytes (#{report.total_retained} objects)"
puts "Per-job allocated: ~#{report.total_allocated_memsize / 1000} bytes (~#{report.total_allocated / 1000} objects)"
puts "Per-job retained:  ~#{report.total_retained_memsize / 1000} bytes (~#{report.total_retained / 1000} objects)"

puts "\n--- Top allocations by gem ---"
report.pretty_print(to_file: "/dev/stdout", scale_bytes: true, detailed_report: false)
