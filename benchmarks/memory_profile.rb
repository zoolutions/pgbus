# frozen_string_literal: true

require_relative "bench_helper"
require "active_job"

ActiveJob::Base.logger = Logger.new(IO::NULL)

puts "=" * 60
puts "Pgbus Memory Profile — Detailed Allocation Analysis"
puts "=" * 60

# Minimal ActiveJob for profiling
class ProfileJob < ActiveJob::Base
  self.queue_adapter = :inline

  def perform(*)
    # no-op
  end
end

client = BenchStubs.build_mock_client
executor = Pgbus::ActiveJob::Executor.new(client: client, config: Pgbus.configuration)

job = ProfileJob.new("hello", 42, { nested: true })
job_payload = job.serialize
message_json = JSON.generate(job_payload)

msg = BenchStubs.build_message(msg_id: 1, message: message_json)
BenchStubs.expect_success!(executor, msg)

# --- Profile individual operations ---

puts "\n=== Client#send_message ==="
report = MemoryProfiler.report(top: 10) { 500.times { client.send_message("default", SMALL_PAYLOAD) } }
report.pretty_print(scale_bytes: true, detailed_report: false)

puts "\n=== Client#send_batch (100 items) ==="
batch = Array.new(100) { SMALL_PAYLOAD }
report = MemoryProfiler.report(top: 10) { 100.times { client.send_batch("default", batch) } }
report.pretty_print(scale_bytes: true, detailed_report: false)

puts "\n=== Executor#execute ==="
report = MemoryProfiler.report(top: 10) { 500.times { executor.execute(msg, "default") } }
report.pretty_print(scale_bytes: true, detailed_report: false)

puts "\n=== JSON.generate (isolation check) ==="
report = MemoryProfiler.report(top: 10) { 500.times { JSON.generate(SMALL_PAYLOAD) } }
report.pretty_print(scale_bytes: true, detailed_report: false)

puts "\n=== JSON.parse (isolation check) ==="
json_str = JSON.generate(SMALL_PAYLOAD)
report = MemoryProfiler.report(top: 10) { 500.times { JSON.parse(json_str) } }
report.pretty_print(scale_bytes: true, detailed_report: false)

# --- Retained object detection (leak check) ---
puts "\n#{"=" * 60}"
puts "Retained Object Detection (potential leaks)"
puts "=" * 60

report = MemoryProfiler.report(top: 20) do
  1000.times do
    client.send_message("default", SMALL_PAYLOAD)
    executor.execute(msg, "default")
  end
end

if report.total_retained.zero?
  puts "\nNo retained objects detected — no leaks."
else
  puts "\nWARNING: #{report.total_retained} retained objects (#{report.total_retained_memsize} bytes)"
  puts "Retained by gem:"
  report.retained_objects_by_gem.each do |stat|
    puts "  #{stat[:data]}: #{stat[:count]} objects"
  end
  puts "\nRetained memory by gem:"
  report.retained_memory_by_gem.each do |stat|
    puts "  #{stat[:data]}: #{stat[:count]} bytes"
  end
  puts "\nRetained by location:"
  report.retained_objects_by_location.first(10).each do |stat|
    puts "  #{stat[:data]}: #{stat[:count]} objects"
  end
end
