# frozen_string_literal: true

# One client process of the streams scale bench (issue #620), spawned by
# benchmarks/streams_scale_bench.rb, never forked: a forked child would inherit
# the parent's PG::Connection sockets and PQfinish them on exit (#440).
#
#   ruby benchmarks/support/streams_scale_client.rb COUNT CONNECT_TIMEOUT READ_TIMEOUT SLOW OFFSET URL...
#
# Builds COUNT SseTestClients, the one at slot i for the stream URL at
# StreamsScaleHarness.stream_index(i, offset: OFFSET, streams: URL count): with
# STREAMS_SCALE_STREAMS=N the bench passes N URLs and each child's global
# OFFSET, so clients are dealt over the streams round-robin (issue #630). Prints STREAMS_SCALE_CHILD_READY and
# waits for "go" on stdin (the start barrier). Then starts all of them at once
# (one reader thread each, none polling) and waits for "stop". A client whose
# connect failed before any response (the kernel's accept queue overflowed, as
# it does at macOS's somaxconn of 128) reconnects after RETRY_S, the retry: the
# streamer sends and an EventSource would wait, at most MAX_RETRIES times.
#
# On stop it prints one STREAMS_SCALE_CHILD_RESULT line: per client the HTTP
# status, the error that ended its last reader if any, how many times it
# reconnected, connected_at (CLOCK_REALTIME receipt of its pgbus:connected
# frame: the moment the dispatcher registered it) and, per broadcast tag, the
# receipt time of its first copy. SLOW=1 makes every client sleep 1 s after
# each event it parses (the slow-client instrument of #621).
#
# Requires only the standard library, the SSE test client and the pure
# harness: it never loads pgbus or ActiveRecord.

require "English"
require "json"
require "net/http"
require "stringio"
require_relative "../../spec/support/sse_test_client"
require_relative "streams_scale_harness"

# A client whose reader thread stalls 1 s after each SSE block, so the server's
# writes back up behind it.
class SlowSseTestClient < SseTestSupport::SseTestClient
  private

  # Only a block with an SSE field is an event; comments (the opening line,
  # heartbeats) pass at full speed.
  def process_block(block)
    super
    sleep 1 if block.lines.any? { |line| line.match?(/\A(?:id|event|data):/) }
  end
end

RETRY_S = 2.0
MAX_RETRIES = 10
TAG = /data-scale-tag="([^"]+)"/

def fail!(message)
  $stdout.puts(message)
  $stdout.flush
  exit 1
end

def monotonic = Process.clock_gettime(Process::CLOCK_MONOTONIC)

count, connect_timeout, read_timeout, slow, offset, *urls = ARGV
count = Integer(count, 10)
offset = Integer(offset, 10)
fail! "streams scale client: no stream URL given" if urls.empty?
klass = slow == "1" ? SlowSseTestClient : SseTestSupport::SseTestClient
build = lambda do |slot|
  url = urls.fetch(StreamsScaleHarness.stream_index(slot, offset: offset, streams: urls.size))
  klass.new(url: url, timeout: Float(connect_timeout), read_timeout: Float(read_timeout))
end
StreamsScaleHarness.raise_fd_limit!(StreamsScaleHarness.fd_needed(count))

slots = Array.new(count) { |slot| { client: build.call(slot), index: slot, retries: 0, failed_at: nil } }
$stdout.puts(StreamsScaleHarness::CHILD_READY)
$stdout.flush
# Every failed connect warns; the result line tallies them instead. A crash
# still reaches the parent: its message goes to stdout (at_exit below).
$stderr = StringIO.new
at_exit do
  error = $ERROR_INFO
  if error && !error.is_a?(SystemExit)
    $stdout.puts("streams scale client crashed: #{error.class}: #{error.message}",
                 *Array(error.backtrace).first(10).map { |frame| "  #{frame}" })
  end
end
signal = $stdin.gets
fail! "streams scale client: no start signal (got #{signal.inspect})" unless signal&.chomp == "go"

slots.each { |slot| slot[:client].start_nowait }

# One thread reconnects every client whose connect failed before a response.
stop = Queue.new
retrier = Thread.new do # rubocop:disable ThreadSafety/NewThread -- joined after "stop" below
  until stop.pop(timeout: 0.1)
    now = monotonic
    slots.each do |slot|
      client = slot[:client]
      next unless client.status.nil? && client.error && slot[:retries] < MAX_RETRIES

      slot[:failed_at] ||= now
      next if now - slot[:failed_at] < RETRY_S

      slot[:retries] += 1
      slot[:failed_at] = nil
      slot[:client] = build.call(slot[:index]).start_nowait
    end
  end
end

signal = $stdin.gets
stop << true
retrier.join
fail! "streams scale client: no stop signal (got #{signal.inspect})" unless signal&.chomp == "stop"

results = slots.map do |slot|
  client = slot[:client]
  connected = client.control_events.find { |e| e.event == "pgbus:connected" }
  events = {}
  client.events.each do |event|
    tag = event.data[TAG, 1]
    events[tag] ||= event.received_at if tag
  end
  { "status" => client.status, "error" => client.error, "retries" => slot[:retries],
    "connected_at" => connected&.received_at, "events" => events }
end

# One write, so no other output can land between the JSON and its newline.
$stdout.write("#{StreamsScaleHarness::CHILD_RESULT}#{JSON.generate("slow" => slow == "1", "clients" => results)}\n")
$stdout.flush
# The sockets close with the process; the server's peer-close watcher reaps them.
