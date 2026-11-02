# frozen_string_literal: true

# Streams fanout micro-benchmark (issue #315 item 3).
#
# The head-of-line-blocking fix threads a `deadline_ms:` keyword through the
# dispatcher's hot fanout path (safe_enqueue -> Connection#enqueue ->
# IoWriter.write). This bench proves that pass-through is ZERO-COST on the
# happy path (all-fast clients): the per-frame enqueue throughput and
# allocations are unchanged whether the deadline is defaulted or overridden.
#
# It does NOT measure the HOL win itself — that only manifests with a genuinely
# slow client whose socket buffer fills, which is a real-socket/OS-dependent
# scenario covered by the streams integration bench (rake bench:streams) and
# bounded deterministically (K * fanout_deadline vs K * write_deadline) by the
# dispatcher unit specs. No DB or Puma needed here.
#
# Issue #621: the dispatcher frames each envelope once per wake and hands every
# connection the frozen bytes (`frames:`). The last section compares enqueue
# with the frame given against enqueue building it, the per-connection saving.
# It is skipped on a tree whose Connection#enqueue has no `frames:` keyword.

require_relative "bench_helper"
require "stringio"

# A writer that always succeeds immediately (a fast client). Records nothing —
# we only care about the enqueue path's own overhead.
FastWriter = Class.new do
  def write(_connection, _bytes, deadline_ms:) # rubocop:disable Lint/UnusedMethodArgument
    :ok
  end
end.new

def build_connection
  Pgbus::Web::Streamer::Connection.new(
    id: "bench",
    stream_name: "chat",
    io: StringIO.new,
    since_id: 0,
    writer: FastWriter,
    write_deadline_ms: 5_000
  )
end

def build_envelopes(count, base_id)
  Array.new(count) do |i|
    Pgbus::Client::ReadAfter::Envelope.new(
      msg_id: base_id + i,
      enqueued_at: "2026-07-05T00:00:00Z",
      payload: "<turbo-stream>#{base_id + i}</turbo-stream>",
      source: "live"
    )
  end
end

BenchSupport.header("Connection#enqueue throughput: default vs explicit deadline (issue #315 item 3)")

# Each report uses a fresh monotonic base_id so the cursor check never dedups
# (msg_id strictly increasing across calls within a report).
default_conn = build_connection
override_conn = build_connection
default_id = 0
override_id = 0

BenchSupport.ips(time: 3, warmup: 1) do |x|
  x.report("enqueue (default deadline)") do
    default_id += 1
    default_conn.enqueue(build_envelopes(1, default_id))
  end

  x.report("enqueue (explicit fanout deadline)") do
    override_id += 1
    override_conn.enqueue(build_envelopes(1, override_id), deadline_ms: 250)
  end
end

BenchSupport.header("allocations per enqueue (retained MUST be 0)")

alloc_conn = build_connection
alloc_id = 0
BenchSupport.allocations("enqueue default deadline") do
  1_000.times do
    alloc_id += 1
    alloc_conn.enqueue(build_envelopes(1, alloc_id))
  end
end

alloc_conn2 = build_connection
alloc_id2 = 0
BenchSupport.allocations("enqueue explicit deadline") do
  1_000.times do
    alloc_id2 += 1
    alloc_conn2.enqueue(build_envelopes(1, alloc_id2), deadline_ms: 250)
  end
end

puts "\nExpected: the two bars are within noise and retained-per-op is 0 — the"
puts "deadline_ms keyword pass-through adds no measurable cost on the happy path."

if Pgbus::Web::Streamer::Connection.instance_method(:enqueue).parameters.include?(%i[key frames])
  BenchSupport.header("Connection#enqueue: frame built per connection vs given (issue #621)")

  # One ephemeral envelope (negative msg_id: no cursor dedup), reused, and the
  # frame the dispatcher built for its wake.
  live = Pgbus::Client::ReadAfter::Envelope.new(msg_id: -1, enqueued_at: "2026-11-02T00:00:00Z",
                                                payload: "<turbo-stream>live</turbo-stream>", source: "ephemeral")
  frames = { -1 => Pgbus::Streams::Envelope.message(id: -1, event: "turbo-stream", data: live.payload).freeze }.freeze
  frame_conn = build_connection

  BenchSupport.ips(time: 3, warmup: 1) do |x|
    x.report("enqueue (frame built here)") { frame_conn.enqueue([live], deadline_ms: 250) }
    x.report("enqueue (frames: given)") { frame_conn.enqueue([live], deadline_ms: 250, frames: frames) }
  end

  BenchSupport.allocations("enqueue frame built here") { 1_000.times { frame_conn.enqueue([live], deadline_ms: 250) } }
  BenchSupport.allocations("enqueue frames: given") do
    1_000.times { frame_conn.enqueue([live], deadline_ms: 250, frames: frames) }
  end
end
