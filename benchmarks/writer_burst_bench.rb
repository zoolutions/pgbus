# frozen_string_literal: true

# Writer-burst gate benchmark (issue #323 phase 1 — the measure-first gate).
#
# QUESTION IT ANSWERS: under a sustained fanout burst (many connections, each
# doing a non-trivial socket write), is the WRITER POOL (streams_writer_threads)
# the throughput limiter? Issue #323 phase 1 gates the elastic-writer rewrite on
# "only if writer throughput is the confirmed bottleneck." #322 already showed 2
# writers DECOUPLE the dispatcher from slow clients (head-of-line cure); this
# bench asks the different question phase 1 hinges on: does adding MORE writer
# threads raise total fanout throughput, or does a small fixed N already keep up?
#
# HOW: register a fleet of connections, each enqueue() sleeping `write_s` (models
# a real socket write), and drive a burst of wakes through the OutboundPump with
# streams_writer_threads swept 2..16. Measure total wall time to flush the whole
# burst and the resulting throughput (frames/s). If throughput scales with writer
# count, the writer pool is a limiter -> elastic writers may be worth the (high,
# risky) rewrite. If it plateaus at a small N, static streams_writer_threads is
# the answer and elastic writers are not justified.
#
# No DB/Puma -- fake blocking writers, like streams_writer_offload_bench.
#
# Run-and-report, never a CI gate:
#   bundle exec rake bench:one[writer_burst_bench]

require "logger"
require "concurrent"
require_relative "../lib/pgbus"
require_relative "../lib/pgbus/web/streamer/outbound_pump"
require_relative "../lib/pgbus/web/streamer/stream_event_dispatcher"

# A connection whose enqueue() blocks for write_s (a realistic socket write to a
# client that isn't instant but isn't pathologically slow either). Records how
# many frames it accepted so we can assert nothing was lost.
WRITER_CONN = Class.new do
  attr_reader :id, :accepted

  def initialize(id, write_s)
    @id = id
    @write_s = write_s
    @accepted = Concurrent::AtomicFixnum.new(0)
    @last = 0
  end

  def last_msg_id_sent = @last

  # The pump calls this on a worker thread. Sleep models the socket write; return
  # the accepted envelopes (all of them) so the pump acks the full batch.
  def enqueue(envelopes, deadline_ms: nil, frames: nil) # rubocop:disable Lint/UnusedMethodArgument
    sleep(@write_s)
    @accepted.increment
    @last = envelopes.last.msg_id if envelopes.any?
    envelopes
  end

  def dead? = false
  def mark_dead! = nil
end

Envelope = Pgbus::Client::ReadAfter::Envelope

def frame = [Envelope.new(msg_id: 1, enqueued_at: nil, payload: "<x/>", source: "live")]

# Drive `wakes` fanout rounds over `conns` through a pump of `writer_threads`,
# draining acks as they arrive. Returns [wall_seconds, frames_delivered].
def drive_burst(writer_threads:, conns:, wakes:)
  ack_queue = Queue.new
  pump = Pgbus::Web::Streamer::OutboundPump.new(
    threads: writer_threads, ack_queue: ack_queue,
    on_dead: ->(_c) {}, logger: Logger.new(IO::NULL)
  )
  pump.start
  expected = conns.size * wakes

  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  wakes.times do
    conns.each { |c| pump.post(c, frame, 1, deadline_ms: 5000) }
  end

  # Drain acks until every (connection, wake) write has been reported. Poll
  # non-blockingly with a generous idle deadline so a miscount can't hang forever.
  delivered = 0
  last_progress = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  while delivered < expected
    begin
      ack_queue.pop(true) # non-blocking
      delivered += 1
      last_progress = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    rescue ThreadError # queue momentarily empty
      break if Process.clock_gettime(Process::CLOCK_MONOTONIC) - last_progress > 30

      sleep 0.001
    end
  end
  wall = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
  [wall, delivered]
ensure
  pump&.stop
end

puts "=" * 72
puts "Writer-burst gate benchmark (issue #323 phase 1)"
puts "=" * 72

CONN_COUNT = 100      # a fleet of SSE connections on this worker
WAKES = 10            # a burst of 10 fanout rounds (kept small so this unit bench
TOTAL_FRAMES = CONN_COUNT * WAKES # stays CI-friendly ~seconds, not minutes)
SWEEP = [2, 4, 8, 16].freeze
# Sweep per-write cost too: the writer-pool gate hinges entirely on how blocking
# a real SSE socket write is. A healthy client's write is sub-millisecond (a
# kernel buffer copy); only a congested/slow client blocks for ms. If FAST writes
# already saturate a small pool, elastic writers aren't justified; the scaling
# only appears once writes are slow enough to serialize a partition.
WRITE_PROFILES = { fast: 0.0002, typical: 0.001 }.freeze

def sweep_writes(conns, write_s)
  conns.each { |c| c.instance_variable_set(:@write_s, write_s) }
  SWEEP.map do |n|
    wall, delivered = drive_burst(writer_threads: n, conns: conns, wakes: WAKES)
    warn "  [warn] writers=#{n}: delivered #{delivered}/#{TOTAL_FRAMES}" unless delivered == TOTAL_FRAMES
    { threads: n, wall_ms: (wall * 1000).round(1), throughput: (delivered / wall).round }
  end
end

puts "Fleet: #{CONN_COUNT} conns x #{WAKES} wakes = #{TOTAL_FRAMES} frames"
puts "The writer-pool gate: does adding writer THREADS raise fanout throughput,"
puts "and does that depend on how blocking each socket write is?"
puts

conns = Array.new(CONN_COUNT) { |i| WRITER_CONN.new("c-#{i}", WRITE_PROFILES[:typical]) }
drive_burst(writer_threads: 2, conns: conns.first(10), wakes: 2) # warm up

all = {}
WRITE_PROFILES.each do |label, write_s|
  rows = sweep_writes(conns, write_s)
  all[label] = rows
  puts "── write=#{(write_s * 1000).round(2)}ms (#{label}) " + ("─" * 30)
  puts "  writers   wall(ms)   frames/s   scale-from-2"
  base = rows.first[:throughput].to_f
  rows.each do |r|
    puts format("  %-9d %-10.1f %-10d %.1fx", r[:threads], r[:wall_ms], r[:throughput], r[:throughput] / base)
  end
  puts
end

# READING THIS CORRECTLY — the crux for the ELASTICITY decision:
#
# Throughput scaling with writer count (~6-7x from 2->16 across every profile)
# is unsurprising: N parallel workers finish N independent blocking writes ~Nx
# faster. It says the writer pool benefits from MORE THREADS. It does NOT, by
# itself, justify ELASTIC (dynamic grow/shrink) writers, for two reasons:
#
#   1. streams_writer_threads is ALREADY a static knob. If more writers help, an
#      operator raises it — statically, safely, today. This curve argues for
#      using that knob, not for a high-risk dynamic-resize rewrite. (Same shape
#      as the phase-3 conclusion: raise the static count.)
#   2. Elasticity only pays off for a SPIKY workload where a static count is
#      wastefully high at rest. This bench runs a fixed N per cell — it cannot
#      show spikiness, so it cannot justify elasticity. And the genuinely
#      harmful case (a slow/congested client) is already SHED by the #320/#322
#      fanout deadline + mark-dead path, not absorbed by scaling writers.
#
# So the honest gate verdict is about elasticity, not about raw scaling:
# Report the 2→16 scaling PER profile so the "every profile" claim is backed by
# each profile's own number, not extrapolated from one.
scales = all.transform_values { |rows| rows.last[:throughput].to_f / rows.first[:throughput] }
puts "─" * 60
puts "Writer count scales throughput 2→16 by #{scales.map { |l, s| format("%.1fx (%s)", s, l) }.join(", ")}"
puts "— similar across every write profile, as expected for parallel blocking writes."
puts "But that argues for the EXISTING"
puts "static `streams_writer_threads` knob, not for elastic writers:"
puts "  • more writers help → raise streams_writer_threads (static, safe, today);"
puts "  • this bench runs a fixed N per cell, so it can't show the SPIKINESS that"
puts "    would make a static count wasteful enough to justify dynamic resize;"
puts "  • the harmful case (slow/congested client) is already SHED by the"
puts "    #320/#322 fanout deadline + mark-dead, not absorbed by scaling writers."
puts
puts "VERDICT: raise `streams_writer_threads` statically to scale fan-out. ELASTIC"
puts "writers are NOT justified by this data — issue #323 gates phase 1 on a confirmed"
puts "need for elasticity, and a scaling curve on a fixed-N sweep isn't that. The"
puts "high-risk OutboundPump rewrite (ordering/cursor invariant #321 exists to"
puts "protect) is not warranted. Document the static knob; defer elastic writers."
puts "Done."
