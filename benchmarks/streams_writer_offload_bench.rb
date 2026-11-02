# frozen_string_literal: true

# Streams writer-offload benchmark (issue #321).
#
# The head-of-line fix in #320 BOUNDED each slow-client stall to
# streams_fanout_write_deadline_ms (250ms); a wake with K slow clients still
# cost ~K*deadline on the single dispatcher thread. #321 moves the durable
# fanout socket write OFF the dispatcher into a writer pool, so the dispatcher's
# per-wake time DECOUPLES from the slow-client count K.
#
# This bench measures the dispatcher's `handle_durable_wake` wall time for a
# broadcast to N fast + K slow connections, offload OFF (main behavior) vs ON.
# No DB/Puma — fake recording/blocking writers, like the other streams benches.
#
#   Scenario A (the win): wall time vs K. OFF scales with K; ON is ~flat.
#   Scenario B (zero-cost happy path): N fast, K=0, ON vs OFF throughput +
#     allocations. The ack round-trip is the only new per-op cost; retained
#     per op MUST be 0.

require_relative "bench_helper"
require "concurrent"

require_relative "../lib/pgbus/web/streamer/registry"

# A fast connection: enqueue returns immediately. context/presence_member are
# present because the dispatcher's visible_envelopes_for + presence path touch
# them (a fake missing them would raise, get swallowed, and skip the fanout).
FAST = Class.new do
  attr_reader :id
  attr_accessor :presence_member

  def initialize(id)
    @id = id
    @presence_member = nil
  end

  def stream_name = "chat"
  def context = nil
  def last_msg_id_sent = 0
  def enqueue(envelopes, deadline_ms: nil, frames: nil) = envelopes # rubocop:disable Lint/UnusedMethodArgument
  def dead? = false
  def mark_dead! = nil
end

# A slow connection: enqueue sleeps `slow_s` (models a full socket buffer that
# blocks up to the fanout deadline) then marks itself dead.
SLOW = Class.new do
  attr_reader :id
  attr_accessor :presence_member

  def initialize(id, slow_s)
    @id = id
    @slow_s = slow_s
    @dead = false
    @presence_member = nil
  end

  def stream_name = "chat"
  def context = nil
  def last_msg_id_sent = 0

  def enqueue(_envelopes, deadline_ms: nil, frames: nil) # rubocop:disable Lint/UnusedMethodArgument
    sleep(@slow_s)
    @dead = true
    []
  end

  def dead? = @dead
  def mark_dead! = @dead = true
end

def build_dispatcher(writer_threads:)
  registry = Pgbus::Web::Streamer::Registry.new
  ack_queue = Queue.new
  pump = nil
  if writer_threads.positive?
    pump = Pgbus::Web::Streamer::OutboundPump.new(
      threads: writer_threads, ack_queue: ack_queue,
      on_dead: ->(_c) {}, logger: Logger.new(IO::NULL)
    )
    pump.start
  end

  config = Pgbus::Configuration.new.tap do |c|
    c.streams_stats_enabled = false
    c.streams_fanout_write_deadline_ms = 250
  end
  client = Object.new
  client.define_singleton_method(:config) { config }
  client.define_singleton_method(:read_after) do |_s, after_id:, limit:| # rubocop:disable Lint/UnusedBlockArgument
    [Pgbus::Client::ReadAfter::Envelope.new(msg_id: 1, enqueued_at: nil, payload: "<x/>", source: "live")]
  end

  dispatcher = Pgbus::Web::Streamer::StreamEventDispatcher.new(
    client: client, registry: registry, listener: nil,
    dispatch_queue: Queue.new, logger: Logger.new(IO::NULL),
    config: config, pump: pump, ack_queue: ack_queue
  )
  [dispatcher, registry, pump]
end

def register_conns(registry, fast:, slow:, slow_s:)
  fast.times { |i| registry.register(FAST.new("fast-#{i}")) }
  slow.times { |i| registry.register(SLOW.new("slow-#{i}", slow_s)) }
end

def time_wake(dispatcher)
  wake = Pgbus::Web::Streamer::StreamEventDispatcher::WakeMessage.new(queue_name: "chat")
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  dispatcher.send(:handle, wake)
  (Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000.0
end

puts "=" * 72
puts "Streams writer-offload benchmark (issue #321)"
puts "=" * 72

SLOW_S = 0.05 # 50ms per slow client

# Warm up JIT/method caches with a throwaway wake before timing.
warm_d, warm_reg, = build_dispatcher(writer_threads: 0)
register_conns(warm_reg, fast: 5, slow: 0, slow_s: SLOW_S)
3.times { time_wake(warm_d) }

BenchSupport.header("Scenario A — handle_durable_wake wall time vs K slow clients")
puts "  K      OFF (inline) ms    ON (2 writers) ms"
[0, 1, 5, 20].each do |k|
  # A fresh dispatcher per K: each times exactly ONE wake, so a slow conn
  # (marked dead after its first stall) is only skipped on a hypothetical
  # second wake — never within the single measured wake.
  off_d, off_reg, = build_dispatcher(writer_threads: 0)
  register_conns(off_reg, fast: 50, slow: k, slow_s: SLOW_S)
  off_ms = time_wake(off_d)

  on_d, on_reg, on_pump = build_dispatcher(writer_threads: 2)
  register_conns(on_reg, fast: 50, slow: k, slow_s: SLOW_S)
  on_ms = time_wake(on_d)
  on_pump&.stop

  puts format("  %-6d %-18.2f %-18.2f", k, off_ms, on_ms)
end
puts "  Expected: OFF grows ~K*#{(SLOW_S * 1000).to_i}ms; ON stays ~flat (decoupled from K)."

BenchSupport.header("Scenario B — dispatcher per-wake cost, happy path (N fast, K=0)")
puts "  NOTE: the ON path's per-wake work is post() (a queue push per conn) +"
puts "  apply_acks (drain). The pump's writer threads flush concurrently; here"
puts "  we drain acks between wakes so we measure DISPATCHER cost at steady"
puts "  state, not the pump backlog. The offload's value is Scenario A, not raw"
puts "  single-wake throughput — the dispatcher does slightly MORE bookkeeping"
puts "  per wake in exchange for never blocking on a socket write.\n\n"

off_d, off_reg, = build_dispatcher(writer_threads: 0)
register_conns(off_reg, fast: 100, slow: 0, slow_s: SLOW_S)
on_d, on_reg, on_pump = build_dispatcher(writer_threads: 2)
register_conns(on_reg, fast: 100, slow: 0, slow_s: SLOW_S)

BenchSupport.ips(time: 3, warmup: 1) do |x|
  x.report("fanout inline (OFF)") { time_wake(off_d) }
  x.report("fanout offload (ON, steady)") do
    time_wake(on_d)
    on_d.send(:apply_acks) # drain this wake's acks so the backlog stays bounded
  end
end
on_pump&.stop

puts "\nHonest framing: this is NOT faster fanout — total bytes written are"
puts "unchanged and slow clients still take their time. What improved: FAST"
puts "clients no longer wait behind slow ones (Scenario A), and the dispatcher"
puts "is freed to process the next wake/connect. The per-wake dispatcher cost is"
puts "slightly higher under offload (post + ack bookkeeping); that overhead buys"
puts "the decoupling. System-level latency win, not a method speedup."
