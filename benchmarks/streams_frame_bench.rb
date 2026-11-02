# frozen_string_literal: true

# Streams frame-path micro-benchmark (issue #527).
#
# The Listener -> StreamEventDispatcher -> Connection#enqueue hand-off carries
# Data values (WakeMessage, StreamEnvelope, ReadAfter::Envelope) that are kept
# Ractor-shareable: their Strings are frozen where the values are built. This
# bench measures what that costs on the SSE hot path: building a wake from a
# NOTIFY, the ephemeral fanout (JSON parse + envelope + enqueue to 10
# connections) and the durable unwrap. No DB or Puma needed.
#
# Issue #621 adds the framing itself: Envelope.message for a one-line and a
# multi-line payload (the one-line fast path skips the split/map/join), and a
# durable wake of 3 broadcasts to 100 real Connections over a writer that takes
# every frame at once: the dispatcher + Connection CPU of one fan-out, which
# frames each envelope once per wake since #621 instead of once per connection.
# FrameSink above frames nothing, so "ephemeral wake -> 10 connections" moved
# the one frame #621 builds per wake into the dispatcher's column.

require_relative "bench_helper"

# An ephemeral wake always records a stat (record_stat ignores
# streams_stats_enabled for ephemeral frames). This bench has no database, so
# the stat is skipped and only the frame path is timed.
Dispatcher = Class.new(Pgbus::Web::Streamer::StreamEventDispatcher) do
  private def record_stat(**) = nil
end

# Accepts every frame and records nothing: only the dispatcher's own cost.
FrameSink = Class.new do
  attr_reader :id, :stream_name, :context
  attr_accessor :last_msg_id_sent, :presence_member

  def initialize(id)
    @id = id
    @stream_name = "chat"
    @context = nil
    @last_msg_id_sent = 0
  end

  def enqueue(envelopes, deadline_ms: nil, frames: nil) = envelopes # rubocop:disable Lint/UnusedMethodArgument
  def dead? = false
end

def build_dispatcher(connections)
  registry = Pgbus::Web::Streamer::Registry.new
  connections.times { |i| registry.register(FrameSink.new("c#{i}")) }
  config = Pgbus::Configuration.new.tap { |c| c.streams_stats_enabled = false }
  client = Struct.new(:config).new(config)
  listener = Struct.new(:ensure_listening, :remove_listening).new
  Dispatcher.new(client: client, registry: registry, listener: listener, dispatch_queue: Queue.new,
                 logger: Logger.new(IO::NULL), config: config)
end

CHANNEL = "pgmq.q_pgbus_bench_chat.INSERT"
EPHEMERAL = JSON.generate("html" => "<turbo-stream action=\"append\" target=\"messages\"><template>hi</template></turbo-stream>")
DURABLE = Pgbus::Client::ReadAfter::Envelope.new(
  msg_id: 42, enqueued_at: +"2026-10-15T00:00:00Z", payload: +EPHEMERAL, source: +"live"
)

dispatcher = build_dispatcher(10)

NullWriter = Class.new do
  def write(_connection, _bytes, deadline_ms:) = :ok # rubocop:disable Lint/UnusedMethodArgument
end.new

# Hands out 3 fresh durable broadcasts per read, as PGMQ would after a NOTIFY.
ScaleClient = Struct.new(:config, :next_id) do
  def read_after(_stream, after_id:, limit:) # rubocop:disable Lint/UnusedMethodArgument
    Array.new(3) do
      self.next_id += 1
      Pgbus::Client::ReadAfter::Envelope.new(msg_id: next_id, enqueued_at: +"2026-11-02T00:00:00Z",
                                             payload: +EPHEMERAL, source: +"live")
    end
  end
end

def build_fanout_dispatcher(connections)
  registry = Pgbus::Web::Streamer::Registry.new
  connections.times do |i|
    registry.register(Pgbus::Web::Streamer::Connection.new(id: "c#{i}", stream_name: "chat", io: nil, since_id: 0,
                                                           writer: NullWriter, write_deadline_ms: 5_000))
  end
  config = Pgbus::Configuration.new.tap { |c| c.streams_stats_enabled = false }
  config.define_singleton_method(:queue_name) { |name| name }
  listener = Struct.new(:ensure_listening, :remove_listening).new
  Dispatcher.new(client: ScaleClient.new(config, 0), registry: registry, listener: listener,
                 dispatch_queue: Queue.new, logger: Logger.new(IO::NULL), config: config)
end

fanout_dispatcher = build_fanout_dispatcher(100)
DURABLE_WAKE = Dispatcher::WakeMessage.new(queue_name: "chat", payload: nil)
ONE_LINE = "<turbo-stream action=\"append\" target=\"messages\"><template>hi</template></turbo-stream>"
MULTI_LINE = "<turbo-stream action=\"append\" target=\"messages\">\n  <template>hi</template>\n</turbo-stream>"

BenchSupport.header("Frame path: wake construction, ephemeral fanout (10 connections), durable unwrap")

BenchSupport.ips(time: 3, warmup: 1) do |x|
  x.report("WakeMessage from a NOTIFY") do
    Dispatcher::WakeMessage.new(queue_name: CHANNEL[7..-8], payload: +EPHEMERAL)
  end

  x.report("ephemeral wake -> 10 connections") do
    dispatcher.send(:handle, Dispatcher::WakeMessage.new(queue_name: "chat", payload: +EPHEMERAL))
  end

  x.report("durable envelope unwrap") do
    dispatcher.send(:unwrap_stream_envelope, DURABLE)
  end
end

BenchSupport.header("Framing (issue #621): Envelope.message, and one durable wake of 3 to 100 Connections")

BenchSupport.ips(time: 3, warmup: 1) do |x|
  x.report("Envelope.message one-line") do
    Pgbus::Streams::Envelope.message(id: 42, event: "turbo-stream", data: ONE_LINE)
  end

  x.report("Envelope.message multi-line") do
    Pgbus::Streams::Envelope.message(id: 42, event: "turbo-stream", data: MULTI_LINE)
  end
end

BenchSupport.ips(time: 3, warmup: 1) do |x|
  x.report("durable wake 3 -> 100 connections") { fanout_dispatcher.send(:handle, DURABLE_WAKE) }
end

BenchSupport.header("allocations per 1000 frames")

BenchSupport.allocations("WakeMessage from a NOTIFY") do
  1_000.times { Dispatcher::WakeMessage.new(queue_name: CHANNEL[7..-8], payload: +EPHEMERAL) }
end

BenchSupport.allocations("ephemeral wake -> 10 connections") do
  1_000.times { dispatcher.send(:handle, Dispatcher::WakeMessage.new(queue_name: "chat", payload: +EPHEMERAL)) }
end

BenchSupport.allocations("durable envelope unwrap") do
  1_000.times { dispatcher.send(:unwrap_stream_envelope, DURABLE) }
end

BenchSupport.allocations("Envelope.message one-line") do
  1_000.times { Pgbus::Streams::Envelope.message(id: 42, event: "turbo-stream", data: ONE_LINE) }
end

BenchSupport.allocations("Envelope.message multi-line") do
  1_000.times { Pgbus::Streams::Envelope.message(id: 42, event: "turbo-stream", data: MULTI_LINE) }
end

BenchSupport.allocations("durable wake 3 -> 100 conns (x10)") do
  10.times { fanout_dispatcher.send(:handle, DURABLE_WAKE) }
end
