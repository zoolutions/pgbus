# frozen_string_literal: true

# Streams frame-path micro-benchmark (issue #527).
#
# The Listener -> StreamEventDispatcher -> Connection#enqueue hand-off carries
# Data values (WakeMessage, StreamEnvelope, ReadAfter::Envelope) that are kept
# Ractor-shareable: their Strings are frozen where the values are built. This
# bench measures what that costs on the SSE hot path: building a wake from a
# NOTIFY, the ephemeral fanout (JSON parse + envelope + enqueue to 10
# connections) and the durable unwrap. No DB or Puma needed.

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

  def enqueue(envelopes, deadline_ms: nil) = envelopes # rubocop:disable Lint/UnusedMethodArgument
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
