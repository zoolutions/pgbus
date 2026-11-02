# frozen_string_literal: true

require "spec_helper"
require "memory_profiler"
require "stringio"

# Issue #621: a hard gate on the durable fan-out's framing and allocations.
# One wake with 3 envelopes to 50 connections frames each envelope once (not
# once per connection), and the pgbus-owned objects per connection per wake
# stay under a budget measured when the frames moved to the dispatcher. Only
# objects allocated from pgbus's own files count, as in
# executor_allocation_budget_spec.rb: JSON and the Ruby version allocate the
# rest.
RSpec.describe Pgbus::Web::Streamer::StreamEventDispatcher do
  subject(:dispatcher) do
    described_class.new(client: client, registry: registry, dispatch_queue: Queue.new,
                        listener: double("Listener", ensure_listening: nil, remove_listening: nil),
                        logger: Logger.new(IO::NULL), config: config)
  end

  let(:connection_count) { 50 }
  let(:config) { Pgbus::Configuration.new.tap { |c| c.streams_stats_enabled = false } }
  let(:client) { double("Pgbus::Client", config: config) }
  let(:registry) { Pgbus::Web::Streamer::Registry.new }
  # A writer that takes every frame at once: the socket is not what is measured.
  let(:writer) do
    Class.new do
      def write(_connection, _bytes, deadline_ms:) = :ok # rubocop:disable Lint/UnusedMethodArgument
    end.new
  end
  let(:connections) do
    Array.new(connection_count) do |i|
      Pgbus::Web::Streamer::Connection.new(id: "c#{i}", stream_name: "chat", io: StringIO.new, since_id: 0,
                                           writer: writer, write_deadline_ms: 5_000)
    end
  end
  let(:next_id) { [0] }

  before do
    allow(config).to receive(:queue_name) { |name| name }
    connections.each { |c| registry.register(c) }
    # Three fresh broadcasts per wake, as PGMQ reads them back.
    allow(client).to receive(:read_after) do
      Array.new(3) do
        next_id[0] += 1
        row = { "msg_id" => next_id[0].to_s, "enqueued_at" => +"2026-11-02 10:00:00+00",
                "message" => JSON.generate("html" => "<turbo-stream action=\"append\">#{next_id[0]}</turbo-stream>"),
                "source" => +"live" }
        Pgbus::Client::ReadAfter.instance_method(:build_envelope).bind_call(Object.new, row)
      end
    end
  end

  def wake
    dispatcher.send(:handle, described_class::WakeMessage.new(queue_name: "chat"))
  end

  def pgbus_allocations(&)
    pgbus_lib = File.expand_path("../../../../lib/pgbus", __dir__)
    report = MemoryProfiler.report(&)
    report.allocated_objects_by_file.select { |row| row[:data].start_with?(pgbus_lib) }.sum { |row| row[:count] }
  end

  it "frames each envelope once per wake, however many connections there are" do
    allow(Pgbus::Streams::Envelope).to receive(:message).and_call_original

    wake

    expect(Pgbus::Streams::Envelope).to have_received(:message).exactly(3).times
    expect(connections.map(&:last_msg_id_sent)).to all(eq(3))
  end

  # Measured 2026-11-02 on Ruby 3.4.2: 22.5 objects per connection per wake
  # before #621 (each connection framed all three envelopes: the frame, the
  # data-line split, map and join), 1.8 after (the connection's filtered
  # batch and its written list; the frames are the dispatcher's, 3 per wake).
  it "allocates fewer than 3 pgbus-owned objects per connection per wake" do
    3.times { wake }

    per_connection = pgbus_allocations { 10.times { wake } } / (10.0 * connection_count)

    expect(per_connection).to be < 3
  end
end
