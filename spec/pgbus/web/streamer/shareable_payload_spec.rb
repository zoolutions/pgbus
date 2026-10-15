# frozen_string_literal: true

require "spec_helper"

# The seam a future Ractor dispatcher would use (issue #527): every value
# handed from the Listener (or the hub client) to the StreamEventDispatcher,
# and from the dispatcher to Connection#enqueue, is Ractor-shareable, so it
# could cross a Ractor::Port by reference. No behaviour changes; the Strings
# inside are frozen where the values are built.
RSpec.describe "the streamer's dispatch payloads" do # rubocop:disable RSpec/DescribeClass
  let(:dispatch_queue) { Queue.new }
  let(:ephemeral_json) do
    # Built the way PG hands a NOTIFY payload over: a fresh, unfrozen String.
    String.new(JSON.generate("html" => "<turbo-stream>hi</turbo-stream>", "exclude" => "conn-1", "event" => "chat"))
  end

  describe "Listener::WakeMessage" do
    let(:listener) do
      Pgbus::Web::Streamer::Listener.new(
        pg_connection: double("PG::Connection"), dispatch_queue: dispatch_queue,
        health_check_ms: 50, logger: Logger.new(IO::NULL), connection_factory: -> {}
      )
    end

    it "is shareable for an ephemeral wake" do
      listener.send(:handle_notify, +"pgmq.q_chat.INSERT", ephemeral_json)

      expect(Ractor.shareable?(dispatch_queue.pop)).to be(true)
    end

    it "is shareable for a durable wake" do
      listener.send(:handle_notify, +"pgmq.q_chat.INSERT")

      expect(Ractor.shareable?(dispatch_queue.pop)).to be(true)
    end

    it "is shareable when the master hub relays it" do
      client = Pgbus::Web::Streamer::HubClient.new(socket_path: "/nonexistent", dispatch_queue: dispatch_queue)

      client.send(:handle_frame, { "t" => "wake", "q" => +"chat", "p" => ephemeral_json })

      expect(Ractor.shareable?(dispatch_queue.pop)).to be(true)
    end
  end

  describe "the frames StreamEventDispatcher enqueues to a Connection" do
    let(:dispatcher_class) { Pgbus::Web::Streamer::StreamEventDispatcher }
    let(:enqueued) { [] }
    let(:connection) do
      frames = enqueued
      double("Connection", id: "conn-2", stream_name: "chat", context: nil, last_msg_id_sent: 0,
                           presence_member: nil, dead?: false).tap do |conn|
        allow(conn).to receive(:enqueue) { |envelopes, **| frames.concat(envelopes) }
      end
    end
    let(:registry) { Pgbus::Web::Streamer::Registry.new.tap { |r| r.register(connection) } }
    let(:config) { Pgbus::Configuration.new.tap { |c| c.streams_stats_enabled = false } }
    let(:client) { double("Pgbus::Client", config: config) }
    let(:dispatcher) do
      dispatcher_class.new(client: client, registry: registry, dispatch_queue: Queue.new,
                           listener: double("Listener", ensure_listening: nil, remove_listening: nil),
                           logger: Logger.new(IO::NULL), config: config)
    end

    before do
      allow(config).to receive(:queue_name) { |name| name }
    end

    it "is shareable for an ephemeral broadcast" do
      dispatcher.send(:handle, dispatcher_class::WakeMessage.new(queue_name: +"chat", payload: ephemeral_json))

      expect(enqueued).not_to be_empty
      expect(enqueued).to all(satisfy { |frame| Ractor.shareable?(frame) })
    end

    it "is shareable for a durable broadcast, as read back from PGMQ" do
      row = { "msg_id" => "7", "enqueued_at" => +"2026-10-15 10:00:00+00", "message" => ephemeral_json, "source" => +"live" }
      envelope = Pgbus::Client::ReadAfter.instance_method(:build_envelope).bind_call(Object.new, row)
      allow(client).to receive(:read_after).and_return([envelope])

      dispatcher.send(:handle, dispatcher_class::WakeMessage.new(queue_name: +"chat"))

      expect(enqueued).not_to be_empty
      expect(enqueued).to all(satisfy { |frame| Ractor.shareable?(frame) })
    end

    it "is shareable for a durable payload that is not a pgbus broadcast (passed through)" do
      row = { "msg_id" => "8", "enqueued_at" => +"2026-10-15 10:00:00+00", "message" => +"plain", "source" => +"live" }
      envelope = Pgbus::Client::ReadAfter.instance_method(:build_envelope).bind_call(Object.new, row)
      allow(client).to receive(:read_after).and_return([envelope])

      dispatcher.send(:handle, dispatcher_class::WakeMessage.new(queue_name: +"chat"))

      expect(enqueued).to all(satisfy { |frame| Ractor.shareable?(frame) })
    end
  end
end
