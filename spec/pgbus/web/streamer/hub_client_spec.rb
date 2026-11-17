# frozen_string_literal: true

require "spec_helper"
require "socket"
require "tmpdir"

RSpec.describe Pgbus::Web::Streamer::HubClient do
  subject(:client) do
    described_class.new(
      socket_path: socket_path,
      dispatch_queue: dispatch_queue,
      ack_timeout: 0.5,
      on_failure: -> { failures << :failed },
      logger: logger
    )
  end

  let(:tmpdir) { Dir.mktmpdir("pgbus-hub-client-spec") }
  let(:socket_path) { File.join(tmpdir, "hub.sock") }
  let(:dispatch_queue) { Queue.new }
  let(:failures) { [] }
  let(:logger) { Logger.new(IO::NULL) }

  let(:server) { UNIXServer.new(socket_path) }
  let(:master_side) { [] }

  after do
    client.stop
    master_side.each { |s| s.close unless s.closed? }
    server.close unless server.closed?
    FileUtils.remove_entry(tmpdir) if File.directory?(tmpdir)
  end

  def accept_master
    server # bind first
    thread = Thread.new { server.accept }
    yield if block_given?
    sock = thread.value
    master_side << sock
    sock
  end

  def master_read(sock)
    Pgbus::Web::Streamer::HubProtocol.read_frame(sock)
  end

  def master_send(sock, message)
    sock.write(Pgbus::Web::Streamer::HubProtocol.encode(message))
  end

  def wait_until(timeout: 2.0)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    sleep 0.01 until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
  end

  describe "subscription round trip" do
    it "ensure_listening blocks until the master acks" do
      master = accept_master { client.connect }
      acker = Thread.new do
        frame = master_read(master)
        master_send(master, { "t" => "ack", "q" => frame["q"] }) if frame["t"] == "sub"
      end

      expect(client.ensure_listening("pgbus_stream_chat")).to eq(:done)
      acker.join
    end

    it "raises HubUnavailableError when the ack never arrives (and marks the transport dead)" do
      accept_master { client.connect } # master reads nothing, acks nothing

      expect { client.ensure_listening("pgbus_stream_chat") }
        .to raise_error(described_class::HubUnavailableError, /ack/i)
      expect { client.ensure_listening("pgbus_stream_other") }
        .to raise_error(described_class::HubUnavailableError)
    end

    it "remove_listening sends an unsub frame without waiting" do
      master = accept_master { client.connect }

      client.remove_listening("pgbus_stream_chat")

      expect(master_read(master)).to eq({ "t" => "unsub", "q" => "pgbus_stream_chat" })
    end
  end

  describe "wake delivery" do
    it "pushes wake frames into the dispatch queue as WakeMessages, payload intact" do
      master = accept_master { client.connect }

      master_send(master, { "t" => "wake", "q" => "pgbus_stream_chat", "p" => "<div>hi</div>" })

      message = dispatch_queue.pop
      expect(message).to be_a(Pgbus::Web::Streamer::Listener::WakeMessage)
      expect(message.queue_name).to eq("pgbus_stream_chat")
      expect(message.payload).to eq("<div>hi</div>")
    end

    it "delivers durable wakes with a nil payload" do
      master = accept_master { client.connect }

      master_send(master, { "t" => "wake", "q" => "pgbus_stream_chat", "p" => nil })

      expect(dispatch_queue.pop.payload).to be_nil
    end
  end

  describe "status tracking" do
    it "tracks the hub's health broadcasts" do
      master = accept_master { client.connect }
      expect(client.hub_healthy?).to be true # optimistic before first status

      master_send(master, { "t" => "status", "healthy" => false })
      wait_until { client.hub_healthy? == false }
      expect(client.hub_healthy?).to be false

      master_send(master, { "t" => "status", "healthy" => true })
      wait_until { client.hub_healthy? == true }
      expect(client.hub_healthy?).to be true
    end
  end

  describe "transport failure" do
    it "fires on_failure and fails pending subs when the master dies (EOF)" do
      master = accept_master { client.connect }

      waiter = Thread.new do
        client.ensure_listening("pgbus_stream_chat")
      rescue described_class::HubUnavailableError
        :raised
      end
      wait_until { client.instance_variable_get(:@pending_acks).values.flatten.any? }
      master.close

      expect(waiter.value).to eq(:raised)
      wait_until { failures.any? }
      expect(failures).to eq([:failed])
    end

    it "raises HubUnavailableError from connect when no socket exists" do
      expect { client.connect }.to raise_error(described_class::HubUnavailableError)
    end

    it "exposes its reader thread via #threads while connected and none after stop (issue #443)" do
      expect(client.threads).to eq([])
      accept_master { client.connect }
      expect(client.threads.size).to eq(1)
      client.stop
      expect(client.threads).to eq([])
    end

    it "does not fire on_failure for a clean stop" do
      accept_master { client.connect }

      client.stop
      sleep 0.1

      expect(failures).to be_empty
    end
  end

  describe "process signals (issue #674)" do
    subject(:client) do
      described_class.new(
        socket_path: socket_path, dispatch_queue: dispatch_queue, ack_timeout: 0.5,
        on_failure: -> { failures << :failed }, signal_sink: sink, logger: logger
      )
    end

    let(:sink) do
      Class.new do
        attr_reader :received

        def initialize = (@received = Queue.new)
        def signal(channel, payload) = @received << [:signal, channel, payload]
        def gap = @received << [:gap]
      end.new
    end

    it "ensure_signal_listening sends an ssub frame and blocks until the master's sack" do
      master = accept_master { client.connect }
      acker = Thread.new do
        frame = master_read(master)
        master_send(master, { "t" => "sack", "c" => frame["c"] }) if frame["t"] == "ssub"
        frame
      end

      expect(client.ensure_signal_listening("pgbus_signal_a")).to eq(:done)
      expect(acker.value).to eq({ "t" => "ssub", "c" => "pgbus_signal_a" })
    end

    # A master from before signals logs the unknown frame and never acks.
    it "fails over when the master never acks the signal subscription" do
      accept_master { client.connect }

      expect { client.ensure_signal_listening("pgbus_signal_a") }
        .to raise_error(described_class::HubUnavailableError)
      wait_until { failures.any? }
      expect(failures).to eq([:failed])
    end

    it "hands signal and gap frames to the signal sink, not the dispatch queue" do
      master = accept_master { client.connect }

      master_send(master, { "t" => "signal", "c" => "pgbus_signal_a", "p" => "id=1" })
      master_send(master, { "t" => "signal", "c" => "pgbus_signal_a", "p" => nil })
      master_send(master, { "t" => "gap" })

      expect(sink.received.pop(timeout: 2)).to eq([:signal, "pgbus_signal_a", "id=1"])
      expect(sink.received.pop(timeout: 2)).to eq([:signal, "pgbus_signal_a", nil])
      expect(sink.received.pop(timeout: 2)).to eq([:gap])
      expect(dispatch_queue.size).to eq(0)
    end

    it "keeps the transport alive when the sink raises" do
      allow(sink).to receive(:signal).and_raise("sink boom")
      master = accept_master { client.connect }

      master_send(master, { "t" => "signal", "c" => "pgbus_signal_a", "p" => nil })
      master_send(master, { "t" => "wake", "q" => "pgbus_stream_chat", "p" => nil })

      expect(dispatch_queue.pop(timeout: 2)&.queue_name).to eq("pgbus_stream_chat")
      expect(client.dead?).to be(false)
      expect(failures).to be_empty
    end
  end
end
