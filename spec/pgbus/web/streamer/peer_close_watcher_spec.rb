# frozen_string_literal: true

require "spec_helper"
require "socket"
require "protocol/http/body/writable"
require "openssl"

RSpec.describe Pgbus::Web::Streamer::PeerCloseWatcher do
  subject(:watcher) do
    described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 0.05, logger: logger)
  end

  let(:registry)       { Pgbus::Web::Streamer::Registry.new }
  let(:dispatch_queue) { Queue.new }
  let(:logger)         { Logger.new(IO::NULL) }
  let(:pairs)          { [] }

  after { pairs.flatten.each { |io| io.close unless io.closed? } }

  def build_connection(id)
    server, client = UNIXSocket.pair
    pairs << [server, client]
    connection = Pgbus::Web::Streamer::Connection.new(
      id: id, stream_name: "chat", io: server, since_id: 0,
      writer: Pgbus::Web::Streamer::IoWriter, write_deadline_ms: 100
    )
    registry.register(connection)
    [connection, client]
  end

  def disconnects
    messages = []
    messages << dispatch_queue.pop until dispatch_queue.empty?
    messages.map { |message| message.connection.id }
  end

  describe "#tick" do
    it "marks a connection whose client closed dead, closes it and posts one disconnect" do
      connection, client = build_connection("a")
      client.close

      watcher.tick

      expect(connection).to be_dead
      expect(connection.io).to be_closed
      expect(disconnects).to eq(["a"])
    end

    it "leaves an open connection alone" do
      connection, = build_connection("a")

      watcher.tick

      expect(connection).not_to be_dead
      expect(disconnects).to be_empty
    end

    it "reaps only the closed one of several connections" do
      build_connection("live")
      _, client = build_connection("gone")
      client.close

      watcher.tick

      expect(disconnects).to eq(["gone"])
    end

    it "skips a connection that is already dead" do
      connection, client = build_connection("a")
      connection.mark_dead!
      client.close

      watcher.tick

      expect(disconnects).to be_empty
    end

    it "discards stray bytes from a client and keeps the connection" do
      connection, client = build_connection("a")
      client.write("x")

      watcher.tick

      expect(connection).not_to be_dead
      expect(disconnects).to be_empty
    end

    it "treats a reset socket as closed" do
      connection, = build_connection("a")
      allow(connection.io).to receive(:read_nonblock).and_raise(Errno::ECONNRESET)
      pairs.last.last.write("x") # make the socket readable so the probe runs

      watcher.tick

      expect(disconnects).to eq(["a"])
    end

    it "keeps a connection whose socket wrapper raises would-block instead of returning it" do
      connection, = build_connection("a")
      allow(connection.io).to receive(:read_nonblock).and_raise(IO::EAGAINWaitReadable)
      pairs.last.last.write("x")

      watcher.tick

      expect(connection).not_to be_dead
      expect(disconnects).to be_empty
    end

    it "ignores a connection without a socket (Falcon's streaming body)" do
      falcon = Pgbus::Web::Streamer::FalconConnection.new(
        id: "f", stream_name: "chat", body: Protocol::HTTP::Body::Writable.new, since_id: 0, write_deadline_ms: 100
      )
      registry.register(falcon)

      expect { watcher.tick }.not_to raise_error
      expect(disconnects).to be_empty
    end

    # macOS interrupts the select with an IOError; Linux keeps waiting out
    # the interval. Either way the pass ends quietly and the next one
    # rebuilds the set without the closed socket.
    it "survives a socket closed by another thread while it waits in select" do
      connection, = build_connection("a")
      closer = Thread.new do
        sleep 0.02
        connection.io.close
      end

      expect { watcher.tick }.not_to raise_error
      closer.join

      _, client = build_connection("b")
      client.close
      watcher.tick
      expect(disconnects).to eq(["b"])
    ensure
      closer&.join
    end

    it "treats a TLS socket's abrupt-EOF error as a close (OpenSSL)" do
      connection, = build_connection("a")
      allow(connection.io).to receive(:read_nonblock).and_raise(OpenSSL::SSL::SSLError, "unexpected eof while reading")
      pairs.last.last.write("x")

      watcher.tick

      expect(disconnects).to eq(["a"])
    end

    it "treats Puma's MiniSSL error as a close" do
      stub_const("Puma::MiniSSL::SSLError", Class.new(StandardError))
      connection, = build_connection("a")
      allow(connection.io).to receive(:read_nonblock).and_raise(Puma::MiniSSL::SSLError, "eof")
      pairs.last.last.write("x")

      watcher.tick

      expect(disconnects).to eq(["a"])
    end

    it "keeps probing the other ready sockets when one probe raises" do
      broken, = build_connection("broken")
      allow(broken.io).to receive(:read_nonblock).and_raise(ArgumentError, "boom")
      pairs.last.last.write("x")
      _, client = build_connection("gone")
      client.close

      expect { watcher.tick }.not_to raise_error

      expect(disconnects).to eq(["gone"])
      expect(broken).not_to be_dead
    end

    it "skips a socket whose connection is mid-write and probes it on a later pass" do
      connection, client = build_connection("a")
      client.close
      connection.mutex.lock

      watcher.tick
      expect(disconnects).to be_empty

      connection.mutex.unlock
      watcher.tick
      expect(disconnects).to eq(["a"])
    end
  end

  describe "#start / #stop" do
    it "notices a close on its own thread" do
      connection, client = build_connection("a")
      watcher.start

      client.close
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
      sleep 0.01 until connection.dead? || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      expect(connection).to be_dead
    ensure
      watcher.stop
    end

    it "stops promptly even with a long interval" do
      slow = described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 30, logger: logger)
      build_connection("a")
      slow.start
      sleep 0.1

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      slow.stop

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
      expect(slow.threads).to be_empty
    end

    it "stops promptly when nothing is watched" do
      slow = described_class.new(registry: registry, dispatch_queue: dispatch_queue, interval: 30, logger: logger)
      slow.start
      sleep 0.1

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      slow.stop

      expect(Process.clock_gettime(Process::CLOCK_MONOTONIC) - started).to be < 1
    end

    it "reports its thread while running and none after stop" do
      watcher.start
      expect(watcher.threads.size).to eq(1)

      watcher.stop

      expect(watcher.threads).to be_empty
    end
  end
end
