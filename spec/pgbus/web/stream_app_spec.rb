# frozen_string_literal: true

require "spec_helper"
require "rack/request"
require "socket"

RSpec.describe Pgbus::Web::StreamApp do
  subject(:app) do
    described_class.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL)
    )
  end

  before do
    stub_const("PG", Module.new) unless defined?(PG)
    stub_const("PG::Error", Class.new(StandardError)) unless defined?(PG::Error)
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
  end

  after do
    Pgbus.configuration.streams_signed_name_secret = nil
    Pgbus::Web::Streamer.reset!
  end

  let(:registry) { Pgbus::Web::Streamer::Registry.new }

  # Lightweight Streamer stand-in that exposes the same interface as
  # Pgbus::Web::Streamer::Instance but doesn't spawn any threads. The
  # StreamApp only needs #registry and #register — keep the test
  # hermetic and avoid booting listener/dispatcher/heartbeat.
  let(:streamer) do
    Class.new do
      attr_reader :registry, :registered

      def initialize(registry)
        @registry = registry
        @registered = []
      end

      def register(connection)
        @registered << connection
      end
    end.new(registry)
  end

  def signed(name)
    Pgbus::Streams::SignedName.sign(name)
  end

  def get_env(path_info, extra = {})
    {
      "REQUEST_METHOD" => "GET",
      "PATH_INFO" => path_info,
      "QUERY_STRING" => "",
      "rack.input" => StringIO.new,
      "rack.errors" => StringIO.new,
      "rack.url_scheme" => "http",
      "SERVER_NAME" => "example.com",
      "SERVER_PORT" => "80"
    }.merge(extra)
  end

  def drain_until(reader_io, needle, timeout: 1)
    buffer = +""
    deadline = Time.now + timeout
    until buffer.include?(needle) || Time.now > deadline
      begin
        buffer << reader_io.read_nonblock(4096)
      rescue IO::WaitReadable
        reader_io.wait_readable(0.1)
      end
    end
    buffer
  end

  describe "error paths (no hijack required)" do
    it "returns 404 when PATH_INFO has no signed name" do
      env = get_env("/pgbus/streams/")
      status, _, body = app.call(env)
      expect(status).to eq(404)
      expect(body.first).to include("missing signed stream name")
    end

    it "returns 404 when the signed name is tampered" do
      env = get_env("/pgbus/streams/not-a-valid-token")
      status, _, body = app.call(env)
      expect(status).to eq(404)
      expect(body.first).to include("invalid signed stream name")
    end

    it "returns 405-as-404 for non-GET requests" do
      env = get_env("/pgbus/streams/#{signed("chat")}", "REQUEST_METHOD" => "POST")
      status, = app.call(env)
      expect(status).to eq(404)
    end

    it "returns 403 when the authorize hook returns false" do
      restricted = described_class.new(
        streamer: streamer,
        config: Pgbus.configuration,
        logger: Logger.new(IO::NULL),
        authorize: ->(_env, _name) { false }
      )
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, _, body = restricted.call(env)
      expect(status).to eq(403)
      expect(body.first).to include("forbidden")
    end

    it "returns 400 for an invalid ?since= cursor" do
      env = get_env("/pgbus/streams/#{signed("chat")}", "QUERY_STRING" => "since=abc")
      status, _, body = app.call(env)
      expect(status).to eq(400)
      expect(body.first).to include("cursor")
    end

    it "returns 501 when rack.hijack? is missing (not on Puma 6.1+/Falcon)" do
      env = get_env("/pgbus/streams/#{signed("chat")}")
      # Intentionally do not set env["rack.hijack?"]
      status, _, body = app.call(env)
      expect(status).to eq(501)
      expect(body.first).to include("does not provide rack.hijack")
    end

    it "returns 503 when the streamer is at capacity" do
      Pgbus.configuration.streams_max_connections = 0 # every request is over capacity
      env = get_env("/pgbus/streams/#{signed("chat")}", "rack.hijack?" => true)

      status, headers, body = app.call(env)

      expect(status).to eq(503)
      expect(headers["retry-after"]).to eq("2")
      expect(body.first).to include("over capacity")
    ensure
      Pgbus.configuration.streams_max_connections = 2_000
    end

    it "returns 500 on an unexpected internal error" do
      allow(Pgbus::Streams::SignedName).to receive(:verify!).and_raise("boom")
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, = app.call(env)
      expect(status).to eq(500)
    end

    it "reports the internal error through ErrorReporter so APM handlers see it (issue #352)" do
      # A 500 that is only logger.error'd is invisible to error trackers —
      # a broken streamer config 500s every SSE request without a single
      # APM event.
      reported = []
      Pgbus.configuration.error_reporters << ->(ex, ctx) { reported << [ex, ctx] }
      allow(Pgbus::Streams::SignedName).to receive(:verify!).and_raise("boom")
      env = get_env("/pgbus/streams/#{signed("chat")}")

      app.call(env)

      expect(reported.size).to eq(1)
      expect(reported.first[0].message).to eq("boom")
      expect(reported.first[1]).to include(action: "stream_connect")
    ensure
      Pgbus.configuration.error_reporters = []
    end
  end

  describe "happy path with a real hijack" do
    let(:sockets)   { UNIXSocket.pair }
    let(:writer_io) { sockets[0] }
    let(:reader_io) { sockets[1] }

    def hijacked_env(query: "", headers: {})
      hijack_called = [false]
      env = get_env(
        "/pgbus/streams/#{signed("chat")}",
        {
          "QUERY_STRING" => query,
          "rack.hijack?" => true,
          "rack.hijack" => lambda {
            hijack_called[0] = true
            writer_io
          },
          "rack.hijack_io" => writer_io
        }.merge(headers)
      )
      [env, hijack_called]
    end

    after do
      writer_io.close unless writer_io.closed?
      reader_io.close unless reader_io.closed?
    end

    it "returns Puma's async sentinel and invokes rack.hijack" do
      env, hijack_called = hijacked_env
      status, _, body = app.call(env)
      expect(status).to eq(-1)
      expect(body).to eq([])
      expect(hijack_called[0]).to be true
    end

    it "writes the SSE response headers and the opening comment" do
      env, = hijacked_env
      app.call(env)
      drained = drain_until(reader_io, "pgbus stream open")
      expect(drained).to include("HTTP/1.1 200 OK")
      expect(drained).to include("content-type: text/event-stream")
      expect(drained).to include("retry: 2000")
      expect(drained).to include("stream=chat")
    end

    it "writes a pgbus:connected frame carrying the registered connection's id (issue #165)" do
      env, = hijacked_env
      app.call(env)
      drained = drain_until(reader_io, "pgbus:connected")
      connection_id = streamer.registered.first.id
      expect(drained).to include("event: pgbus:connected")
      expect(drained).to include(%(data: {"connectionId":"#{connection_id}"}))
    end

    it "registers a Connection with the streamer" do
      env, = hijacked_env
      app.call(env)
      expect(streamer.registered.length).to eq(1)
      expect(streamer.registered.first.stream_name).to eq("chat")
    end

    it "carries the since_id through to the registered connection" do
      env, = hijacked_env(query: "since=1247")
      app.call(env)
      expect(streamer.registered.first.last_msg_id_sent).to eq(1247)
    end

    it "prefers Last-Event-ID over ?since= on reconnect" do
      env, = hijacked_env(query: "since=100", headers: { "HTTP_LAST_EVENT_ID" => "200" })
      app.call(env)
      expect(streamer.registered.first.last_msg_id_sent).to eq(200)
    end

    it "stores the client's ?seat= on the connection (issue #515)" do
      env, = hijacked_env(query: "since=1&seat=9b2f-tab_1")
      app.call(env)
      expect(streamer.registered.first.seat).to eq("9b2f-tab_1")
    end

    it "leaves the seat nil when none is sent" do
      env, = hijacked_env
      app.call(env)
      expect(streamer.registered.first.seat).to be_nil
    end

    it "drops a seat that is too long or carries unexpected characters" do
      env, = hijacked_env(query: "seat=#{"a" * 65}")
      app.call(env)
      env2, = hijacked_env(query: "seat=%3Cscript%3E")
      app.call(env2)
      expect(streamer.registered.map(&:seat)).to eq([nil, nil])
    end

    it "drops a seat that decodes to invalid UTF-8 instead of raising" do
      env, = hijacked_env(query: "seat=%FF%FE")
      app.call(env)
      expect(streamer.registered.map(&:seat)).to eq([nil])
    end
  end

  describe "test mode (streams_test_mode)" do
    before { Pgbus.configuration.streams_test_mode = true }
    after { Pgbus.configuration.streams_test_mode = false }

    it "returns 200 with SSE headers" do
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, headers, = app.call(env)
      expect(status).to eq(200)
      expect(headers["content-type"]).to eq("text/event-stream")
      expect(headers["cache-control"]).to eq("no-cache, no-transform")
      expect(headers["x-accel-buffering"]).to eq("no")
    end

    it "does not invoke rack.hijack" do
      hijack_called = false
      env = get_env(
        "/pgbus/streams/#{signed("chat")}",
        "rack.hijack?" => true,
        "rack.hijack" => lambda {
          hijack_called = true
          StringIO.new
        }
      )
      app.call(env)
      expect(hijack_called).to be false
    end

    it "does not register any connection with the streamer" do
      env = get_env("/pgbus/streams/#{signed("chat")}", "rack.hijack?" => true)
      app.call(env)
      expect(streamer.registered).to be_empty
    end

    it "returns a body containing a stub SSE comment" do
      env = get_env("/pgbus/streams/#{signed("chat")}")
      _, _, body = app.call(env)
      joined = body.join
      expect(joined).to include(": pgbus test mode")
    end

    it "tells EventSource not to reconnect so a teardown race cannot start a live streamer (issue #443)" do
      env = get_env("/pgbus/streams/#{signed("chat")}")
      _, _, body = app.call(env)
      expect(body.join).to start_with("retry: #{described_class::TEST_MODE_RETRY_MS}\n\n")
      expect(described_class::TEST_MODE_RETRY_MS).to be >= 3_600_000
    end

    it "still validates the signed stream name" do
      env = get_env("/pgbus/streams/not-a-valid-token")
      status, = app.call(env)
      expect(status).to eq(404)
    end

    it "still runs the authorize hook" do
      restricted = described_class.new(
        streamer: streamer,
        config: Pgbus.configuration,
        logger: Logger.new(IO::NULL),
        authorize: ->(_env, _name) { false }
      )
      env = get_env("/pgbus/streams/#{signed("chat")}")
      status, = restricted.call(env)
      expect(status).to eq(403)
    end

    it "takes priority over both hijack and Falcon streaming body" do
      Pgbus.configuration.streams_falcon_streaming_body = true
      env = get_env(
        "/pgbus/streams/#{signed("chat")}",
        "rack.hijack?" => true
      )
      status, _, body = app.call(env)
      expect(status).to eq(200)
      # Body should be a plain Array, not a Writable
      expect(body).to be_a(Array)
      expect(body.first).to include(": pgbus test mode")
    ensure
      Pgbus.configuration.streams_falcon_streaming_body = false
    end
  end

  describe "Falcon streaming body path" do
    before { Pgbus.configuration.streams_falcon_streaming_body = true }
    after { Pgbus.configuration.streams_falcon_streaming_body = false }

    it "returns [200, sse_headers, Writable] when flag is enabled" do
      env = get_env("/#{signed("chat")}")
      status, headers, body = app.call(env)

      expect(status).to eq(200)
      expect(headers["content-type"]).to eq("text/event-stream")
      expect(headers["cache-control"]).to eq("no-cache, no-transform")
      expect(headers["x-accel-buffering"]).to eq("no")
      expect(body).to be_a(Protocol::HTTP::Body::Writable)
    ensure
      body&.close
    end

    it "registers a FalconConnection with the streamer" do
      env = get_env("/#{signed("chat")}")
      _, _, body = app.call(env)

      expect(streamer.registered.length).to eq(1)
      expect(streamer.registered.first).to be_a(Pgbus::Web::Streamer::FalconConnection)
      expect(streamer.registered.first.stream_name).to eq("chat")
    ensure
      body&.close
    end

    it "body contains opening SSE frames" do
      env = get_env("/#{signed("chat")}", "QUERY_STRING" => "since=42")
      _, _, body = app.call(env)

      # Read the first two chunks (retry directive + opening comment)
      chunk1 = body.read
      chunk2 = body.read

      expect(chunk1).to include("retry: 2000")
      expect(chunk2).to include("since_id=42")
      expect(chunk2).to include("stream=chat")
    ensure
      body&.close
    end

    it "carries the since_id through to the FalconConnection" do
      env = get_env("/#{signed("chat")}", "QUERY_STRING" => "since=999")
      _, _, body = app.call(env)

      expect(streamer.registered.first.last_msg_id_sent).to eq(999)
    ensure
      body&.close
    end

    it "stores the client's ?seat= on the FalconConnection (issue #515)" do
      env = get_env("/#{signed("chat")}", "QUERY_STRING" => "seat=tab-1")
      _, _, body = app.call(env)

      expect(streamer.registered.first.seat).to eq("tab-1")
    ensure
      body&.close
    end

    it "writes a pgbus:connected frame carrying the FalconConnection's id (issue #165)" do
      env = get_env("/#{signed("chat")}")
      _, _, body = app.call(env)

      # retry directive, opening comment, then the connected frame
      frames = +""
      3.times { frames << body.read.to_s }
      connection_id = streamer.registered.first.id
      expect(frames).to include("event: pgbus:connected")
      expect(frames).to include(%(data: {"connectionId":"#{connection_id}"}))
    ensure
      body&.close
    end

    it "takes priority over rack.hijack? when flag is set" do
      env = get_env("/#{signed("chat")}", "rack.hijack?" => true)
      status, _, body = app.call(env)

      # Should return 200 (streaming body), not -1 (hijack sentinel)
      expect(status).to eq(200)
      expect(body).to be_a(Protocol::HTTP::Body::Writable)
    ensure
      body&.close
    end

    it "returns 503 when at capacity" do
      Pgbus.configuration.streams_max_connections = 0
      env = get_env("/#{signed("chat")}")

      status, headers, _body = app.call(env)

      expect(status).to eq(503)
      expect(headers["retry-after"]).to eq("2")
    ensure
      Pgbus.configuration.streams_max_connections = 2_000
    end
  end
end
