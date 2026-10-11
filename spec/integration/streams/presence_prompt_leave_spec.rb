# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/falcon_test_harness"
require_relative "../../support/sse_test_client"

# A closed tab leaves its presence seat at once (issue #519), not on the
# next heartbeat. The heartbeat here is 30 s, so a leave within a couple of
# seconds can only come from noticing the client's close on the socket.
RSpec.describe "Streams: a closed connection leaves its presence seat promptly", :integration do
  before(:all) do
    @saved = %i[listen_notify streams_presence_patterns streams_heartbeat_interval streams_signed_name_secret
                streams_listen_health_check_ms streams_write_deadline_ms streams_presence_ttl
                streams_peer_close_interval].to_h do |key|
      [key, Pgbus.configuration.public_send(key)]
    end
    Pgbus.configuration.listen_notify = true
    Pgbus.configuration.streams_signed_name_secret = "a" * 64
    Pgbus.configuration.streams_listen_health_check_ms = 100
    Pgbus.configuration.streams_heartbeat_interval = 30
    Pgbus.configuration.streams_peer_close_interval = 1.0
    Pgbus.configuration.streams_write_deadline_ms = 5_000
    Pgbus.configuration.streams_presence_patterns = [/./]
    Pgbus.configuration.streams_presence_ttl = 120
    Pgbus.configuration.streams_peer_close_interval = 1.0
    Pgbus.reset_client!
  end

  after(:all) do
    @saved.each { |key, value| Pgbus.configuration.public_send(:"#{key}=", value) }
    Pgbus.reset_client!
  end

  let(:stream_name) { "leave_#{SecureRandom.hex(4)}" }
  let(:changes) { Queue.new }

  let(:streamer) do
    Pgbus::Web::Streamer::Instance.new(
      client: Pgbus.client,
      config: Pgbus.configuration,
      pg_connection: build_pg_listen_connection,
      logger: Logger.new(IO::NULL)
    )
  end

  let(:stream_app) do
    Pgbus::Web::StreamApp.new(
      streamer: streamer,
      config: Pgbus.configuration,
      logger: Logger.new(IO::NULL),
      authorize: ->(_env, _stream) { { member_id: "user-7", metadata: {} } }
    )
  end

  before do
    Pgbus::Streams::Presence.reset_schema_cache!
    Pgbus.configuration.streams_presence_on_change = ->(stream, change) { changes << [stream, change] }
    Pgbus.client.ensure_stream_queue(stream_name)
    streamer.start
  end

  after do
    Pgbus.configuration.streams_presence_on_change = nil
    streamer.shutdown!
    @harness&.shutdown
  end

  def connect(seat)
    SseTestSupport::SseTestClient.connect(
      url: "#{@harness.url("/#{Pgbus::Streams::SignedName.sign(stream_name)}")}?since=0&seat=#{seat}",
      timeout: 5
    )
  end

  def wait_until(timeout: 5)
    deadline = Time.now + timeout
    sleep 0.02 until yield || Time.now > deadline
    yield
  end

  def seats
    Pgbus.stream(stream_name).presence.members.map { |member| member["seat"] }
  end

  shared_examples "a prompt leave" do
    it "removes the closed tab's seat at once and reports one leave" do
      tab1 = connect("tab-1")
      tab2 = connect("tab-2")
      expect(wait_until { seats.sort == %w[tab-1 tab-2] }).to be(true)
      # Past one watch interval (1 s), so both sockets are in the watched set.
      sleep 1.2
      changes.clear

      closed_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      tab1.close
      expect(wait_until(timeout: 3) { seats == ["tab-2"] }).to be(true)
      latency = Process.clock_gettime(Process::CLOCK_MONOTONIC) - closed_at

      expect(latency).to be < 0.5
      sleep 0.3
      reported = []
      reported << changes.pop until changes.empty?
      expect(reported.map { |(_, change)| [change[:type], change[:seat]] }).to eq([[:leave, "tab-1"]])

      tab2.close
    end

    it "keeps an open tab's seat" do
      tab = connect("tab-1")
      expect(wait_until { seats == ["tab-1"] }).to be(true)

      sleep 2.5

      expect(seats).to eq(["tab-1"])
      tab.close
    end
  end

  context "with Puma" do
    before { @harness = SseTestSupport::PumaTestHarness.boot(rack_app: stream_app) }

    it_behaves_like "a prompt leave"
  end

  context "with Falcon" do
    before { @harness = SseTestSupport::FalconTestHarness.boot(rack_app: stream_app) }

    it_behaves_like "a prompt leave"
  end
end
