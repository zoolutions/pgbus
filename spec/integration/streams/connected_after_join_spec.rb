# frozen_string_literal: true

require_relative "../../integration_helper"
require_relative "../../support/puma_test_harness"
require_relative "../../support/falcon_test_harness"
require_relative "../../support/sse_test_client"

# Issue #521: `pgbus:connected` must reach the client only once the
# connection is registered and its presence seat has joined. Before the fix
# StreamApp wrote the frame at hijack time, while the dispatcher had not yet
# handled the ConnectMessage, so a write keyed by the connection id right
# after the event (Presence#set_state) found no row.
#
# The member extractor sleeps to make the window deterministic: the join
# runs on the dispatcher thread, so a frame written before the join is
# visible to the client long before the row exists.
RSpec.describe "Streams: pgbus:connected after the presence join", :integration do
  before(:all) do
    config = Pgbus.configuration
    @saved = {
      listen_notify: config.listen_notify,
      streams_signed_name_secret: config.streams_signed_name_secret,
      streams_listen_health_check_ms: config.streams_listen_health_check_ms,
      streams_heartbeat_interval: config.streams_heartbeat_interval,
      streams_write_deadline_ms: config.streams_write_deadline_ms,
      streams_presence_patterns: config.streams_presence_patterns,
      streams_presence_member: config.streams_presence_member
    }
    config.listen_notify = true
    config.streams_signed_name_secret = "a" * 64
    config.streams_listen_health_check_ms = 100
    config.streams_heartbeat_interval = 30
    config.streams_write_deadline_ms = 5_000
    config.streams_presence_patterns = [/\Ajoined_/]
    config.streams_presence_member = lambda { |context|
      sleep 0.4
      { id: context[:member_id], metadata: {} }
    }
    Pgbus.reset_client!
  end

  after(:all) do
    @saved.each { |key, value| Pgbus.configuration.public_send(:"#{key}=", value) }
    Pgbus.reset_client!
  end

  before { Pgbus::Streams::Presence.reset_schema_cache! }

  let(:stream_name) { "joined_#{SecureRandom.hex(4)}" }

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
      authorize: ->(_env, _stream) { { member_id: "user-42" } }
    )
  end

  shared_examples "a connection whose presence row exists at pgbus:connected" do
    before do
      Pgbus.client.ensure_stream_queue(stream_name)
      streamer.start
    end

    after do
      streamer.shutdown!
      @booted_harness&.shutdown
    end

    it "accepts a set_state keyed by the connection id on the first try" do
      @booted_harness = harness
      url = "#{@booted_harness.url("/#{Pgbus::Streams::SignedName.sign(stream_name)}")}?since=0&seat=tab-1"
      client = SseTestSupport::SseTestClient.connect(url: url, timeout: 5)

      connection_id = client.wait_for_connection_id(timeout: 5)
      expect(connection_id).not_to be_nil

      record = Pgbus.stream(stream_name).presence.set_state(connection_id: connection_id, state: { selected: 7 })

      expect(record).not_to be_nil
      expect(record["state"]).to eq("selected" => 7)
      client.close
    end
  end

  context "with Puma (rack.hijack)" do
    let(:harness) { SseTestSupport::PumaTestHarness.boot(rack_app: stream_app) }

    it_behaves_like "a connection whose presence row exists at pgbus:connected"
  end

  context "with Falcon (rack.hijack)" do
    let(:harness) { SseTestSupport::FalconTestHarness.boot(rack_app: stream_app) }

    it_behaves_like "a connection whose presence row exists at pgbus:connected"
  end

  context "with Falcon (streams_falcon_streaming_body)" do
    let(:harness) { SseTestSupport::FalconTestHarness.boot(rack_app: stream_app) }

    around do |example|
      Pgbus.configuration.streams_falcon_streaming_body = true
      example.run
    ensure
      Pgbus.configuration.streams_falcon_streaming_body = false
    end

    it_behaves_like "a connection whose presence row exists at pgbus:connected"
  end
end
