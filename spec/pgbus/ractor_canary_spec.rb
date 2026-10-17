# frozen_string_literal: true

require "spec_helper"

# A streamable the canary builds inside the Ractor: a constant, so it is
# shareable, and plain data, so it needs no ActiveRecord.
PgbusRactorCanaryRecord = Struct.new(:id) do
  def to_gid_param = "Z2lkOi8vYXBwL0NoYXQv#{id}"
end

# The Ractor canary of the conformance gate (issue #527): pgbus's pure
# operations run inside a non-main Ractor on Ruby 4.0 and return what they
# return on the main Ractor (the kit's "runs inside a non-main Ractor", tagged
# :ractor and skipped below 4.0). Nothing here touches the database. A
# non-main Ractor cannot read Pgbus.configuration, which holds the logger and
# the app's callables; the global settings it reads come from
# Pgbus.configuration_snapshot.
RSpec.describe "pgbus inside a non-main Ractor" do # rubocop:disable RSpec/DescribeClass
  # Below 4.0 a non-main Ractor cannot autoload: load what the blocks touch.
  before do
    [Pgbus::Streams::Stream, Pgbus::Streams::Key, Pgbus::Streams::Cursor, Pgbus::Streams::Envelope,
     Pgbus::Configuration, Pgbus::Shareable, Pgbus::StatBuffer, Pgbus::Serializer, Pgbus::Event,
     Pgbus::Web::Streamer::Listener, Pgbus::Web::Streamer::StreamEventDispatcher,
     Pgbus::Client::ReadAfter, Pgbus::QueueNameValidator, Pgbus::Configuration::Snapshot]
  end

  describe "stream names" do
    let(:subject_call) do
      lambda do |parts|
        streamables = parts.map { |part| part.is_a?(Integer) ? PgbusRactorCanaryRecord.new(part) : part }
        [Pgbus::Streams::Stream.name_from(streamables), Pgbus::Streams::Stream.name_from("chat")]
      end
    end

    it_behaves_like "runs inside a non-main Ractor", inputs: [["chat", 7, :messages]]
  end

  describe "stream key fragments" do
    let(:subject_call) do
      lambda do |id|
        key = Pgbus::Streams::Key
        [key.normalize(:messages), key.normalize(PgbusRactorCanaryRecord.new(id)), key.short_id(PgbusRactorCanaryRecord.new(id))]
      end
    end

    it_behaves_like "runs inside a non-main Ractor", inputs: [7]
  end

  describe "durable and presence pattern resolution on a finalized configuration" do
    let(:subject_call) do
      lambda do |durable, presence|
        config = Pgbus::Configuration.new
        config.streams_durable_patterns = durable
        config.streams_presence_patterns = presence
        config.finalize!
        [config.stream_durable?("chat:1"), config.stream_durable?("orders"), config.stream_durable?("other"),
         config.stream_presence?("room:9"), config.queue_name("default"), Ractor.shareable?(config.streams_durable_patterns)]
      end
    end

    it_behaves_like "runs inside a non-main Ractor", inputs: [["orders", /\Achat:/], [/\Aroom:/]]
  end

  describe "event serialization round trip" do
    let(:subject_call) do
      lambda do |json, payload|
        event = Pgbus::Serializer.deserialize_event(json, configuration: Pgbus::Configuration.new)
        written = JSON.parse(Pgbus::Serializer.serialize_event(payload))
        [event.event_id, event.payload, event.published_at.utc.iso8601(6), event.to_h["payload"], written["payload"]]
      end
    end

    it_behaves_like "runs inside a non-main Ractor",
                    inputs: [%({"event_id":"e-1","payload":{"order_id":42},"published_at":"2026-10-15T10:00:00.000000Z"}),
                             { "order_id" => 42 }]
  end

  describe "the dispatch seam: a NOTIFY wake, a durable read's unwrap, the SSE frame" do
    let(:subject_call) do
      lambda do |queue_name, payload|
        wake = Pgbus::Web::Streamer::Listener::WakeMessage.new(queue_name: queue_name, payload: payload)
        read = Pgbus::Client::ReadAfter::Envelope.new(msg_id: 7, enqueued_at: "2026-10-15 10:00:00+00",
                                                      payload: payload, source: "live")
        frame = Pgbus::Web::Streamer::StreamEventDispatcher.allocate.send(:unwrap_stream_envelope, read)
        [Ractor.shareable?(wake), Ractor.shareable?(frame), frame.payload, frame.exclude, frame.event,
         Pgbus::Streams::Envelope.message(id: frame.msg_id, event: frame.event, data: frame.payload),
         Pgbus::Streams::Cursor.parse(query_since: "42", last_event_id: nil)]
      end
    end

    it_behaves_like "runs inside a non-main Ractor",
                    inputs: ["pgbus_chat", %({"html":"<turbo-stream>hi</turbo-stream>","exclude":"conn-1","event":"chat"})]

    # The shared example compares the main thread's answer with the Ractor's,
    # and shareability is the same on both, so it cannot catch a seam value
    # that stops being shareable. Pinned here, from unfrozen Strings as a
    # NOTIFY hands them over.
    it "hands shareable values across the seam" do
      payload = +%({"html":"<turbo-stream>hi</turbo-stream>","exclude":"conn-1","event":"chat"})

      expect(subject_call.call(+"pgbus_chat", payload).first(2)).to eq([true, true])
    end
  end

  # Streams::Key checks the queue-name budget against the queue_prefix. A
  # non-main Ractor cannot read Pgbus.configuration (the logger, the app's
  # callables), so there it reads Pgbus.configuration_snapshot: the plain-data
  # settings the last Pgbus.finalize! or Pgbus.configure published.
  describe "full stream keys and the queue-name budget" do
    # As after boot: the snapshot is the one the live configuration published.
    before { Pgbus.finalize! }

    let(:subject_call) do
      lambda do |parts|
        [Pgbus::Streams::Key.stream_key(*parts), Pgbus.stream_key(*parts), Pgbus.stream_key!("chat:messages"),
         Pgbus.stream_name_budget, Ractor.shareable?(Pgbus.configuration_snapshot)]
      end
    end

    it_behaves_like "runs inside a non-main Ractor", inputs: [["chat", :messages]]

    it "builds a full stream key with Streams::Key.stream_key", :ractor do
      skip "needs Ruby 4.0+ (Ractor#value)" unless Ractor.method_defined?(:value)

      expect(Ractor.new { Pgbus::Streams::Key.stream_key("chat", :messages) }.value).to eq("chat:messages")
    end

    context "when Pgbus.configure changes the queue_prefix" do
      around do |example|
        original = Pgbus.configuration.queue_prefix
        Pgbus.configure { |c| c.queue_prefix = "ractor_canary" }
        example.run
      ensure
        Pgbus.configure { |c| c.queue_prefix = original }
      end

      it "checks the budget against the queue_prefix it published", :ractor do
        skip "needs Ruby 4.0+ (Ractor#value)" unless Ractor.method_defined?(:value)

        budget, error = Ractor.new do
          message = begin
            Pgbus.stream_key("x" * 60)
          rescue ArgumentError => e
            e.message
          end
          [Pgbus.stream_name_budget, message]
        end.value

        expect(budget).to eq(Pgbus::QueueNameValidator::MAX_QUEUE_NAME_LENGTH - "ractor_canary".length - 1)
        expect(error).to include('queue_prefix="ractor_canary"')
      end
    end
  end
end
