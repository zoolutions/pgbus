# frozen_string_literal: true

require_relative "../integration_helper"

# End-to-end coverage of the event bus: publish -> PGMQ topic queue ->
# consumer reads a real message -> idempotent handler runs -> a
# pgbus_processed_events row is claimed -> redelivery is skipped. Previously
# only the unit specs under spec/pgbus/event_bus/ exercised these pieces, all
# with mocks and never against a real database.
RSpec.describe "Event bus flow (integration)", :integration do
  # A real handler that records every event it handles, so the spec can assert
  # the handler actually ran (not merely that a row was written).
  let(:handler_class) do
    Class.new(Pgbus::EventBus::Handler) do
      idempotent!

      def self.handled
        @handled ||= []
      end

      def self.name
        "EventBusFlowSpec::RecordingHandler"
      end

      def handle(event)
        self.class.handled << event.event_id
      end
    end
  end

  let(:registry) { Pgbus::EventBus::Registry.instance }
  let(:consumer) { Pgbus::Process::Consumer.new(topics: ["orders.#"], threads: 1) }
  let(:client) { Pgbus.client }
  let(:queue_name) { "event_bus_flow" }

  before do
    stub_const("EventBusFlowSpec", Module.new)
    stub_const("EventBusFlowSpec::RecordingHandler", handler_class)

    registry.clear!
    registry.subscribe("orders.created", handler_class, queue_name: queue_name)
    registry.setup_all! # ensure_queue + bind_topic for the pattern

    # Drop any dedup cache carried over from a prior example so the DB is the
    # sole source of idempotency truth for each run.
    handler_class.dedup_cache.clear!

    consumer.send(:setup_subscriptions)
  end

  after do
    registry.clear!
  end

  def publish_and_read
    Pgbus::EventBus::Publisher.publish("orders.created", { "order_id" => 99 })
    message = client.read_message(queue_name)
    expect(message).not_to be_nil
    message
  end

  describe "happy path" do
    it "runs the handler and records a processed_events row" do
      message = publish_and_read

      consumer.send(:handle_message, message, queue_name)

      expect(handler_class.handled.size).to eq(1)
      expect(Pgbus::ProcessedEvent.count).to eq(1)

      row = Pgbus::ProcessedEvent.first
      expect(row.handler_class).to eq("EventBusFlowSpec::RecordingHandler")
      expect(row.event_id).to eq(handler_class.handled.first)
    end

    it "archives the message off the queue after handling" do
      message = publish_and_read
      consumer.send(:handle_message, message, queue_name)

      expect(client.read_message(queue_name)).to be_nil
    end
  end

  describe "idempotency" do
    it "skips a redelivered event and never writes a second row" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "order_id" => 99 })

      # First delivery: claims idempotency, handler runs, message archived.
      first = client.read_message(queue_name, vt: 0)
      consumer.send(:handle_message, first, queue_name)

      # The in-memory dedup cache would short-circuit the second claim before
      # the DB. Clear it so the redelivery genuinely exercises the
      # INSERT ... ON CONFLICT DO NOTHING path and proves DB-level dedup.
      handler_class.dedup_cache.clear!

      # Re-publish the SAME event_id by handing the handler the same message
      # body again through a fresh handler instance.
      handler = handler_class.new
      result = handler.process(first)

      expect(result).to eq(:skipped)
      expect(handler_class.handled.size).to eq(1)
      expect(Pgbus::ProcessedEvent.count).to eq(1)
    end
  end

  # Issue #470: a row with completed_at NULL describes both "the holder was
  # SIGKILLed mid-handler" and "the holder is still running". Against the real
  # table, the liveness stamp is what separates them.
  describe "pending claim ownership (issue #470)" do
    let(:pending_claim) do
      Pgbus::ProcessedEvent.create!(
        event_id: event_id, handler_class: "EventBusFlowSpec::RecordingHandler",
        processed_at: processed_at, completed_at: nil
      )
    end
    let(:event_id) { SecureRandom.uuid }
    let(:processed_at) { Time.now.utc }
    let(:message) do
      Pgbus::EventBus::Publisher.publish("orders.created", { "order_id" => 99 })
      client.read_message(queue_name, vt: 0)
    end

    context "when the holder is still heartbeating its claim" do
      let(:processed_at) { Time.now.utc }

      it "skips rather than running the handler beside the holder" do
        pending_claim
        raw = JSON.parse(message.message).merge("event_id" => event_id).to_json

        expect(handler_class.new.process(double(message: raw, msg_id: 1, read_ct: 2))).to eq(:skipped)

        expect(handler_class.handled).to be_empty
        # The holder's claim is untouched: it still owns the completion stamp.
        expect(pending_claim.reload.completed_at).to be_nil
      end
    end

    context "when the holder has gone quiet past the ownership window" do
      let(:processed_at) { Time.now.utc - 3600 }

      it "re-runs the handler so a crash mid-handler is not a silent drop" do
        pending_claim
        raw = JSON.parse(message.message).merge("event_id" => event_id).to_json

        expect(handler_class.new.process(double(message: raw, msg_id: 1, read_ct: 2))).to eq(:handled)

        expect(handler_class.handled).to eq([event_id])
        expect(pending_claim.reload.completed_at).not_to be_nil
      end
    end

    it "moves a pending claim's processed_at forward on a beat" do
      pending_claim
      beat = Pgbus::EventBus::ClaimBeat.new
      beat.register(event_id, "EventBusFlowSpec::RecordingHandler")

      expect { beat.touch! }.to(change { pending_claim.reload.processed_at })
    end

    context "with a completed claim" do
      let(:processed_at) { Time.now.utc - 3600 }

      it "leaves a beat with nothing to refresh" do
        pending_claim.update!(completed_at: Time.now.utc)
        beat = Pgbus::EventBus::ClaimBeat.new
        beat.register(event_id, "EventBusFlowSpec::RecordingHandler")

        expect { beat.touch! }.not_to(change { pending_claim.reload.processed_at })
      end
    end
  end
end
