# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing"

RSpec.describe Pgbus::Testing do
  after do
    Pgbus::Testing.disabled! # rubocop:disable RSpec/DescribedClass
    Pgbus::Testing.store.clear! # rubocop:disable RSpec/DescribedClass
  end

  describe ".mode!" do
    it "sets global mode" do
      described_class.mode!(:fake)
      expect(described_class.mode).to eq(:fake)
    end

    it "rejects unknown modes" do
      expect { described_class.mode!(:bogus) }.to raise_error(ArgumentError, /Unknown mode/)
    end

    it "scopes mode to block and restores afterward" do
      described_class.mode!(:disabled)

      described_class.mode!(:fake) do
        expect(described_class.mode).to eq(:fake)
      end

      expect(described_class.mode).to eq(:disabled)
    end

    it "restores mode even when block raises" do
      described_class.mode!(:disabled)

      begin
        described_class.mode!(:inline) { raise "boom" }
      rescue RuntimeError
        nil
      end

      expect(described_class.mode).to eq(:disabled)
    end
  end

  describe ".fake!" do
    it "sets mode to fake" do
      described_class.fake!
      expect(described_class).to be_fake
    end

    it "accepts a block" do
      described_class.fake! do
        expect(described_class).to be_fake
      end
      expect(described_class).not_to be_fake
    end
  end

  describe ".inline!" do
    it "sets mode to inline" do
      described_class.inline!
      expect(described_class).to be_inline
    end

    it "accepts a block" do
      described_class.inline! do
        expect(described_class).to be_inline
      end
      expect(described_class).not_to be_inline
    end
  end

  describe ".disabled!" do
    it "sets mode to disabled" do
      described_class.fake!
      described_class.disabled!
      expect(described_class).to be_disabled
    end
  end

  describe "streams_test_mode sync" do
    after { Pgbus.configuration.streams_test_mode = false }

    it "enables streams_test_mode when entering fake mode" do
      described_class.fake!
      expect(Pgbus.configuration.streams_test_mode).to be true
    end

    it "enables streams_test_mode when entering inline mode" do
      described_class.inline!
      expect(Pgbus.configuration.streams_test_mode).to be true
    end

    it "disables streams_test_mode when entering disabled mode" do
      Pgbus.configuration.streams_test_mode = true
      described_class.disabled!
      expect(Pgbus.configuration.streams_test_mode).to be false
    end

    it "enables streams_test_mode inside a scoped block and restores afterward" do
      described_class.fake! do
        expect(Pgbus.configuration.streams_test_mode).to be true
      end
      expect(Pgbus.configuration.streams_test_mode).to be false
    end

    # After the block the mode falls back to the process-wide one, not to
    # :disabled: a fake-mode test that runs a `disabled! do ... end` block
    # must keep the streamer out of its SSE requests afterwards.
    it "restores the stream test mode of the process-wide mode after a scoped block" do
      described_class.fake!
      described_class.disabled! { nil }

      expect(described_class.mode).to eq(:fake)
      expect(Pgbus.configuration.streams_test_mode).to be true
    ensure
      described_class.disabled!
    end
  end

  describe ".store" do
    it "returns a EventStore instance" do
      expect(described_class.store).to be_a(Pgbus::Testing::EventStore)
    end

    it "returns the same store across calls" do
      expect(described_class.store).to equal(described_class.store)
    end
  end

  describe Pgbus::Testing::EventStore do
    subject(:store) { described_class.new }

    describe "#push_event" do
      it "stores an event" do
        event = Pgbus::Event.new(event_id: "e1", payload: { "a" => 1 }, routing_key: "orders.created")
        store.push_event(event)

        expect(store.events).to contain_exactly(event)
      end
    end

    describe "#events" do
      it "returns events filtered by routing_key" do
        e1 = Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "orders.created")
        e2 = Pgbus::Event.new(event_id: "e2", payload: {}, routing_key: "orders.shipped")
        store.push_event(e1)
        store.push_event(e2)

        expect(store.events(routing_key: "orders.created")).to contain_exactly(e1)
      end

      it "returns all events when no filter" do
        e1 = Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "orders.created")
        e2 = Pgbus::Event.new(event_id: "e2", payload: {}, routing_key: "orders.shipped")
        store.push_event(e1)
        store.push_event(e2)

        expect(store.events).to contain_exactly(e1, e2)
      end

      it "returns a copy so mutations don't affect the store" do
        e1 = Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "x")
        store.push_event(e1)

        store.events.clear
        expect(store.events).to contain_exactly(e1)
      end
    end

    describe "#clear!" do
      it "empties all events" do
        store.push_event(Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "x"))
        store.clear!
        expect(store.events).to be_empty
      end
    end

    describe "#drain!" do
      let(:handler_class) do
        # A spec double: built per example and driven from the example's thread.
        # rubocop:disable-next ThreadSafety/ClassAndModuleAttributes
        klass = Class.new(Pgbus::EventBus::Handler) do
          class << self
            attr_accessor :handled_events
          end
          self.handled_events = []

          def handle(event)
            self.class.handled_events << event
          end
        end
        stub_const("TestDrainHandler", klass)
        klass
      end

      before do
        Pgbus::EventBus::Registry.instance.clear!
      end

      after do
        Pgbus::EventBus::Registry.instance.clear!
      end

      it "dispatches stored events to matching handlers and clears the store" do
        Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)

        event = Pgbus::Event.new(event_id: "e1", payload: { "id" => 1 }, routing_key: "orders.created")
        store.push_event(event)
        store.drain!

        expect(handler_class.handled_events.size).to eq(1)
        expect(handler_class.handled_events.first.event_id).to eq("e1")
        expect(store.events).to be_empty
      end

      it "skips events with no matching handlers" do
        event = Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "no.match")
        store.push_event(event)

        expect { store.drain! }.not_to raise_error
        expect(store.events).to be_empty
      end

      it "preserves unprocessed events when a handler raises" do
        failing_handler = Class.new(Pgbus::EventBus::Handler) do
          def handle(_event)
            raise "boom"
          end
        end
        stub_const("TestFailingHandler", failing_handler)

        Pgbus::EventBus::Registry.instance.subscribe("fail.me", failing_handler)
        Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)

        store.push_event(Pgbus::Event.new(event_id: "e1", payload: {}, routing_key: "fail.me"))
        store.push_event(Pgbus::Event.new(event_id: "e2", payload: {}, routing_key: "orders.created"))

        expect { store.drain! }.to raise_error(RuntimeError, "boom")
        expect(store.events.map(&:event_id)).to eq(%w[e1 e2])
      end
    end
  end

  describe "streamer teardown in .disabled! (issue #443)" do
    after { Pgbus.configuration.streams_test_mode = false }

    it "raises StreamerLeakError naming the stuck threads and the append_after fix when shutdown left threads running" do
      # First call (the example's) reports a leak; the top-level after hook's call must be clean.
      allow(Pgbus::Web::Streamer).to receive(:reset!).and_return(["dispatcher"], nil)

      expect { described_class.disabled! }
        .to raise_error(Pgbus::Testing::StreamerLeakError, /dispatcher.*append_after/m)
    end

    it "still records :disabled as the mode when the teardown raises" do
      allow(Pgbus::Web::Streamer).to receive(:reset!).and_return(["dispatcher"], nil)
      described_class.fake!

      expect { described_class.disabled! }.to raise_error(Pgbus::Testing::StreamerLeakError)
      expect(described_class.mode).to eq(:disabled)
    end

    it "does not raise when no streamer was live" do
      allow(Pgbus::Web::Streamer).to receive(:reset!).and_return(nil)
      expect { described_class.disabled! }.not_to raise_error
    end

    it "warns (but does not raise) when it tore down a live streamer cleanly" do
      allow(Pgbus::Web::Streamer).to receive(:reset!).and_return([])
      log = StringIO.new
      allow(Pgbus).to receive(:logger).and_return(Logger.new(log))

      expect { described_class.disabled! }.not_to raise_error
      expect(log.string).to include("live Pgbus::Web::Streamer")
    end
  end
end
