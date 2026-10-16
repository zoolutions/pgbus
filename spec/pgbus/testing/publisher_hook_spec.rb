# frozen_string_literal: true

require "spec_helper"
require "active_job"
require "pgbus/testing"

RSpec.describe Pgbus::EventBus::Publisher do
  include PgmqDoubles

  let(:mock_client) { build_mock_client }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    Pgbus::Testing.store.clear!
  end

  after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.store.clear!
  end

  describe "fake mode" do
    before { Pgbus::Testing.fake! }

    it "captures the event without calling PGMQ" do
      described_class.publish("orders.created", { "id" => 1 })

      expect(mock_client).not_to have_received(:publish_to_topic)
      expect(Pgbus::Testing.store.events.size).to eq(1)

      event = Pgbus::Testing.store.events.first
      expect(event.routing_key).to eq("orders.created")
      expect(event.payload).to eq("id" => 1)
      expect(event.event_id).to be_a(String)
    end

    it "captures headers" do
      described_class.publish("orders.created", { "id" => 1 }, headers: { "x-trace" => "abc" })

      event = Pgbus::Testing.store.events.first
      expect(event.headers).to eq("x-trace" => "abc")
    end
  end

  describe "inline mode" do
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
      stub_const("TestInlineHandler", klass)
      klass
    end

    before do
      Pgbus::Testing.inline!
      Pgbus::EventBus::Registry.instance.clear!
      handler_class.handled_events = []
    end

    after do
      Pgbus::EventBus::Registry.instance.clear!
    end

    it "captures the event AND dispatches to handlers" do
      Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)
      described_class.publish("orders.created", { "id" => 42 })

      expect(mock_client).not_to have_received(:publish_to_topic)
      expect(Pgbus::Testing.store.events.size).to eq(1)
      expect(handler_class.handled_events.size).to eq(1)
      expect(handler_class.handled_events.first.payload).to eq("id" => 42)
    end

    it "captures but does not dispatch delayed events" do
      Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)
      described_class.publish("orders.created", { "id" => 99 }, delay: 300)

      expect(Pgbus::Testing.store.events.size).to eq(1)
      expect(handler_class.handled_events).to be_empty
    end
  end

  describe "Current attributes round-trip (issue #431)" do
    let(:current_class) { Class.new(ActiveSupport::CurrentAttributes) { attribute :tenant } }
    let(:handler_class) do
      # A spec double: built per example and driven from the example's thread.
      # rubocop:disable-next ThreadSafety/ClassAndModuleAttributes
      klass = Class.new(Pgbus::EventBus::Handler) do
        class << self
          attr_accessor :seen
        end
        self.seen = []

        def handle(_event)
          self.class.seen << TestingSpecCurrent.tenant
        end
      end
      stub_const("TestCurrentHandler", klass)
      klass
    end

    before do
      stub_const("TestingSpecCurrent", current_class)
      Pgbus.configuration.current_attributes = ["TestingSpecCurrent"]
      Pgbus::EventBus::Registry.instance.clear!
      handler_class.seen = []
    end

    after do
      Pgbus.configuration.current_attributes = nil
      ActiveSupport::CurrentAttributes.clear_all
      Pgbus::EventBus::Registry.instance.clear!
    end

    it "fake mode stores the captured context on the event" do
      Pgbus::Testing.fake!
      TestingSpecCurrent.tenant = "acme"

      described_class.publish("orders.created", { "id" => 1 })

      expect(Pgbus::Testing.store.events.first.context).to include("TestingSpecCurrent" => hash_including("tenant" => "acme"))
    end

    it "inline mode restores the context around the handler" do
      Pgbus::Testing.inline!
      Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)
      TestingSpecCurrent.tenant = "acme"
      described_class.publish("orders.created", { "id" => 1 })
      TestingSpecCurrent.tenant = nil

      expect(handler_class.seen).to eq(["acme"])
    end

    it "drain! restores the context around the handler" do
      Pgbus::Testing.fake!
      Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)
      TestingSpecCurrent.tenant = "acme"
      described_class.publish("orders.created", { "id" => 1 })
      TestingSpecCurrent.tenant = nil

      Pgbus::Testing.store.drain!

      expect(handler_class.seen).to eq(["acme"])
      expect(TestingSpecCurrent.tenant).to be_nil
    end
  end

  describe "disabled mode" do
    before { Pgbus::Testing.disabled! }

    it "passes through to the real publisher" do
      described_class.publish("orders.created", { "id" => 1 })

      expect(mock_client).to have_received(:publish_to_topic)
      expect(Pgbus::Testing.store.events).to be_empty
    end
  end
end
