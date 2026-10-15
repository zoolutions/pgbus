# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing"

RSpec.describe Pgbus::Testing::Assertions do
  include described_class

  before do
    Pgbus::Testing.fake!
    Pgbus::Testing.store.clear!
    allow(Pgbus).to receive(:client).and_return(double("Pgbus::Client", publish_to_topic: nil))
  end

  after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.store.clear!
  end

  describe "#pgbus_published_events" do
    it "returns all published events" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 2 })

      expect(pgbus_published_events.size).to eq(2)
    end

    it "filters by routing_key" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
      Pgbus::EventBus::Publisher.publish("orders.shipped", { "id" => 2 })

      result = pgbus_published_events(routing_key: "orders.created")
      expect(result.size).to eq(1)
      expect(result.first.routing_key).to eq("orders.created")
    end
  end

  describe "#assert_pgbus_published" do
    it "passes when expected events are published" do
      expect do
        assert_pgbus_published(count: 1, routing_key: "orders.created") do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end
      end.not_to raise_error
    end

    it "fails when count doesn't match" do
      expect do
        assert_pgbus_published(count: 2, routing_key: "orders.created") do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end
      end.to raise_error(/Expected 2 event\(s\) published.*got 1/)
    end

    it "only counts events published within the block" do
      Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 0 })

      expect do
        assert_pgbus_published(count: 1) do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end
      end.not_to raise_error
    end
  end

  describe "#assert_no_pgbus_published" do
    it "passes when no events are published" do
      expect do
        assert_no_pgbus_published { "no-op" }
      end.not_to raise_error
    end

    it "fails when events are published" do
      expect do
        assert_no_pgbus_published do
          Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 1 })
        end
      end.to raise_error(/Expected no events published.*got 1/)
    end
  end

  describe "#perform_published_events" do
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
      stub_const("TestPerformHandler", klass)
      klass
    end

    before do
      Pgbus::EventBus::Registry.instance.clear!
      handler_class.handled_events = []
    end

    after do
      Pgbus::EventBus::Registry.instance.clear!
    end

    it "publishes events and then dispatches them to handlers" do
      Pgbus::EventBus::Registry.instance.subscribe("orders.created", handler_class)

      perform_published_events do
        Pgbus::EventBus::Publisher.publish("orders.created", { "id" => 42 })
      end

      expect(handler_class.handled_events.size).to eq(1)
      expect(handler_class.handled_events.first["id"]).to eq(42)
    end
  end
end
