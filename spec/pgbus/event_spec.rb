# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Event do
  subject(:event) do
    described_class.new(
      event_id: "abc-123",
      payload: { "order_id" => 42, "status" => "completed" },
      routing_key: "orders.completed"
    )
  end

  describe "#[]" do
    it "accesses payload keys" do
      expect(event["order_id"]).to eq(42)
    end

    it "returns nil for missing keys" do
      expect(event["nonexistent"]).to be_nil
    end
  end

  describe "#dig" do
    subject(:event) do
      described_class.new(
        event_id: "e-1",
        payload: { "meta" => { "type" => "widget.created" }, "data" => { "items" => [{ "id" => 7 }] } }
      )
    end

    it "returns a nested payload value" do
      expect(event.dig("meta", "type")).to eq("widget.created")
    end

    it "reads Symbol keys as Strings at every level" do
      expect(event.dig(:meta, :type)).to eq("widget.created")
    end

    it "passes Integer indexes through to arrays" do
      expect(event.dig("data", "items", 0, "id")).to eq(7)
    end

    it "returns nil for a missing key" do
      expect(event.dig("missing", "type")).to be_nil
    end

    it "returns the same value as #[] for a single key" do
      expect(event.dig("meta")).to eq(event["meta"]) # rubocop:disable Style/SingleArgumentDig -- the single-key form is under test
    end

    it "returns nil for a String payload" do
      raw = described_class.new(event_id: "e-2", payload: "raw")

      expect(raw.dig("meta", "type")).to be_nil
    end

    it "returns nil for a nil payload" do
      empty = described_class.new(event_id: "e-3", payload: nil)

      expect(empty.dig("meta", "type")).to be_nil
    end

    it "raises TypeError when an intermediate value cannot be dug into, like Hash#dig" do
      flat = described_class.new(event_id: "e-4", payload: { "meta" => "x" })

      expect { flat.dig("meta", "type") }.to raise_error(TypeError, /String does not have #dig method/)
    end
  end

  describe "#to_h" do
    it "returns a hash representation" do
      hash = event.to_h
      expect(hash["event_id"]).to eq("abc-123")
      expect(hash["payload"]["order_id"]).to eq(42)
      expect(hash["routing_key"]).to eq("orders.completed")
    end
  end

  describe "#published_at" do
    it "defaults to current time" do
      event = described_class.new(event_id: "x", payload: {})
      expect(event.published_at).to be_within(1).of(Time.now.utc)
    end
  end

  describe "#context (issue #431)" do
    it "defaults to nil" do
      expect(described_class.new(event_id: "x", payload: {}).context).to be_nil
    end

    it "exposes the persisted Current attributes handed in" do
      ctx = { "Current" => { "tenant" => "t" } }
      expect(described_class.new(event_id: "x", payload: {}, context: ctx).context).to eq(ctx)
    end
  end
end
