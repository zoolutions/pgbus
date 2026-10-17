# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Workflow::SignalMatcher do
  def signal(name, payload = {})
    double("WorkflowSignal", name: name, payload: payload.with_indifferent_access)
  end

  it "matches a signal by name" do
    matcher = described_class.new(:shipped)

    expect(matcher.matches?(signal("shipped"))).to be(true)
    expect(matcher.matches?(signal("refunded"))).to be(false)
    expect(matcher.signal_name).to eq("shipped")
    expect(matcher).to be_frozen
  end

  it "narrows the match with a block that sees the payload with indifferent access" do
    seen = []
    matcher = described_class.new("shipped") do |payload|
      seen << payload[:carrier]
      payload["carrier"] == "dhl"
    end

    expect(matcher.matches?(signal("shipped", "carrier" => "dhl"))).to be(true)
    expect(matcher.matches?(signal("shipped", carrier: "ups"))).to be(false)
    expect(matcher.matches?(signal("refunded", carrier: "dhl"))).to be(false)
    expect(seen).to eq(%w[dhl ups])
  end

  it "ignores surrounding whitespace in both names, as signal! does" do
    expect(described_class.new(" shipped ").matches?(signal("shipped"))).to be(true)
    expect(described_class.new(:shipped).matches?(signal(" shipped "))).to be(true)
  end

  it "answers true or false whatever the block returns" do
    expect(described_class.new(:x) { |_payload| "yes" }.matches?(signal("x"))).to be(true)
    expect(described_class.new(:x) { |_payload| nil }.matches?(signal("x"))).to be(false)
  end

  it "rejects a blank name" do
    expect { described_class.new("") }.to raise_error(ArgumentError, /signal name/)
    expect { described_class.new(nil) }.to raise_error(ArgumentError, /signal name/)
  end

  describe ".coerce" do
    it "turns a name into a matcher and keeps matchers and matches? objects as they are" do
      custom = Object.new.tap { |object| def object.matches?(_signal) = true }
      matcher = described_class.new(:a)

      expect(described_class.coerce(:shipped).signal_name).to eq("shipped")
      expect(described_class.coerce(matcher)).to be(matcher)
      expect(described_class.coerce(custom)).to be(custom)
    end

    it "rejects anything else" do
      expect { described_class.coerce(1) }.to raise_error(ArgumentError, /wait_for: takes a signal name/)
    end
  end

  describe ".label" do
    it "names the signal a matcher waits for: its signal_name, else its class" do
      custom = Class.new { def self.name = "EuShipment" }.new

      expect(described_class.label(described_class.new(:shipped))).to eq("shipped")
      expect(described_class.label(custom)).to eq("EuShipment")
    end
  end
end
