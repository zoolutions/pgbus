# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Shareable do
  describe ".data?" do
    it "accepts plain data all the way down" do
      value = { "a" => [1, 2.5, :sym, nil, true, /re/, String, 1..3], b: Set[+"x"] }

      expect(described_class.data?(value)).to be(true)
    end

    it "rejects a callable anywhere inside" do
      expect(described_class.data?([{ hook: -> {} }])).to be(false)
    end

    it "rejects a Hash with a default proc" do
      expect(described_class.data?(Hash.new { |h, k| h[k] = [] })).to be(false)
    end

    it "rejects other objects" do
      expect(described_class.data?([Logger.new(IO::NULL)])).to be(false)
    end
  end

  describe ".freeze_copy" do
    it "returns a deep-frozen, shareable copy and leaves the original alone" do
      original = { +"key" => [+"value"] }

      copy = described_class.freeze_copy(original)

      expect(copy).to eq(original)
      expect(Ractor.shareable?(copy)).to be(true)
      expect(original).not_to be_frozen
      expect(original.values.first).not_to be_frozen
    end
  end

  describe ".deep_freeze" do
    it "freezes copies of nested collections and strings" do
      original = { +"key" => [+"value", Set[+"x"]] }

      frozen = described_class.deep_freeze(original)

      expect(frozen).to be_frozen
      expect(frozen.keys.first).to be_frozen
      expect(frozen.values.first).to all(be_frozen)
      expect(original.values.first.first).not_to be_frozen
    end
  end
end
