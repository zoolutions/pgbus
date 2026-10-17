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

    # A collection that contains itself is left as it is, like any other
    # non-data value, instead of recursing until SystemStackError at boot.
    it "rejects a collection that contains itself" do
      looped = [+"x"]
      looped << looped
      keyed = {}
      keyed[:self] = keyed

      expect([described_class.data?(looped), described_class.data?(keyed)]).to eq([false, false])
    end

    it "accepts the same value twice in one collection" do
      shared = [+"x"]

      expect(described_class.data?([shared, shared])).to be(true)
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

  describe ".main_ractor?" do
    it "is true on the main Ractor" do
      expect(described_class.main_ractor?).to be(true)
    end

    # Ruby 4.0+ only: spawning a Ractor on 3.3/3.4 costs SimpleCov a sixth of
    # the suite's measured lines (93.5% -> 76.9% on one seed), which fails the
    # coverage floor. The example below covers 3.3's fallback without one.
    it "is false inside a non-main Ractor", :ractor do
      skip "needs Ruby 4.0+ (Ractor#value)" unless Ractor.method_defined?(:value)

      expect(Ractor.new(described_class, &:main_ractor?).value).to be(false)
    end

    it "compares against Ractor.main where Ractor.main? is missing (Ruby 3.3)" do
      allow(Ractor).to receive(:respond_to?).and_call_original
      allow(Ractor).to receive(:respond_to?).with(:main?).and_return(false)
      allow(Ractor).to receive(:current).and_return(Object.new)

      expect(described_class.main_ractor?).to be(false)
    end
  end

  # The fallback where the engine has no Ractor (JRuby, TruffleRuby).
  describe ".deep_freeze" do
    it "freezes copies of nested collections and strings" do
      original = { +"key" => [+"value", Set[+"x"]] }

      frozen = described_class.deep_freeze(original)
      nested = frozen.values.first

      expect([frozen, frozen.keys.first, nested, nested.first, nested.last]).to all(be_frozen)
      expect(nested.last.first).to be_frozen
      expect(original.values.first.first).not_to be_frozen
    end

    it "keeps a Hash's default, frozen" do
      original = Hash.new(+"fallback").merge(+"key" => 1)

      frozen = described_class.deep_freeze(original)

      expect(frozen[:missing]).to eq("fallback")
      expect(frozen.default).to be_frozen
    end

    it "freezes a Range's endpoints" do
      original = (+"a")..(+"m")

      frozen = described_class.deep_freeze(original)

      expect(frozen).to eq(original)
      expect([frozen, frozen.begin, frozen.end]).to all(be_frozen)
      expect(original.begin).not_to be_frozen
    end
  end
end
