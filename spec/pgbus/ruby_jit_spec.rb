# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::RubyJit do
  let(:yjit) { Module.new }
  let(:zjit) { Module.new }

  before do
    stub_const("RubyVM::YJIT", yjit)
    stub_const("RubyVM::ZJIT", zjit)
    allow(yjit).to receive(:enabled?).and_return(false)
    allow(zjit).to receive(:enabled?).and_return(false)
  end

  describe ".label" do
    it "is yjit when YJIT is enabled" do
      allow(yjit).to receive(:enabled?).and_return(true)
      expect(described_class.label).to eq("yjit")
    end

    it "is zjit when ZJIT is enabled" do
      allow(zjit).to receive(:enabled?).and_return(true)
      expect(described_class.label).to eq("zjit")
    end

    it "is none when no JIT is enabled" do
      expect(described_class.label).to eq("none")
    end

    it "is none on a Ruby without either JIT module" do
      hide_const("RubyVM::YJIT")
      hide_const("RubyVM::ZJIT")
      expect(described_class.label).to eq("none")
    end
  end

  describe ".yjit_available?" do
    it "is true when RubyVM::YJIT is defined" do
      expect(described_class.yjit_available?).to be(true)
    end

    it "is false when RubyVM::YJIT is not defined" do
      hide_const("RubyVM::YJIT")
      expect(described_class.yjit_available?).to be(false)
    end
  end
end
