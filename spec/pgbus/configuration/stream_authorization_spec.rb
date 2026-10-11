# frozen_string_literal: true

require "spec_helper"

# The authorize hook for the engine-mounted StreamApp (issue #522).
RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#streams_authorize" do
    it "defaults to nil" do
      expect(config.streams_authorize).to be_nil
    end

    it "accepts a callable" do
      config.streams_authorize = ->(_env, _stream_name) { true }
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-callable at boot" do
      config.streams_authorize = "nope"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_authorize/)
    end
  end
end
