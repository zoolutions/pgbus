# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/integer/time"

# Seat expiry ttl and the change hook (issue #515).
RSpec.describe Pgbus::Configuration do
  subject(:config) { described_class.new }

  describe "#streams_presence_ttl" do
    it "defaults to 60 seconds for every stream" do
      expect(config.streams_presence_ttl).to eq(60)
      expect(config.presence_ttl_for("room:1")).to eq(60)
      expect(config.presence_min_ttl).to eq(60)
    end

    it "accepts a single number or Duration for every stream" do
      config.streams_presence_ttl = 2.minutes
      expect(config.presence_ttl_for("room:1")).to eq(120)
      expect { config.validate! }.not_to raise_error
    end

    it "maps patterns to seconds, falling back to :default" do
      config.streams_presence_ttl = { "lobby" => 30, /^doc:/ => 5.minutes, default: 90 }

      expect(config.presence_ttl_for("lobby")).to eq(30)
      expect(config.presence_ttl_for("doc:7")).to eq(300)
      expect(config.presence_ttl_for("room:1")).to eq(90)
      expect(config.presence_min_ttl).to eq(30)
    end

    it "keeps fractional seconds instead of truncating them" do
      config.streams_presence_ttl = { "lobby" => 0.5, default: 90.5 }

      expect(config.presence_ttl_for("lobby")).to eq(0.5)
      expect(config.presence_ttl_for("room:1")).to eq(90.5)
      expect(config.presence_min_ttl).to eq(0.5)
      expect { config.validate! }.not_to raise_error
    end

    it "falls back to 60 seconds when the map has no :default" do
      config.streams_presence_ttl = { "lobby" => 30 }
      expect(config.presence_ttl_for("room:1")).to eq(60)
    end

    it "rejects a non-positive ttl at boot" do
      config.streams_presence_ttl = { "lobby" => 0 }
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_presence_ttl/)
    end

    it "warns when a ttl does not outlast the heartbeat interval (live seats would churn)" do
      logger = instance_double(Logger, warn: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)
      config.streams_heartbeat_interval = 15
      config.streams_presence_ttl = { "lobby" => 10, default: 60 }

      config.validate!

      expect(logger).to have_received(:warn) do |&block|
        expect(block.call).to include("streams_presence_ttl", "streams_heartbeat_interval")
      end
    end

    it "rejects a value that is neither a number nor a Hash" do
      config.streams_presence_ttl = "60"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_presence_ttl/)
    end
  end

  describe "#streams_presence_on_change" do
    it "defaults to nil" do
      expect(config.streams_presence_on_change).to be_nil
    end

    it "accepts a callable" do
      config.streams_presence_on_change = ->(_stream, _change) {}
      expect { config.validate! }.not_to raise_error
    end

    it "rejects a non-callable at boot" do
      config.streams_presence_on_change = "nope"
      expect { config.validate! }.to raise_error(Pgbus::ConfigurationError, /streams_presence_on_change/)
    end
  end

  describe "#notify_presence_change" do
    let(:change) { { type: :join, connection_id: "c1", seat: "s1", member_id: "7" } }

    it "calls the hook with the stream name and the change" do
      calls = []
      config.streams_presence_on_change = ->(stream, c) { calls << [stream, c] }

      config.notify_presence_change("room:1", change)

      expect(calls).to eq([["room:1", change]])
    end

    it "does nothing without a hook" do
      expect { config.notify_presence_change("room:1", change) }.not_to raise_error
    end

    it "logs and swallows an error raised by the hook" do
      logger = instance_double(Logger, error: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)
      config.streams_presence_on_change = ->(_s, _c) { raise "boom" }

      expect { config.notify_presence_change("room:1", change) }.not_to raise_error
      expect(logger).to have_received(:error) do |&block|
        expect(block.call).to include("streams_presence_on_change", "boom")
      end
    end
  end
end
