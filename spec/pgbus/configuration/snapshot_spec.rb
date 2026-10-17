# frozen_string_literal: true

require "spec_helper"

# A non-main Ractor cannot read Pgbus.configuration: it holds the logger and
# the app's callables. Pgbus.configuration_snapshot is the shareable copy of
# the plain-data settings such a Ractor needs, published by Pgbus.finalize!
# and Pgbus.configure (issue #527).
RSpec.describe Pgbus::Configuration::Snapshot do
  describe ".of" do
    it "copies the queue_prefix into a Ractor-shareable value and leaves the configuration's own alone" do
      config = Pgbus::Configuration.new
      config.queue_prefix = +"tenant_a"

      snapshot = described_class.of(config)

      expect(snapshot.queue_prefix).to eq("tenant_a")
      expect(Ractor.shareable?(snapshot)).to be(true)
      expect(config.queue_prefix).not_to be_frozen
    end
  end

  describe "Pgbus.configuration_snapshot" do
    # Each example works on a fresh global configuration; the suite's
    # configuration and the snapshot it published are put back.
    around do |example|
      original = Pgbus.configuration
      published = Pgbus.configuration_snapshot
      Pgbus.instance_variable_set(:@configuration, Pgbus::Configuration.new)
      example.run
    ensure
      Pgbus.instance_variable_set(:@configuration, original)
      Pgbus.instance_variable_set(:@configuration_snapshot, published)
    end

    it "is Ractor-shareable" do
      expect(Ractor.shareable?(Pgbus.configuration_snapshot)).to be(true)
    end

    it "is published at the end of Pgbus.configure" do
      Pgbus.configure { |c| c.queue_prefix = "configured" }

      expect(Pgbus.configuration_snapshot.queue_prefix).to eq("configured")
      expect(Ractor.shareable?(Pgbus.configuration_snapshot)).to be(true)
    end

    it "is published by Pgbus.finalize!" do
      Pgbus.configuration.queue_prefix = +"finalized"

      Pgbus.finalize!

      expect(Pgbus.configuration_snapshot.queue_prefix).to eq("finalized")
    end

    it "keeps the last published settings until the next finalize" do
      Pgbus.configure { |c| c.queue_prefix = "published" }

      Pgbus.configuration.queue_prefix = "unpublished"

      expect(Pgbus.configuration_snapshot.queue_prefix).to eq("published")
    end

    it "is the defaults again after Pgbus.reset!" do
      Pgbus.configure { |c| c.queue_prefix = "configured" }

      Pgbus.reset!

      expect(Pgbus.configuration_snapshot.queue_prefix).to eq(Pgbus::Configuration.new.queue_prefix)
    end
  end
end
