# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus do
  it "has a version number" do
    expect(Pgbus::VERSION).not_to be_nil
  end

  describe ".configuration" do
    it "returns a Configuration instance" do
      expect(described_class.configuration).to be_a(Pgbus::Configuration)
    end

    it "memoizes the configuration" do
      expect(described_class.configuration).to be(described_class.configuration)
    end
  end

  describe ".configure" do
    it "yields the configuration" do
      described_class.configure do |config|
        expect(config).to be_a(Pgbus::Configuration)
      end
    end
  end

  describe ".logger" do
    it "reads from configuration" do
      expect(described_class.logger).to eq(described_class.configuration.logger)
    end
  end

  describe ".logger=" do
    it "writes to configuration" do
      custom_logger = Logger.new(IO::NULL)
      original = described_class.configuration.logger

      described_class.logger = custom_logger
      expect(described_class.configuration.logger).to be(custom_logger)
    ensure
      described_class.configuration.logger = original
    end
  end

  describe ".reset!" do
    it "clears configuration" do
      old_config = described_class.configuration
      described_class.reset!
      expect(described_class.configuration).not_to be(old_config)
    end
  end

  describe ".reset_client!" do
    it "drops cached streams so none keeps using the discarded client" do
      described_class.instance_variable_set(:@client, instance_double(Pgbus::Client))
      cached = described_class.stream("chat")

      described_class.reset_client!

      expect(described_class.stream("chat")).not_to be(cached)
    ensure
      described_class.reset_client!
    end
  end

  describe ".stream_name_budget (issue #334)" do
    it "returns the max stream-name length: pgmq budget minus the queue prefix" do
      # A stream name becomes the physical queue `#{queue_prefix}_<name>`, so the
      # usable budget is MAX_QUEUE_NAME_LENGTH - queue_prefix.length - 1 (the _).
      described_class.configuration.queue_prefix = "pgbus"
      expected = Pgbus::QueueNameValidator::MAX_QUEUE_NAME_LENGTH - "pgbus".length - 1

      expect(described_class.stream_name_budget).to eq(expected)
    end

    it "shrinks as the queue prefix grows" do
      described_class.configuration.queue_prefix = "a_longer_prefix"
      short = described_class.stream_name_budget

      described_class.configuration.queue_prefix = "x"
      long = described_class.stream_name_budget

      expect(long).to be > short
    ensure
      described_class.configuration.queue_prefix = "pgbus"
    end
  end
end
