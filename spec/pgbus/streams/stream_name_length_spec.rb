# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Stream do
  let(:max) { Pgbus::QueueNameValidator::MAX_QUEUE_NAME_LENGTH }
  let(:budget) { Pgbus::Streams::Key.queue_name_budget }

  describe "Pgbus::Streams::Stream.validate_name_length!" do
    it "returns silently when the name fits the budget" do
      name = "x" * budget
      expect do
        described_class.validate_name_length!(name, [name])
      end.not_to raise_error
    end

    it "raises StreamNameTooLong when the name overflows" do
      name = "x" * (budget + 1)
      expect do
        described_class.validate_name_length!(name, [name])
      end.to raise_error(Pgbus::Streams::StreamNameTooLong, /exceeds pgbus budget/)
    end

    it "embeds the offending length, budget, and streamables in the message" do
      name = "y" * (budget + 10)
      streamables = [name, :messages]
      err = begin
        described_class.validate_name_length!(name, streamables)
        nil
      rescue Pgbus::Streams::StreamNameTooLong => e
        e
      end

      expect(err).not_to be_nil
      expect(err.message).to include("#{name.length} chars")
      expect(err.message).to include("budget of #{budget}")
      expect(err.message).to include(streamables.inspect)
      expect(err.message).to include("Pgbus.stream_key")
    end

    it "is an ArgumentError subclass (backcompat with QueueNameValidator)" do
      expect(Pgbus::Streams::StreamNameTooLong.ancestors).to include(ArgumentError)
    end

    it "adjusts the budget when config.queue_prefix changes" do
      original = Pgbus.configuration.queue_prefix
      Pgbus.configuration.queue_prefix = "a" # 1 + 1 = 2 chars reserved
      long_prefix_budget_before = max - 2
      name = "z" * long_prefix_budget_before
      expect do
        described_class.validate_name_length!(name, [name])
      end.not_to raise_error

      Pgbus.configuration.queue_prefix = "very_long_prefix" # 16 + 1 = 17 chars reserved
      expect do
        described_class.validate_name_length!(name, [name])
      end.to raise_error(Pgbus::Streams::StreamNameTooLong)
    ensure
      Pgbus.configuration.queue_prefix = original
    end
  end

  describe "Pgbus::Streams::Stream#initialize" do
    before { Pgbus.reset_client! }

    it "raises StreamNameTooLong for a literal overflowing name" do
      long = "abc_#{"d" * budget}"
      expect { described_class.new(long, client: double("client")) }
        .to raise_error(Pgbus::Streams::StreamNameTooLong)
    end

    it "accepts a short name" do
      expect { described_class.new("short", client: double("client")) }
        .not_to raise_error
    end
  end

  describe "Pgbus.stream caching interaction" do
    before { Pgbus.reset_client! }

    it "does not cache a Stream when construction raises on overflow" do
      long = "e" * (budget + 5)
      expect { Pgbus.stream(long) }.to raise_error(Pgbus::Streams::StreamNameTooLong)
      # Second call must raise again — the cache must not have swallowed the first failure
      expect { Pgbus.stream(long) }.to raise_error(Pgbus::Streams::StreamNameTooLong)
    end
  end
end
