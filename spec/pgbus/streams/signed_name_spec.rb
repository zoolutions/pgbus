# frozen_string_literal: true

require "spec_helper"
require "active_support"
require "active_support/message_verifier"

RSpec.describe Pgbus::Streams::SignedName do
  let(:secret) { "a" * 64 }
  let(:verifier) { ActiveSupport::MessageVerifier.new(secret, digest: "SHA256", serializer: JSON) }

  before do
    # SignedName reads the secret from configuration when Turbo is not loaded.
    Pgbus.configuration.streams_signed_name_secret = secret
  end

  after do
    Pgbus.configuration.streams_signed_name_secret = nil
  end

  describe ".verify!" do
    it "round-trips a signed stream name" do
      token = verifier.generate("order:42")
      expect(described_class.verify!(token)).to eq("order:42")
    end

    it "round-trips a multi-segment stream name" do
      token = verifier.generate("gid://app/Order/42:messages")
      expect(described_class.verify!(token)).to eq("gid://app/Order/42:messages")
    end

    it "raises InvalidSignedName on a tampered token" do
      token = verifier.generate("order:42")
      tampered = "#{token}xxx"
      expect { described_class.verify!(tampered) }
        .to raise_error(Pgbus::Streams::SignedName::InvalidSignedName)
    end

    it "raises InvalidSignedName on a token signed with a different key" do
      other_verifier = ActiveSupport::MessageVerifier.new("b" * 64, digest: "SHA256", serializer: JSON)
      token = other_verifier.generate("order:42")
      expect { described_class.verify!(token) }
        .to raise_error(Pgbus::Streams::SignedName::InvalidSignedName)
    end

    it "raises InvalidSignedName on a blank token" do
      expect { described_class.verify!("") }
        .to raise_error(Pgbus::Streams::SignedName::InvalidSignedName, /blank/)
    end

    it "raises InvalidSignedName on a nil token" do
      expect { described_class.verify!(nil) }
        .to raise_error(Pgbus::Streams::SignedName::InvalidSignedName, /blank/)
    end

    context "when Turbo is loaded" do
      before do
        # Simulate turbo-rails' Turbo module with its own verifier_key.
        # Reusing turbo's key is the whole point — existing broadcasts_to calls
        # generate tokens we must be able to verify.
        turbo_secret = "c" * 64
        # A spec double: built per example and driven from the example's thread.
        # rubocop:disable-next ThreadSafety/ClassInstanceVariable
        turbo_module = Module.new do
          define_singleton_method(:signed_stream_verifier_key) { turbo_secret }
          define_singleton_method(:signed_stream_verifier) do
            @signed_stream_verifier ||= ActiveSupport::MessageVerifier.new(
              turbo_secret, digest: "SHA256", serializer: JSON
            )
          end
        end
        stub_const("Turbo", turbo_module)
        Pgbus.configuration.streams_signed_name_secret = nil
      end

      it "uses Turbo's verifier in preference to the configuration secret" do
        turbo_verifier = ActiveSupport::MessageVerifier.new(
          "c" * 64, digest: "SHA256", serializer: JSON
        )
        token = turbo_verifier.generate("room:42")
        expect(described_class.verify!(token)).to eq("room:42")
      end

      it "falls back to the configuration secret when Turbo is loaded but no key is set" do
        Pgbus.configuration.streams_signed_name_secret = secret
        # Make Turbo.signed_stream_verifier_key raise the way real turbo-rails does
        allow(Turbo).to receive(:signed_stream_verifier_key).and_raise(ArgumentError)
        token = verifier.generate("order:42")
        expect(described_class.verify!(token)).to eq("order:42")
      end
    end

    context "when neither Turbo nor a configuration secret is available" do
      before { Pgbus.configuration.streams_signed_name_secret = nil }

      it "raises a clear configuration error" do
        token = verifier.generate("order:42")
        expect { described_class.verify!(token) }
          .to raise_error(Pgbus::Streams::SignedName::MissingSecret, /streams_signed_name_secret/)
      end
    end
  end

  describe ".sign" do
    it "produces a token that .verify! accepts" do
      token = described_class.sign("order:42")
      expect(described_class.verify!(token)).to eq("order:42")
    end
  end
end
