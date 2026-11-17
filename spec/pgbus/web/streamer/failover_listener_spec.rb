# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::FailoverListener do
  subject(:failover) do
    described_class.new(
      hub_client: hub_client,
      local_listener_factory: local_listener_factory,
      logger: logger
    )
  end

  let(:hub_client) do
    instance_double(Pgbus::Web::Streamer::HubClient,
                    ensure_listening: :done, remove_listening: nil, stop: nil)
  end
  let(:local_listener) do
    instance_double(Pgbus::Web::Streamer::Listener,
                    ensure_listening: :done, remove_listening: nil, stop: nil)
  end
  let(:factory_calls) { [] }
  let(:local_listener_factory) do
    lambda do
      factory_calls << :built
      local_listener
    end
  end
  let(:logger) { Logger.new(IO::NULL) }

  describe "hub mode (healthy)" do
    it "delegates ensure_listening to the hub client and records the subscription" do
      expect(failover.ensure_listening("pgbus_stream_chat")).to eq(:done)
      expect(hub_client).to have_received(:ensure_listening).with("pgbus_stream_chat")
      expect(factory_calls).to be_empty
    end

    it "delegates remove_listening and forgets the subscription" do
      failover.ensure_listening("pgbus_stream_chat")
      failover.remove_listening("pgbus_stream_chat")

      expect(hub_client).to have_received(:remove_listening).with("pgbus_stream_chat")
    end
  end

  describe "failover on asynchronous transport death (on_failure)" do
    it "builds ONE local listener and re-subscribes every recorded subscription" do
      failover.ensure_listening("pgbus_stream_a")
      failover.ensure_listening("pgbus_stream_b")
      failover.remove_listening("pgbus_stream_a")

      failover.fail_over!
      failover.fail_over! # idempotent — e.g. on_failure raced with an ensure error

      expect(factory_calls).to eq([:built])
      expect(local_listener).to have_received(:ensure_listening).with("pgbus_stream_b")
      expect(local_listener).not_to have_received(:ensure_listening).with("pgbus_stream_a")
    end

    it "routes subsequent calls to the local listener" do
      failover.fail_over!
      failover.ensure_listening("pgbus_stream_chat")

      expect(local_listener).to have_received(:ensure_listening).with("pgbus_stream_chat")
      expect(hub_client).not_to have_received(:ensure_listening)
    end
  end

  describe "failover on a synchronous ensure failure" do
    it "falls over and completes the sub on the local listener (ack contract preserved)" do
      allow(hub_client).to receive(:ensure_listening)
        .and_raise(Pgbus::Web::Streamer::HubClient::HubUnavailableError, "dead")

      expect(failover.ensure_listening("pgbus_stream_chat")).to eq(:done)
      # Twice: once rebuilding the recorded set inside fail_over!, once for
      # the retried call itself — both land on the local listener.
      expect(local_listener).to have_received(:ensure_listening).with("pgbus_stream_chat").twice
    end
  end

  describe "replay failure after the factory started the listener" do
    it "stops the never-swapped listener so its LISTEN connection cannot leak" do
      failover.ensure_listening("pgbus_stream_chat")
      allow(local_listener).to receive(:ensure_listening).and_raise(StandardError, "replay boom")
      allow(logger).to receive(:error)

      failover.fail_over!

      expect(local_listener).to have_received(:stop)
      expect(logger).to have_received(:error)
    end
  end

  describe "double failure (local listener factory raises)" do
    let(:local_listener_factory) { -> { raise StandardError, "db down" } }

    it "never raises to the dispatcher — logs and returns nil (Listener's timeout contract)" do
      allow(hub_client).to receive(:ensure_listening)
        .and_raise(Pgbus::Web::Streamer::HubClient::HubUnavailableError, "dead")
      allow(logger).to receive(:error)

      expect(failover.ensure_listening("pgbus_stream_chat")).to be_nil
      expect(logger).to have_received(:error)
    end
  end

  describe "#threads (issue #443)" do
    it "reports the hub client's threads in hub mode" do
      allow(hub_client).to receive(:threads).and_return([:hub_thread])
      expect(failover.threads).to eq([:hub_thread])
    end

    it "reports the local listener's threads after failover" do
      allow(local_listener).to receive(:threads).and_return([:local_thread])
      failover.fail_over!
      expect(failover.threads).to eq([:local_thread])
    end
  end

  describe "#stop" do
    it "stops the hub client in hub mode" do
      failover.stop
      expect(hub_client).to have_received(:stop)
    end

    it "stops the local listener after failover" do
      failover.fail_over!
      failover.stop

      expect(local_listener).to have_received(:stop)
    end
  end

  describe "process signals (issue #674)" do
    subject(:failover) do
      described_class.new(
        hub_client: hub_client, local_listener_factory: local_listener_factory,
        on_gap: -> { gaps << :gap }, logger: logger
      )
    end

    let(:gaps) { [] }
    let(:hub_client) do
      instance_double(Pgbus::Web::Streamer::HubClient,
                      ensure_listening: :done, ensure_signal_listening: :done, remove_listening: nil, stop: nil)
    end
    let(:local_listener) do
      instance_double(Pgbus::Web::Streamer::Listener,
                      ensure_listening: :done, ensure_signal_listening: :done, remove_listening: nil, stop: nil)
    end

    it "subscribes a signal channel through the hub" do
      expect(failover.ensure_signal_listening("pgbus_signal_a")).to eq(:done)
      expect(hub_client).to have_received(:ensure_signal_listening).with("pgbus_signal_a")
    end

    it "replays every signal channel on the local listener at failover, then reports one gap" do
      failover.ensure_signal_listening("pgbus_signal_a")
      failover.ensure_listening("pgbus_stream_chat")

      failover.fail_over!
      failover.fail_over!

      expect(local_listener).to have_received(:ensure_signal_listening).with("pgbus_signal_a")
      expect(local_listener).to have_received(:ensure_listening).with("pgbus_stream_chat")
      expect(gaps).to eq([:gap])
    end

    it "reports no gap at a failover without signal channels" do
      failover.ensure_listening("pgbus_stream_chat")

      failover.fail_over!

      expect(gaps).to eq([])
    end

    it "fails over when the hub cannot ack a signal subscription, and subscribes locally" do
      allow(hub_client).to receive(:ensure_signal_listening)
        .and_raise(Pgbus::Web::Streamer::HubClient::HubUnavailableError, "ack timeout")

      expect(failover.ensure_signal_listening("pgbus_signal_a")).to eq(:done)
      expect(factory_calls).to eq([:built])
      expect(local_listener).to have_received(:ensure_signal_listening).with("pgbus_signal_a").at_least(:once)
    end
  end
end
