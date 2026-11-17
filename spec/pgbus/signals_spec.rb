# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Signals do
  let(:client) { instance_double(Pgbus::Client, notify_signal: nil) }

  before do
    described_class.reset!
    allow(Pgbus).to receive(:client).and_return(client)
    allow(Pgbus::OpenTransaction).to receive(:defer).and_return(nil)
  end

  after { described_class.reset! }

  describe ".subscribe (Pgbus.on_signal)" do
    it "registers a block under the signal's name" do
      Pgbus.on_signal(:widgets_changed) { |_payload| nil }

      expect(described_class.names).to eq(["widgets_changed"])
      expect(described_class.subscribed?).to be(true)
    end

    it "is empty until something subscribes" do
      expect(described_class.subscribed?).to be(false)
      expect(described_class.channels).to eq([])
    end

    it "maps the name to a prefixed NOTIFY channel and back" do
      Pgbus.on_signal(:widgets_changed) { nil }
      channel = described_class.channel_for(:widgets_changed)

      expect(channel).to eq(Pgbus.configuration.queue_name("signal_widgets_changed"))
      expect(described_class.channels).to eq([channel])
      expect(described_class.name_for(channel)).to eq("widgets_changed")
      expect(described_class.name_for("pgmq.q_pgbus_x.INSERT")).to be_nil
    end

    it "rejects a name outside lowercase letters, digits and underscores" do
      expect { Pgbus.on_signal("Widgets") { nil } }.to raise_error(ArgumentError, /signal name/)
      expect { Pgbus.on_signal("a.b") { nil } }.to raise_error(ArgumentError, /signal name/)
      expect { Pgbus.on_signal(:"") { nil } }.to raise_error(ArgumentError, /signal name/)
    end

    it "rejects a name whose channel would overflow the queue-name budget" do
      expect { Pgbus.on_signal("x" * 60) { nil } }.to raise_error(ArgumentError, /too long/)
    end

    it "requires a block" do
      expect { Pgbus.on_signal(:widgets_changed) }.to raise_error(ArgumentError, /block/)
    end

    it "rejects an on_gap that is not callable" do
      expect { Pgbus.on_signal(:widgets_changed, on_gap: :nope) { nil } }.to raise_error(ArgumentError, /on_gap/)
    end

    it "tells a running streamer to LISTEN on the new channel" do
      allow(Pgbus::Web::Streamer).to receive(:listen_signal)

      Pgbus.on_signal(:widgets_changed) { nil }

      expect(Pgbus::Web::Streamer).to have_received(:listen_signal).with(described_class.channel_for(:widgets_changed))
    end
  end

  describe ".publish (Pgbus.signal)" do
    it "sends a NOTIFY through the client" do
      Pgbus.signal(:widgets_changed, "id=42")

      expect(client).to have_received(:notify_signal).with("widgets_changed", "id=42")
    end

    it "sends a NOTIFY without a payload" do
      Pgbus.signal(:widgets_changed)

      expect(client).to have_received(:notify_signal).with("widgets_changed", nil)
    end

    it "defers the NOTIFY to the commit of an open transaction" do
      deferred = nil
      allow(Pgbus::OpenTransaction).to receive(:defer) do |&block|
        deferred = block
        :transaction
      end

      expect(Pgbus.signal(:widgets_changed)).to be_nil
      expect(client).not_to have_received(:notify_signal)

      deferred.call
      expect(client).to have_received(:notify_signal).with("widgets_changed", nil)
    end

    it "rejects a payload that is not a String" do
      expect { Pgbus.signal(:widgets_changed, { id: 1 }) }.to raise_error(ArgumentError, /String/)
    end

    it "rejects a payload over the NOTIFY cap" do
      expect { Pgbus.signal(:widgets_changed, "x" * 8_000) }.to raise_error(ArgumentError, /7999/)
    end

    it "rejects an invalid name before sending anything" do
      expect { Pgbus.signal("Bad Name") }.to raise_error(ArgumentError)
      expect(client).not_to have_received(:notify_signal)
    end
  end

  describe ".deliver" do
    let(:reported) { [] }

    before do
      allow(Pgbus::ErrorReporter).to receive(:report) { |e, ctx| reported << [e, ctx] }
    end

    it "runs every subscriber of the signal with the payload" do
      seen = []
      Pgbus.on_signal(:widgets_changed) { |payload| seen << [:a, payload] }
      Pgbus.on_signal(:widgets_changed) { |payload| seen << [:b, payload] }
      Pgbus.on_signal(:other) { |payload| seen << [:other, payload] }

      described_class.deliver("widgets_changed", "id=1")

      expect(seen).to eq([[:a, "id=1"], [:b, "id=1"]])
    end

    it "reports a raising subscriber and still runs the others" do
      seen = []
      Pgbus.on_signal(:widgets_changed) { raise "boom" }
      Pgbus.on_signal(:widgets_changed) { |payload| seen << payload }

      described_class.deliver("widgets_changed", nil)

      expect(seen).to eq([nil])
      expect(reported.map { |e, _| e.message }).to eq(["boom"])
      expect(reported.first.last).to include(action: "signal_subscriber", signal: "widgets_changed")
    end
  end

  describe ".deliver_gap" do
    before { allow(Pgbus::ErrorReporter).to receive(:report) }

    it "runs every on_gap once, across signals, and skips subscribers without one" do
      seen = []
      Pgbus.on_signal(:a, on_gap: -> { seen << :a }) { nil }
      Pgbus.on_signal(:b, on_gap: -> { seen << :b }) { nil }
      Pgbus.on_signal(:c) { nil }

      described_class.deliver_gap

      expect(seen).to eq(%i[a b])
    end

    it "reports a raising on_gap and still runs the others" do
      seen = []
      Pgbus.on_signal(:a, on_gap: -> { raise "gap boom" }) { nil }
      Pgbus.on_signal(:b, on_gap: -> { seen << :b }) { nil }

      described_class.deliver_gap

      expect(seen).to eq([:b])
      expect(Pgbus::ErrorReporter).to have_received(:report).with(an_instance_of(RuntimeError), hash_including(action: "signal_on_gap"))
    end

    it "is false without an on_gap and true with one" do
      Pgbus.on_signal(:a) { nil }
      expect(described_class.gap_handlers?).to be(false)

      Pgbus.on_signal(:b, on_gap: -> {}) { nil }
      expect(described_class.gap_handlers?).to be(true)
    end
  end

  describe ".start_listening!" do
    let(:config) { Pgbus.configuration }
    let!(:saved) { [config.streams_enabled, config.streams_test_mode] }

    before do
      allow(Pgbus::Web::Streamer).to receive(:current)
      allow(Pgbus).to receive(:database_task?).and_return(false)
      config.streams_enabled = true
      config.streams_test_mode = false
      described_class.instance_variable_set(:@started_pid, nil)
    end

    after { config.streams_enabled, config.streams_test_mode = saved }

    it "does nothing without a subscriber" do
      described_class.start_listening!

      expect(Pgbus::Web::Streamer).not_to have_received(:current)
    end

    it "starts the streamer once per process when a subscriber exists" do
      Pgbus.on_signal(:a) { nil }

      described_class.start_listening!
      described_class.start_listening!

      expect(Pgbus::Web::Streamer).to have_received(:current).once
    end

    it "does nothing under streams_test_mode, with streams disabled, or in a database task" do
      Pgbus.on_signal(:a) { nil }

      config.streams_test_mode = true
      described_class.start_listening!
      config.streams_test_mode = false
      config.streams_enabled = false
      described_class.start_listening!
      config.streams_enabled = true
      allow(Pgbus).to receive(:database_task?).and_return(true)
      described_class.start_listening!

      expect(Pgbus::Web::Streamer).not_to have_received(:current)
    end

    it "reports a failed start once and never raises into the request" do
      allow(Pgbus::ErrorReporter).to receive(:report)
      allow(Pgbus::Web::Streamer).to receive(:current).and_raise(Pgbus::ConfigurationError, "no dedicated connection")
      Pgbus.on_signal(:a) { nil }

      expect { 2.times { described_class.start_listening! } }.not_to raise_error
      expect(Pgbus::Web::Streamer).to have_received(:current).once
      expect(Pgbus::ErrorReporter).to have_received(:report).once
    end
  end
end
