# frozen_string_literal: true

require "spec_helper"
require "pgbus/testing"

RSpec.describe "Pgbus::Testing and process signals (issue #674)" do # rubocop:disable RSpec/DescribeClass
  let(:client) { instance_double(Pgbus::Client, notify_signal: nil) }
  let(:seen) { [] }

  before do
    Pgbus::Signals.reset!
    allow(Pgbus).to receive(:client).and_return(client)
    allow(Pgbus::Web::Streamer).to receive(:listen_signal)
    Pgbus.on_signal(:widgets_changed) { |payload| seen << payload }
  end

  after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.clear_signals!
    Pgbus::Signals.reset!
  end

  it "records a signal in fake mode without running subscribers or sending a NOTIFY" do
    Pgbus::Testing.fake!

    Pgbus.signal(:widgets_changed, "id=1")

    expect(Pgbus::Testing.signals).to eq([Pgbus::Testing::SignalRecord.new(name: "widgets_changed", payload: "id=1")])
    expect(seen).to eq([])
    expect(client).not_to have_received(:notify_signal)
  end

  it "runs the subscribers synchronously in inline mode" do
    Pgbus::Testing.inline!

    Pgbus.signal(:widgets_changed, "id=2")

    expect(seen).to eq(["id=2"])
    expect(Pgbus::Testing.signals.map(&:name)).to eq(["widgets_changed"])
    expect(client).not_to have_received(:notify_signal)
  end

  it "runs the subscribers at the commit of an open transaction in inline mode" do
    deferred = nil
    allow(Pgbus::OpenTransaction).to receive(:defer) do |&block|
      deferred = block
      :transaction
    end
    Pgbus::Testing.inline!

    Pgbus.signal(:widgets_changed)
    expect(seen).to eq([])

    deferred.call
    expect(seen).to eq([nil])
  end

  it "sends the NOTIFY in disabled mode" do
    Pgbus::Testing.disabled!
    allow(Pgbus::OpenTransaction).to receive(:defer).and_return(nil)

    Pgbus.signal(:widgets_changed)

    expect(client).to have_received(:notify_signal).with("widgets_changed", nil)
    expect(Pgbus::Testing.signals).to eq([])
  end

  it "clears the recorded signals" do
    Pgbus::Testing.fake!
    Pgbus.signal(:widgets_changed)

    Pgbus::Testing.clear_signals!

    expect(Pgbus::Testing.signals).to eq([])
  end
end
