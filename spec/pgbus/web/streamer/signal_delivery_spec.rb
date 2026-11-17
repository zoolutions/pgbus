# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::Streamer::SignalDelivery do
  subject(:delivery) { described_class.new(logger: Logger.new(IO::NULL)) }

  let(:seen) { Queue.new }

  before do
    Pgbus::Signals.reset!
    allow(Pgbus::Web::Streamer).to receive(:listen_signal)
    allow(Pgbus::ErrorReporter).to receive(:report)
  end

  after do
    delivery.stop
    Pgbus::Signals.reset!
  end

  def channel(name) = Pgbus::Signals.channel_for(name)

  it "starts no thread until a signal arrives" do
    expect(delivery.threads).to eq([])

    Pgbus.on_signal(:widgets) { |payload| seen << payload }
    delivery.signal(channel(:widgets), "id=1")

    expect(seen.pop(timeout: 2)).to eq("id=1")
    expect(delivery.threads.size).to eq(1)
  end

  it "runs the subscribers off the caller's thread" do
    caller_thread = Thread.current
    Pgbus.on_signal(:widgets) { seen << Thread.current }

    delivery.signal(channel(:widgets), nil)

    expect(seen.pop(timeout: 2)).not_to eq(caller_thread)
  end

  it "ignores a channel no subscriber registered, without starting a thread" do
    delivery.signal(channel(:unknown), nil)

    expect(delivery.threads).to eq([])
  end

  it "runs every on_gap on a gap" do
    Pgbus.on_signal(:widgets, on_gap: -> { seen << :gap }) { nil }

    delivery.gap

    expect(seen.pop(timeout: 2)).to eq(:gap)
  end

  it "runs on_gap once for gaps that arrive before the first one ran, and again for a later gap" do
    gate = Queue.new
    Pgbus.on_signal(:widgets, on_gap: lambda {
      gate.pop
      seen << :gap
    }) { nil }

    delivery.gap # starts running, parks on the gate
    sleep 0.05
    delivery.gap # queued
    delivery.gap # coalesced with the queued one
    2.times { gate << :go }

    expect([seen.pop(timeout: 2), seen.pop(timeout: 2)]).to eq(%i[gap gap])
    expect(seen.pop(timeout: 0.2)).to be_nil
  end

  it "starts no thread on a gap when no subscriber has an on_gap" do
    Pgbus.on_signal(:widgets) { nil }

    delivery.gap

    expect(delivery.threads).to eq([])
  end

  it "keeps delivering after a subscriber raises" do
    Pgbus.on_signal(:widgets) { |payload| raise "boom" if payload == "bad" }
    Pgbus.on_signal(:widgets) { |payload| seen << payload }

    delivery.signal(channel(:widgets), "bad")
    delivery.signal(channel(:widgets), "good")

    expect([seen.pop(timeout: 2), seen.pop(timeout: 2)]).to eq(%w[bad good])
    expect(Pgbus::ErrorReporter).to have_received(:report).once
  end

  it "joins its thread on stop and starts no new one after" do
    Pgbus.on_signal(:widgets) { |payload| seen << payload }
    delivery.signal(channel(:widgets), nil)
    seen.pop(timeout: 2)
    thread = delivery.threads.first

    delivery.stop

    expect(thread).not_to be_alive
    expect(delivery.threads).to eq([])
    delivery.signal(channel(:widgets), nil)
    expect(delivery.threads).to eq([])
  end
end
