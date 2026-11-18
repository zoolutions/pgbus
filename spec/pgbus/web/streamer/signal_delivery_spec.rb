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
      seen << :running
      gate.pop
      seen << :gap
    }) { nil }

    delivery.gap # starts running, parks on the gate
    expect(seen.pop(timeout: 2)).to eq(:running)
    delivery.gap # queued
    delivery.gap # coalesced with the queued one
    2.times { gate << :go }

    expect([seen.pop(timeout: 2), seen.pop(timeout: 2), seen.pop(timeout: 2)]).to eq(%i[gap running gap])
    expect(seen.pop(timeout: 0.2)).to be_nil
  end

  it "drops a signal past the backlog limit and reports a gap instead" do
    stub_const("#{described_class}::BACKLOG_LIMIT", 2)
    gate = Queue.new
    parked = Queue.new
    Pgbus.on_signal(:widgets, on_gap: -> { seen << :gap }) do |payload|
      if payload == "slow"
        parked << true
        gate.pop
      end
      seen << payload
    end
    delivery.signal(channel(:widgets), "slow")
    parked.pop(timeout: 2) # the thread took "slow" and parks on the gate

    delivery.signal(channel(:widgets), "a")
    delivery.signal(channel(:widgets), "b") # backlog: a + b
    delivery.signal(channel(:widgets), "dropped")
    gate << :go

    expect(Array.new(4) { seen.pop(timeout: 2) }).to eq(["slow", "a", "b", :gap])
  end

  it "warns once per overflow episode, not once per dropped signal" do
    stub_const("#{described_class}::BACKLOG_LIMIT", 1)
    logger = instance_double(Logger, warn: nil, error: nil)
    quiet = described_class.new(logger: logger)
    gate = Queue.new
    parked = Queue.new
    Pgbus.on_signal(:widgets, on_gap: -> { seen << :gap }) do |payload|
      if payload == "slow"
        parked << true
        gate.pop
      end
    end
    quiet.signal(channel(:widgets), "slow")
    parked.pop(timeout: 2)
    quiet.signal(channel(:widgets), "queued")

    3.times { quiet.signal(channel(:widgets), "dropped") }
    gate << :go

    expect(seen.pop(timeout: 2)).to eq(:gap)
    expect(logger).to have_received(:warn).once
  ensure
    quiet&.stop
  end

  it "lets a later gap through when the first could not be queued" do
    Pgbus.on_signal(:widgets, on_gap: -> { seen << :gap }) { nil }
    allow(Thread).to receive(:new).and_raise(ThreadError, "can't create Thread")

    expect { delivery.gap }.to raise_error(ThreadError)
    allow(Thread).to receive(:new).and_call_original
    delivery.gap

    expect(seen.pop(timeout: 2)).to eq(:gap)
  end

  it "drops what is still queued at stop instead of running it" do
    gate = Queue.new
    parked = Queue.new
    Pgbus.on_signal(:widgets) do |payload|
      if payload == "slow"
        parked << true
        gate.pop
      end
      seen << payload
    end
    delivery.signal(channel(:widgets), "slow")
    parked.pop(timeout: 2)
    delivery.signal(channel(:widgets), "queued")

    stopper = Thread.new { delivery.stop } # rubocop:disable ThreadSafety/NewThread -- joined below
    # stop has cleared the queue once it marked itself stopped (both under one lock)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2
    sleep 0.01 until delivery.instance_variable_get(:@stopped) || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    gate << :go
    stopper.join(3)

    expect(seen.pop(timeout: 1)).to eq("slow")
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
