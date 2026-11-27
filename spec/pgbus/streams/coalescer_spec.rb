# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Streams::Coalescer do
  subject(:coalescer) { described_class.new(scheduler: scheduler, flush: flush) }

  # A controllable scheduler: records scheduled flushes and runs them on
  # demand instead of after a real wall-clock delay. Mirrors the surface
  # the Coalescer needs: schedule(delay_seconds) { ... } -> handle.
  let(:scheduler) do
    Class.new do
      attr_reader :scheduled

      def initialize = (@scheduled = [])

      def schedule(_delay_seconds, &block)
        @scheduled << block
        @scheduled.size # a simple handle
      end

      def run_all
        pending = @scheduled.dup
        @scheduled.clear
        pending.each(&:call)
      end
    end.new
  end

  # Records the final flushed broadcasts.
  let(:flushed) { [] }
  let(:flush) do
    lambda do |stream_name:, target:, payload:, opts:|
      flushed << { stream: stream_name, target: target, payload: payload, opts: opts }
    end
  end

  def submit(stream: "chat", target: "cursor", payload: "x", window_ms: 50, **opts)
    coalescer.submit(stream_name: stream, target: target, payload: payload, window_ms: window_ms, opts: opts)
  end

  it "schedules a single flush for the first submit of a (stream, target)" do
    submit(payload: "a")
    expect(scheduler.scheduled.size).to eq(1)
  end

  it "keeps only the latest payload within the window (last-write-wins)" do
    submit(payload: "a")
    submit(payload: "b")
    submit(payload: "c")

    scheduler.run_all

    expect(flushed.size).to eq(1)
    expect(flushed.first[:payload]).to eq("c")
  end

  it "does NOT schedule a second flush while one is pending for the same key" do
    submit(payload: "a")
    submit(payload: "b")
    expect(scheduler.scheduled.size).to eq(1)
  end

  it "schedules independent flushes for different targets on the same stream" do
    submit(target: "cursor", payload: "a")
    submit(target: "typing", payload: "b")
    expect(scheduler.scheduled.size).to eq(2)

    scheduler.run_all
    expect(flushed.map { |f| [f[:target], f[:payload]] }).to contain_exactly(%w[cursor a], %w[typing b])
  end

  it "schedules independent flushes for the same target on different streams" do
    submit(stream: "room1", payload: "a")
    submit(stream: "room2", payload: "b")
    expect(scheduler.scheduled.size).to eq(2)
  end

  it "allows a new window after the previous one flushed" do
    submit(payload: "a")
    scheduler.run_all
    expect(flushed.map { |f| f[:payload] }).to eq(["a"])

    submit(payload: "b")
    expect(scheduler.scheduled.size).to eq(1)
    scheduler.run_all
    expect(flushed.map { |f| f[:payload] }).to eq(%w[a b])
  end

  it "carries the broadcast opts (visible_to/exclude/durable/event) through to flush" do
    submit(payload: "a", visible_to: :admins, exclude: "c1", event: "reactive")
    scheduler.run_all
    expect(flushed.first[:opts]).to eq(visible_to: :admins, exclude: "c1", event: "reactive")
  end

  it "uses the latest submit's opts on a coalesced flush" do
    submit(payload: "a", event: "first")
    submit(payload: "b", event: "second")
    scheduler.run_all
    expect(flushed.first[:payload]).to eq("b")
    expect(flushed.first[:opts]).to eq(event: "second")
  end

  it "passes the window (ms -> seconds) to the scheduler" do
    allow(scheduler).to receive(:schedule).and_call_original
    submit(window_ms: 250)
    expect(scheduler).to have_received(:schedule).with(0.25)
  end

  it "does not flush a key that received no submit" do
    submit(target: "cursor", payload: "a")
    scheduler.run_all
    expect(flushed.map { |f| f[:target] }).to eq(["cursor"])
  end

  # Issue #391: the flush runs on the scheduler's thread — a raise there
  # never reaches any caller, so a swallowed error is invisible to APM by
  # construction. Errors must route through ErrorReporter (same reasoning
  # as the StreamApp report-don't-log fix in #352).
  describe "flush errors" do
    let(:boom) { StandardError.new("payload string too long") }
    let(:flush) { ->(**) { raise boom } }

    it "routes a raising flush through ErrorReporter with stream/target context" do
      allow(Pgbus::ErrorReporter).to receive(:report)

      submit(stream: "chat", target: "cursor", payload: "a")
      scheduler.run_all

      expect(Pgbus::ErrorReporter).to have_received(:report).with(
        boom, hash_including(
                component: "streams.coalescer", stream: "chat", target: "cursor"
              )
      )
    end

    it "does not re-raise out of the flush" do
      allow(Pgbus::ErrorReporter).to receive(:report)
      submit(payload: "a")
      expect { scheduler.run_all }.not_to raise_error
    end

    it "keeps the key usable for the next window after a failed flush" do
      allow(Pgbus::ErrorReporter).to receive(:report)
      submit(payload: "a")
      scheduler.run_all

      # A new submit after the failed flush schedules a fresh window.
      submit(payload: "b")
      expect(scheduler.scheduled.size).to eq(1)
    end
  end

  # Issue #694: the default scheduler reads config.streams_coalesce_scheduler
  # at every schedule, so a scheduler set after the coalescer was built (an
  # initializer, Pgbus::Testing) still takes effect.
  describe "the configured scheduler" do
    subject(:coalescer) { described_class.new(flush: flush) }

    after { Pgbus.configuration.streams_coalesce_scheduler = nil }

    it "flushes on the calling thread, before submit returns, with InlineScheduler" do
      Pgbus.configuration.streams_coalesce_scheduler = described_class::InlineScheduler.new
      flush_threads = []
      allow(flush).to receive(:call).and_wrap_original do |original, **kwargs|
        flush_threads << Thread.current
        original.call(**kwargs)
      end

      submit(payload: "a")

      expect(flushed.map { |f| f[:payload] }).to eq(["a"])
      expect(flush_threads).to eq([Thread.current])
    end

    it "uses a scheduler configured after the coalescer was built" do
      coalescer # built before the setting changes
      custom = scheduler
      Pgbus.configuration.streams_coalesce_scheduler = custom

      submit(payload: "a")

      expect(custom.scheduled.size).to eq(1)
    end

    it "falls back to a Concurrent::ScheduledTask timer when none is configured" do
      allow(Concurrent::ScheduledTask).to receive(:execute)

      submit(payload: "a", window_ms: 250)

      expect(Concurrent::ScheduledTask).to have_received(:execute).with(0.25)
      expect(flushed).to be_empty
    end
  end

  describe Pgbus::Streams::Coalescer::InlineScheduler do
    it "runs the block at once and ignores the delay" do
      ran_on = nil
      described_class.new.schedule(5) { ran_on = Thread.current }

      expect(ran_on).to eq(Thread.current)
    end
  end
end
