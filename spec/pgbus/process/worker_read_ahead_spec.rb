# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/time"

# Issue #486: the worker claims up to `read_ahead` messages beyond its free
# threads, holds them in a ClaimBuffer (heartbeated), feeds them to the pool as
# slots free, and hands them back to the queue on drain.
RSpec.describe Pgbus::Process::Worker, "#claim_and_execute with read-ahead (issue #486)" do
  # A pool whose free slots move like the real ones: post takes a slot and
  # parks the block; finish! runs the oldest parked block and frees its slot.
  let(:fake_pool_class) do
    Class.new do
      attr_reader :posted
      attr_accessor :free

      def initialize(free)
        @free = free
        @posted = []
      end

      def available_capacity = @free
      def idle? = @free.positive?
      def quiesced? = true
      def metadata = { mode: :threads, capacity: 5, busy: 0 }
      def shutdown = true
      def kill = true
      def wait_for_termination(*) = true

      def post(&block)
        @free -= 1
        @posted << block
      end

      def finish!
        @posted.shift.call
        @free += 1
      end
    end
  end

  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:executor) { instance_double(Pgbus::ActiveJob::Executor, execute: :success) }
  let(:circuit_breaker) { instance_double(Pgbus::CircuitBreaker, paused?: false, record_success: nil, record_failure: nil) }
  let(:pool) { fake_pool_class.new(2) }
  let(:read_ahead) { 3 }
  let(:worker) { described_class.new(queues: %w[default], threads: 5, read_ahead: read_ahead) }
  let(:wake_signal) { worker.wake_signal }

  def messages(*ids)
    ids.map { |id| build_message_double(msg_id: id) }
  end

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
    allow(Pgbus::CircuitBreaker).to receive(:new).and_return(circuit_breaker)
    allow(wake_signal).to receive(:wait)
  end

  after do
    Pgbus::VisibilityHeartbeat.reset!
    worker.config.prefetch_limit = nil
    worker.config.max_jobs_per_worker = nil
    Pgbus.stopping = false
  end

  it "defaults read_ahead to config.read_ahead" do
    worker.config.read_ahead = 7
    expect(described_class.new(queues: %w[default]).read_ahead).to eq(7)
  ensure
    worker.config.read_ahead = 0
  end

  context "with read_ahead: 0" do
    let(:read_ahead) { 0 }

    it "fetches the free slots and posts every message (pre-#486 behaviour)" do
      allow(mock_client).to receive(:read_batch).and_return(messages(1, 2))

      worker.send(:claim_and_execute)

      expect(mock_client).to have_received(:read_batch).with("default", qty: 2)
      expect(pool.posted.size).to eq(2)
      expect(worker.stats).to include(in_flight: 2, buffered: 0)
      expect(wake_signal).not_to have_received(:wait)
    end
  end

  it "claims free slots plus read_ahead, posts what fits and buffers the rest" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))

    worker.send(:claim_and_execute)

    expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
    expect(pool.posted.size).to eq(2)
    expect(worker.stats).to include(in_flight: 5, buffered: 3)
    # Every claim is held until its run starts: 3 buffered + 2 posted but not yet started.
    expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(5)
  end

  it "posts from the buffer before reading, then reads only the deficit" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    worker.send(:claim_and_execute)
    pool.finish! # one job done: 1 free slot, 3 buffered

    order = []
    allow(pool).to receive(:post).and_wrap_original do |original, &block|
      order << :post
      original.call(&block)
    end
    allow(mock_client).to receive(:read_batch) do |_queue, qty:|
      order << [:read, qty]
      []
    end

    worker.send(:claim_and_execute)

    # deficit = 0 free + 3 read_ahead - 2 still buffered
    expect(order).to eq([:post, [:read, 1]])
    expect(worker.stats).to include(in_flight: 4, buffered: 2)
  end

  it "counts buffered messages against prefetch_limit" do
    worker.config.prefetch_limit = 4
    allow(mock_client).to receive(:read_batch).and_return([])

    worker.send(:claim_and_execute)

    expect(mock_client).to have_received(:read_batch).with("default", qty: 4)
  end

  it "drains the buffer even when prefetch_limit leaves no room to read" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    worker.send(:claim_and_execute)
    pool.finish!
    worker.config.prefetch_limit = 4 # 4 in flight: no room

    worker.send(:claim_and_execute)

    expect(mock_client).to have_received(:read_batch).once
    expect(pool.posted.size).to eq(2)
    expect(worker.stats).to include(buffered: 2)
  end

  it "releases a buffered message's hold before the executor runs it" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3), [])
    pool.free = 0
    worker.send(:claim_and_execute) # all 3 buffered
    pool.free = 1

    order = []
    allow(Pgbus::VisibilityHeartbeat).to receive(:release).and_wrap_original do |original, entry|
      order << :release
      original.call(entry)
    end
    allow(executor).to receive(:execute) do
      order << :execute
      :success
    end

    worker.send(:claim_and_execute)
    pool.finish!

    expect(order).to eq(%i[release execute])
    expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(2)
  end

  it "does not wait on the wake signal when a slot freed while the read was in flight" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4)) # 2 posted, 2 buffered
    worker.send(:claim_and_execute)
    allow(mock_client).to receive(:read_batch) do
      pool.free = 1 # a job finished during the round trip
      []
    end

    worker.send(:claim_and_execute)

    expect(pool.posted.size).to eq(3)
    expect(wake_signal).not_to have_received(:wait)
  end

  it "waits instead of spinning when the pool is full and the buffer is topped up" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    worker.send(:claim_and_execute)

    worker.send(:claim_and_execute)

    expect(mock_client).to have_received(:read_batch).once
    expect(wake_signal).to have_received(:wait).once
  end

  it "runs zombie detection once per claimed message, at claim time" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    allow(worker).to receive(:detect_zombie)

    worker.send(:claim_and_execute)

    expect(worker).to have_received(:detect_zombie).exactly(5).times
  end

  describe "handing the buffer back" do
    def run_until_buffered
      allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3), [])
      pool.free = 0
      allow(worker).to receive(:start_notify_listener)
      runner = Thread.new { worker.run }
      deadline = Time.now + 2
      sleep 0.01 until worker.stats[:buffered] == 3 || Time.now > deadline
      expect(worker.stats[:buffered]).to eq(3)
      runner
    end

    it "returns every buffered message (vt: 0) on graceful shutdown and exits" do
      runner = run_until_buffered

      worker.graceful_shutdown

      expect(runner.join(2)).to eq(runner)
      [1, 2, 3].each do |id|
        expect(mock_client).to have_received(:set_visibility_timeout).with("default", id, vt: 0, prefixed: true)
      end
      expect(pool.posted).to be_empty
      expect(worker.stats).to include(in_flight: 0, buffered: 0)
      expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(0)
    ensure
      runner&.kill
    end

    it "hands the buffer back when the worker is paused" do
      runner = run_until_buffered

      worker.lifecycle.transition_to(:paused)
      deadline = Time.now + 2
      sleep 0.01 until worker.stats[:in_flight].zero? || Time.now > deadline

      expect(mock_client).to have_received(:set_visibility_timeout).with("default", anything, vt: 0, prefixed: true)
                                                                   .exactly(3).times
      expect(worker.stats).to include(in_flight: 0, buffered: 0)
      worker.graceful_shutdown
      expect(runner.join(2)).to eq(runner)
    ensure
      runner&.kill
    end

    it "takes the same path when a recycle limit starts the drain" do
      runner = run_until_buffered

      worker.config.max_jobs_per_worker = 1
      worker.jobs_processed = 1

      expect(runner.join(2)).to eq(runner)
      expect(mock_client).to have_received(:set_visibility_timeout).with(anything, anything, vt: 0, prefixed: true)
                                                                   .exactly(3).times
      expect(worker.stats).to include(in_flight: 0, buffered: 0)
    ensure
      runner&.kill
    end
  end
end
