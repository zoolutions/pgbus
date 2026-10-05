# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/time"

# Issue #486: the event consumer's claim loop gets the same read-ahead buffer
# as the worker (it has no prefetch_limit and no zombie detection).
RSpec.describe Pgbus::Process::Consumer, "#consume with read-ahead (issue #486)" do
  # Free slots move like a real pool's: post takes a slot and parks the
  # block; finish! runs the oldest parked block and frees its slot.
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
      def metadata = { mode: :threads, capacity: 3, busy: 0 }
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

  let(:mock_client) { build_mock_client }
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:registry) { instance_double(Pgbus::EventBus::Registry, queue_names_for_topics: %w[q_orders]) }
  let(:pool) { fake_pool_class.new(2) }
  let(:read_ahead) { 3 }
  let(:consumer) do
    described_class.new(topics: ["orders.#"], threads: 3, queue_names: %w[q_orders], read_ahead: read_ahead)
  end
  let(:wake_signal) { consumer.wake_signal }

  def messages(*ids)
    ids.map { |id| build_message_double(msg_id: id, message: '{"routing_key":"orders.created"}') }
  end

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
    allow(wake_signal).to receive(:wait)
  end

  after do
    Pgbus::VisibilityHeartbeat.reset!
    consumer.config.max_jobs_per_worker = nil
  end

  it "defaults read_ahead to config.read_ahead" do
    consumer.config.read_ahead = 7
    expect(described_class.new(topics: ["orders.#"]).read_ahead).to eq(7)
  ensure
    consumer.config.read_ahead = 0
  end

  context "with read_ahead: 0" do
    let(:read_ahead) { 0 }

    it "fetches the free slots and posts every message (pre-#486 behaviour)" do
      allow(mock_client).to receive(:read_batch).and_return(messages(1, 2))

      consumer.send(:consume)

      expect(mock_client).to have_received(:read_batch).with("q_orders", qty: 2)
      expect(pool.posted.size).to eq(2)
      expect(consumer.buffered).to eq(0)
      expect(wake_signal).not_to have_received(:wait)
    end
  end

  it "claims free slots plus read_ahead, posts what fits and buffers the rest" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))

    consumer.send(:consume)

    expect(mock_client).to have_received(:read_batch).with("q_orders", qty: 5)
    expect(pool.posted.size).to eq(2)
    expect(consumer.buffered).to eq(3)
  end

  it "posts from the buffer before reading, then reads only the deficit" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    allow(consumer).to receive(:handle_message)
    consumer.send(:consume)
    pool.finish!

    order = []
    allow(pool).to receive(:post).and_wrap_original do |original, &block|
      order << :post
      original.call(&block)
    end
    allow(mock_client).to receive(:read_batch) do |_queue, qty:|
      order << [:read, qty]
      []
    end

    consumer.send(:consume)

    expect(order).to eq([:post, [:read, 1]])
    expect(consumer.buffered).to eq(2)
  end

  it "releases a buffered message's hold before handle_message runs" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3), [])
    pool.free = 0
    consumer.send(:consume)
    pool.free = 1

    order = []
    allow(Pgbus::VisibilityHeartbeat).to receive(:release).and_wrap_original do |original, entry|
      order << :release
      original.call(entry)
    end
    allow(consumer).to receive(:handle_message) { order << :handle }

    consumer.send(:consume)
    pool.finish!

    expect(order).to eq(%i[release handle])
    expect(consumer).to have_received(:handle_message).with(anything, "q_orders")
  end

  it "does not wait on the wake signal when a slot freed while the read was in flight" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4))
    consumer.send(:consume)
    allow(mock_client).to receive(:read_batch) do
      pool.free = 1
      []
    end

    consumer.send(:consume)

    expect(pool.posted.size).to eq(3)
    expect(wake_signal).not_to have_received(:wait)
  end

  it "waits instead of spinning when the pool is full and the buffer is topped up" do
    allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3, 4, 5))
    consumer.send(:consume)

    consumer.send(:consume)

    expect(mock_client).to have_received(:read_batch).once
    expect(wake_signal).to have_received(:wait).once
  end

  describe "handing the buffer back" do
    def run_until_buffered
      allow(mock_client).to receive(:read_batch).and_return(messages(1, 2, 3), [])
      pool.free = 0
      allow(consumer).to receive(:start_notify_listener)
      runner = Thread.new { consumer.run }
      deadline = Time.now + 2
      sleep 0.01 until consumer.buffered == 3 || Time.now > deadline
      expect(consumer.buffered).to eq(3)
      runner
    end

    it "returns every buffered message (vt: 0) on graceful shutdown and exits" do
      runner = run_until_buffered

      consumer.graceful_shutdown

      expect(runner.join(2)).to eq(runner)
      [1, 2, 3].each do |id|
        expect(mock_client).to have_received(:set_visibility_timeout).with("q_orders", id, vt: 0, prefixed: true)
      end
      expect(pool.posted).to be_empty
      expect(consumer.buffered).to eq(0)
      expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(0)
    ensure
      runner&.kill
    end

    it "takes the same path when a recycle limit stops the loop" do
      runner = run_until_buffered

      consumer.config.max_jobs_per_worker = 1
      consumer.jobs_processed = 1

      expect(runner.join(2)).to eq(runner)
      expect(mock_client).to have_received(:set_visibility_timeout).with("q_orders", anything, vt: 0, prefixed: true)
                                                                   .exactly(3).times
      expect(consumer.buffered).to eq(0)
    ensure
      runner&.kill
    end
  end
end
