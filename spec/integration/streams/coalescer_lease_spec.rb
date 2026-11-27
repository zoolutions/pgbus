# frozen_string_literal: true

require_relative "../../integration_helper"

# Issue #694: a coalesced broadcast flushes on a Concurrent IO thread that no
# Rails executor wraps. The first durable broadcast of a stream records it in
# the StreamQueue registry there; a bare `connection` leased a BusRecord
# connection that thread kept for its life. Under a pinned pool (Rails 7.2+
# transactional tests) that lease pointed at the test's pinned connection and
# survived the test boundary, so two threads drove one PG::Connection.
RSpec.describe "Streams: the coalescer flush returns its connection lease", :integration do
  let(:config) { Pgbus.configuration }
  let(:pool) { Pgbus::BusRecord.connection_pool }
  # ensure_stream_queue memoizes per process: a fresh name per example, so the
  # flush does reach StreamQueue.record!.
  let(:stream_name) { "lease_#{SecureRandom.hex(4)}" }
  let(:queue_name) { config.queue_name(stream_name) }
  let(:flushes) { Queue.new }

  # The coalescer's own timer (a Concurrent::ScheduledTask on the global IO
  # executor), with a probe after the flush: whether that thread still holds
  # a lease, and on which connection.
  let(:probing_timer) do
    probe_pool = pool
    done = flushes
    timer = Pgbus::Streams::Coalescer::ScheduledTaskScheduler.new
    Object.new.tap do |scheduler|
      scheduler.define_singleton_method(:schedule) do |delay, &flush|
        timer.schedule(delay) do
          flush.call
          done << [:flushed, probe_pool.active_connection?]
        rescue StandardError => e
          done << [:raised, e]
        end
      end
    end
  end

  before do
    # The probe must cover table_exists?'s first lease too.
    Pgbus::StreamQueue.remove_instance_variable(:@table_exists) if Pgbus::StreamQueue.instance_variable_defined?(:@table_exists)
    Pgbus::StreamQueue.reset_cache!
    config.streams_coalesce_scheduler = probing_timer
    pool.release_connection
  end

  after { config.streams_coalesce_scheduler = nil }

  def broadcast_coalesced
    Pgbus.stream(stream_name).broadcast("<turbo-stream></turbo-stream>", target: "t", coalesce: 1, durable: true)
  end

  def flush_result
    outcome, value = flushes.pop(timeout: 10)
    raise "the coalescer never flushed" if outcome.nil?
    raise value if outcome == :raised

    value
  end

  def registered?
    pool.with_connection do |conn|
      conn.select_value("SELECT 1 FROM pgbus_stream_queues WHERE queue_name = #{conn.quote(queue_name)}") == 1
    end
  end

  it "holds no lease on the flush thread after a first broadcast, and busy returns to its prior value" do
    busy_before = pool.stat[:busy]

    broadcast_coalesced
    lease = flush_result

    expect(lease).to be_falsey, "the flush thread kept a BusRecord lease on #{lease.inspect}"
    expect(pool.stat[:busy]).to eq(busy_before)
    expect(registered?).to be(true)
  end

  context "with the pool pinned, as a Rails 7.2+ transactional test pins it" do
    before { skip "pin_connection! is Rails 7.2+" unless pool.respond_to?(:pin_connection!) }

    it "leaves the flush thread no lease on the pinned connection, so the next pin shares it with no thread" do
      pool.pin_connection!(true)
      begin
        pinned = pool.with_connection(&:itself)
        broadcast_coalesced
        lease = flush_result

        expect(lease).not_to be(pinned), "the flush thread kept the test's pinned connection"
        expect(lease).to be_falsey
        expect(registered?).to be(true)
      ensure
        pool.unpin_connection!
      end
    end
  end

  describe "under Pgbus::Testing.fake!" do
    before do
      require "pgbus/testing"
      config.streams_coalesce_scheduler = nil
      Pgbus::Testing.fake!
    end

    after { Pgbus::Testing.disabled! }

    it "records the stream on the calling thread before broadcast returns" do
      recorded_on = []
      allow(Pgbus::StreamQueue).to receive(:record!).and_wrap_original do |original, name, &on_durable|
        recorded_on << [name, Thread.current]
        original.call(name, &on_durable)
      end

      broadcast_coalesced

      expect(recorded_on).to eq([[queue_name, Thread.current]])
      expect(registered?).to be(true)
    end

    it "restores the timer on disabled!" do
      Pgbus::Testing.disabled!

      expect(config.streams_coalesce_scheduler).to be_nil
    end
  end
end
