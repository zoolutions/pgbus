# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/time"

# Issue #486: the read-ahead buffer a Worker/Consumer owns. It is only ever
# touched from the process's loop thread (push, shift, drain, return), so it
# carries no mutex; the heartbeat it registers holds with is the only shared
# state, and that synchronizes itself.
RSpec.describe Pgbus::Process::ClaimBuffer do
  subject(:buffer) { described_class.new(config: config) }

  let(:client) { instance_double(Pgbus::Client, set_visibility_timeout: nil) }
  let(:config) do
    Pgbus::Configuration.new.tap do |c|
      c.visibility_timeout = 30
      c.visibility_heartbeat = true
    end
  end

  def message(id, read_ct: 1)
    build_message_double(msg_id: id.to_s, read_ct: read_ct)
  end

  def pool(capacity)
    free = capacity
    double("pool").tap do |p|
      allow(p).to receive(:available_capacity) { free }
      allow(p).to receive(:post) { free -= 1 }
    end
  end

  after { Pgbus::VisibilityHeartbeat.reset! }

  describe "#push" do
    it "holds a logical-queue message under the executor's key (prefixed) and grows" do
      allow(Pgbus::VisibilityHeartbeat).to receive(:hold).and_call_original

      buffer.push("default", message(7), nil, client: client)

      expect(buffer.size).to eq(1)
      expect(Pgbus::VisibilityHeartbeat).to have_received(:hold)
        .with(client: client, queue_name: "default", msg_id: "7", prefixed: true, job_class: anything, config: config)
    end

    it "holds a priority sub-queue message under the physical name, unprefixed" do
      allow(Pgbus::VisibilityHeartbeat).to receive(:hold).and_call_original

      buffer.push("default", message(8), "pgbus_default_p1", client: client)

      expect(Pgbus::VisibilityHeartbeat).to have_received(:hold)
        .with(hash_including(queue_name: "pgbus_default_p1", prefixed: false))
    end
  end

  describe "#shift" do
    it "returns the oldest claim with its hold, and nil when empty" do
      first = message(1)
      buffer.push("default", first, nil, client: client)
      buffer.push("default", message(2), nil, client: client)

      claim = buffer.shift

      expect(claim.message).to be(first)
      expect(claim.queue_name).to eq("default")
      expect(claim.source_queue).to be_nil
      expect(claim.hold).to be_a(Pgbus::VisibilityHeartbeat::Entry)
      expect(buffer.size).to eq(1)
      buffer.shift
      expect(buffer.shift).to be_nil
      expect(buffer).to be_empty
    end
  end

  describe "#drain_into" do
    it "yields the oldest claims while the pool has a free slot, and returns how many" do
      3.times { |i| buffer.push("default", message(i), nil, client: client) }
      target = pool(2)
      yielded = []

      drained = buffer.drain_into(target) do |claim|
        yielded << claim.message.msg_id
        target.post
      end

      expect(drained).to eq(2)
      expect(yielded).to eq(%w[0 1])
      expect(buffer.size).to eq(1)
    end

    it "yields nothing when the pool is full" do
      buffer.push("default", message(1), nil, client: client)

      expect(buffer.drain_into(pool(0)) { raise "must not yield" }).to eq(0)
    end
  end

  describe "#deficit" do
    it "is free slots plus read-ahead minus what is already buffered" do
      2.times { |i| buffer.push("default", message(i), nil, client: client) }

      expect(buffer.deficit(free_slots: 1, read_ahead: 3)).to eq(2)
    end

    it "is capped by prefetch room" do
      expect(buffer.deficit(free_slots: 2, read_ahead: 3, prefetch_room: 4)).to eq(4)
    end

    it "never goes below zero" do
      expect(buffer.deficit(free_slots: 0, read_ahead: 0, prefetch_room: -2)).to eq(0)
      3.times { |i| buffer.push("default", message(i), nil, client: client) }
      expect(buffer.deficit(free_slots: 0, read_ahead: 1)).to eq(0)
    end

    it "equals the free slots when read-ahead is off and the buffer is empty (today's qty)" do
      expect(buffer.deficit(free_slots: 5, read_ahead: 0)).to eq(5)
    end
  end

  describe "#return_all!" do
    it "makes every buffered message visible again, drops every hold and empties the buffer" do
      buffer.push("default", message(1), nil, client: client)
      buffer.push("default", message(2), "pgbus_default_p0", client: client)

      returned = buffer.return_all!(client: client)

      expect(returned).to eq(2)
      expect(client).to have_received(:set_visibility_timeout).with("default", 1, vt: 0, prefixed: true)
      expect(client).to have_received(:set_visibility_timeout).with("pgbus_default_p0", 2, vt: 0, prefixed: false)
      expect(buffer).to be_empty
      expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(0)
    end

    # A heartbeat tick racing the return must find no hold to extend, or it
    # would re-hide the message for a full visibility_timeout.
    it "drops the hold before it writes vt: 0" do
      buffer.push("default", message(1), nil, client: client)
      held_at_write = nil
      allow(client).to receive(:set_visibility_timeout) { held_at_write = Pgbus::VisibilityHeartbeat.tracked_count }

      buffer.return_all!(client: client)

      expect(held_at_write).to eq(0)
    end

    it "logs a failed return and keeps going" do
      buffer.push("default", message(1), nil, client: client)
      buffer.push("default", message(2), nil, client: client)
      allow(client).to receive(:set_visibility_timeout).with("default", 1, vt: 0, prefixed: true)
                                                       .and_raise(StandardError, "db gone")
      allow(Pgbus.logger).to receive(:warn)

      expect(buffer.return_all!(client: client)).to eq(2)
      expect(client).to have_received(:set_visibility_timeout).with("default", 2, vt: 0, prefixed: true)
      expect(Pgbus.logger).to have_received(:warn)
      expect(Pgbus::VisibilityHeartbeat.tracked_count).to eq(0)
    end

    it "returns 0 and touches nothing when empty" do
      expect(buffer.return_all!(client: client)).to eq(0)
      expect(client).not_to have_received(:set_visibility_timeout)
    end
  end

  context "when the visibility heartbeat is disabled" do
    before { config.visibility_heartbeat = false }

    it "buffers without a hold and still returns the message" do
      buffer.push("default", message(1), nil, client: client)

      expect(buffer.shift.hold).to be_nil
      buffer.push("default", message(2), nil, client: client)
      expect(buffer.return_all!(client: client)).to eq(1)
    end
  end
end
