# frozen_string_literal: true

require "rails_helper"

# Regression for the durable-broadcast connection leak (Zazu incident:
# fan-out spec pool exhaustion, 2026-08-05).
#
# `Stream#current_open_transaction` (now Pgbus::OpenTransaction.current) used to probe via
# `ActiveRecord::Base.connection`, which on Rails 7.2+ takes a STICKY,
# executor-scoped lease:
#
#   - On a thread/fiber with no prior lease (the Coalescer's flush thread,
#     any non-executor background thread), the lease is never released —
#     one AR pool connection pinned forever per thread.
#   - Inside `connection_pool.with_connection`, the sticky flag makes
#     with_connection SKIP its release at block exit, so the caller's
#     connection leaks when the thread dies.
#
# A freshly-leased connection can never carry the caller's open transaction
# anyway (transactions are per-lease), so the probe must only inspect an
# EXISTING lease: `connection_pool.active_connection?` — no checkout.
RSpec.describe Pgbus::Streams::Stream do
  subject(:stream) { described_class.new("probe-leak", client: client, durable: true) }

  let(:client) do
    instance_double(
      Pgbus::Client,
      ensure_stream_queue: nil,
      send_stream_message: 1,
      stream_current_msg_id: 0,
      read_after: []
    )
  end

  let(:pool) { ActiveRecord::Base.connection_pool }

  it "does not lease an AR connection when the probing thread holds none" do
    leaked = Thread.new do
      Pgbus::OpenTransaction.current
      pool.active_connection?
    end.value

    expect(leaked).to be_falsey
  end

  it "returns nil (broadcast immediately) when the probing thread holds no connection" do
    expect(Thread.new { Pgbus::OpenTransaction.current }.value).to be_nil
  end

  it "does not defeat with_connection's release when broadcasting from a worker thread" do
    Thread.new do
      pool.with_connection { stream.broadcast("<turbo-stream>x</turbo-stream>") }
    end.join

    dead_leases = pool.connections.select { |c| c.in_use? && !c.owner.alive? }
    expect(dead_leases).to be_empty
  end

  it "still finds the open transaction when the probing thread holds a connection inside one" do
    probe = Thread.new do
      pool.with_connection do
        ActiveRecord::Base.transaction do
          seen = Pgbus::OpenTransaction.current
          { present: !seen.nil?, open: seen&.open? }
        end
      end
    end.value

    expect(probe).to eq(present: true, open: true)
  end
end
