# frozen_string_literal: true

require_relative "../integration_helper"

# Connection-failure recovery against a REAL PostgreSQL. Everything above the
# socket is unit-tested with fake error classes (client_spec.rb stubs a fake
# PGMQ::Errors::ConnectionError); nothing proves the real recovery path end to
# end. Here we kill backends with pg_terminate_backend and assert:
#
#   * Client#with_stale_connection_retry recovers a killed pooled socket and
#     loses no enqueued message (pgmq-ruby auto_reconnect + our backed-off retry).
#   * A ConnectionError whose text misses STALE_CONNECTION_PATTERNS propagates
#     un-retried (a saturated pool / genuinely-broken conn must not be retried).
#   * NotifyListener#reconnect! rebuilds its dedicated LISTEN connection and
#     re-LISTENs every channel after its backend is killed.
#   * A pool-timeout ConnectionError surfaces (deliberately not retried).
#
# All queue access goes through Pgbus::Client. The AR connection is used only
# as an out-of-band control channel to run pg_terminate_backend / NOTIFY, since
# killing your own backend from the same connection is not a stable test.
RSpec.describe "Connection failure recovery (integration)", :integration do
  let(:client) { Pgbus.client }

  # Out-of-band admin connection: the AR pool's backend, used to terminate
  # OTHER backends and to NOTIFY. Never the target of a kill itself.
  def admin_conn
    ActiveRecord::Base.connection
  end

  # Poll `block` until it returns truthy or the deadline passes. Returns the
  # last value (truthy on success, falsey on timeout) so callers assert on it.
  # No bare sleeps: every wait in this file is a bounded poll with a deadline.
  def poll_until(timeout: 5.0, interval: 0.05)
    deadline = monotonic_now + timeout
    loop do
      value = yield
      return value if value
      return value if monotonic_now >= deadline

      sleep interval
    end
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # Terminate every backend on the current database EXCEPT the AR admin backend
  # issuing the command. This kills whatever pooled sockets pgbus's client and
  # any listener own, forcing the recovery paths to fire. Returns the count.
  def terminate_other_backends
    admin_conn.select_value(<<~SQL).to_i
      SELECT count(pg_terminate_backend(pid))
      FROM pg_stat_activity
      WHERE datname = current_database()
        AND pid <> pg_backend_pid()
        AND backend_type = 'client backend'
    SQL
  end

  describe "stale client connection recovery" do
    before { client.ensure_queue("conn_fail") }

    it "recovers a killed pooled socket and loses no message" do
      # Enqueue through the client so pgmq-ruby opens (and pools) a real backend.
      # pgmq-ruby returns the msg_id as a String; normalize for the later compare.
      msg_id = client.send_message("conn_fail", { "n" => 1 }).to_i
      expect(msg_id).to be > 0

      # Kill every non-admin backend — including the pooled socket the client
      # just used. The next checkout hits a dead connection.
      expect(terminate_other_backends).to be >= 1

      # pgmq-ruby's auto_reconnect recovers on the next checkout, and
      # with_stale_connection_retry gives a transient window a second, backed-off
      # attempt. Poll the read: the message survives the backend kill (it was
      # committed before the kill) and becomes readable once the socket recovers.
      message = poll_until(timeout: 10.0) { client.read_message("conn_fail", vt: 0) }

      expect(message).not_to be_nil
      expect(JSON.parse(message.message)["n"]).to eq(1)
      expect(message.msg_id.to_i).to eq(msg_id)
    end

    it "keeps serving reads after a mid-stream backend kill" do
      3.times { |i| client.send_message("conn_fail", { "n" => i }) }

      # Drain one, kill the backend, then keep draining. The retry path must
      # bridge the kill so no message is dropped and none is duplicated.
      seen = []
      first = client.read_message("conn_fail", vt: 0)
      seen << JSON.parse(first.message)["n"]
      client.delete_message("conn_fail", first.msg_id.to_i)

      terminate_other_backends

      poll_until(timeout: 10.0) do
        msg = client.read_message("conn_fail", vt: 0)
        next false unless msg

        seen << JSON.parse(msg.message)["n"]
        client.delete_message("conn_fail", msg.msg_id.to_i)
        seen.size == 3
      end

      expect(seen.sort).to eq([0, 1, 2])
    end
  end

  # Issue #645: a reset Thread#raise interrupted mid-poll leaves the pooled
  # PG::Connection in CONNECTION_STARTED. pgmq-ruby's checkout health check
  # resets only a finished or CONNECTION_BAD connection, so it keeps handing
  # this one out and libpq refuses every send ("no connection to the server").
  # reset_start (PQresetStart) leaves exactly that state: it closes the socket
  # and starts a reconnect that only advances when polled.
  describe "a pooled connection left mid-reset (CONNECTION_STARTED)" do
    # One slot, so the connection staged below is the one send_message gets.
    let(:one_slot_config) do
      Pgbus.configuration.dup.tap { |c| c.pool_size = 1 }
    end
    let(:one_slot_client) { Pgbus::Client.new(one_slot_config) }
    let(:warnings) { [] }

    # The pooled connection's status after staging. The queue is created
    # first, so send_message is the only call that meets the staged one.
    def stage_mid_reset
      one_slot_client.ensure_queue("conn_mid_reset")
      one_slot_client.pgmq.with_connection(&:reset_start)
      one_slot_client.pgmq.with_connection(&:status)
    end

    before do
      allow(Pgbus.logger).to receive(:warn).and_wrap_original do |original, *args, &blk|
        warnings << blk.call if blk
        original.call(*args, &blk)
      end
    end

    after { one_slot_client.close }

    it "recovers on the last stale retry and the message is enqueued" do
      expect(stage_mid_reset).to eq(PG::CONNECTION_STARTED)

      msg_id = one_slot_client.send_message("conn_mid_reset", { "n" => 645 }).to_i

      expect(msg_id).to be > 0
      message = one_slot_client.read_message("conn_mid_reset", vt: 0)
      expect(message.msg_id.to_i).to eq(msg_id)
      expect(JSON.parse(message.message)["n"]).to eq(645)
      expect(warnings.grep(/Dropping pooled connections/).size).to eq(1)
    end

    it "keeps failing when the job pool is not reloaded (the reload, not the backoff, recovers)" do
      expect(stage_mid_reset).to eq(PG::CONNECTION_STARTED)
      allow(one_slot_client.pgmq).to receive(:reload)

      expect { one_slot_client.send_message("conn_mid_reset", { "n" => 645 }) }
        .to raise_error(PGMQ::Errors::ConnectionError, /no connection to the server/)
      expect(one_slot_client.pgmq).to have_received(:reload).once
    end
  end

  describe "non-stale connection errors" do
    before { client.ensure_queue("conn_fail") }

    it "propagates a ConnectionError whose text misses the stale patterns" do
      # Only the message text differs from a real stale error — the class is the
      # same PGMQ::Errors::ConnectionError. stale_connection_error? matches on
      # substrings (client.rb STALE_CONNECTION_PATTERNS); this text matches none,
      # so with_stale_connection_retry must NOT retry — it propagates on attempt 1.
      boom = PGMQ::Errors::ConnectionError.new("permission denied for schema pgmq")
      call_count = 0
      allow(client.pgmq).to receive(:read) do
        call_count += 1
        raise boom
      end

      expect { client.read_message("conn_fail", vt: 0) }
        .to raise_error(PGMQ::Errors::ConnectionError, /permission denied/)
      expect(call_count).to eq(1) # no retry
    end

    it "retries a ConnectionError whose text matches a stale pattern" do
      # Same class, but the text matches STALE_CONNECTION_PATTERNS ("connection
      # is closed"): fail once, succeed on the retry. Proves the pattern list —
      # not the class — gates retry, and that recovery re-invokes the block.
      real_read = client.pgmq.method(:read)
      calls = 0
      allow(client.pgmq).to receive(:read) do |*args, **kwargs|
        calls += 1
        raise PGMQ::Errors::ConnectionError, "connection is closed" if calls == 1

        real_read.call(*args, **kwargs)
      end

      client.send_message("conn_fail", { "n" => 42 })
      message = client.read_message("conn_fail", vt: 0)

      expect(calls).to eq(2) # one failure, one successful retry
      expect(JSON.parse(message.message)["n"]).to eq(42)
    end
  end

  describe "NotifyListener reconnect and re-LISTEN" do
    let(:physical_queue) { Pgbus.configuration.queue_name("notify_reconnect") }
    let(:channel) { "pgmq.q_#{physical_queue}.INSERT" }

    before { client.ensure_queue("notify_reconnect") }

    it "re-LISTENs every channel after its backend is killed" do
      wakes = Concurrent::AtomicFixnum.new(0)
      listener = Pgbus::Process::NotifyListener.new(
        physical_queues: [physical_queue],
        on_wake: ->(_channel) { wakes.increment },
        connection_options: Pgbus.configuration.worker_notify_connection_options,
        health_check_ms: 200
      )
      listener.start

      begin
        # Wait until the listener has actually issued its LISTEN before we kill
        # anything, so the reconnect (not the initial LISTEN) is what re-subscribes.
        listening = poll_until(timeout: 5.0) { listener.listening_to.include?(channel) }
        expect(listening).to be_truthy

        # Kill the listener's dedicated backend. wait_once rescues PG::Error/IOError
        # and calls reconnect!, which rebuilds the connection and re-LISTENs the
        # channel set. Killing all non-admin backends is a superset of its backend.
        terminate_other_backends

        # After reconnect the channel must be re-registered. Poll listening_to,
        # which reflects the set the listener actually LISTENed on the new conn.
        relistened = poll_until(timeout: 8.0) { listener.listening_to.include?(channel) }
        expect(relistened).to be_truthy

        # Prove delivery works on the rebuilt connection: NOTIFY from the admin
        # channel and assert on_wake fires. A stale (non-re-LISTENed) connection
        # would never receive this.
        before = wakes.value
        poll_until(timeout: 8.0) do
          admin_conn.execute(%(NOTIFY "#{channel}"))
          wakes.value > before
        end

        expect(wakes.value).to be > before
      ensure
        listener.stop
      end
    end
  end

  describe "connection pool exhaustion" do
    # A dedicated client whose pool holds exactly one connection with a short
    # checkout timeout, so a second concurrent op deterministically times out.
    let(:small_pool_config) do
      Pgbus.configuration.dup.tap do |c|
        c.pool_size = 1
        c.pool_timeout = 1
      end
    end
    let(:small_pool_client) { Pgbus::Client.new(small_pool_config) }

    it "surfaces a pool-timeout ConnectionError instead of retrying" do
      small_pool_client.ensure_queue("pool_exhaust")

      holding = Concurrent::CountDownLatch.new(1)
      release = Concurrent::CountDownLatch.new(1)

      # Hold the pool's only connection open in a background thread.
      holder = Thread.new do
        small_pool_client.pgmq.with_connection do |conn|
          conn.exec("SELECT 1")
          holding.count_down
          release.wait(5)
        end
      end

      begin
        expect(holding.wait(5)).to be(true)

        # With the sole connection held, any client op must wait pool_timeout
        # seconds for a checkout, then raise a pool-timeout ConnectionError.
        # POOL_TIMEOUT_MARKER is deliberately NOT in STALE_CONNECTION_PATTERNS,
        # so this propagates on the first attempt (no retry that would pile more
        # waiters onto an exhausted pool).
        expect { small_pool_client.read_message("pool_exhaust", vt: 0) }
          .to raise_error(PGMQ::Errors::ConnectionError, /pool timeout/i)
      ensure
        release.count_down
        holder.join(5)
      end
    end
  end
end
