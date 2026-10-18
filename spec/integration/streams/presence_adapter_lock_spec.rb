# frozen_string_literal: true

require_relative "../../integration_helper"

# Presence statements on a pinned connection (issue #566). Transactional tests
# pin the pool: every thread gets the same adapter, and the adapter's lock is
# the only thing keeping two threads off the one PG::Connection. The streamer
# sweeps seats on its own thread (PresenceTracker -> expire_seats!) while
# request threads run, so every presence statement has to hold that lock.
RSpec.describe "Streams: presence statements hold the adapter lock", :integration do
  let(:pool) { ActiveRecord::Base.connection_pool }
  let(:presence_class) { Pgbus::Streams::Presence }
  let(:presence) { Pgbus.stream("lock_#{SecureRandom.hex(4)}").presence }
  let(:threads) { [] }

  # Pinned after the suite's own before hooks, so their cleanup does not run
  # (and hold row locks) inside the pinned transaction.
  before do
    presence_class.reset_schema_cache!
    pin_pool
  end

  after do
    threads.each(&:kill)
    unpin_pool
    presence_class.reset_schema_cache!
  end

  # What ActiveRecord::TestFixtures does with lock_threads on: Rails 7.2+
  # pins the pool, Rails 7.1 locks it to the current thread.
  def pin_pool
    return pool.pin_connection!(true) if pool.respond_to?(:pin_connection!)

    pool.connection.begin_transaction(joinable: false, _lazy: false)
    pool.lock_thread = true
  end

  def unpin_pool
    return pool.unpin_connection! if pool.respond_to?(:unpin_connection!)

    connection = pool.connection
    connection.rollback_transaction if connection.transaction_open?
    pool.lock_thread = false
  end

  def spawn(&)
    Thread.new(&).tap { |thread| threads << thread }
  end

  def sweep
    presence_class.expire_seats!(ttl_for: ->(_) { 30 }, min_ttl: 30)
  end

  def monotonic_now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  # A second session holds the table, so the sweep's SELECT waits in Postgres:
  # the window in which the sweeping thread is inside exec_params.
  def lock_presence_table
    blocker = build_pg_listen_connection
    blocker.exec("SET lock_timeout = '5s'")
    blocker.exec("BEGIN")
    blocker.exec("LOCK TABLE pgbus_presence_members IN ACCESS EXCLUSIVE MODE")
    blocker
  end

  # Waits until the pinned backend is waiting on the table lock: a presence
  # statement is in flight on the shared PG::Connection. Matched by backend,
  # not by SQL text, so it holds whichever presence statement got there first.
  def wait_until_statement_waits(blocker, backend, sweeper)
    deadline = monotonic_now + 5
    loop do
      activity = blocker.exec_params(<<~SQL, [backend]).first
        SELECT wait_event_type, left(query, 120) AS query FROM pg_stat_activity WHERE pid = $1
      SQL
      return if activity&.fetch("wait_event_type") == "Lock"

      sweeper.value unless sweeper.alive? # surfaces the sweep's exception
      raise "no presence statement waited on the table lock: #{activity.inspect}" if monotonic_now > deadline

      sleep 0.02
    end
  end

  def wait_until_blocked(thread)
    deadline = monotonic_now + 5
    Thread.pass until thread.status != "run" || monotonic_now > deadline
  end

  it "gives every thread the same PG::Connection (the shape this guards)" do
    main_raw = presence_class.connection

    expect(spawn { presence_class.connection }.value).to equal(main_raw)
  end

  it "keeps the adapter's lock until a sweep's statement has finished" do
    adapter = ActiveRecord::Base.connection
    backend = adapter.raw_connection.backend_pid
    # Probed first: the schema probe would also wait on the table lock.
    expect(presence_class.seats_supported?).to be(true)
    blocker = lock_presence_table
    sweeper = spawn { sweep }
    wait_until_statement_waits(blocker, backend, sweeper)

    lock_taker = spawn { adapter.lock.synchronize { :taken } }
    request = spawn { ActiveRecord::Base.connection.select_value("SELECT 42") }

    expect(lock_taker.join(0.5)).to be_nil
    expect(request.join(0.5)).to be_nil

    blocker.exec("ROLLBACK")

    expect(sweeper.join(5)&.value).to eq([])
    expect(lock_taker.join(5)&.value).to eq(:taken)
    expect(request.join(5)&.value).to eq(42)
  ensure
    blocker&.close
  end

  it "does not start a sweep's statement while another thread holds the adapter's lock" do
    adapter = ActiveRecord::Base.connection
    backend = adapter.raw_connection.backend_pid
    observer = build_pg_listen_connection
    sweeper = nil

    adapter.lock.synchronize do
      sweeper = spawn { sweep }

      expect(sweeper.join(0.5)).to be_nil
      activity = observer.exec_params("SELECT state, query FROM pg_stat_activity WHERE pid = $1", [backend]).first
      expect(activity["state"]).to eq("idle in transaction")
      expect(activity["query"]).not_to include("pgbus_presence_members")
    end

    expect(sweeper.join(5)&.value).to eq([])
  ensure
    observer&.close
  end

  it "joins and sets state inside a transaction (the lock is re-entered, not waited on)" do
    request = spawn do
      ActiveRecord::Base.transaction do
        presence.join(member_id: "7", connection_id: "c1", seat: "s1")
        presence.set_state(connection_id: "c1", state: { selected: 3 })
      end
    end

    expect(request.join(5)&.value).to include("connection_id" => "c1", "state" => { "selected" => 3 })
  end

  # within_new_transaction holds the adapter's lock for its whole block. A
  # sweep that waited for that lock while holding the schema mutex would
  # leave the request stuck in seats? -> seats_supported?, and the request
  # holds the lock the sweep waits for.
  it "lets a request inside a transaction read presence while a cold-cache sweep waits for its lock" do
    in_transaction = Queue.new
    go = Queue.new
    request = spawn do
      ActiveRecord::Base.transaction do
        in_transaction << true
        go.pop
        presence.members
      end
    end
    in_transaction.pop
    sweeper = spawn { sweep }
    wait_until_blocked(sweeper)
    go << true

    expect(request.join(5)&.value).to eq([])
    expect(sweeper.join(5)&.value).to eq([])
  end
end
