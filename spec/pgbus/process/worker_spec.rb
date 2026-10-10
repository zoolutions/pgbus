# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Worker do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:executor) { instance_double(Pgbus::ActiveJob::Executor) }
  let(:pool) do
    instance_double(
      Pgbus::ExecutionPools::ThreadPool,
      capacity: 5, available_capacity: 5, idle?: true,
      shutdown: true, kill: true, wait_for_termination: true,
      metadata: { mode: :threads, capacity: 5, busy: 0 }
    )
  end
  let(:circuit_breaker) { instance_double(Pgbus::CircuitBreaker, paused?: false, record_success: nil, record_failure: nil) }
  let(:worker) { described_class.new(queues: %w[default], threads: 5) }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ActiveJob::Executor).to receive(:new).and_return(executor)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(pool)
    allow(Pgbus::CircuitBreaker).to receive(:new).and_return(circuit_breaker)
  end

  describe "#initialize" do
    it "stores config, queues, and creates an execution pool" do
      expect(worker.queues).to eq(%w[default])
      expect(worker.threads).to eq(5)
      expect(worker.config).to be_a(Pgbus::Configuration)
      expect(Pgbus::ExecutionPools).to have_received(:build).with(
        mode: :threads, capacity: 5, on_state_change: an_instance_of(Proc)
      )
    end

    it "defaults to threads execution mode" do
      expect(worker.execution_mode).to eq(:threads)
    end

    it "accepts async execution mode" do
      async_pool = instance_double(
        Pgbus::ExecutionPools::AsyncPool,
        capacity: 50, available_capacity: 50, idle?: true,
        shutdown: true, kill: true, wait_for_termination: true,
        metadata: { mode: :async, capacity: 50, busy: 0 }
      )
      allow(Pgbus::ExecutionPools).to receive(:build)
        .with(mode: :async, capacity: 50, on_state_change: an_instance_of(Proc))
        .and_return(async_pool)

      async_worker = described_class.new(queues: %w[default], threads: 50, execution_mode: :async)
      expect(async_worker.execution_mode).to eq(:async)
    end

    it "initializes stats tracking" do
      expect(worker.stats[:jobs_processed]).to eq(0)
      expect(worker.stats[:jobs_failed]).to eq(0)
      expect(worker.stats[:in_flight]).to eq(0)
      expect(worker.stats[:state]).to eq(:starting)
      expect(worker.stats[:execution_mode]).to eq(:threads)
      expect(worker.stats[:started_at]).to be_a(Time)
      expect(worker.stats[:mode]).to eq(:threads)
      expect(worker.stats[:capacity]).to eq(5)
    end

    it "raises ArgumentError for an invalid group_mode" do
      expect do
        described_class.new(queues: %w[default], threads: 5, group_mode: :nope)
      end.to raise_error(ArgumentError, /group_mode/)
    end
  end

  describe "heartbeat pool observability" do
    it "snapshots the rate counter on each beat" do
      counter = worker.rate_counter
      allow(counter).to receive(:snapshot!)

      worker.send(:on_heartbeat)

      expect(counter).to have_received(:snapshot!)
    end

    it "emits pgbus.client.pool with the client's pool stats on each beat" do
      allow(mock_client).to receive(:pool_stats).and_return(size: 5, available: 2, pool_timeout: 5)
      events = []
      callback = ->(_name, _start, _finish, _id, payload) { events << payload }

      ActiveSupport::Notifications.subscribed(callback, "pgbus.client.pool") do
        worker.send(:on_heartbeat)
      end

      expect(events).to contain_exactly(hash_including(size: 5, available: 2, pool_timeout: 5))
    end

    it "does not let a pool-stats failure break the beat" do
      allow(mock_client).to receive(:pool_stats).and_raise(StandardError, "boom")

      expect { worker.send(:on_heartbeat) }.not_to raise_error
    end

    it "wires on_heartbeat as the heartbeat's on_beat hook" do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)

      worker.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new)
        .with(hash_including(on_beat: an_instance_of(Proc)))
    end

    it "wires a metadata_supplier that reports rates and counters" do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)

      worker.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new)
        .with(hash_including(metadata_supplier: an_instance_of(Proc)))
    end

    it "supplies string-keyed rates and current counters" do
      supplier = nil
      allow(Pgbus::Process::Heartbeat).to receive(:new) do |**kwargs|
        supplier = kwargs[:metadata_supplier]
        heartbeat
      end

      worker.send(:start_heartbeat)
      payload = supplier.call

      expect(payload).to include(
        "rates" => hash_including("processed", "failed", "dequeued"),
        "jobs_processed" => 0,
        "jobs_failed" => 0,
        "in_flight" => 0
      )
    end

    # Issue #503: the Processes page tells sibling forks of one capsule apart.
    it "names the capsule and the process slot in the static metadata" do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
      forked = described_class.new(queues: %w[render], threads: 1, capsule: "render", process: "2/4")

      forked.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new)
        .with(hash_including(metadata: hash_including(capsule: "render", process: "2/4")))
    end

    it "leaves capsule and process out of the metadata when they are not set" do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)

      worker.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new)
        .with(hash_including(metadata: hash_excluding(:capsule, :process)))
    end

    it "snapshots the rate counter before the supplier reads it" do
      supplier = nil
      allow(Pgbus::Process::Heartbeat).to receive(:new) do |**kwargs|
        supplier = kwargs[:metadata_supplier]
        heartbeat
      end
      counter = worker.rate_counter
      counter.increment(:processed, 3)

      worker.send(:start_heartbeat)
      worker.send(:on_heartbeat) # simulates Heartbeat calling on_beat first
      payload = supplier.call

      # snapshot! in on_heartbeat computed a positive processed rate from the
      # 3 increments; the supplier reads those refreshed rates.
      expect(payload["rates"]["processed"]).to be > 0
    end
  end

  describe "stat buffer construction" do
    it "builds the StatBuffer with the configured flush thresholds" do
      Pgbus.configuration.stats_flush_size = 250
      Pgbus.configuration.stats_flush_interval = 2

      allow(Pgbus::StatBuffer).to receive(:new).and_call_original
      described_class.new(queues: %w[default], threads: 5)

      expect(Pgbus::StatBuffer).to have_received(:new).with(flush_size: 250, flush_interval: 2)
    ensure
      Pgbus.configuration.stats_flush_size = Pgbus::StatBuffer::DEFAULT_FLUSH_SIZE
      Pgbus.configuration.stats_flush_interval = Pgbus::StatBuffer::DEFAULT_FLUSH_INTERVAL
    end

    it "does not build a StatBuffer when stats are disabled" do
      Pgbus.configuration.stats_enabled = false

      allow(Pgbus::StatBuffer).to receive(:new)
      built = described_class.new(queues: %w[default], threads: 5)

      expect(Pgbus::StatBuffer).not_to have_received(:new)
      expect(built.stat_buffer).to be_nil
    ensure
      Pgbus.configuration.stats_enabled = true
    end
  end

  describe "#graceful_shutdown" do
    before { worker.lifecycle.transition_to!(:running) }
    after { Pgbus.stopping = false }

    it "transitions to draining state" do
      worker.graceful_shutdown
      expect(worker.lifecycle.state).to eq(:draining)
    end

    it "sets Pgbus.stopping for ActiveJob::Continuation support" do
      expect { worker.graceful_shutdown }.to change(Pgbus, :stopping).from(false).to(true)
    end

    it "flushes the stat buffer so entries survive a subsequent SIGKILL" do
      buffer = instance_double(Pgbus::StatBuffer, flush: nil)
      worker.stat_buffer = buffer

      worker.graceful_shutdown

      expect(buffer).to have_received(:flush)
    end

    it "does not raise when stats are disabled (nil buffer)" do
      worker.stat_buffer = nil

      expect { worker.graceful_shutdown }.not_to raise_error
    end
  end

  describe "#check_recycle (private) drain flush" do
    before do
      worker.lifecycle.transition_to!(:running)
      worker.config.max_jobs_per_worker = 1
      worker.jobs_processed = 5
    end

    after do
      worker.config.max_jobs_per_worker = nil
      Pgbus.stopping = false
    end

    it "flushes the stat buffer when a recycle triggers drain" do
      buffer = instance_double(Pgbus::StatBuffer, flush: nil)
      worker.stat_buffer = buffer

      worker.send(:check_recycle)

      expect(worker.lifecycle.state).to eq(:draining)
      expect(buffer).to have_received(:flush)
    end

    it "does not raise when recycling with stats disabled (nil buffer)" do
      worker.stat_buffer = nil

      expect { worker.send(:check_recycle) }.not_to raise_error
    end
  end

  describe "#immediate_shutdown" do
    before { worker.lifecycle.transition_to!(:running) }
    after { Pgbus.stopping = false }

    it "transitions to stopped state and kills the pool" do
      worker.immediate_shutdown
      expect(worker.lifecycle.state).to eq(:stopped)
      expect(pool).to have_received(:kill)
    end

    it "sets Pgbus.stopping for ActiveJob::Continuation support" do
      expect { worker.immediate_shutdown }.to change(Pgbus, :stopping).from(false).to(true)
    end
  end

  # The drain loop must wait for ALL in-flight jobs, not exit as soon as one
  # pool slot frees up. pool.idle? is true with 4 of 5 slots busy — using it
  # as the drain condition abandoned in-flight jobs to the 30s shutdown
  # timeout, killing any job that ran longer.
  describe "drain behavior during #run" do
    after { Pgbus.stopping = false }

    it "keeps the loop alive until the pool quiesces, then stops" do
      quiesced = Concurrent::AtomicBoolean.new(false)
      allow(pool).to receive(:quiesced?) { quiesced.value }
      allow(worker).to receive(:start_notify_listener)
      allow(worker).to receive(:claim_and_execute)

      runner = Thread.new { worker.run }
      deadline = Time.now + 2
      sleep 0.01 while worker.stats[:state] != :running && Time.now < deadline
      expect(worker.stats[:state]).to eq(:running)

      worker.graceful_shutdown
      sleep 0.3

      # One free slot (idle? true) but work still in flight (quiesced? false):
      # the worker must keep draining.
      expect(runner).to be_alive

      quiesced.make_true
      expect(runner.join(2)).to eq(runner), "worker did not stop after pool quiesced"
    ensure
      Pgbus.stopping = false
      runner&.kill
    end

    # Waiting for quiesced? must be bounded: a permanently-stuck job (e.g. a
    # TCP read with no timeout) would otherwise hold the drain loop open
    # forever — recycling never completes, TERM shutdown of the whole process
    # tree hangs, and the loop keeps stamping loop_tick so the supervisor
    # watchdog never intervenes. After config.drain_timeout the loop must fall
    # through to shutdown, whose wait_for_termination(30) bounds the rest.
    it "exits the drain loop after drain_timeout even if a job never finishes" do
      worker.config.drain_timeout = 0.2
      allow(pool).to receive(:quiesced?).and_return(false)
      allow(worker).to receive(:start_notify_listener)
      allow(worker).to receive(:claim_and_execute)
      allow(Pgbus.logger).to receive(:warn)

      runner = Thread.new { worker.run }
      deadline = Time.now + 2
      sleep 0.01 while worker.stats[:state] != :running && Time.now < deadline
      expect(worker.stats[:state]).to eq(:running)

      worker.graceful_shutdown

      expect(runner.join(3)).to eq(runner), "worker did not stop after the drain deadline"
    ensure
      Pgbus.stopping = false
      worker.config.drain_timeout = 30
      runner&.kill
    end
  end

  describe "#recycle_needed? (private)" do
    # worker.config is the globally-memoized Pgbus.configuration, so the recycle
    # limits set in the contexts below leak across examples — and across spec
    # files — unless reset. Without this, a stray max_worker_lifetime/max_memory
    # left set by an earlier example makes "returns false" tests trip on the
    # wrong reason under some random orderings (seen only on CI Ruby 3.3).
    before do
      worker.config.max_jobs_per_worker = nil
      worker.config.max_memory_mb = nil
      worker.config.max_worker_lifetime = nil
    end

    after do
      worker.config.max_jobs_per_worker = nil
      worker.config.max_memory_mb = nil
      worker.config.max_worker_lifetime = nil
    end

    it "returns false when no limits are configured" do
      expect(worker.send(:recycle_needed?)).to be false
    end

    context "when max_jobs_per_worker is exceeded" do
      before { worker.config.max_jobs_per_worker = 100 }

      it "returns true when jobs_processed reaches the limit" do
        worker.jobs_processed = 100
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        worker.jobs_processed = 50
        expect(worker.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { worker.config.max_worker_lifetime = 60 }

      it "returns true when lifetime is exceeded" do
        aged = described_class.new(
          queues: %w[default], threads: 5,
          started_at_monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120
        )
        expect(aged.send(:recycle_needed?)).to be true
      end
    end

    context "when max_memory_mb is exceeded" do
      before do
        worker.config.max_memory_mb = 256
        Pgbus::Process::MemoryUsage.reset!
      end

      after { Pgbus::Process::MemoryUsage.reset! }

      it "returns true when memory exceeds the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(512)
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "returns false when memory is within the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(128)
        expect(worker.send(:recycle_needed?)).to be false
      end
    end

    # Issue #503: a capsule's own limits override the global ones for its
    # workers only; unset capsule limits fall back to the global value.
    context "with capsule-level recycle limits" do
      before { allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(1_024) }

      it "recycles on the capsule's max_memory_mb, not the higher global one" do
        worker.config.max_memory_mb = 4_096
        heavy = described_class.new(queues: %w[render], threads: 1, recycle_limits: { max_memory_mb: 512 })
        expect(heavy.send(:recycle_needed?)).to be true
      end

      it "keeps running under a capsule max_memory_mb above the global one" do
        worker.config.max_memory_mb = 512
        heavy = described_class.new(queues: %w[render], threads: 1, recycle_limits: { max_memory_mb: 1_536 })
        expect(heavy.send(:recycle_needed?)).to be false
      end

      it "still applies the global max_memory_mb to a worker without capsule limits" do
        worker.config.max_memory_mb = 512
        expect(worker.send(:recycle_needed?)).to be true
      end

      it "falls back to the global value for limits the capsule does not set" do
        worker.config.max_jobs_per_worker = 10
        heavy = described_class.new(queues: %w[render], threads: 1, recycle_limits: { max_memory_mb: 1_536 })
        heavy.jobs_processed = 10
        expect(heavy.send(:recycle_reason)).to eq(:max_jobs)
      end

      it "applies the capsule's max_worker_lifetime" do
        aged = described_class.new(
          queues: %w[render], threads: 1, recycle_limits: { max_worker_lifetime: 60 },
          started_at_monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120
        )
        expect(aged.send(:recycle_reason)).to eq(:max_lifetime)
      end
    end
  end

  describe "#fetch_messages (private)" do
    # Pin the worker's monotonic clock so cooldown windows are deterministic.
    def stub_monotonic(seconds)
      allow(worker).to receive(:monotonic_now).and_return(seconds.to_f)
    end

    context "with a single queue" do
      it "reads a batch from the single queue" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default priority], threads: 4) }

      it "uses read_multi to fetch from all queues in a single call, capping the total with limit:" do
        allow(mock_client).to receive(:read_multi).and_return([])
        worker.send(:fetch_messages, 4)
        expect(mock_client).to have_received(:read_multi).with(%w[default priority], qty: 4, limit: 4)
      end

      it "tags each message with its source queue from the queue_name field" do
        prefix = worker.config.queue_prefix
        msg1 = build_message_double(msg_id: 1, message: '{"a":1}')
        msg2 = build_message_double(msg_id: 2, message: '{"b":2}')
        allow(msg1).to receive(:queue_name).and_return("#{prefix}_default")
        allow(msg2).to receive(:queue_name).and_return("#{prefix}_priority")

        allow(mock_client).to receive(:read_multi).and_return([msg1, msg2])

        results = worker.send(:fetch_messages, 4)
        expect(results).to eq([["default", msg1], ["priority", msg2]])
      end
    end

    context "with three or more queues under backpressure (regression: issue #123)" do
      let(:worker) { described_class.new(queues: %w[default low statistics], threads: 5) }

      it "caps the total across queues at qty so the execution pool cannot overflow" do
        allow(mock_client).to receive(:read_multi).and_return([])
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_multi)
          .with(%w[default low statistics], qty: 5, limit: 5)
      end
    end

    context "when an error occurs" do
      before { allow(mock_client).to receive(:read_batch).and_raise(StandardError, "connection lost") }

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end
    end

    context "when the client connection breaker is open (issue #197)" do
      before do
        allow(mock_client).to receive(:read_batch).and_raise(Pgbus::ConnectionCircuitOpenError)
        allow(Pgbus::ErrorReporter).to receive(:report)
      end

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end

      it "does not flood the error tracker while the breaker is open" do
        3.times { worker.send(:fetch_messages, 5) }
        expect(Pgbus::ErrorReporter).not_to have_received(:report)
      end
    end

    context "when a queue table is missing (deleted queue)" do
      let(:worker) { described_class.new(queues: %w[default stale_queue], threads: 5) }
      let(:prefix) { worker.config.queue_prefix }
      let(:error_message) do
        "Database connection error: ERROR:  relation \"pgmq.q_#{prefix}_stale_queue\" does not exist"
      end

      before do
        allow(mock_client).to receive(:read_multi)
          .and_raise(StandardError, error_message)
      end

      it "evicts the deleted queue from the worker queue list" do
        worker.send(:fetch_messages, 5)
        expect(worker.queues).to eq(%w[default])
      end

      it "returns an empty array" do
        expect(worker.send(:fetch_messages, 5)).to eq([])
      end
    end

    context "when eviction removes ALL queues (regression: issue #174)" do
      let(:worker) { described_class.new(queues: %w[only_queue], threads: 5) }
      let(:prefix) { worker.config.queue_prefix }

      before do
        error_message = "Database connection error: ERROR:  relation \"pgmq.q_#{prefix}_only_queue\" does not exist"
        allow(mock_client).to receive(:read_batch)
          .and_raise(StandardError, error_message)
      end

      it "defers falling back to the initial queues until the cooldown elapses (issue #209)" do
        # First fetch evicts the last queue and stamps the eviction clock; the
        # restore is deferred until RESTORE_COOLDOWN_BASE seconds have passed.
        stub_monotonic(0)
        worker.send(:fetch_messages, 5)
        expect(worker.queues).to be_empty
        expect(worker.restore_streak).to eq(0)

        # After the cooldown the leading restore reinstates the initial queues;
        # the trailing read still fails and re-evicts, so the restore is observed
        # via the streak increment rather than the (again empty) queue list.
        stub_monotonic(Pgbus::Process::Worker::RESTORE_COOLDOWN_BASE)
        worker.send(:fetch_messages, 5)
        expect(worker.restore_streak).to eq(1)
      end
    end

    context "with a cooldown on the evict/restore cycle (issue #209)" do
      let(:worker) { described_class.new(queues: %w[gone], threads: 5) }
      let(:prefix) { worker.config.queue_prefix }
      let(:base) { Pgbus::Process::Worker::RESTORE_COOLDOWN_BASE }
      let(:max) { Pgbus::Process::Worker::RESTORE_COOLDOWN_MAX }
      let(:warn_messages) { [] }

      # Every read raises "relation does not exist" so the queue stays
      # permanently evicted — the pathological case the cooldown protects.
      before do
        error_message = "ERROR:  relation \"pgmq.q_#{prefix}_gone\" does not exist"
        allow(mock_client).to receive(:read_batch).and_raise(StandardError, error_message)
        allow(Pgbus.logger).to receive(:warn) do |&block|
          warn_messages << block.call if block
        end
      end

      # One loop tick at a pinned time. When the queue is permanently gone a
      # single fetch may BOTH restore the initial queues (leading restore, if the
      # cooldown elapsed) AND re-evict them (the trailing read still fails), so
      # the final queue list can't observe "did a restore fire". The restore
      # streak is the reliable observable: it increments once per actual restore.
      def tick(now)
        stub_monotonic(now)
        worker.send(:fetch_messages, 5)
      end

      def streak
        worker.restore_streak
      end

      it "does not restore before the base cooldown after full eviction" do
        tick(0) # evicts, arms the window, defers restore
        expect(worker.queues).to be_empty
        expect(streak).to eq(0) # no restore has fired

        tick(base - 1)         # still inside the window
        expect(streak).to eq(0)
      end

      it "restores once the base cooldown has elapsed" do
        tick(0)
        expect(streak).to eq(0)

        tick(base)             # cooldown met → restore fires
        expect(streak).to eq(1)
      end

      it "doubles the wait on each consecutive failed restore, capped at the max" do
        tick(0)                # window opens at t=0, wait=base
        tick(base)             # restore #1 → streak 1, re-evicts, window at t=base, wait=2*base
        expect(streak).to eq(1)

        tick(base + (2 * base) - 1) # one second short of 2*base → no restore
        expect(streak).to eq(1)
        tick(base + (2 * base))     # 2*base elapsed → restore #2
        expect(streak).to eq(2)

        # Walk the streak up until the raw exponential would exceed the cap, each
        # step advancing by exactly the (capped) wait so a restore fires.
        t = base + (2 * base)
        t += tick_until_capped(from: t)

        # Now base * 2**streak > max: prove the wait is clamped. Advancing by the
        # raw exponential would obviously restore; advancing by only `max` (which
        # is strictly less) must ALSO restore — that's the clamp doing its job.
        expect(base * (2**streak)).to be > max
        before = streak
        t += max
        tick(t)
        expect(streak).to eq(before + 1)
      end

      it "logs one deferral warning per cooldown window, not one per tick" do
        tick(0) # first window: exactly one deferral warn
        first_window = warn_messages.count { |m| m.include?("next restore attempt") }
        expect(first_window).to eq(1)

        # Many more ticks inside the SAME window emit no further deferral warns.
        9.times { |i| tick((i + 1) * (base / 100.0)) }
        total = warn_messages.count { |m| m.include?("next restore attempt") }
        expect(total).to eq(1)
      end

      it "emits a deferral warning naming the next attempt when a window opens" do
        tick(0)
        deferral = warn_messages.find { |m| m.include?("next restore attempt") }
        expect(deferral).to include("#{base}s")
      end

      # Advance the clock through capped windows until the raw exponential wait
      # (base * 2**streak) first exceeds RESTORE_COOLDOWN_MAX. Returns elapsed.
      def tick_until_capped(from:)
        elapsed = 0
        while base * (2**streak) <= max
          wait = [base * (2**streak), max].min
          elapsed += wait
          tick(from + elapsed)
        end
        elapsed
      end
    end

    context "when a successful fetch resets the restore streak (issue #209)" do
      let(:worker) { described_class.new(queues: %w[flaky], threads: 5) }
      let(:prefix) { worker.config.queue_prefix }
      let(:base) { Pgbus::Process::Worker::RESTORE_COOLDOWN_BASE }
      let(:message) { build_message_double(msg_id: 1) }

      it "restores promptly after a good fetch instead of on the escalated wait" do
        error_message = "ERROR:  relation \"pgmq.q_#{prefix}_flaky\" does not exist"
        reads = 0
        allow(mock_client).to receive(:read_batch) do
          reads += 1
          raise StandardError, error_message if reads == 1 # first fetch: queue gone

          [message] # queue recreated: subsequent fetches succeed
        end

        # Evict, then restore once so the streak advances to 1.
        stub_monotonic(0)
        worker.send(:fetch_messages, 5)
        expect(worker.queues).to be_empty
        stub_monotonic(base)
        worker.send(:fetch_messages, 5) # restore (streak → 1) then a clean read

        expect(worker.queues).to eq(%w[flaky])
        expect(worker.restore_streak).to eq(0)
      end
    end

    context "when circuit breaker pauses all queues (#174 diagnostic)" do
      let(:worker) { described_class.new(queues: %w[default low], threads: 5) }
      let(:warn_messages) { [] }

      before do
        allow(circuit_breaker).to receive(:paused?).and_return(true)
        allow(Pgbus.logger).to receive(:debug) do |&block|
          warn_messages << block.call if block
        end
      end

      it "logs which queues were filtered out" do
        worker.send(:fetch_messages, 5)
        filtered_log = warn_messages.find { |m| m.include?("all queues filtered") }
        expect(filtered_log).not_to be_nil
      end
    end

    context "with group_mode: :fifo" do
      let(:worker) { described_class.new(queues: %w[default], threads: 5, group_mode: :fifo) }
      let(:messages) { [build_message_double(msg_id: 1), build_message_double(msg_id: 2)] }

      before do
        allow(mock_client).to receive(:read_grouped).and_return(messages)
      end

      it "uses read_grouped instead of read_batch" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_grouped).with("default", qty: 5)
        expect(mock_client).not_to have_received(:read_batch)
      end

      it "returns [queue_name, message] pairs" do
        results = worker.send(:fetch_messages, 5)
        expect(results).to eq([["default", messages[0]], ["default", messages[1]]])
      end
    end

    context "with group_mode: :round_robin" do
      let(:worker) { described_class.new(queues: %w[default], threads: 5, group_mode: :round_robin) }
      let(:messages) { [build_message_double(msg_id: 1)] }

      before do
        allow(mock_client).to receive(:read_grouped_rr).and_return(messages)
      end

      it "uses read_grouped_rr instead of read_batch" do
        worker.send(:fetch_messages, 5)
        expect(mock_client).to have_received(:read_grouped_rr).with("default", qty: 5)
        expect(mock_client).not_to have_received(:read_batch)
      end
    end

    context "with group_mode: :fifo and multiple queues" do
      let(:worker) { described_class.new(queues: %w[critical default], threads: 5, group_mode: :fifo) }
      let(:critical_msgs) { [build_message_double(msg_id: 1)] }
      let(:default_msgs) { [build_message_double(msg_id: 2)] }

      before do
        allow(mock_client).to receive(:read_grouped) do |queue, **_kwargs|
          case queue
          when "critical" then critical_msgs
          when "default" then default_msgs
          else []
          end
        end
      end

      it "reads from each queue with grouped reads, respecting qty limit" do
        results = worker.send(:fetch_messages, 5)
        expect(results.size).to eq(2)
        expect(mock_client).to have_received(:read_grouped).with("critical", qty: 5)
        expect(mock_client).to have_received(:read_grouped).with("default", qty: 4)
      end
    end

    context "with fair share enabled (issue #426)" do
      let(:worker) { described_class.new(queues: %w[critical default], threads: 5) }
      let(:critical_msgs) { [build_message_double(msg_id: 1), build_message_double(msg_id: 2)] }
      let(:default_msgs) { [build_message_double(msg_id: 3)] }

      before do
        Pgbus.configuration.fair_share = ->(_job) { "tenant" }
        allow(mock_client).to receive(:read_batch_fair) do |queue, **_kwargs|
          case queue
          when "critical" then critical_msgs
          when "default" then default_msgs
          else []
          end
        end
      end

      after { Pgbus.configuration.fair_share = nil }

      it "fair-reads each queue in list order with the remaining capacity" do
        results = worker.send(:fetch_messages, 5)

        expect(mock_client).to have_received(:read_batch_fair).with("critical", qty: 5).ordered
        expect(mock_client).to have_received(:read_batch_fair).with("default", qty: 3).ordered
        expect(mock_client).not_to have_received(:read_multi)
        expect(mock_client).not_to have_received(:read_batch)
        expect(results).to eq([["critical", critical_msgs[0]], ["critical", critical_msgs[1]], ["default", default_msgs[0]]])
      end

      it "stops reading once capacity is filled" do
        worker.send(:fetch_messages, 2)

        expect(mock_client).to have_received(:read_batch_fair).with("critical", qty: 2)
        expect(mock_client).not_to have_received(:read_batch_fair).with("default", qty: anything)
      end

      it "uses the fair read on the single-queue path too" do
        single = described_class.new(queues: %w[default], threads: 5)
        single.send(:fetch_messages, 5)

        expect(mock_client).to have_received(:read_batch_fair).with("default", qty: 5)
        expect(mock_client).not_to have_received(:read_batch)
      end

      it "still goes through fetch_prioritized when priority_levels is set" do
        Pgbus.configuration.priority_levels = 2
        allow(mock_client).to receive(:read_batch_prioritized).and_return([])

        worker.send(:fetch_messages, 5)

        expect(mock_client).to have_received(:read_batch_prioritized).with("critical", qty: 5)
        expect(mock_client).not_to have_received(:read_batch_fair)
      ensure
        Pgbus.configuration.priority_levels = nil
      end

      it "skips queues paused by the circuit breaker" do
        allow(circuit_breaker).to receive(:paused?).with("critical").and_return(true)

        worker.send(:fetch_messages, 5)

        expect(mock_client).not_to have_received(:read_batch_fair).with("critical", qty: anything)
        expect(mock_client).to have_received(:read_batch_fair).with("default", qty: 5)
      end
    end
  end

  describe "fair share index + config guards (issue #426)" do
    before { Pgbus.configuration.fair_share = ->(_job) { "tenant" } }

    after do
      Pgbus.configuration.fair_share = nil
      Pgbus.configuration.group_mode = nil
    end

    it "raises when group_mode is also set" do
      Pgbus.configuration.group_mode = :round_robin

      expect do
        described_class.new(queues: %w[default], threads: 5)
      end.to raise_error(Pgbus::ConfigurationError, /fair_share.*group_mode/)
    end

    it "raises when a per-worker group_mode is passed" do
      expect do
        described_class.new(queues: %w[default], threads: 5, group_mode: :fifo)
      end.to raise_error(Pgbus::ConfigurationError, /fair_share.*group_mode/)
    end

    it "ensures the fair index for every queue it serves" do
      allow(mock_client).to receive(:ensure_fair_index)
      worker = described_class.new(queues: %w[critical default], threads: 5)

      worker.send(:ensure_fair_indexes)

      expect(mock_client).to have_received(:ensure_fair_index).with("critical")
      expect(mock_client).to have_received(:ensure_fair_index).with("default")
    end

    it "does nothing when fair share is off" do
      Pgbus.configuration.fair_share = nil
      allow(mock_client).to receive(:ensure_fair_index)
      worker = described_class.new(queues: %w[default], threads: 5)

      worker.send(:ensure_fair_indexes)

      expect(mock_client).not_to have_received(:ensure_fair_index)
    end
  end

  describe "prefetch flow control" do
    let(:wake_signal) { worker.wake_signal }

    before { allow(wake_signal).to receive(:wait) }

    context "when prefetch_limit is nil (default)" do
      before { worker.config.prefetch_limit = nil }

      it "does not cap fetch quantity" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      end
    end

    context "when prefetch_limit is configured" do
      before { worker.config.prefetch_limit = 3 }
      after { worker.config.prefetch_limit = nil }

      it "caps fetch to prefetch_limit when below idle threads" do
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 3)
      end

      it "waits when in_flight >= prefetch_limit" do
        worker.in_flight = 3
        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch)
        expect(wake_signal).to have_received(:wait)
      end

      it "uses min of idle and available prefetch" do
        # 2 in flight, limit 3 => available = 1, idle = 5 => fetch 1
        worker.in_flight = 2
        allow(mock_client).to receive(:read_batch).and_return([])
        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_batch).with("default", qty: 1)
      end
    end

    it "increments in_flight when messages are fetched" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:claim_and_execute)
      # After process_message completes (via yield), in_flight should be back to 0
      expect(worker.stats[:in_flight]).to eq(0)
    end

    it "decrements in_flight even when executor raises" do
      msg = build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}')
      allow(mock_client).to receive(:read_batch).and_return([msg])
      allow(pool).to receive(:post).and_yield

      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:claim_and_execute)
      expect(worker.stats[:in_flight]).to eq(0)
    end
  end

  describe "#process_message (private)" do
    let(:message) { build_message_double(msg_id: 1, message: '{"job_class":"TestJob"}') }

    it "executes the message and increments jobs_processed" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(executor).to have_received(:execute).with(message, "default", source_queue: nil)
      expect(worker.stats[:jobs_processed]).to eq(1)
    end

    it "increments jobs_failed when executor returns :failed" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_processed]).to eq(1)
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "increments jobs_failed when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(worker.stats[:jobs_failed]).to eq(1)
    end

    it "signals circuit breaker success on successful execution" do
      allow(executor).to receive(:execute).and_return(:success)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_success).with("default")
    end

    it "signals circuit breaker failure on failed execution" do
      allow(executor).to receive(:execute).and_return(:failed)
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end

    it "signals circuit breaker failure when executor raises" do
      allow(executor).to receive(:execute).and_raise(StandardError, "boom")
      worker.send(:process_message, message, "default")
      expect(circuit_breaker).to have_received(:record_failure).with("default")
    end
  end

  describe "circuit breaker integration" do
    before { allow(worker.wake_signal).to receive(:wait) }

    it "skips paused queues" do
      allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
      allow(mock_client).to receive(:read_batch).and_return([])

      worker.send(:claim_and_execute)
      expect(mock_client).not_to have_received(:read_batch)
    end

    context "with multiple queues" do
      let(:worker) { described_class.new(queues: %w[default events], threads: 4) }

      it "only reads from non-paused queues" do
        allow(circuit_breaker).to receive(:paused?).with("default").and_return(true)
        allow(circuit_breaker).to receive(:paused?).with("events").and_return(false)
        allow(mock_client).to receive(:read_batch).and_return([])

        worker.send(:claim_and_execute)
        expect(mock_client).not_to have_received(:read_batch).with("default", anything)
        # With only one non-paused queue, falls back to single-queue read_batch
        expect(mock_client).to have_received(:read_batch).with("events", qty: 5)
      end

      it "uses read_multi when multiple queues are active" do
        allow(circuit_breaker).to receive(:paused?).and_return(false)
        allow(mock_client).to receive(:read_multi).and_return([])

        worker.send(:claim_and_execute)
        expect(mock_client).to have_received(:read_multi).with(%w[default events], qty: 5, limit: 5)
      end
    end
  end

  describe "single active consumer" do
    let(:queue_lock) { instance_double(Pgbus::Process::QueueLock, unlock_all: nil, held_queues: []) }
    let(:worker) { described_class.new(queues: %w[default events], threads: 4, single_active_consumer: true) }

    before do
      allow(Pgbus::Process::QueueLock).to receive(:new).and_return(queue_lock)
      allow(worker.wake_signal).to receive(:wait)
    end

    it "only reads from queues where advisory lock is held" do
      allow(queue_lock).to receive(:try_lock).with("default").and_return(true)
      allow(queue_lock).to receive(:try_lock).with("events").and_return(false)
      allow(mock_client).to receive(:read_batch).and_return([])

      worker.send(:claim_and_execute)
      expect(mock_client).to have_received(:read_batch).with("default", qty: 5)
      expect(mock_client).not_to have_received(:read_batch).with("events", anything)
    end

    it "skips all queues when no locks acquired" do
      allow(queue_lock).to receive(:try_lock).and_return(false)
      allow(mock_client).to receive(:read_batch)

      worker.send(:claim_and_execute)
      expect(mock_client).not_to have_received(:read_batch)
    end

    it "exposes single_active_consumer in stats" do
      expect(worker.stats[:single_active_consumer]).to be true
    end
  end

  describe "#refresh_wildcard_queues (private)" do
    let(:worker) { described_class.new(queues: %w[*], threads: 5) }
    let(:prefix) { worker.config.queue_prefix }
    let(:conn) { double("connection") }

    before do
      allow(ActiveRecord::Base).to receive(:connection).and_return(conn)
      # Default: no known streams, so wildcard resolution is unchanged.
      allow(Pgbus::StreamQueue).to receive_messages(reset_cache!: nil, known_names: Set.new)
      # Default: no registered event subscribers.
      registry = instance_double(Pgbus::EventBus::Registry, event_queue_names: Set.new)
      allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
    end

    it "ensures the fair index for newly resolved queues when fair share is on (issue #426)" do
      Pgbus.configuration.fair_share = ->(_job) { "tenant" }
      allow(mock_client).to receive(:ensure_fair_index)
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default", "#{prefix}_events"])

      worker.send(:refresh_wildcard_queues)

      expect(mock_client).to have_received(:ensure_fair_index).with("default")
      expect(mock_client).to have_received(:ensure_fair_index).with("events")
    ensure
      Pgbus.configuration.fair_share = nil
    end

    it "excludes registered stream queues (issue #309)" do
      # A stream queue is named like a job queue (pgbus_<name>); only the
      # registry/fingerprint distinguishes it. A wildcard worker must NOT adopt
      # it, or it would claim durable broadcasts and DLQ-move them.
      allow(Pgbus::StreamQueue).to receive(:known_names).and_return(Set.new(["#{prefix}_chat_42"]))
      allow(conn).to receive(:select_values)
        .and_return(["#{prefix}_default", "#{prefix}_chat_42", "#{prefix}_events"])

      worker.send(:resolve_wildcard_queues)

      expect(worker.queues).to eq(%w[default events])
    end

    it "excludes fingerprint-matched dormant streams not yet registered (issue #366)" do
      allow(Pgbus::StreamQueue).to receive(:known_names).and_return(Set.new(["#{prefix}_dormant_99"]))
      allow(conn).to receive(:select_values)
        .and_return(["#{prefix}_default", "#{prefix}_dormant_99", "#{prefix}_events"])

      worker.send(:resolve_wildcard_queues)

      expect(worker.queues).to eq(%w[default events])
    end

    it "excludes registered event-subscriber queues (issue #333)" do
      # Event-bus subscriber queues share the job namespace (pgbus_<handler>) but
      # carry event payloads, not ActiveJob jobs. A wildcard worker that adopts
      # one hands the event to the executor, which fails to deserialize it and
      # DLQ-moves it out of the consumer's reach. Exclude them, like streams.
      registry = instance_double(Pgbus::EventBus::Registry, event_queue_names: Set.new(["#{prefix}_orders_handler"]))
      allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
      allow(conn).to receive(:select_values)
        .and_return(["#{prefix}_default", "#{prefix}_orders_handler", "#{prefix}_events"])

      worker.send(:resolve_wildcard_queues)

      expect(worker.queues).to eq(%w[default events])
    end

    it "is a no-op for non-wildcard workers" do
      static_worker = described_class.new(queues: %w[default], threads: 5)
      static_worker.send(:refresh_wildcard_queues)
      expect(static_worker.queues).to eq(%w[default])
    end

    it "re-resolves when enough time has passed" do
      # Drive the refresh interval gate through a controllable monotonic clock
      # instead of poking @last_wildcard_resolve: resolve at t=0, refresh at
      # t=60 (past WILDCARD_REFRESH_INTERVAL of 30s).
      clock = [0.0]
      allow(worker).to receive(:monotonic_now) { clock[0] }

      allow(conn).to receive(:select_values).and_return(["#{prefix}_default", "#{prefix}_events"])
      worker.send(:resolve_wildcard_queues)
      expect(worker.queues).to eq(%w[default events])

      # Simulate time passing and queues changing
      clock[0] = 60.0
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default"])
      worker.send(:refresh_wildcard_queues)
      expect(worker.queues).to eq(%w[default])
    end

    it "skips re-resolve when interval has not elapsed" do
      allow(conn).to receive(:select_values).and_return(["#{prefix}_default"])
      worker.send(:resolve_wildcard_queues)

      # Try to refresh immediately — should be a no-op
      worker.send(:refresh_wildcard_queues)
      # Only called once (the initial resolve)
      expect(conn).to have_received(:select_values).once
    end
  end

  describe "zombie message detection" do
    let(:wake_signal) { worker.wake_signal }

    before do
      allow(wake_signal).to receive(:wait)
      allow(Pgbus::FailedEventRecorder).to receive(:exists?).and_return(false)
    end

    context "when zombie_detection is enabled (default)" do
      let(:warn_messages) { [] }

      before do
        allow(Pgbus.logger).to receive(:warn) do |&block|
          warn_messages << block.call if block
        end
      end

      it "logs a warning for redelivered messages with no prior failure" do
        msg = build_message_double(msg_id: 99, message: '{"job_class":"TestJob"}', read_ct: 2)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(Pgbus::FailedEventRecorder).to have_received(:exists?)
          .with(queue_name: "default", msg_id: 99)
        zombie_log = warn_messages.find { |m| m.include?("Zombie message redelivered") }
        expect(zombie_log).to include("msg_id=99")
        expect(zombie_log).to include("read_ct=2")
      end

      it "does not warn for first-time messages (read_ct=1)" do
        msg = build_message_double(msg_id: 100, message: '{"job_class":"TestJob"}', read_ct: 1)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(warn_messages).not_to include(a_string_matching(/Zombie/))
      end

      it "does not warn when a prior failure is recorded" do
        msg = build_message_double(msg_id: 101, message: '{"job_class":"TestJob"}', read_ct: 2)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)
        allow(Pgbus::FailedEventRecorder).to receive(:exists?).and_return(true)

        worker.send(:claim_and_execute)

        expect(warn_messages).not_to include(a_string_matching(/Zombie/))
      end
    end

    context "when zombie_detection is disabled" do
      before { worker.config.zombie_detection = false }
      after { worker.config.zombie_detection = true }

      it "does not check for zombie messages" do
        msg = build_message_double(msg_id: 102, message: '{"job_class":"TestJob"}', read_ct: 3)
        allow(mock_client).to receive(:read_batch).and_return([msg])
        allow(pool).to receive(:post).and_yield
        allow(executor).to receive(:execute).and_return(:success)

        worker.send(:claim_and_execute)

        expect(Pgbus::FailedEventRecorder).not_to have_received(:exists?)
      end
    end
  end

  describe "consumer priority" do
    it "defaults to 0" do
      expect(worker.stats[:consumer_priority]).to eq(0)
    end

    it "accepts a custom priority" do
      priority_worker = described_class.new(queues: %w[default], threads: 5, consumer_priority: 10)
      expect(priority_worker.stats[:consumer_priority]).to eq(10)
    end
  end

  describe "NOTIFY-gated wakeup wiring" do
    # These specs pin the load-bearing private API around the NotifyListener:
    # construction, queue sync, channel ↔ physical-queue conversion, the
    # raised wait_timeout when the listener is active, and shutdown cleanup.
    # The listener itself is stubbed — its internals are covered in
    # notify_listener_spec.rb; here we only exercise how the worker wires it.

    let(:fake_listener) do
      instance_double(
        Pgbus::Process::NotifyListener,
        start: nil, stop: nil, add_queue: nil, remove_queue: nil,
        listening_to: [], running?: true, delivering?: true
      ).tap { |l| allow(l).to receive(:start).and_return(l) }
    end

    describe "#notify_wakeup?" do
      it "is true when config.worker_notify_wakeup? is true" do
        allow(worker.config).to receive(:worker_notify_wakeup?).and_return(true)
        expect(worker.send(:notify_wakeup?)).to be true
      end

      it "is false when config.worker_notify_wakeup? is false" do
        allow(worker.config).to receive(:worker_notify_wakeup?).and_return(false)
        expect(worker.send(:notify_wakeup?)).to be false
      end

      it "is false when config does not respond to worker_notify_wakeup? (old configs)" do
        stripped_config = double("Config")
        allow(stripped_config).to receive(:respond_to?).with(:worker_notify_wakeup?).and_return(false)
        # Stub the public config reader rather than construct with the stripped
        # double (which lacks the stats_* methods the constructor calls).
        allow(worker).to receive(:config).and_return(stripped_config)
        expect(worker.send(:notify_wakeup?)).to be false
      end
    end

    describe "#physical_queue_names" do
      let(:prefix) { worker.config.queue_prefix }

      it "prefixes each logical queue with the configured queue_prefix" do
        multi_worker = described_class.new(queues: %w[default low], threads: 4)
        expect(multi_worker.send(:physical_queue_names))
          .to eq(["#{prefix}_default", "#{prefix}_low"])
      end

      it "normalizes names the same way queue creation does (LISTEN channel parity)" do
        # config.queue_name normalizes (hyphens → underscores); the queue
        # TABLE is created under the normalized name and the NOTIFY channel
        # derives from the table, so a raw-concat listener would be deaf for
        # a name like "orders-handler".
        hyphen_worker = described_class.new(queues: %w[orders-handler], threads: 1)
        expect(hyphen_worker.send(:physical_queue_names)).to eq(["#{prefix}_orders_handler"])
      end

      it "uses the current queues, not the initial set (post-eviction safe)" do
        # Build with two queues so @initial_queues stays %w[default events], then
        # drive a real eviction of `default` so @queues diverges to %w[events].
        # physical_queue_names must reflect the CURRENT list, not the frozen
        # initial set — reading @initial_queues here would wrongly include default.
        evicting_worker = described_class.new(queues: %w[default events], threads: 5)
        allow(mock_client).to receive(:read_multi).and_raise(
          StandardError,
          "ERROR:  relation \"pgmq.q_#{prefix}_default\" does not exist"
        )
        evicting_worker.send(:fetch_messages, 5)

        expect(evicting_worker.queues).to eq(%w[events])
        expect(evicting_worker.send(:physical_queue_names)).to eq(["#{prefix}_events"])
      end
    end

    describe "#channel_to_physical" do
      it "round-trips a channel name back to its physical queue" do
        prefix = worker.config.queue_prefix
        channel = "#{Pgbus::Process::NotifyListener::CHANNEL_PREFIX}#{prefix}_default" \
                  "#{Pgbus::Process::NotifyListener::CHANNEL_SUFFIX}"
        expect(worker.send(:channel_to_physical, channel)).to eq("#{prefix}_default")
      end
    end

    describe "#wake_timeout" do
      it "returns effective_polling_interval when notify_wakeup? is off" do
        allow(worker).to receive(:notify_wakeup?).and_return(false)
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end

      it "returns effective_polling_interval when wakeup is on but no listener attached yet" do
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        worker.notify_listener = nil
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end

      it "raises the wait to the NOTIFY_FALLBACK_POLL_SECONDS ceiling when a listener is active" do
        # When NOTIFY drives latency, the empty-fetch wait is a safety net,
        # not the steady-state cadence. The ceiling lets one missed NOTIFY
        # cost bounded latency, never a stuck queue.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        worker.notify_listener = fake_listener
        expect(worker.send(:wake_timeout))
          .to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
      end

      it "returns effective_polling_interval when the listener thread has died" do
        # A listener whose thread crashed (running? false) will never wake the
        # loop. Treating it as active would keep the wait at the long NOTIFY
        # ceiling — the loop would sleep 15s between retries while jobs pile
        # up. Fall back to the polling interval until it's restarted.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(fake_listener).to receive(:running?).and_return(false)
        worker.notify_listener = fake_listener
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end

      it "returns effective_polling_interval when a live listener is not delivering (pooler-deaf, issue #332)" do
        # A transaction-pooler LISTEN connection is alive (running? true) but its
        # self-NOTIFY probe failed, so it will never actually wake the loop.
        # Keeping the 15s NOTIFY ceiling would degrade latency to WORSE than a
        # plain no-NOTIFY deployment; fall back to fast polling instead.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(fake_listener).to receive_messages(running?: true, delivering?: false)
        worker.notify_listener = fake_listener
        expect(worker.send(:wake_timeout)).to eq(worker.config.polling_interval)
      end
    end

    describe ":supervisor scope (supervisor-mediated wakes, issue #381)" do
      let(:pipe_ios) { IO.pipe }
      let(:piped_worker) { described_class.new(queues: %w[default], threads: 5, wake_pipe: pipe_ios[0]) }

      after do
        piped_worker.wake_pipe&.stop
        pipe_ios.each { |io| io.close unless io.closed? }
      end

      def wait_for(timeout: 2)
        deadline = Time.now + timeout
        until yield
          raise "timed out waiting for condition" if Time.now > deadline

          sleep 0.01
        end
      end

      it "does not construct a local NotifyListener" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)
        allow(Pgbus::Process::NotifyListener).to receive(:new)

        piped_worker.send(:start_wake_source)

        expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
      end

      it "starts the wake-pipe watcher instead" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)

        piped_worker.send(:start_wake_source)

        expect(piped_worker.wake_pipe.running?).to be true
      end

      it "wakes the loop on a W byte from the supervisor" do
        piped_worker.send(:start_wake_source)

        pipe_ios[1].write(Pgbus::Process::WakePipe::WAKE)

        expect(piped_worker.wake_signal.wait(timeout: 2)).to be true
      end

      it "skips the per-fork listener self-healing loop" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)
        allow(piped_worker).to receive(:start_notify_listener)
        piped_worker.notify_retry_at = 0.0

        piped_worker.send(:ensure_notify_listener)

        expect(piped_worker).not_to have_received(:start_notify_listener)
      end

      it "uses the NOTIFY ceiling while the hub reports delivering" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)
        piped_worker.send(:start_wake_source)

        expect(piped_worker.send(:wake_timeout)).to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
      end

      it "falls back to fast polling when the hub broadcasts degraded" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)
        piped_worker.send(:start_wake_source)

        pipe_ios[1].write(Pgbus::Process::WakePipe::DEGRADED)
        wait_for { !piped_worker.wake_pipe.delivering? }

        expect(piped_worker.send(:wake_timeout)).to eq(piped_worker.config.polling_interval)
      end

      it "falls back to fast polling when the supervisor pipe reaches EOF" do
        allow(piped_worker).to receive(:notify_wakeup?).and_return(true)
        piped_worker.send(:start_wake_source)

        pipe_ios[1].close
        wait_for { !piped_worker.wake_pipe.delivering? }

        expect(piped_worker.send(:wake_timeout)).to eq(piped_worker.config.polling_interval)
      end

      it "stops the watcher via stop_wake_source" do
        piped_worker.send(:start_wake_source)
        piped_worker.send(:stop_wake_source)

        expect(piped_worker.wake_pipe.running?).to be false
      end
    end

    describe ":supervisor scope WITHOUT a wake pipe (hub failed to start)" do
      # The poll-only guarantee: a hub outage must not balloon the host back
      # to one direct LISTEN connection per fork — the fork polls until the
      # supervisor restarts (issue #381 review).
      let(:bare_worker) { described_class.new(queues: %w[default], threads: 5) }

      before do
        allow(bare_worker).to receive(:notify_wakeup?).and_return(true)
        allow(bare_worker.config).to receive(:worker_notify_scope).and_return(:supervisor)
      end

      it "start_wake_source builds no local listener" do
        allow(Pgbus::Process::NotifyListener).to receive(:new)

        bare_worker.send(:start_wake_source)

        expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
      end

      it "self-healing does not resurrect a local listener either" do
        allow(bare_worker).to receive(:start_notify_listener)
        bare_worker.notify_retry_at = 0.0

        bare_worker.send(:ensure_notify_listener)

        expect(bare_worker).not_to have_received(:start_notify_listener)
      end

      it "wake_timeout stays at the fast polling interval" do
        expect(bare_worker.send(:wake_timeout)).to eq(bare_worker.config.polling_interval)
      end
    end

    describe "#start_notify_listener" do
      # worker.config is the globally-memoized Pgbus.configuration, so mutating
      # polling_interval here leaks into any later example whose worker is also
      # built from the global config (everything in this file). Snapshot and
      # restore around every example in this block to keep #wake_timeout and
      # #effective_polling_interval reading the original value.
      around do |example|
        original = Pgbus.configuration.polling_interval
        example.run
      ensure
        Pgbus.configuration.polling_interval = original
      end

      it "does not construct a listener when notify_wakeup? is off" do
        allow(worker).to receive(:notify_wakeup?).and_return(false)
        allow(Pgbus::Process::NotifyListener).to receive(:new)
        worker.send(:start_notify_listener)
        expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
        expect(worker.notify_listener).to be_nil
      end

      it "constructs and starts a listener for every physical queue when wakeup is on" do
        prefix = worker.config.queue_prefix
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new).with(
          physical_queues: ["#{prefix}_default"],
          on_wake: an_instance_of(Proc),
          connection_options: { dbname: "test" },
          health_check_ms: an_instance_of(Integer),
          logger: Pgbus.logger
        )
        expect(fake_listener).to have_received(:start)
        expect(worker.notify_listener).to be(fake_listener)
      end

      it "clamps health_check_ms below 250ms up to the floor" do
        # polling_interval 0.1s → 100ms → clamped up to 250ms minimum.
        worker.config.polling_interval = 0.1
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new)
          .with(hash_including(health_check_ms: 250))
      end

      it "clamps health_check_ms above 5000ms down to the ceiling" do
        # polling_interval 30s → 30000ms → clamped down to 5000ms maximum.
        # The around hook above restores polling_interval after the example.
        worker.config.polling_interval = 30
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_return({ dbname: "test" })
        allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

        worker.send(:start_notify_listener)

        expect(Pgbus::Process::NotifyListener).to have_received(:new)
          .with(hash_including(health_check_ms: 5000))
      end

      it "swallows listener startup errors and falls back to polling without crashing the worker" do
        # A NotifyListener failure must NEVER take down the worker — that
        # was the whole point of making it default-on without a feature
        # flag at the worker layer. The rescue logs and leaves
        # @notify_listener nil; wake_timeout reverts to the polling
        # interval as if NOTIFY had never been enabled.
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        allow(worker.config).to receive(:worker_notify_connection_options)
          .and_raise(StandardError, "no direct port configured")
        allow(Pgbus.logger).to receive(:error)

        expect { worker.send(:start_notify_listener) }.not_to raise_error
        expect(worker.notify_listener).to be_nil
        expect(Pgbus.logger).to have_received(:error)
      end
    end

    describe "#ensure_notify_listener" do
      # The self-healing loop: a listener that never started (nil) or whose
      # thread died (running? false) must be retried from the worker loop,
      # throttled by exponential backoff so a persistent outage doesn't spin
      # start attempts every tick.
      let(:base) { described_class::NOTIFY_RETRY_BASE_SECONDS }

      before do
        allow(worker).to receive(:notify_wakeup?).and_return(true)
        # The local-listener lifecycle exists only under :fork scope; the
        # default is :supervisor (issue #381).
        allow(worker.config).to receive(:worker_notify_scope).and_return(:fork)
      end

      it "is a no-op when notify_wakeup? is off" do
        allow(worker).to receive(:notify_wakeup?).and_return(false)
        allow(worker).to receive(:start_notify_listener)
        worker.send(:ensure_notify_listener)
        expect(worker).not_to have_received(:start_notify_listener)
      end

      it "is a no-op while a healthy listener is running" do
        worker.notify_listener = fake_listener
        allow(worker).to receive(:start_notify_listener)
        worker.send(:ensure_notify_listener)
        expect(worker).not_to have_received(:start_notify_listener)
      end

      it "does not retry before the backoff window elapses" do
        # Listener is absent and a retry was just scheduled into the future.
        worker.notify_listener = nil
        worker.notify_retry_at = worker.send(:monotonic_now) + base
        allow(worker).to receive(:start_notify_listener)
        worker.send(:ensure_notify_listener)
        expect(worker).not_to have_received(:start_notify_listener)
      end

      it "retries start once the backoff window has elapsed" do
        worker.notify_listener = nil
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        allow(worker).to receive(:start_notify_listener) do
          worker.notify_listener = fake_listener
        end

        worker.send(:ensure_notify_listener)
        expect(worker).to have_received(:start_notify_listener)
      end

      it "doubles the backoff each time the restart fails" do
        worker.notify_listener = nil
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        # start leaves @notify_listener nil (failed to start).
        allow(worker).to receive(:start_notify_listener)

        worker.send(:ensure_notify_listener)
        expect(worker.notify_retry_backoff).to eq(base * 2)

        # Second failed attempt (advance clock past the new window) doubles again.
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        worker.send(:ensure_notify_listener)
        expect(worker.notify_retry_backoff).to eq(base * 4)
      end

      it "caps the backoff at NOTIFY_RETRY_MAX_SECONDS" do
        worker.notify_listener = nil
        worker.notify_retry_backoff = described_class::NOTIFY_RETRY_MAX_SECONDS
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        allow(worker).to receive(:start_notify_listener)

        worker.send(:ensure_notify_listener)
        expect(worker.notify_retry_backoff)
          .to eq(described_class::NOTIFY_RETRY_MAX_SECONDS)
      end

      it "resets the backoff after a successful restart" do
        worker.notify_listener = nil
        worker.notify_retry_backoff = base * 8
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        allow(worker).to receive(:start_notify_listener) do
          worker.notify_listener = fake_listener
        end

        worker.send(:ensure_notify_listener)
        expect(worker.notify_retry_backoff).to eq(base)
      end

      it "stops a dead listener before restarting it" do
        dead = fake_listener
        allow(dead).to receive(:running?).and_return(false)
        worker.notify_listener = dead
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        allow(worker).to receive(:start_notify_listener)

        worker.send(:ensure_notify_listener)
        expect(dead).to have_received(:stop)
      end

      it "reconciles queues after a successful restart (wildcard workers)" do
        worker.notify_listener = nil
        worker.notify_retry_at = worker.send(:monotonic_now) - 1
        allow(worker).to receive(:start_notify_listener) do
          worker.notify_listener = fake_listener
        end
        allow(worker).to receive(:sync_notify_listener_queues)

        worker.send(:ensure_notify_listener)
        expect(worker).to have_received(:sync_notify_listener_queues)
      end
    end

    describe "#sync_notify_listener_queues" do
      let(:multi_worker) { described_class.new(queues: %w[default low], threads: 4) }
      let(:queue_prefix) { multi_worker.config.queue_prefix }

      before { multi_worker.notify_listener = fake_listener }

      it "adds new physical queues and removes dropped ones based on the set difference" do
        # Listener currently watches default + statistics; @queues is
        # default + low. Expect statistics to be unlistened and low to
        # be added; default is untouched because it overlaps both sets.
        ch_prefix = Pgbus::Process::NotifyListener::CHANNEL_PREFIX
        ch_suffix = Pgbus::Process::NotifyListener::CHANNEL_SUFFIX
        allow(fake_listener).to receive(:listening_to).and_return(
          ["#{ch_prefix}#{queue_prefix}_default#{ch_suffix}",
           "#{ch_prefix}#{queue_prefix}_statistics#{ch_suffix}"]
        )

        multi_worker.send(:sync_notify_listener_queues)

        expect(fake_listener).to have_received(:add_queue).with("#{queue_prefix}_low")
        expect(fake_listener).to have_received(:remove_queue).with("#{queue_prefix}_statistics")
        expect(fake_listener).not_to have_received(:add_queue).with("#{queue_prefix}_default")
      end

      it "is a no-op when no listener is attached" do
        multi_worker.notify_listener = nil
        expect { multi_worker.send(:sync_notify_listener_queues) }.not_to raise_error
        expect(fake_listener).not_to have_received(:add_queue)
        expect(fake_listener).not_to have_received(:remove_queue)
      end

      it "logs and survives a listener error so a sync failure doesn't crash the loop" do
        allow(fake_listener).to receive(:listening_to).and_raise(StandardError, "listener gone")
        allow(Pgbus.logger).to receive(:warn)

        expect { multi_worker.send(:sync_notify_listener_queues) }.not_to raise_error
        expect(Pgbus.logger).to have_received(:warn)
      end
    end

    describe "#shutdown" do
      it "stops the notify listener before draining the pool" do
        worker.notify_listener = fake_listener
        worker.send(:shutdown)
        expect(fake_listener).to have_received(:stop)
      end

      it "is a no-op for the listener when none was started (default-off path)" do
        worker.notify_listener = nil
        expect { worker.send(:shutdown) }.not_to raise_error
      end

      it "bounds the residual pool wait with POOL_TERMINATION_WAIT, not a second drain window (issue #386)" do
        # The drain loop already waited up to config.drain_timeout for in-flight
        # jobs; a job still running here has proven it won't finish, and a long
        # residual wait only pushes the worker past the supervisor's
        # shutdown_timeout deadline.
        worker.send(:shutdown)

        expect(pool).to have_received(:wait_for_termination).with(described_class::POOL_TERMINATION_WAIT)
      end
    end
  end

  describe "#stamp_loop_tick liveness pipe" do
    let(:reader_writer) { IO.pipe }
    let(:reader) { reader_writer[0] }
    let(:writer) { reader_writer[1] }

    after do
      reader.close unless reader.closed?
      writer.close unless writer.closed?
    end

    it "writes a liveness byte to the pipe when a liveness_pipe is given" do
      w = described_class.new(queues: %w[default], threads: 5, liveness_pipe: writer)

      w.send(:stamp_loop_tick)

      expect(reader.read_nonblock(16)).to eq("\0")
    end

    it "still advances the DB loop-tick timestamp when a pipe is present" do
      w = described_class.new(queues: %w[default], threads: 5, liveness_pipe: writer)

      w.send(:stamp_loop_tick)

      expect(w.last_loop_tick).to be_a(Float)
    end

    it "does not write to a pipe and behaves as today when no liveness_pipe is given" do
      w = described_class.new(queues: %w[default], threads: 5)

      expect { w.send(:stamp_loop_tick) }.not_to raise_error
      expect(w.last_loop_tick).to be_a(Float)
    end

    it "does not raise when the pipe is full (write dropped, worker still alive)" do
      w = described_class.new(queues: %w[default], threads: 5, liveness_pipe: writer)
      # Fill the OS pipe buffer so the next write_nonblock cannot proceed.
      filler = "x" * 65_536
      begin
        writer.write_nonblock(filler)
      rescue IO::WaitWritable
        # already full
      end

      expect { w.send(:stamp_loop_tick) }.not_to raise_error
    end

    it "does not raise when the reader end is already closed" do
      w = described_class.new(queues: %w[default], threads: 5, liveness_pipe: writer)
      reader.close

      expect { w.send(:stamp_loop_tick) }.not_to raise_error
    end
  end
end
