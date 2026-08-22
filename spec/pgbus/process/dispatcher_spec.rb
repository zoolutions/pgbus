# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Process::Dispatcher do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:dispatcher) { described_class.new }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  # Stubs Pgbus.logger to capture the resolved text of every info-level block,
  # so a single summary line can be asserted precisely (info fires several
  # times per pass; a plain have_received matcher checks only the first call).
  def capture_info_logs
    logged = []
    logger = instance_double(Logger, warn: nil, error: nil, debug: nil)
    allow(logger).to receive(:info) { |&block| logged << block.call if block }
    allow(Pgbus).to receive(:logger).and_return(logger)
    logged
  end

  describe described_class::LoopOutcome do
    subject(:outcome) { described_class.new }

    it "returns nil when nothing was attempted" do
      expect(outcome.result).to be_nil
    end

    it "returns nil when every item succeeded" do
      outcome.record_success
      outcome.record_success
      expect(outcome.result).to be_nil
    end

    it "returns nil on a partial failure (at least one success)" do
      outcome.record_success
      outcome.record_failure(StandardError.new("boom"))
      expect(outcome.result).to be_nil
    end

    it "returns the first error when every attempt failed" do
      first = StandardError.new("first")
      outcome.record_failure(first)
      outcome.record_failure(StandardError.new("second"))
      expect(outcome.result).to be(first)
    end
  end

  describe "constants" do
    it "has a cleanup interval of 1 hour" do
      expect(described_class::CLEANUP_INTERVAL).to eq(3600)
    end

    it "has a reap interval of 5 minutes" do
      expect(described_class::REAP_INTERVAL).to eq(300)
    end

    it "has a concurrency interval of 5 minutes" do
      expect(described_class::CONCURRENCY_INTERVAL).to eq(300)
    end

    it "has a batch cleanup interval of 1 hour" do
      expect(described_class::BATCH_CLEANUP_INTERVAL).to eq(3600)
    end

    it "has a recurring cleanup interval of 1 hour" do
      expect(described_class::RECURRING_CLEANUP_INTERVAL).to eq(3600)
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      dispatcher.graceful_shutdown
      expect(dispatcher.shutting_down?).to be true
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down flag" do
      dispatcher.immediate_shutdown
      expect(dispatcher.shutting_down?).to be true
    end
  end

  describe "#cleanup_processed_events (private)" do
    it "deletes expired processed events" do
      scope = double("scope", delete_all: 5)
      allow(Pgbus::ProcessedEvent).to receive(:expired).and_return(scope)

      dispatcher.send(:cleanup_processed_events)

      expect(Pgbus::ProcessedEvent).to have_received(:expired).with(a_kind_of(Time))
      expect(scope).to have_received(:delete_all)
    end

    it "returns early when idempotency_ttl is not set" do
      original_ttl = dispatcher.config.idempotency_ttl
      dispatcher.config.idempotency_ttl = nil
      allow(Pgbus::ProcessedEvent).to receive(:expired)

      dispatcher.send(:cleanup_processed_events)

      expect(Pgbus::ProcessedEvent).not_to have_received(:expired)
    ensure
      dispatcher.config.idempotency_ttl = original_ttl
    end

    it "rescues StandardError and logs a warning" do
      allow(Pgbus::ProcessedEvent).to receive(:expired).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_processed_events) }.not_to raise_error
    end
  end

  describe "#reap_stale_processes (private)" do
    it "deletes stale processes" do
      scope = double("scope", delete_all: 2)
      allow(Pgbus::ProcessEntry).to receive(:stale).and_return(scope)

      dispatcher.send(:reap_stale_processes)

      expect(Pgbus::ProcessEntry).to have_received(:stale).with(a_kind_of(Time))
      expect(scope).to have_received(:delete_all)
    end

    it "rescues StandardError and logs a warning" do
      allow(Pgbus::ProcessEntry).to receive(:stale).and_raise(StandardError, "db error")
      expect { dispatcher.send(:reap_stale_processes) }.not_to raise_error
    end
  end

  def past_monotonic(seconds_ago)
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - seconds_ago
  end

  describe "#run_maintenance (private)" do
    it "skips cleanup when interval not elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.send(:run_maintenance)

      expect(dispatcher).not_to have_received(:cleanup_processed_events)
      expect(dispatcher).not_to have_received(:reap_stale_processes)
    end

    it "runs cleanup when cleanup interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.set_maintenance_timestamp(:@last_cleanup_at, past_monotonic(described_class::CLEANUP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_processed_events)
    end

    it "runs reap when reap interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_processed_events)
      allow(dispatcher).to receive(:reap_stale_processes)

      dispatcher.set_maintenance_timestamp(:@last_reap_at, past_monotonic(described_class::REAP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:reap_stale_processes)
    end

    it "runs concurrency cleanup when concurrency interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_concurrency)

      dispatcher.set_maintenance_timestamp(:@last_concurrency_at, past_monotonic(described_class::CONCURRENCY_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_concurrency)
    end

    it "runs batch cleanup when batch interval has elapsed" do
      allow(dispatcher).to receive(:cleanup_batches)

      dispatcher.set_maintenance_timestamp(:@last_batch_cleanup_at, past_monotonic(described_class::BATCH_CLEANUP_INTERVAL + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:cleanup_batches)
    end

    it "rescues errors from maintenance methods" do
      dispatcher.set_maintenance_timestamp(:@last_cleanup_at, past_monotonic(described_class::CLEANUP_INTERVAL + 1))
      allow(dispatcher).to receive(:cleanup_processed_events).and_raise(StandardError, "boom")
      expect { dispatcher.send(:run_maintenance) }.not_to raise_error
    end

    it "releases pooled AR connections after the cycle so an idle backend is never reused" do
      allow(dispatcher).to receive(:release_maintenance_connections)

      dispatcher.set_maintenance_timestamp(:@last_reap_at, past_monotonic(described_class::REAP_INTERVAL + 1))
      allow(dispatcher).to receive(:reap_stale_processes)
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:release_maintenance_connections)
    end

    it "still releases connections even when a maintenance task raised" do
      allow(dispatcher).to receive(:release_maintenance_connections)
      dispatcher.set_maintenance_timestamp(:@last_cleanup_at, past_monotonic(described_class::CLEANUP_INTERVAL + 1))
      allow(dispatcher).to receive(:cleanup_processed_events).and_raise(StandardError, "boom")

      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:release_maintenance_connections)
    end
  end

  describe "#release_maintenance_connections (private)" do
    it "clears active connections back to the pool" do
      handler = instance_double(ActiveRecord::ConnectionAdapters::ConnectionHandler, clear_active_connections!: nil)
      allow(Pgbus::BusRecord).to receive(:connection_handler).and_return(handler)

      dispatcher.send(:release_maintenance_connections)

      expect(handler).to have_received(:clear_active_connections!)
    end

    it "never raises when the pool release itself fails" do
      handler = instance_double(ActiveRecord::ConnectionAdapters::ConnectionHandler)
      allow(handler).to receive(:clear_active_connections!).and_raise(StandardError, "pool gone")
      allow(Pgbus::BusRecord).to receive(:connection_handler).and_return(handler)

      expect { dispatcher.send(:release_maintenance_connections) }.not_to raise_error
    end
  end

  describe "#cleanup_concurrency (private)" do
    it "expires stale semaphores and promotes blocked executions" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive_messages(expire_stale: 0, promote_next: false)

      dispatcher.send(:cleanup_concurrency)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:expire_stale)
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:expire_stale)
    end

    it "promotes blocked executions atomically" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_return([{ "key" => "TestJob-42" }])
      allow(Pgbus::Concurrency::BlockedExecution).to receive_messages(expire_stale: 0, promote_next: true)

      dispatcher.send(:cleanup_concurrency)

      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next).with("TestJob-42", client: mock_client)
    end

    it "rescues errors gracefully" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_concurrency) }.not_to raise_error
    end

    it "logs a terminated-connection error calmly (info, not warn) since it self-heals next cycle" do
      terminated = StandardError.new(
        "PQconsumeInput() FATAL:  terminating connection due to administrator command"
      )
      allow(Pgbus::Concurrency::Semaphore).to receive(:expire_stale).and_raise(terminated)
      logger = instance_double(Logger, warn: nil, error: nil, debug: nil, info: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)

      dispatcher.send(:cleanup_concurrency)

      expect(logger).to have_received(:info)
      expect(logger).not_to have_received(:warn)
    end
  end

  describe "#cleanup_batches (private)" do
    it "cleans up finished batches older than 7 days" do
      allow(Pgbus::Batch).to receive(:cleanup).and_return(3)

      dispatcher.send(:cleanup_batches)

      expect(Pgbus::Batch).to have_received(:cleanup).with(older_than: a_kind_of(Time))
    end

    it "rescues errors gracefully" do
      allow(Pgbus::Batch).to receive(:cleanup).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_batches) }.not_to raise_error
    end
  end

  describe "#compact_archives (private)" do
    let(:connection) { double("connection") }

    before do
      dispatcher.config.archive_retention = 7 * 24 * 3600
      prefix = dispatcher.config.queue_prefix
      allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
      allow(connection).to receive(:select_values).and_return(["#{prefix}_default", "#{prefix}_events"])
      allow(mock_client).to receive(:purge_archive).and_return(0)
      # No streams registered — compact_archives handles only job queues.
      allow(Pgbus::StreamQueue).to receive_messages(reset_cache!: nil, known_names: Set.new)
    end

    it "skips queues registered as streams (handled by prune_stream_archives)" do
      allow(mock_client).to receive(:purge_archive).and_return(5)
      prefix = dispatcher.config.queue_prefix
      allow(Pgbus::StreamQueue).to receive(:known_names).and_return(Set.new(["#{prefix}_events"]))

      dispatcher.send(:compact_archives)

      expect(mock_client).to have_received(:purge_archive).with("default", older_than: a_kind_of(Time), batch_size: 1000)
      expect(mock_client).not_to have_received(:purge_archive).with("events", any_args)
    end

    it "purges archive entries older than retention period" do
      allow(mock_client).to receive(:purge_archive).and_return(5)

      dispatcher.send(:compact_archives)

      expect(mock_client).to have_received(:purge_archive).with("default", older_than: a_kind_of(Time), batch_size: 1000)
      expect(mock_client).to have_received(:purge_archive).with("events", older_than: a_kind_of(Time), batch_size: 1000)
    end

    it "skips when archive_retention is nil" do
      dispatcher.config.archive_retention = nil

      dispatcher.send(:compact_archives)

      expect(mock_client).not_to have_received(:purge_archive)
    end

    it "rescues errors gracefully" do
      allow(connection).to receive(:select_values).and_raise(StandardError, "db error")
      expect { dispatcher.send(:compact_archives) }.not_to raise_error
    end

    it "rescues per-queue errors without stopping others" do
      call_count = 0
      allow(mock_client).to receive(:purge_archive) do |queue_name, **_opts|
        call_count += 1
        raise StandardError, "fail" if queue_name == "default"

        3
      end

      dispatcher.send(:compact_archives)

      expect(call_count).to eq(2)
    end

    it "returns the first error when every queue fails (surfaces total outage)" do
      allow(mock_client).to receive(:purge_archive).and_raise(StandardError, "db down")

      result = dispatcher.send(:compact_archives)

      expect(result).to be_a(StandardError)
    end

    it "does not surface a failure when at least one queue succeeds (partial)" do
      allow(mock_client).to receive(:purge_archive) do |queue_name, **_opts|
        raise StandardError, "fail" if queue_name == "default"

        3
      end

      result = dispatcher.send(:compact_archives)

      expect(result).not_to be_a(StandardError)
    end

    it "stops before the next queue when shutdown begins mid-loop" do
      allow(mock_client).to receive(:purge_archive) do |_queue_name, **_opts|
        dispatcher.graceful_shutdown
        1
      end

      dispatcher.send(:compact_archives)

      expect(mock_client).to have_received(:purge_archive).once
      expect(mock_client).to have_received(:purge_archive).with("default", older_than: a_kind_of(Time), batch_size: 1000)
      expect(mock_client).not_to have_received(:purge_archive).with("events", any_args)
    end

    it "logs a single summary line when interrupted mid-loop" do
      logged = capture_info_logs
      allow(mock_client).to receive(:purge_archive) do |_queue_name, **_opts|
        dispatcher.graceful_shutdown
        0
      end

      dispatcher.send(:compact_archives)

      expect(logged.grep(/Archive compaction interrupted by shutdown after 1 of 2/).size).to eq(1)
    end
  end

  describe "#prune_stream_archives (private)" do
    let(:connection) { double("connection") }

    before do
      prefix = dispatcher.config.queue_prefix
      stream_names = ["#{prefix}_room1", "#{prefix}_room2"]
      allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
      allow(connection).to receive(:select_values).and_return(stream_names)
      allow(mock_client).to receive(:purge_archive).and_return(0)
      # Both room queues are known streams (named like job queues).
      allow(Pgbus::StreamQueue).to receive_messages(reset_cache!: nil, known_names: Set.new(stream_names))
    end

    it "prunes every stream queue when no shutdown occurs" do
      dispatcher.send(:prune_stream_archives)

      expect(mock_client).to have_received(:purge_archive).twice
    end

    it "purges with the ARCHIVE_COMPACTION_BATCH_SIZE constant, not a config accessor" do
      # archive_compaction_batch_size was culled from Configuration into the
      # ARCHIVE_COMPACTION_BATCH_SIZE constant (ca5d346); prune_stream_archives
      # must use the constant like compact_archives does. Reading a removed
      # accessor would raise NoMethodError and silently skip all pruning.
      dispatcher.send(:prune_stream_archives)

      expect(mock_client).to have_received(:purge_archive)
        .with(anything, older_than: a_kind_of(Time), batch_size: described_class::ARCHIVE_COMPACTION_BATCH_SIZE)
        .twice
    end

    it "does not depend on a config.archive_compaction_batch_size accessor" do
      # The accessor does not exist on Configuration; guard against its
      # reintroduction as a dependency.
      expect(dispatcher.config).not_to respond_to(:archive_compaction_batch_size)
      expect { dispatcher.send(:prune_stream_archives) }.not_to raise_error
      expect(mock_client).to have_received(:purge_archive).twice
    end

    it "stops before the next stream queue when shutdown begins mid-loop" do
      allow(mock_client).to receive(:purge_archive) do |_queue_name, **_opts|
        dispatcher.graceful_shutdown
        1
      end

      dispatcher.send(:prune_stream_archives)

      expect(mock_client).to have_received(:purge_archive).once
    end

    it "logs a single summary line when interrupted mid-loop" do
      logged = capture_info_logs
      allow(mock_client).to receive(:purge_archive) do |_queue_name, **_opts|
        dispatcher.graceful_shutdown
        0
      end

      dispatcher.send(:prune_stream_archives)

      expect(logged.grep(/Stream archive compaction interrupted by shutdown after 1 of 2/).size).to eq(1)
    end
  end

  describe "#sweep_orphan_streams (private)" do
    let(:connection) { double("connection") }

    before do
      prefix = dispatcher.config.queue_prefix
      stream_names = ["#{prefix}_room1", "#{prefix}_room2"]
      allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
      # Every queue reads as empty, so both are drop candidates.
      allow(connection).to receive_messages(
        select_values: stream_names,
        select_one: { "queue_length" => 0 }
      )
      allow(mock_client).to receive(:drop_queue).and_return(true)
      allow(Pgbus::StreamQueue).to receive_messages(reset_cache!: nil, known_names: Set.new(stream_names))
    end

    it "drops every empty orphan queue when no shutdown occurs" do
      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).to have_received(:drop_queue).twice
    end

    it "stops before checking the next queue when shutdown begins mid-loop" do
      allow(mock_client).to receive(:drop_queue) do |*_args, **_opts|
        dispatcher.graceful_shutdown
        true
      end

      dispatcher.send(:sweep_orphan_streams)

      expect(mock_client).to have_received(:drop_queue).once
      expect(connection).to have_received(:select_one).once
    end

    it "logs a single summary line when interrupted mid-loop" do
      logged = capture_info_logs
      allow(mock_client).to receive(:drop_queue) do |*_args, **_opts|
        dispatcher.graceful_shutdown
        true
      end

      dispatcher.send(:sweep_orphan_streams)

      expect(logged.grep(/Orphan stream sweep interrupted by shutdown after 1 of 2/).size).to eq(1)
    end
  end

  describe "#run_maintenance includes archive compaction" do
    it "runs compact_archives when interval has elapsed" do
      allow(dispatcher).to receive(:compact_archives)

      dispatcher.set_maintenance_timestamp(:@last_archive_compaction_at, past_monotonic(3601))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:compact_archives)
    end
  end

  describe "#cleanup_recurring_executions (private)" do
    it "deletes old recurring execution records" do
      relation = instance_double(ActiveRecord::Relation, delete_all: 5)
      allow(Pgbus::RecurringExecution).to receive(:older_than).and_return(relation)

      dispatcher.send(:cleanup_recurring_executions)

      expect(Pgbus::RecurringExecution).to have_received(:older_than).with(a_kind_of(Time))
      expect(relation).to have_received(:delete_all)
    end

    it "rescues errors gracefully" do
      allow(Pgbus::RecurringExecution).to receive(:older_than).and_raise(StandardError, "db error")
      expect { dispatcher.send(:cleanup_recurring_executions) }.not_to raise_error
    end

    it "skips when retention is not positive" do
      allow(Pgbus.configuration).to receive(:recurring_execution_retention).and_return(0)
      allow(Pgbus::RecurringExecution).to receive(:older_than)

      dispatcher.send(:cleanup_recurring_executions)

      expect(Pgbus::RecurringExecution).not_to have_received(:older_than)
    end
  end

  describe "#cleanup_stats (private)" do
    before do
      stub_const("Pgbus::JobStat", Class.new)
      allow(Pgbus::JobStat).to receive(:cleanup!).and_return(10)
    end

    it "cleans up old job stats" do
      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).to have_received(:cleanup!).with(older_than: a_kind_of(Time))
    end

    it "skips when stats_enabled is false" do
      allow(dispatcher.config).to receive(:stats_enabled).and_return(false)

      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).not_to have_received(:cleanup!)
    end

    it "skips when stats_retention is not positive" do
      allow(dispatcher.config).to receive(:stats_retention).and_return(0)

      dispatcher.send(:cleanup_stats)

      expect(Pgbus::JobStat).not_to have_received(:cleanup!)
    end
  end

  describe "#run_table_maintenance (private)" do
    let(:raw_conn) { double("raw_connection") }
    let(:connection) { double("connection", raw_connection: raw_conn) }

    before do
      allow(ActiveRecord::Base).to receive(:connection).and_return(connection)
    end

    it "runs table maintenance via TableMaintenance module" do
      allow(Pgbus::TableMaintenance).to receive(:run_maintenance).and_return(2)

      dispatcher.send(:run_table_maintenance)

      expect(Pgbus::TableMaintenance).to have_received(:run_maintenance).with(
        raw_conn,
        threshold: Pgbus::TableMaintenance::BLOAT_THRESHOLD,
        reindex: true,
        stop_check: a_kind_of(Proc)
      )
    end

    it "rescues errors gracefully" do
      allow(ActiveRecord::Base).to receive(:connection).and_raise(StandardError, "db error")
      expect { dispatcher.send(:run_table_maintenance) }.not_to raise_error
    end

    it "passes a stop_check that reflects the shutdown flag" do
      captured = nil
      allow(Pgbus::TableMaintenance).to receive(:run_maintenance) do |_conn, **opts|
        captured = opts[:stop_check]
        0
      end

      dispatcher.send(:run_table_maintenance)

      expect(captured).to respond_to(:call)
      expect(captured.call).to be false
      dispatcher.graceful_shutdown
      expect(captured.call).to be true
    end
  end

  describe "#run_maintenance includes table maintenance" do
    it "runs table maintenance when interval has elapsed" do
      allow(dispatcher).to receive(:run_table_maintenance)

      interval = Pgbus::Process::Dispatcher::TABLE_MAINTENANCE_INTERVAL
      dispatcher.set_maintenance_timestamp(:@last_table_maintenance_at, past_monotonic(interval + 1))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).to have_received(:run_table_maintenance)
    end

    it "skips table maintenance when interval has not elapsed" do
      allow(dispatcher).to receive(:run_table_maintenance)

      dispatcher.set_maintenance_timestamp(:@last_table_maintenance_at, Process.clock_gettime(Process::CLOCK_MONOTONIC))
      dispatcher.send(:run_maintenance)

      expect(dispatcher).not_to have_received(:run_table_maintenance)
    end
  end

  describe "#start_heartbeat (private)" do
    it "creates and starts a heartbeat with a loop_tick_supplier" do
      dispatcher.send(:start_heartbeat)
      expect(Pgbus::Process::Heartbeat).to have_received(:new).with(
        kind: "dispatcher",
        metadata: { pid: Process.pid },
        loop_tick_supplier: kind_of(Proc)
      )
      expect(heartbeat).to have_received(:start)
    end

    it "supplies the latest stamped loop tick to the heartbeat" do
      supplier = nil
      allow(Pgbus::Process::Heartbeat).to receive(:new) do |**kwargs|
        supplier = kwargs[:loop_tick_supplier]
        heartbeat
      end

      dispatcher.send(:start_heartbeat)
      expect(supplier.call).to be_nil

      dispatcher.send(:stamp_loop_tick)
      expect(supplier.call).to be_within(1).of(Time.now.to_f)
    end
  end

  describe "#stamp_loop_tick (private)" do
    it "advances the beacon on each call" do
      dispatcher.send(:stamp_loop_tick)
      first = dispatcher.last_loop_tick
      expect(first).to be_within(1).of(Time.now.to_f)
    end
  end

  describe "maintenance backoff during systemic outages" do
    # Drive run_maintenance with every task due and force each task to either
    # raise (outage) or succeed. monotonic_now is stubbed so backoff windows
    # are deterministic. All task methods are stubbed; the real run_if_due
    # runs, so its :success/:failed/:skipped result and the streak/backoff
    # logic in run_maintenance are exercised end to end.
    let(:logger) { instance_double(Logger, info: nil, debug: nil, warn: nil, error: nil) }

    # Maps each maintenance timestamp ivar to its task method, matching
    # run_maintenance. Keeping this in the spec lets us force every task due
    # and control which succeed or fail.
    let(:tasks) do
      {
        "@last_cleanup_at": :cleanup_processed_events,
        "@last_reap_at": :reap_stale_processes,
        "@last_concurrency_at": :cleanup_concurrency,
        "@last_batch_cleanup_at": :cleanup_batches,
        "@last_recurring_cleanup_at": :cleanup_recurring_executions,
        "@last_archive_compaction_at": :compact_archives,
        "@last_stream_archive_compaction_at": :prune_stream_archives,
        "@last_outbox_cleanup_at": :cleanup_outbox,
        "@last_job_lock_cleanup_at": :cleanup_job_locks,
        "@last_stats_cleanup_at": :cleanup_stats,
        "@last_orphan_stream_sweep_at": :sweep_orphan_streams,
        "@last_table_maintenance_at": :run_table_maintenance
      }
    end

    let(:task_methods) { tasks.values }
    let(:clock) { 1_000_000.0 }

    before do
      allow(Pgbus).to receive(:logger).and_return(logger)
      allow(dispatcher).to receive(:monotonic_now) { clock }
      allow(dispatcher.config).to receive(:streams_orphan_sweep_interval).and_return(3600)
      allow(Pgbus::ErrorReporter).to receive(:report)
    end

    # Make every task due as of the current stubbed clock.
    def force_all_due
      force_due(tasks.keys)
    end

    # Make only the listed timestamp ivars due as of the current stubbed clock.
    def force_due(ivars)
      ivars.each { |ivar| dispatcher.set_maintenance_timestamp(ivar, clock - 1_000_000) }
    end

    # Reset every timestamp ivar to now, so no task is due until forced.
    # (initialize runs before monotonic_now is stubbed, so the ivars start at
    # the real monotonic clock — normalize them here.)
    def reset_all_not_due
      tasks.each_key { |ivar| dispatcher.set_maintenance_timestamp(ivar, clock) }
    end

    # Stub every task method: fail the named tasks (default: all), succeed the rest.
    def stub_tasks(failing: task_methods)
      task_methods.each do |method|
        if failing.include?(method)
          allow(dispatcher).to receive(method).and_raise(StandardError, "db down: #{method}")
        else
          allow(dispatcher).to receive(method)
        end
      end
    end

    def run_cycle(now: clock)
      dispatcher.send(:run_maintenance, now)
    end

    it "reports every task error individually on the first failing cycle" do
      force_all_due
      stub_tasks

      run_cycle

      expect(Pgbus::ErrorReporter).to have_received(:report).exactly(tasks.size).times
      expect(logger).not_to have_received(:warn).with(a_string_matching(/backoff/i))
    end

    it "enters backoff after two consecutive all-failed cycles with a single summary warn" do
      force_all_due
      stub_tasks
      run_cycle # streak -> 1, per-task reports

      # Second all-failed cycle: streak -> 2, enter backoff, one summary warn,
      # per-task reports suppressed.
      force_all_due
      run_cycle

      expect(logger).to have_received(:warn).once
      expect(dispatcher.maintenance_failure_streak).to eq(2)
    end

    it "suppresses per-task ErrorReporter calls once in backoff" do
      force_all_due
      stub_tasks
      run_cycle # first failing cycle: tasks.size reports

      force_all_due
      run_cycle # enters backoff: no per-task reports, summary warn only

      expect(Pgbus::ErrorReporter).to have_received(:report).exactly(tasks.size).times
    end

    it "skips all maintenance while inside the backoff window" do
      force_all_due
      stub_tasks
      run_cycle
      force_all_due
      run_cycle # now in backoff until clock + BACKOFF_BASE

      force_all_due
      run_cycle(now: clock + 1) # still inside backoff window

      # The third cycle is inside the backoff window, so no task runs again:
      # each task was invoked exactly twice (the two pre-backoff cycles).
      task_methods.each do |method|
        expect(dispatcher).to have_received(method).exactly(2).times
      end
    end

    it "does not enter backoff on a partial failure (some tasks succeed)" do
      succeeding = :reap_stale_processes

      force_all_due
      stub_tasks(failing: task_methods - [succeeding])
      run_cycle
      force_all_due
      run_cycle

      expect(dispatcher.maintenance_failure_streak).to eq(0)
      expect(logger).not_to have_received(:warn)
    end

    it "resets the streak and restores cadence when a task succeeds" do
      force_all_due
      stub_tasks
      run_cycle # streak -> 1

      # Recovery cycle: all tasks succeed.
      task_methods.each { |method| allow(dispatcher).to receive(method) }
      force_all_due
      run_cycle

      expect(dispatcher.maintenance_failure_streak).to eq(0)
      expect(dispatcher.maintenance_backoff_until).to be_nil
    end

    it "exits backoff as soon as a maintenance task succeeds again" do
      force_all_due
      stub_tasks
      run_cycle
      force_all_due
      run_cycle # in backoff

      backoff_end = dispatcher.maintenance_backoff_until
      task_methods.each { |method| allow(dispatcher).to receive(method) } # all succeed
      force_all_due
      run_cycle(now: backoff_end + 1)

      expect(dispatcher.maintenance_failure_streak).to eq(0)
      expect(dispatcher.maintenance_backoff_until).to be_nil
    end

    it "doubles the backoff window per consecutive failed cycle" do
      stub_tasks
      base = described_class::MAINTENANCE_BACKOFF_BASE

      # Cycle 1: streak 1 (no backoff). Cycle 2: streak 2 -> base * 2**0.
      force_all_due
      run_cycle(now: clock)
      force_all_due
      run_cycle(now: clock)
      expect(dispatcher.maintenance_backoff_until).to eq(clock + base)

      # Next failing cycle after window: streak 3 -> base * 2**1.
      later = clock + base + 1
      force_all_due
      run_cycle(now: later)
      expect(dispatcher.maintenance_backoff_until).to eq(later + (base * 2))
    end

    it "caps the backoff window at MAINTENANCE_BACKOFF_MAX" do
      stub_tasks
      max = described_class::MAINTENANCE_BACKOFF_MAX

      now = clock
      window = nil
      # Drive many consecutive failing cycles, each starting just after the
      # previous window ends. Capture the last window (deadline - cycle start).
      20.times do
        force_all_due
        run_cycle(now: now)
        deadline = dispatcher.maintenance_backoff_until
        window = deadline - now if deadline
        now = (deadline || now) + 1
      end

      expect(window).to eq(max)
    end

    context "when a task swallows its own error and returns the exception" do
      # Most task methods rescue StandardError internally (to log a
      # descriptive warning) and hand the exception back to run_if_due via
      # `return e`. run_if_due must treat that as a failure just like a raise,
      # otherwise a real outage (where those rescues fire) would never trip
      # backoff.
      it "normalizes a returned StandardError to a :failed result" do
        dispatcher.set_maintenance_timestamp(:@last_cleanup_at, clock - 1_000_000)
        result = dispatcher.send(:run_if_due, clock, :@last_cleanup_at, described_class::CLEANUP_INTERVAL) do
          StandardError.new("db down")
        end

        expect(result.status).to eq(:failed)
        expect(result.error).to be_a(StandardError)
        # Timestamp must NOT advance on failure, so the next tick retries.
        expect(dispatcher.maintenance_timestamp(:@last_cleanup_at)).to eq(clock - 1_000_000)
      end

      it "enters backoff during a real outage where the task rescues fire" do
        # Drive genuine task bodies (task methods are NOT stubbed). Make their
        # underlying dependencies raise so each task's own rescue returns the
        # exception to run_if_due. Only the three tasks below are forced due;
        # the rest stay skipped, so this proves the swallow-and-return-e path
        # surfaces failure without depending on every task's guard clauses.
        allow(Pgbus::ProcessedEvent).to receive(:expired).and_raise(StandardError, "db down")
        allow(Pgbus::ProcessEntry).to receive(:stale).and_raise(StandardError, "db down")
        allow(Pgbus::Batch).to receive(:cleanup).and_raise(StandardError, "db down")

        due = %i[@last_cleanup_at @last_reap_at @last_batch_cleanup_at]
        reset_all_not_due

        force_due(due)
        run_cycle # first failing cycle: per-task reports, streak -> 1
        reset_all_not_due
        force_due(due)
        run_cycle # second failing cycle: streak -> 2, enter backoff

        expect(dispatcher.maintenance_failure_streak).to eq(2)
        expect(dispatcher.maintenance_backoff_until).to eq(clock + described_class::MAINTENANCE_BACKOFF_BASE)
      end
    end
  end

  describe "#reap_orphaned_uniqueness_keys (issue #418)" do
    let(:delete_scope) { double("scope", delete_all: 1) }
    let(:keys) { [] }

    def lock(lock_key:, queue_name:, msg_id:, created_at: 10.minutes.ago)
      double("UniquenessKey", lock_key: lock_key, queue_name: queue_name, msg_id: msg_id, created_at: created_at)
    end

    before do
      allow(Pgbus::UniquenessKey).to receive_messages(all: keys, where: delete_scope)
      allow(mock_client).to receive(:message_exists?)
      allow(mock_client).to receive(:uniqueness_keys_present).and_return(Set.new)
    end

    it "reaps an aged pending/msg_id=0 lock when no live queue holds the key" do
      orphan = lock(lock_key: "ERP::Manager", queue_name: "pending", msg_id: 0)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([orphan])
      allow(delete_scope).to receive(:delete_all).and_return(1)

      reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

      expect(reaped).to eq(1)
      expect(mock_client).to have_received(:uniqueness_keys_present).with(["ERP::Manager"])
      expect(mock_client).not_to have_received(:message_exists?)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: ["ERP::Manager"])
    end

    it "does not reap an aged pending/msg_id=0 lock whose key is still in a real queue" do
      inflight = lock(lock_key: "ERP::Manager", queue_name: "pending", msg_id: 0)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([inflight])
      allow(mock_client).to receive(:uniqueness_keys_present).and_return(Set["ERP::Manager"])

      reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

      expect(reaped).to eq(0)
      expect(mock_client).not_to have_received(:message_exists?)
      expect(Pgbus::UniquenessKey).not_to have_received(:where)
    end

    it "does not query the synthetic pending queue for msg_id=0 recurring locks" do
      recurring = lock(lock_key: "RecurringJob", queue_name: "default", msg_id: 0)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([recurring])
      allow(mock_client).to receive(:uniqueness_keys_present).and_return(Set["RecurringJob"])

      dispatcher.send(:reap_orphaned_uniqueness_keys)

      expect(mock_client).to have_received(:uniqueness_keys_present).with(["RecurringJob"])
      expect(mock_client).not_to have_received(:message_exists?)
    end

    it "uses msg_id lookup for bound locks and reaps when the message is gone" do
      bound = lock(lock_key: "Orphan:gone", queue_name: "default", msg_id: 99)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([bound])
      allow(mock_client).to receive(:message_exists?)
        .with("default", msg_id: 99, uniqueness_key: "Orphan:gone").and_return(false)
      allow(delete_scope).to receive(:delete_all).and_return(1)

      reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

      expect(reaped).to eq(1)
      expect(mock_client).not_to have_received(:uniqueness_keys_present)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: ["Orphan:gone"])
    end

    it "keeps a bound lock when message_exists? returns nil (unknown)" do
      bound = lock(lock_key: "Maybe", queue_name: "default", msg_id: 99)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([bound])
      allow(mock_client).to receive(:message_exists?)
        .with("default", msg_id: 99, uniqueness_key: "Maybe").and_return(nil)

      expect(dispatcher.send(:reap_orphaned_uniqueness_keys)).to eq(0)
      expect(Pgbus::UniquenessKey).not_to have_received(:where)
    end

    it "does not reap locks newer than visibility_timeout * 2" do
      recent = lock(lock_key: "Recent", queue_name: "pending", msg_id: 0, created_at: Time.current)
      allow(Pgbus::UniquenessKey).to receive(:all).and_return([recent])

      expect(dispatcher.send(:reap_orphaned_uniqueness_keys)).to eq(0)
      expect(mock_client).not_to have_received(:uniqueness_keys_present)
      expect(mock_client).not_to have_received(:message_exists?)
    end
  end
end
