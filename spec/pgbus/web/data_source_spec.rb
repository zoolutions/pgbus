# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource do
  subject(:data_source) { described_class.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }
  let(:mock_connection) { double("ActiveRecord::Connection") }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    # Several DataSource methods touch AR model classes (QueueState, RecurringTask)
    # as side paths — e.g. #summary_stats reads recurring_tasks_count and
    # fetch_queues_with_metrics reads paused_queue_names. These specs mock only
    # the AR connection, so under ActiveRecord 7.1 the eager schema load asks the
    # bare connection double for :schema_cache, which raises RSpec's
    # MockExpectationError (an Exception, NOT a StandardError, so those methods'
    # `rescue StandardError` misses it). AR 8.1 resolves schema lazily and never
    # touches the double, which is why the gap only surfaces on the 7.1 endpoint.
    # Stubbing the class-method side paths makes behavior identical across Rails
    # versions. Dedicated describe blocks re-stub these where they matter.
    allow(Pgbus::QueueState).to receive(:paused).and_return(
      instance_double(ActiveRecord::Relation, pluck: [])
    )
    allow(Pgbus::RecurringTask).to receive(:count).and_return(0)
  end

  describe "#queues_with_metrics" do
    it "returns formatted metrics via batched SQL" do
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_default"])
      allow(mock_connection).to receive(:quote) { |v| "'#{v}'" }
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{
                             "queue_name" => "pgbus_default",
                             "queue_length" => 5,
                             "queue_visible_length" => 3,
                             "oldest_msg_age_sec" => 120,
                             "newest_msg_age_sec" => 10,
                             "total_messages" => 1000
                           }]))

      result = data_source.queues_with_metrics
      expect(result.size).to eq(1)
      expect(result.first[:name]).to eq("pgbus_default")
      expect(result.first[:queue_length]).to eq(5)
      expect(result.first[:queue_visible_length]).to eq(3)
      expect(result.first[:total_messages]).to eq(1000)
    end

    it "returns empty array on error" do
      allow(mock_connection).to receive(:select_values).and_raise(StandardError)

      expect(data_source.queues_with_metrics).to eq([])
    end
  end

  describe "#stream_queue_names" do
    it "returns the freshly-loaded known set (registry ∪ fingerprints, issues #359/#366)" do
      # Fresh per call: the health verdict runs on coarse intervals, and a
      # long-lived process must see streams registered since the last check.
      # known_names also includes dormant pre-registry streams by fingerprint.
      allow(Pgbus::StreamQueue).to receive(:reset_cache!)
      allow(Pgbus::StreamQueue).to receive(:known_names)
        .and_return(Set.new(%w[pgbus_chat_1_messages pgbus_dormant_99]))

      expect(data_source.stream_queue_names)
        .to eq(Set.new(%w[pgbus_chat_1_messages pgbus_dormant_99]))
      expect(Pgbus::StreamQueue).to have_received(:reset_cache!).ordered
      expect(Pgbus::StreamQueue).to have_received(:known_names).ordered
    end
  end

  describe "#unregistered_stream_queue_count" do
    it "counts fingerprint matches missing from the registry (issue #366)" do
      allow(Pgbus::StreamQueue).to receive_messages(
        table_exists?: true,
        all_names: Set.new(%w[pgbus_chat_1]),
        fingerprint_matched_names: Set.new(%w[pgbus_chat_1 pgbus_dormant_99 pgbus_checkout_3])
      )

      expect(data_source.unregistered_stream_queue_count).to eq(2)
    end

    it "returns 0 when the registry table is absent" do
      allow(Pgbus::StreamQueue).to receive(:table_exists?).and_return(false)

      expect(data_source.unregistered_stream_queue_count).to eq(0)
    end
  end

  describe "#processes" do
    let(:worker_metadata) do
      {
        "queues" => %w[default],
        "rates" => { "processed" => 12.4, "failed" => 0.2, "dequeued" => 10.1 },
        "jobs_processed" => 42,
        "jobs_failed" => 1,
        "in_flight" => 3
      }
    end

    let(:process_row) do
      {
        "id" => 7,
        "kind" => "worker",
        "hostname" => "host-1",
        "pid" => 1234,
        "metadata" => JSON.generate(worker_metadata),
        "last_heartbeat_at" => Time.now,
        "created_at" => Time.now
      }
    end

    it "passes worker throughput rates through in metadata" do
      allow(mock_connection).to receive(:select_all).and_return(double(to_a: [process_row]))

      result = data_source.processes

      expect(result.size).to eq(1)
      expect(result.first[:metadata]).to include(
        "rates" => { "processed" => 12.4, "failed" => 0.2, "dequeued" => 10.1 },
        "jobs_processed" => 42
      )
    end

    it "returns an empty array on error" do
      allow(mock_connection).to receive(:select_all).and_raise(StandardError)

      expect(data_source.processes).to eq([])
    end
  end

  describe "#queue_detail" do
    it "returns formatted metrics for a single queue" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return({
                      "queue_length" => 10,
                      "queue_visible_length" => 8,
                      "oldest_msg_age_sec" => 60,
                      "newest_msg_age_sec" => 5,
                      "total_messages" => 500
                    })

      result = data_source.queue_detail("pgbus_critical")
      expect(result[:name]).to eq("pgbus_critical")
      expect(result[:queue_length]).to eq(10)
    end

    it "returns nil when metrics query returns nil" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Queue Metrics")
        .and_return(nil)

      expect(data_source.queue_detail("missing")).to be_nil
    end

    it "exposes a vt-aware oldest_claimable_age_sec, nil when only parked messages remain" do
      captured_sql = nil
      allow(mock_connection).to receive(:select_one) do |sql, _label|
        captured_sql = sql
        {
          "queue_length" => 1,
          "queue_visible_length" => 0,
          "oldest_msg_age_sec" => 17_045,
          "newest_msg_age_sec" => 17_045,
          "oldest_claimable_age_sec" => nil,
          "total_messages" => 500
        }
      end

      result = data_source.queue_detail("pgbus_critical")

      expect(captured_sql).to include("oldest_claimable_age_sec")
      expect(captured_sql).to include("min(vt)")
      expect(result[:oldest_msg_age_sec]).to eq(17_045)
      expect(result[:oldest_claimable_age_sec]).to be_nil
    end
  end

  describe "#summary_stats" do
    before do
      allow(mock_connection).to receive(:select_values).and_return(%w[pgbus_default pgbus_default_dlq])
      allow(mock_connection).to receive(:quote) { |v| "'#{v}'" }
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [
                             { "queue_name" => "pgbus_default", "queue_length" => 10, "queue_visible_length" => 8,
                               "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil, "total_messages" => 100 },
                             { "queue_name" => "pgbus_default_dlq", "queue_length" => 2, "queue_visible_length" => 2,
                               "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil, "total_messages" => 5 }
                           ]))
      allow(mock_connection).to receive(:select_all).with(anything, "Pgbus All Table Health").and_return([])
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Concurrency Summary").and_return(double(to_a: []))
      allow(mock_connection).to receive(:select_one).with(anything, "Pgbus Oldest Transaction").and_return(nil)
      allow(data_source).to receive_messages(failed_events_count: 3, processes: [{ id: 1 }, { id: 2 }])
    end

    it "computes aggregate stats" do
      stats = data_source.summary_stats
      expect(stats[:total_queues]).to eq(2)
      expect(stats[:total_depth]).to eq(12)
      expect(stats[:dlq_depth]).to eq(2)
      expect(stats[:active_processes]).to eq(2)
      expect(stats[:failed_count]).to eq(3)
      expect(stats).to have_key(:total_dead_tuples)
      expect(stats).to have_key(:oldest_transaction_age_sec)
    end
  end

  describe "#purge_queue" do
    it "passes the queue name directly without re-prefixing" do
      allow(mock_client).to receive(:purge_queue)
      allow(mock_connection).to receive(:select_all).and_return([])

      data_source.purge_queue("pgbus_default")

      expect(mock_client).to have_received(:purge_queue).with("pgbus_default", prefixed: false)
    end
  end

  describe "#drop_queue" do
    it "passes the queue name directly without re-prefixing" do
      allow(mock_client).to receive(:drop_queue)
      allow(mock_connection).to receive(:select_all).and_return([])

      data_source.drop_queue("pgbus_default")

      expect(mock_client).to have_received(:drop_queue).with("pgbus_default", prefixed: false)
    end
  end

  describe "#registered_subscribers" do
    before do
      mock_config = double("Pgbus::Configuration")
      allow(mock_config).to receive(:queue_name) { |n| "pgbus_#{n}" }
      allow(mock_client).to receive(:config).and_return(mock_config)
    end

    after { Pgbus::EventBus::Registry.instance.clear! }

    it "returns subscriber info from registry with physical queue name" do
      handler_class = Class.new(Pgbus::EventBus::Handler)
      stub_const("MyHandler", handler_class)

      registry = Pgbus::EventBus::Registry.instance
      registry.clear!
      registry.subscribe("orders.#", handler_class)

      result = data_source.registered_subscribers
      expect(result.size).to eq(1)
      expect(result.first[:pattern]).to eq("orders.#")
      expect(result.first[:handler_class]).to eq("MyHandler")
      expect(result.first[:queue_name]).to eq("my_handler")
      expect(result.first[:physical_queue_name]).to eq("pgbus_my_handler")
    end
  end

  describe "#recurring_tasks" do
    it "returns formatted recurring tasks" do
      mock_record = double("RecurringTask",
                           id: 1, key: "daily_cleanup", class_name: "CleanupJob",
                           command: nil, schedule: "0 2 * * *", queue_name: "maintenance",
                           arguments: nil, priority: 0, description: "Cleanup",
                           enabled: true, static: true,
                           created_at: Time.now, updated_at: Time.now)

      relation = double("relation", to_a: [mock_record])
      allow(Pgbus::RecurringTask).to receive(:order).with(:key).and_return(relation)

      # Mock the aggregate query for last runs
      empty_scope = double("scope")
      allow(Pgbus::RecurringExecution).to receive(:where).and_return(empty_scope)
      allow(empty_scope).to receive_messages(select: empty_scope, group: empty_scope, index_by: {})

      result = data_source.recurring_tasks
      expect(result.size).to eq(1)
      expect(result.first[:key]).to eq("daily_cleanup")
      expect(result.first[:class_name]).to eq("CleanupJob")
      expect(result.first[:enabled]).to be true
    end

    it "returns empty array on error" do
      allow(Pgbus::RecurringTask).to receive(:order).and_raise(StandardError)

      expect(data_source.recurring_tasks).to eq([])
    end
  end

  describe "#recurring_tasks_count" do
    it "returns the count of recurring tasks" do
      allow(Pgbus::RecurringTask).to receive(:count).and_return(5)

      expect(data_source.recurring_tasks_count).to eq(5)
    end

    it "returns 0 on error" do
      allow(Pgbus::RecurringTask).to receive(:count).and_raise(StandardError)

      expect(data_source.recurring_tasks_count).to eq(0)
    end
  end

  describe "#toggle_recurring_task" do
    it "returns :disabled when toggling an enabled task" do
      mock_record = double("RecurringTask")
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 1).and_return(mock_record)
      allow(mock_record).to receive(:enabled).and_return(true, false)
      allow(mock_record).to receive(:update!).with(enabled: false).and_return(true)

      expect(data_source.toggle_recurring_task(1)).to eq(:disabled)
    end

    it "returns :enabled when toggling a disabled task" do
      mock_record = double("RecurringTask")
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 2).and_return(mock_record)
      allow(mock_record).to receive(:enabled).and_return(false, true)
      allow(mock_record).to receive(:update!).with(enabled: true).and_return(true)

      expect(data_source.toggle_recurring_task(2)).to eq(:enabled)
    end

    it "returns nil when task not found" do
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 99).and_return(nil)

      expect(data_source.toggle_recurring_task(99)).to be_nil
    end

    it "returns nil when update fails" do
      mock_record = double("RecurringTask", enabled: true)
      allow(Pgbus::RecurringTask).to receive(:find_by).with(id: 3).and_return(mock_record)
      allow(mock_record).to receive(:update!).and_raise(StandardError, "boom")

      expect(data_source.toggle_recurring_task(3)).to be_nil
    end
  end

  describe "#discard_job" do
    it "archives the message and releases the uniqueness lock" do
      allow(mock_client).to receive(:archive_message)

      # Mock reading the message to extract uniqueness key
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"job_class":"ImportJob","pgbus_uniqueness_key":"import-42"}',
                      "headers" => nil, "last_read_at" => nil
                    })

      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_job("pgbus_default", 42)

      expect(mock_client).to have_received(:archive_message).with("pgbus_default", 42, prefixed: false)
      expect(Pgbus::UniquenessKey).to have_received(:release!).with("import-42")
    end

    it "does not release lock when message has no uniqueness key" do
      allow(mock_client).to receive(:archive_message)

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"job_class":"PlainJob"}',
                      "headers" => nil, "last_read_at" => nil
                    })

      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_job("pgbus_default", 42)

      expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    end
  end

  describe "#discard_failed_event" do
    it "deletes the event, releases the uniqueness lock, and archives the queue message" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Failed Event", [1])
        .and_return({
                      "id" => 1,
                      "queue_name" => "default",
                      "msg_id" => 42,
                      "payload" => '{"job_class":"FailedJob","pgbus_uniqueness_key":"failed-1"}'
                    })

      allow(mock_connection).to receive(:exec_delete)
      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)
      allow(mock_client).to receive(:archive_message)

      data_source.discard_failed_event(1)

      expect(mock_connection).to have_received(:exec_delete)
      expect(Pgbus::UniquenessKey).to have_received(:release!).with("failed-1")
      expect(mock_client).to have_received(:archive_message).with("default", 42)
    end
  end

  describe "#discard_all_failed" do
    before do
      allow(mock_connection).to receive(:select_all)
        .with("SELECT payload FROM pgbus_failed_events", "Pgbus Collect Failed Keys")
        .and_return([
                      { "payload" => '{"pgbus_uniqueness_key":"k1"}' },
                      { "payload" => '{"pgbus_uniqueness_key":"k2"}' },
                      { "payload" => '{"job_class":"PlainJob"}' }
                    ])

      allow(mock_connection).to receive(:select_all)
        .with(
          "SELECT id, queue_name, msg_id FROM pgbus_failed_events WHERE msg_id IS NOT NULL",
          "Pgbus Collect Failed Messages"
        )
        .and_return([
                      { "id" => 1, "queue_name" => "default", "msg_id" => 10 },
                      { "id" => 2, "queue_name" => "default", "msg_id" => 11 },
                      { "id" => 3, "queue_name" => "low", "msg_id" => 99 }
                    ])

      allow(mock_connection).to receive(:execute)
        .with("DELETE FROM pgbus_failed_events")
        .and_return(double("result", cmd_tuples: 3))

      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 2))
      allow(mock_client).to receive(:archive_batch)
      allow(mock_client).to receive(:archive_message)
    end

    it "releases the uniqueness locks for every failed event" do
      data_source.discard_all_failed
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: %w[k1 k2])
    end

    it "batches archives by queue to avoid per-row PGMQ roundtrips" do
      data_source.discard_all_failed
      expect(mock_client).to have_received(:archive_batch).with("default", [10, 11])
      expect(mock_client).to have_received(:archive_batch).with("low", [99])
    end

    it "falls back to per-row archive when archive_batch raises" do
      allow(mock_client).to receive(:archive_batch)
        .with("default", anything)
        .and_raise(StandardError, "boom")

      data_source.discard_all_failed

      expect(mock_client).to have_received(:archive_message).with("default", 10)
      expect(mock_client).to have_received(:archive_message).with("default", 11)
      # The good queue still uses archive_batch.
      expect(mock_client).to have_received(:archive_batch).with("low", [99])
    end

    it "returns the number of rows deleted" do
      expect(data_source.discard_all_failed).to eq(3)
    end
  end

  describe "#discard_all_enqueued" do
    before do
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_default"])
      allow(mock_connection).to receive(:quote) { |v| "'#{v}'" }
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{
                             "queue_name" => "pgbus_default",
                             "queue_length" => 2, "queue_visible_length" => 2,
                             "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil,
                             "total_messages" => 10
                           }]))

      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Queue Messages", anything)
        .and_return([
                      { "msg_id" => 1, "read_ct" => 0, "enqueued_at" => Time.now.to_s,
                        "vt" => Time.now.to_s, "last_read_at" => nil, "headers" => nil,
                        "message" => '{"job_class":"ImportJob","pgbus_uniqueness_key":"k1"}' },
                      { "msg_id" => 2, "read_ct" => 0, "enqueued_at" => Time.now.to_s,
                        "vt" => Time.now.to_s, "last_read_at" => nil, "headers" => nil,
                        "message" => '{"job_class":"PlainJob"}' }
                    ])

      allow(mock_client).to receive(:archive_batch).and_return([1, 2])
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 1))
    end

    it "archives all messages from non-DLQ queues and releases locks" do
      count = data_source.discard_all_enqueued

      expect(count).to eq(2)
      expect(mock_client).to have_received(:archive_batch).with("pgbus_default", [1, 2], prefixed: false)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: ["k1"])
    end
  end

  describe "#discard_lock" do
    it "deletes the lock by key" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 1))

      result = data_source.discard_lock("import-42")

      expect(result).to eq(1)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: "import-42")
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_raise(StandardError)

      expect(data_source.discard_lock("import-42")).to eq(0)
    end
  end

  describe "#discard_locks" do
    it "deletes multiple locks by keys" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(double(delete_all: 3))

      result = data_source.discard_locks(%w[k1 k2 k3])

      expect(result).to eq(3)
      expect(Pgbus::UniquenessKey).to have_received(:where).with(lock_key: %w[k1 k2 k3])
    end

    it "returns 0 for empty array" do
      expect(data_source.discard_locks([])).to eq(0)
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:where).and_raise(StandardError)

      expect(data_source.discard_locks(%w[k1])).to eq(0)
    end
  end

  describe "#discard_all_locks" do
    it "deletes all locks" do
      allow(Pgbus::UniquenessKey).to receive(:delete_all).and_return(5)

      result = data_source.discard_all_locks

      expect(result).to eq(5)
      expect(Pgbus::UniquenessKey).to have_received(:delete_all)
    end

    it "returns 0 on error" do
      allow(Pgbus::UniquenessKey).to receive(:delete_all).and_raise(StandardError)

      expect(data_source.discard_all_locks).to eq(0)
    end
  end

  describe "#concurrency_stats" do
    let(:summary_row) do
      { "parked_total" => 7, "oldest_parked_age_sec" => 812, "slots_held" => 3, "keys_at_limit" => 2 }
    end

    let(:key_rows) do
      [
        { "key" => "ProcessOrder-42", "value" => 1, "max_value" => 1,
          "expires_at" => Time.now + 300, "lease_fresh" => true,
          "parked_count" => 5, "oldest_parked_age_sec" => 812 },
        { "key" => "SyncUser-7", "value" => 2, "max_value" => 3,
          "expires_at" => Time.now - 300, "lease_fresh" => false,
          "parked_count" => 0, "oldest_parked_age_sec" => nil }
      ]
    end

    # summary_stats also walks the queue/health/process reads; those have their
    # own coverage, so quiet them down to isolate the concurrency merge.
    def stub_health_queries
      allow(mock_connection).to receive(:select_all).with(anything, "Pgbus All Table Health").and_return([])
      allow(mock_connection).to receive(:select_one).with(anything, "Pgbus Oldest Transaction").and_return(nil)
      allow(data_source).to receive_messages(failed_events_count: 0, processes: [])
    end

    before do
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Concurrency Summary").and_return(double(to_a: [summary_row]))
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Concurrency Keys").and_return(double(to_a: key_rows))
    end

    it "returns the aggregate numbers" do
      stats = data_source.concurrency_stats

      expect(stats).to include(parked_total: 7, oldest_parked_age_sec: 812,
                               slots_held: 3, keys_at_limit: 2)
    end

    it "returns one row per key, ordered as the query returned them" do
      keys = data_source.concurrency_stats[:keys]

      expect(keys.map { |k| k[:key] }).to eq(%w[ProcessOrder-42 SyncUser-7])
      expect(keys.first).to include(value: 1, max_value: 1, parked_count: 5, oldest_parked_age_sec: 812)
    end

    it "carries lease_fresh straight from the query" do
      keys = data_source.concurrency_stats[:keys]

      expect(keys.first[:lease_fresh]).to be(true)
      expect(keys.last[:lease_fresh]).to be(false)
    end

    it "reads a postgres boolean string as lease_fresh" do
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Concurrency Keys")
        .and_return(double(to_a: [key_rows.first.merge("lease_fresh" => "t")]))

      expect(data_source.concurrency_stats[:keys].first[:lease_fresh]).to be(true)
    end

    it "falls back to zeros and an empty key list when the tables are absent" do
      allow(mock_connection).to receive(:select_all).and_raise(StandardError, "no such table")

      expect(data_source.concurrency_stats).to eq(
        parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0, keys: []
      )
    end

    it "merges the aggregate numbers into summary_stats" do
      allow(mock_connection).to receive(:select_values).and_return([])
      stub_health_queries

      expect(data_source.summary_stats).to include(parked_total: 7, oldest_parked_age_sec: 812,
                                                   slots_held: 3, keys_at_limit: 2)
    end

    it "runs the summary query once per instance" do
      allow(mock_connection).to receive(:select_values).and_return([])
      stub_health_queries

      data_source.summary_stats
      data_source.concurrency_stats

      expect(mock_connection).to have_received(:select_all)
        .with(anything, "Pgbus Concurrency Summary").once
    end
  end

  describe "#release_concurrency_key" do
    before do
      allow(Pgbus::Semaphore).to receive(:where).and_return(double(delete_all: 1))
    end

    it "deletes the semaphore row and promotes until nothing is left" do
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next)
        .and_return(true, true, false)

      expect(data_source.release_concurrency_key("ProcessOrder-42")).to eq(2)
      expect(Pgbus::Semaphore).to have_received(:where).with(key: "ProcessOrder-42")
      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:promote_next)
        .with("ProcessOrder-42", client: mock_client).exactly(3).times
    end

    it "returns 0 when nothing was parked" do
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(false)

      expect(data_source.release_concurrency_key("ProcessOrder-42")).to eq(0)
    end

    it "stops after the promotion cap" do
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:promote_next).and_return(true)

      expect(data_source.release_concurrency_key("ProcessOrder-42")).to eq(100)
    end

    it "does nothing for a blank key" do
      expect(data_source.release_concurrency_key("")).to eq(0)
      expect(Pgbus::Semaphore).not_to have_received(:where)
    end

    it "returns 0 on error" do
      allow(Pgbus::Semaphore).to receive(:where).and_raise(StandardError)

      expect(data_source.release_concurrency_key("ProcessOrder-42")).to eq(0)
    end
  end

  describe "#discard_parked_jobs" do
    let(:plain_payload) { { "job_class" => "PlainJob", "job_id" => "j1" } }
    let(:batch_payload) do
      { "job_class" => "BatchedJob", "job_id" => "j2", Pgbus::Batch::METADATA_KEY => "batch-1" }
    end
    let(:unique_payload) do
      { "job_class" => "UniqueJob", "job_id" => "j3",
        Pgbus::Uniqueness::METADATA_KEY => "import-42",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed" }
    end

    let(:rows) do
      [plain_payload, batch_payload, unique_payload].each_with_index.map do |payload, i|
        double("BlockedExecution", id: i + 1, concurrency_key: "ProcessOrder-42", payload: payload)
      end
    end

    let(:relation) { double("Relation", lock: double(to_a: rows)) }

    before do
      allow(Pgbus::BlockedExecution).to receive(:transaction).and_yield
      allow(Pgbus::BlockedExecution).to receive(:for_key).with("ProcessOrder-42").and_return(relation)
      allow(Pgbus::BlockedExecution).to receive(:where).and_return(double(delete_all: 3))
      allow(Pgbus::Batch).to receive(:job_discarded)
      allow(Pgbus::Uniqueness).to receive(:release_lock)
    end

    it "deletes the locked rows and returns the count" do
      expect(data_source.discard_parked_jobs("ProcessOrder-42")).to eq(3)
      expect(Pgbus::BlockedExecution).to have_received(:where).with(id: [1, 2, 3])
    end

    it "resolves a parked batch child as failed" do
      data_source.discard_parked_jobs("ProcessOrder-42")

      expect(Pgbus::Batch).to have_received(:job_discarded).with("batch-1", job_id: "j2").once
    end

    it "releases an until_executed uniqueness lock" do
      data_source.discard_parked_jobs("ProcessOrder-42")

      expect(Pgbus::Uniqueness).to have_received(:release_lock).with("import-42").once
    end

    it "leaves a plain payload alone" do
      data_source.discard_parked_jobs("ProcessOrder-42")

      expect(Pgbus::Batch).to have_received(:job_discarded).once
      expect(Pgbus::Uniqueness).to have_received(:release_lock).once
    end

    it "keeps an until_start lock held" do
      allow(relation).to receive(:lock).and_return(
        double(to_a: [double("BlockedExecution", id: 9, concurrency_key: "k",
                                                 payload: unique_payload.merge(Pgbus::Uniqueness::STRATEGY_KEY => "until_start"))])
      )

      data_source.discard_parked_jobs("ProcessOrder-42")

      expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
    end

    it "instruments one event per discarded row" do
      events = []
      subscriber = ActiveSupport::Notifications.subscribe("pgbus.blocked_execution_discarded") do |*args|
        events << ActiveSupport::Notifications::Event.new(*args)
      end

      data_source.discard_parked_jobs("ProcessOrder-42")
      ActiveSupport::Notifications.unsubscribe(subscriber)

      expect(events.size).to eq(3)
      expect(events.first.payload).to include(concurrency_key: "ProcessOrder-42", job_class: "PlainJob")
    end

    it "reads a double-encoded legacy payload" do
      allow(relation).to receive(:lock).and_return(
        double(to_a: [double("BlockedExecution", id: 4, concurrency_key: "k",
                                                 payload: batch_payload.to_json)])
      )

      data_source.discard_parked_jobs("ProcessOrder-42")

      expect(Pgbus::Batch).to have_received(:job_discarded).with("batch-1", job_id: "j2")
    end

    it "returns 0 when nothing is parked" do
      allow(relation).to receive(:lock).and_return(double(to_a: []))

      expect(data_source.discard_parked_jobs("ProcessOrder-42")).to eq(0)
      expect(Pgbus::BlockedExecution).not_to have_received(:where)
    end

    it "does nothing for a blank key" do
      expect(data_source.discard_parked_jobs(nil)).to eq(0)
      expect(Pgbus::BlockedExecution).not_to have_received(:for_key)
    end

    it "returns 0 on error" do
      allow(Pgbus::BlockedExecution).to receive(:for_key).and_raise(StandardError)

      expect(data_source.discard_parked_jobs("ProcessOrder-42")).to eq(0)
    end
  end

  describe "#queue_health_stats" do
    let(:all_table_rows) do
      [
        { "table_name" => "pgmq.q_pgbus_default", "kind" => "queue",
          "n_live_tup" => 1000, "n_dead_tup" => 200,
          "last_vacuum_ago_sec" => 300, "last_vacuum" => "2026-04-10 10:00:00",
          "last_autovacuum" => "2026-04-10 09:00:00" },
        { "table_name" => "pgmq.a_pgbus_default", "kind" => "archive",
          "n_live_tup" => 1000, "n_dead_tup" => 200,
          "last_vacuum_ago_sec" => 300, "last_vacuum" => nil,
          "last_autovacuum" => "2026-04-10 09:00:00" }
      ]
    end

    it "returns aggregated health stats via a single query" do
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus All Table Health")
        .and_return(all_table_rows)

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Oldest Transaction")
        .and_return({ "age_sec" => 5 })

      result = data_source.queue_health_stats

      expect(result[:total_dead_tuples]).to eq(400)
      expect(result[:total_live_tuples]).to eq(2000)
      expect(result[:worst_bloat_ratio]).to be_within(0.001).of(0.1667)
      expect(result[:tables_needing_vacuum]).to eq(2)
      expect(result[:oldest_vacuum_ago_sec]).to eq(300)
      expect(result[:oldest_transaction_age_sec]).to eq(5)
      expect(result[:tables].size).to eq(2)
    end

    it "returns zero defaults on error" do
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus All Table Health")
        .and_raise(StandardError, "db gone")

      result = data_source.queue_health_stats

      expect(result[:total_dead_tuples]).to eq(0)
      expect(result[:tables]).to eq([])
    end

    it "handles empty results gracefully" do
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus All Table Health")
        .and_return([])

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Oldest Transaction")
        .and_return(nil)

      result = data_source.queue_health_stats

      expect(result[:total_dead_tuples]).to eq(0)
      expect(result[:tables]).to be_empty
      expect(result[:oldest_transaction_age_sec]).to be_nil
    end
  end

  describe "#queue_health_detail" do
    it "returns per-queue health for queue and archive tables" do
      stats_row = {
        "n_live_tup" => 500,
        "n_dead_tup" => 50,
        "last_vacuum_ago_sec" => 120,
        "last_vacuum" => "2026-04-10 10:00:00",
        "last_autovacuum" => nil
      }

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Table Health", anything)
        .and_return(stats_row)

      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Oldest Transaction")
        .and_return({ "age_sec" => 2 })

      result = data_source.queue_health_detail("pgbus_default")

      expect(result[:tables].size).to eq(2)
      expect(result[:tables].first[:table]).to eq("pgmq.q_pgbus_default")
      expect(result[:tables].first[:kind]).to eq("queue")
      expect(result[:tables].last[:kind]).to eq("archive")
      expect(result[:tables].first[:bloat_ratio]).to be_within(0.001).of(0.0909)
      expect(result[:oldest_transaction_age_sec]).to eq(2)
    end

    it "returns empty on error" do
      allow(mock_connection).to receive(:select_one).and_raise(StandardError, "boom")

      result = data_source.queue_health_detail("missing")

      expect(result[:tables]).to eq([])
      expect(result[:oldest_transaction_age_sec]).to be_nil
    end
  end

  describe "#pending_events" do
    let(:handler_class) { Class.new(Pgbus::EventBus::Handler) }
    let(:mock_config) { double("Pgbus::Configuration") }

    before do
      stub_const("TaskCompletionHandler", handler_class)
      registry = Pgbus::EventBus::Registry.instance
      registry.clear!
      registry.subscribe("task.completed", handler_class, queue_name: "task_completion_handler")
      allow(mock_config).to receive(:queue_name) { |n| "pgbus_#{n}" }
      allow(mock_client).to receive(:config).and_return(mock_config)
    end

    after { Pgbus::EventBus::Registry.instance.clear! }

    it "normalizes subscriber queue names to physical before intersecting with pgmq.meta" do
      allow(mock_connection).to receive(:select_values)
        .with("SELECT queue_name FROM pgmq.meta ORDER BY queue_name", "Pgbus Queue Names")
        .and_return(["pgbus_task_completion_handler"])

      event_msg = '{"event_id":"evt-123","payload":{"foo":"bar"},' \
                  '"published_at":"2026-04-09T00:00:00Z"}'
      allow(mock_connection).to receive(:select_all).and_return([
                                                                  {
                                                                    "msg_id" => 1, "read_ct" => 3,
                                                                    "enqueued_at" => "2026-04-09T00:00:00Z",
                                                                    "last_read_at" => "2026-04-09T00:01:00Z",
                                                                    "vt" => "2026-04-09T00:02:00Z",
                                                                    "message" => event_msg,
                                                                    "headers" => nil,
                                                                    "queue_name" => "pgbus_task_completion_handler"
                                                                  }
                                                                ])

      result = data_source.pending_events(page: 1, per_page: 25)

      expect(result.size).to eq(1)
      expect(result.first[:msg_id]).to eq(1)
      expect(result.first[:queue_name]).to eq("pgbus_task_completion_handler")
    end

    it "returns empty when subscriber queues have not been created in pgmq.meta" do
      # Logical name "task_completion_handler" becomes physical
      # "pgbus_task_completion_handler"; but pgmq.meta returns an unrelated queue
      # so the intersection is empty.
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_other"])

      expect(data_source.pending_events).to eq([])
    end

    it "returns empty array when no handler queues exist" do
      allow(mock_connection).to receive(:select_values).and_return([])

      expect(data_source.pending_events).to eq([])
    end

    it "returns empty array on error" do
      allow(mock_connection).to receive(:select_values).and_raise(StandardError, "boom")

      expect(data_source.pending_events).to eq([])
    end
  end

  describe "#handler_queue_physical_names" do
    let(:handler_class) { Class.new(Pgbus::EventBus::Handler) }
    let(:mock_config) { double("Pgbus::Configuration") }

    before do
      stub_const("TaskCompletionHandler", handler_class)
      registry = Pgbus::EventBus::Registry.instance
      registry.clear!
      registry.subscribe("task.completed", handler_class, queue_name: "task_completion_handler")
      allow(mock_config).to receive(:queue_name) { |n| "pgbus_#{n}" }
      allow(mock_client).to receive(:config).and_return(mock_config)
    end

    after { Pgbus::EventBus::Registry.instance.clear! }

    it "returns subscriber queue names prefixed with config.queue_prefix" do
      expect(data_source.handler_queue_physical_names).to eq(["pgbus_task_completion_handler"])
    end
  end

  describe "#handler_class_for_queue" do
    let(:handler_class) { Class.new(Pgbus::EventBus::Handler) }
    let(:mock_config) { double("Pgbus::Configuration") }

    before do
      stub_const("TaskCompletionHandler", handler_class)
      registry = Pgbus::EventBus::Registry.instance
      registry.clear!
      registry.subscribe("task.completed", handler_class, queue_name: "task_completion_handler")
      allow(mock_config).to receive(:queue_name) { |n| "pgbus_#{n}" }
      allow(mock_client).to receive(:config).and_return(mock_config)
    end

    after { Pgbus::EventBus::Registry.instance.clear! }

    it "returns the handler class for a registered physical queue" do
      expect(data_source.handler_class_for_queue("pgbus_task_completion_handler"))
        .to eq("TaskCompletionHandler")
    end

    it "returns nil for an unregistered queue" do
      expect(data_source.handler_class_for_queue("pgbus_unknown")).to be_nil
    end
  end

  describe "#discard_event" do
    it "archives the message from the handler queue" do
      allow(mock_client).to receive(:archive_message)
      allow(mock_connection).to receive(:select_one).and_return(nil)

      data_source.discard_event("task_completion_handler", 42)

      expect(mock_client).to have_received(:archive_message).with("task_completion_handler", 42, prefixed: false)
    end

    it "releases uniqueness lock when present" do
      allow(mock_client).to receive(:archive_message)
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"event_id":"evt-1","pgbus_uniqueness_key":"uk-42"}',
                      "headers" => nil, "last_read_at" => nil
                    })
      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.discard_event("task_completion_handler", 42)

      expect(Pgbus::UniquenessKey).to have_received(:release!).with("uk-42")
    end
  end

  describe "#mark_event_handled" do
    let(:event_detail) do
      {
        "msg_id" => 42, "read_ct" => 3,
        "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
        "message" => '{"event_id":"evt-123","pgbus_uniqueness_key":"uk-42","payload":{"foo":"bar"}}',
        "headers" => nil, "last_read_at" => nil
      }
    end

    it "performs insert -> release -> archive in strict order" do
      call_order = []
      allow(Pgbus::ProcessedEvent).to receive(:insert) { call_order << :insert }
      allow(Pgbus::UniquenessKey).to receive(:release!) { call_order << :release }
      allow(mock_client).to receive(:archive_message) { call_order << :archive }
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42]).and_return(event_detail)

      data_source.mark_event_handled("task_completion_handler", 42, "TaskCompletionHandler")

      expect(call_order).to eq(%i[insert release archive])
    end

    it "inserts the ProcessedEvent row for the event" do
      allow(Pgbus::ProcessedEvent).to receive(:insert)
      allow(Pgbus::UniquenessKey).to receive(:release!)
      allow(mock_client).to receive(:archive_message)
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42]).and_return(event_detail)

      result = data_source.mark_event_handled("task_completion_handler", 42, "TaskCompletionHandler")

      expect(result).to be true
      expect(Pgbus::ProcessedEvent).to have_received(:insert).with(
        hash_including(event_id: "evt-123", handler_class: "TaskCompletionHandler"),
        unique_by: %i[event_id handler_class]
      )
    end

    it "returns false when message not found" do
      allow(mock_connection).to receive(:select_one).and_return(nil)

      result = data_source.mark_event_handled("task_completion_handler", 99, "TaskCompletionHandler")

      expect(result).to be false
    end

    it "releases the uniqueness lock for the event payload" do
      allow(mock_client).to receive(:archive_message)
      allow(Pgbus::ProcessedEvent).to receive(:insert)
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"event_id":"evt-1","pgbus_uniqueness_key":"uk-42"}',
                      "headers" => nil, "last_read_at" => nil
                    })
      allow(Pgbus::UniquenessKey).to receive(:release!).and_return(1)

      data_source.mark_event_handled("task_completion_handler", 42, "TaskCompletionHandler")

      expect(Pgbus::UniquenessKey).to have_received(:release!).with("uk-42")
    end
  end

  describe "#edit_event_payload" do
    let(:txn) { double("txn") }

    it "uses a PGMQ transaction to produce new message and delete the old atomically" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"event_id":"evt-1","payload":{"old":"data"}}',
                      "headers" => '{"x-routing":"task.completed"}',
                      "last_read_at" => nil
                    })
      allow(txn).to receive(:produce)
      allow(txn).to receive(:delete)
      allow(mock_client).to receive(:transaction).and_yield(txn)

      new_payload = '{"event_id":"evt-1","payload":{"corrected":"data"}}'
      result = data_source.edit_event_payload("task_completion_handler", 42, new_payload)

      expect(result).to be true
      expect(txn).to have_received(:produce)
        .with("task_completion_handler", new_payload, headers: '{"x-routing":"task.completed"}')
      expect(txn).to have_received(:delete).with("task_completion_handler", 42)
    end

    it "returns false when message not found" do
      allow(mock_connection).to receive(:select_one).and_return(nil)

      result = data_source.edit_event_payload("task_completion_handler", 99, "{}")

      expect(result).to be false
    end

    it "returns false for invalid JSON payload" do
      result = data_source.edit_event_payload("task_completion_handler", 42, "not json")

      expect(result).to be false
    end
  end

  describe "#reroute_event" do
    let(:txn) { double("txn") }

    it "uses a PGMQ transaction to produce on target and delete on source atomically" do
      allow(mock_connection).to receive(:select_one)
        .with(anything, "Pgbus Job Detail", [42])
        .and_return({
                      "msg_id" => 42, "read_ct" => 0,
                      "enqueued_at" => Time.now.to_s, "vt" => Time.now.to_s,
                      "message" => '{"event_id":"evt-1","payload":{"foo":"bar"}}',
                      "headers" => '{"x-routing":"task.completed"}',
                      "last_read_at" => nil
                    })
      allow(txn).to receive(:produce)
      allow(txn).to receive(:delete)
      allow(mock_client).to receive(:transaction).and_yield(txn)

      result = data_source.reroute_event("task_completion_handler", 42, "webhook_handler")

      expect(result).to be true
      expect(txn).to have_received(:produce)
        .with("webhook_handler", '{"event_id":"evt-1","payload":{"foo":"bar"}}',
              headers: '{"x-routing":"task.completed"}')
      expect(txn).to have_received(:delete).with("task_completion_handler", 42)
    end

    it "returns false when message not found" do
      allow(mock_connection).to receive(:select_one).and_return(nil)

      result = data_source.reroute_event("task_completion_handler", 99, "webhook_handler")

      expect(result).to be false
    end
  end

  describe "#discard_selected_events" do
    it "archives multiple messages and returns count" do
      allow(mock_client).to receive(:archive_message)
      allow(mock_connection).to receive(:select_one).and_return(nil)

      selections = [
        { queue_name: "task_completion_handler", msg_id: 1 },
        { queue_name: "task_completion_handler", msg_id: 2 },
        { queue_name: "webhook_handler", msg_id: 3 }
      ]

      result = data_source.discard_selected_events(selections)

      expect(result).to eq(3)
      expect(mock_client).to have_received(:archive_message).exactly(3).times
    end

    it "returns 0 for empty selections" do
      expect(data_source.discard_selected_events([])).to eq(0)
    end
  end

  describe "#dlq_messages (SQL pagination pushdown)" do
    let(:queue_metrics) do
      [
        { name: "pgbus_default_dlq", queue_length: 5, queue_visible_length: 5,
          oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: 5 },
        { name: "pgbus_low_dlq", queue_length: 2, queue_visible_length: 2,
          oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: 2 },
        { name: "pgbus_default", queue_length: 10, queue_visible_length: 10,
          oldest_msg_age_sec: nil, newest_msg_age_sec: nil, total_messages: 10 }
      ]
    end

    before do
      allow(data_source).to receive(:queues_with_metrics).and_return(queue_metrics)
    end

    it "issues a single UNION ALL query with LIMIT/OFFSET pushed down" do
      captured_sql = nil
      allow(mock_connection).to receive(:select_all) do |sql, _name, _binds|
        captured_sql = sql
        []
      end

      data_source.dlq_messages(page: 3, per_page: 10)

      expect(captured_sql).to include("UNION ALL")
      expect(captured_sql).to include("ORDER BY msg_id DESC")
      expect(captured_sql).to include("LIMIT $1 OFFSET $2")
      # Both DLQ tables appear in the SQL; the non-DLQ default queue does not.
      expect(captured_sql).to include("pgmq.q_pgbus_default_dlq")
      expect(captured_sql).to include("pgmq.q_pgbus_low_dlq")
      expect(captured_sql).not_to include("pgmq.q_pgbus_default ")
    end

    it "binds limit + offset from the page calculation" do
      captured_binds = nil
      allow(mock_connection).to receive(:select_all) do |_sql, _name, binds|
        captured_binds = binds
        []
      end

      data_source.dlq_messages(page: 4, per_page: 25)

      expect(captured_binds).to eq([25, 75])
    end

    it "returns [] when there are no DLQ queues (skips the query entirely)" do
      allow(data_source).to receive(:queues_with_metrics).and_return([])
      allow(mock_connection).to receive(:select_all)

      expect(data_source.dlq_messages).to eq([])
      # If select_all gets called with an empty SQL, that's a bug —
      # paginated_queue_messages should short-circuit instead.
      expect(mock_connection).not_to have_received(:select_all)
    end

    it "formats each row with the source queue_name" do
      allow(mock_connection).to receive(:select_all).and_return([
                                                                  {
                                                                    "msg_id" => 42, "read_ct" => 3,
                                                                    "enqueued_at" => "2026-04-09T00:00:00Z",
                                                                    "last_read_at" => "2026-04-09T00:01:00Z",
                                                                    "vt" => "2026-04-09T00:02:00Z",
                                                                    "message" => "{}", "headers" => nil,
                                                                    "queue_name" => "pgbus_default_dlq"
                                                                  }
                                                                ])

      result = data_source.dlq_messages(page: 1, per_page: 10)
      expect(result.size).to eq(1)
      expect(result.first[:msg_id]).to eq(42)
      expect(result.first[:queue_name]).to eq("pgbus_default_dlq")
    end
  end

  describe "#dlq_total_count" do
    let(:queue_metrics) do
      [
        { name: "pgbus_default_dlq", queue_length: 5 },
        { name: "pgbus_low_dlq", queue_length: 2 },
        { name: "pgbus_default", queue_length: 10 }
      ]
    end

    it "sums queue_length across DLQ queues only" do
      allow(data_source).to receive(:queues_with_metrics).and_return(queue_metrics)

      expect(data_source.dlq_total_count).to eq(7)
    end

    it "returns 0 when there are no DLQ queues" do
      allow(data_source).to receive(:queues_with_metrics).and_return([{ name: "pgbus_default", queue_length: 10 }])

      expect(data_source.dlq_total_count).to eq(0)
    end
  end

  describe "queue-metrics memoization" do
    it "fetches queue metrics only once across repeated calls in one request" do
      allow(mock_connection).to receive(:select_values).and_return(["pgbus_default_dlq"])
      allow(mock_connection).to receive(:quote) { |v| "'#{v}'" }
      allow(mock_connection).to receive(:select_all)
        .with(anything, "Pgbus Batched Queue Metrics")
        .and_return(double(to_a: [{
                             "queue_name" => "pgbus_default_dlq",
                             "queue_length" => 4, "queue_visible_length" => 4,
                             "oldest_msg_age_sec" => nil, "newest_msg_age_sec" => nil,
                             "total_messages" => 4
                           }]))

      # The DLQ page reads both the rows and the total count; the meta query
      # must run once, not once per call.
      data_source.queues_with_metrics
      data_source.dlq_total_count

      expect(mock_connection).to have_received(:select_values)
        .with(a_string_matching(/pgmq\.meta/)).once
    end
  end

  describe "#live_stream_metrics" do
    let(:counter) { Pgbus::Web::Streamer::StreamCounter.new }

    before do
      allow(Pgbus::Web::Streamer).to receive(:stream_counter).and_return(counter)
    end

    it "returns per-stream metrics from the in-memory counter" do
      counter.increment_broadcasts("chat")
      counter.increment_broadcasts("chat")
      counter.increment_connections("chat")
      counter.increment_total_connections("chat")
      counter.increment_broadcasts("alerts")

      result = data_source.live_stream_metrics

      expect(result[:streams]).to include(
        "chat" => hash_including(broadcasts: 2, active_connections: 1, total_connections: 1),
        "alerts" => hash_including(broadcasts: 1, active_connections: 0, total_connections: 0)
      )
      expect(result[:totals]).to include(
        broadcasts: 3,
        active_connections: 1,
        streams: 2
      )
    end

    it "returns empty metrics when no streamer is running" do
      allow(Pgbus::Web::Streamer).to receive(:stream_counter).and_return(nil)

      result = data_source.live_stream_metrics

      expect(result[:streams]).to eq({})
      expect(result[:totals]).to eq(broadcasts: 0, active_connections: 0, total_connections: 0, streams: 0)
    end
  end

  describe "#notify_throttles" do
    it "returns formatted notify throttle data from the client" do
      throttle = double("NotifyThrottle",
                        queue_name: "pgbus_default",
                        throttle_interval_ms: 250,
                        last_notified_at: "2026-06-14 12:00:00")
      allow(mock_client).to receive(:list_notify_insert_throttles).and_return([throttle])

      result = data_source.notify_throttles
      expect(result).to eq([{
                             queue_name: "pgbus_default",
                             throttle_interval_ms: 250,
                             last_notified_at: "2026-06-14 12:00:00"
                           }])
    end

    it "returns empty array on error" do
      allow(mock_client).to receive(:list_notify_insert_throttles).and_raise(StandardError, "boom")
      expect(data_source.notify_throttles).to eq([])
    end
  end

  describe "#queue_group_heads" do
    it "returns formatted group head messages" do
      msg = double("PGMQ::Message",
                   msg_id: 42, message: '{"type":"order"}',
                   read_ct: 0, headers: nil,
                   enqueued_at: "2026-06-14T12:00:00.000000Z")
      allow(msg).to receive(:respond_to?).with(:vt).and_return(false)
      allow(msg).to receive(:respond_to?).with(:last_read_at).and_return(false)
      allow(mock_client).to receive(:read_grouped_head).and_return([msg])

      result = data_source.queue_group_heads("pgbus_default", qty: 5)
      expect(result.size).to eq(1)
      expect(result.first[:msg_id]).to eq(42)
    end

    it "returns empty array on error" do
      allow(mock_client).to receive(:read_grouped_head).and_raise(StandardError, "boom")
      expect(data_source.queue_group_heads("pgbus_default")).to eq([])
    end
  end
end
