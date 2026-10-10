# frozen_string_literal: true

require "spec_helper"

require_relative "../../../lib/pgbus/web/data_source"

RSpec.describe Pgbus::Web::DataSource::DeadLetter do
  # DeadLetter is a DataSource mixin: exercise it through a DataSource.
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

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
end
