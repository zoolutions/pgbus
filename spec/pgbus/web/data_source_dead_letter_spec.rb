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

  describe "#dlq_messages filters" do
    let(:queue_metrics) do
      [
        { name: "pgbus_default_dlq", queue_length: 5 },
        { name: "pgbus_orders_dlq", queue_length: 2 },
        { name: "pgbus_default", queue_length: 10 }
      ]
    end
    let(:captured) { [] }

    before do
      allow(data_source).to receive(:queues_with_metrics).and_return(queue_metrics)
      allow(mock_connection).to receive(:select_all) do |sql, _name, binds|
        captured << [sql, binds]
        []
      end
    end

    it "keeps the unfiltered SQL free of any WHERE clause" do
      data_source.dlq_messages(page: 1, per_page: 10)

      expect(captured.last.first).not_to include("WHERE")
      expect(captured.last.last).to eq([10, 0])
    end

    it "reads only the named DLQ when dlq: is given" do
      data_source.dlq_messages(page: 1, per_page: 10, dlq: "pgbus_orders_dlq")

      sql = captured.last.first
      expect(sql).to include("pgmq.q_pgbus_orders_dlq")
      expect(sql).not_to include("pgmq.q_pgbus_default_dlq")
    end

    it "returns [] without a query for an unknown or non-DLQ queue" do
      expect(data_source.dlq_messages(dlq: "pgbus_default")).to eq([])
      expect(data_source.dlq_messages(dlq: "pgbus_missing_dlq")).to eq([])
      expect(captured).to be_empty
    end

    it "filters every UNION fragment on the header's error class, as a bound parameter" do
      data_source.dlq_messages(page: 2, per_page: 10, error_class: "Stripe::CardError")

      sql, binds = captured.last
      expect(sql.scan("WHERE headers #>> '{pgbus_dead_letter,error_class}' = $3").size).to eq(2)
      expect(sql).not_to include("Stripe")
      expect(binds).to eq([10, 10, "Stripe::CardError"])
    end

    it "combines dlq: and error_class:" do
      data_source.dlq_messages(page: 1, per_page: 10, dlq: "pgbus_orders_dlq", error_class: "KeyError")

      sql, binds = captured.last
      expect(sql).to include("pgmq.q_pgbus_orders_dlq")
      expect(sql).not_to include("pgmq.q_pgbus_default_dlq")
      expect(binds).to eq([10, 0, "KeyError"])
    end
  end

  describe "#dlq_total_count filters" do
    let(:queue_metrics) do
      [
        { name: "pgbus_default_dlq", queue_length: 5 },
        { name: "pgbus_orders_dlq", queue_length: 2 },
        { name: "pgbus_default", queue_length: 10 }
      ]
    end

    before { allow(data_source).to receive(:queues_with_metrics).and_return(queue_metrics) }

    it "returns the named DLQ's length for dlq:" do
      expect(data_source.dlq_total_count(dlq: "pgbus_orders_dlq")).to eq(2)
    end

    it "returns 0 for an unknown DLQ" do
      expect(data_source.dlq_total_count(dlq: "pgbus_default")).to eq(0)
    end

    it "counts the filtered UNION for error_class:" do
      captured = nil
      allow(mock_connection).to receive(:select_value) do |sql, _name, binds|
        captured = [sql, binds]
        3
      end

      expect(data_source.dlq_total_count(error_class: "KeyError")).to eq(3)
      expect(captured.first).to include("SELECT COUNT(*)")
      expect(captured.first.scan("WHERE headers #>> '{pgbus_dead_letter,error_class}' = $1").size).to eq(2)
      expect(captured.last).to eq(["KeyError"])
    end
  end

  describe "#dlq_counts_by_queue" do
    it "maps every DLQ to its length from the cached metrics" do
      allow(data_source).to receive(:queues_with_metrics).and_return(
        [{ name: "pgbus_default_dlq", queue_length: 5 }, { name: "pgbus_default", queue_length: 10 },
         { name: "pgbus_orders_dlq", queue_length: 2 }]
      )

      expect(data_source.dlq_counts_by_queue).to eq("pgbus_default_dlq" => 5, "pgbus_orders_dlq" => 2)
    end
  end

  describe "#retry_dlq_message" do
    let(:txn) { double("txn", delete: true) }
    let(:produced) { [] }

    before do
      allow(txn).to receive(:produce) { |queue, body, headers:| produced << [queue, body, headers] }
      allow(mock_client).to receive(:transaction).and_yield(txn)
    end

    def dlq_row(headers) = { "msg_id" => 5, "message" => "{}", "headers" => headers }

    def dead_headers(existing: nil)
      Pgbus::DeadLetterHeader.build(existing: existing, reason: "max_retries_exceeded", source: "worker",
                                    source_queue: "pgbus_default", attempts: 6, max_retries: 5)
    end

    it "re-enqueues without the dead-letter block and with the retry counter at 1" do
      allow(mock_connection).to receive(:select_one).and_return(dlq_row(dead_headers(existing: '{"trace_id":"t"}')))

      expect(data_source.retry_dlq_message("pgbus_default_dlq", 5)).to be true

      queue, body, headers = produced.last
      expect([queue, body]).to eq(["pgbus_default", "{}"])
      expect(JSON.parse(headers)).to eq("trace_id" => "t", "pgbus_dlq_retries" => 1)
      expect(txn).to have_received(:delete).with("pgbus_default_dlq", 5)
    end

    it "counts the second trip out of the DLQ from the block's retries_from_dlq" do
      second_death = dead_headers(existing: '{"pgbus_dlq_retries":1}')
      allow(mock_connection).to receive(:select_one).and_return(dlq_row(second_death))

      data_source.retry_dlq_message("pgbus_default_dlq", 5)

      expect(JSON.parse(produced.last.last)).to eq("pgbus_dlq_retries" => 2)
    end

    it "gives a legacy row (no headers) a counter of 1" do
      allow(mock_connection).to receive(:select_one).and_return(dlq_row(nil))

      data_source.retry_dlq_message("pgbus_default_dlq", 5)

      expect(JSON.parse(produced.last.last)).to eq("pgbus_dlq_retries" => 1)
    end
  end
end
