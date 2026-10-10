# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::DataSource::EventList do
  subject(:data_source) { Pgbus::Web::DataSource.new(client: mock_client) }

  let(:mock_client) { double("Pgbus::Client") }
  let(:mock_connection) { double("ActiveRecord::Connection") }
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:subscribers) do
    [{ pattern: "orders.#", handler_class: "OrderHandler", queue_name: "order_handler",
       physical_queue_name: "pgbus_test_order_handler" },
     { pattern: "webhook.*", handler_class: "WebhookHandler", queue_name: "webhook_handler",
       physical_queue_name: "pgbus_test_webhook_handler" }]
  end
  let(:queue_names) { %w[pgbus_test_default pgbus_test_order_handler pgbus_test_webhook_handler] }

  before do
    allow(Pgbus::BusRecord).to receive(:connection).and_return(mock_connection)
    allow(Pgbus.configuration).to receive(:queue_prefix).and_return("pgbus_test")
    allow(data_source).to receive_messages(queues_with_metrics: queue_names.map { |name| { name: name } },
                                           registered_subscribers: subscribers)
  end

  describe "#event_rows" do
    it "lists only the handler queues" do
      allow(data_source).to receive(:job_rows).and_return([])

      data_source.event_rows(state: "retrying", queue_name: nil, page: 2, per_page: 10)

      expect(data_source).to have_received(:job_rows).with(
        state: "retrying", queue_name: nil, queues: %w[pgbus_test_order_handler pgbus_test_webhook_handler],
        page: 2, per_page: 10
      )
    end

    it "returns [] without querying when nothing subscribes" do
      allow(data_source).to receive(:registered_subscribers).and_return([])
      allow(mock_connection).to receive(:select_all)

      expect(data_source.event_rows).to eq([])
      expect(mock_connection).not_to have_received(:select_all)
    end

    it "names each row's handler and pattern, for queue rows and orphaned failed rows alike" do
      allow(data_source).to receive(:job_rows).and_return(
        [{ source: "queue", queue_name: "pgbus_test_order_handler", msg_id: 1 },
         { source: "failed", queue_name: "webhook_handler", msg_id: 2 },
         { source: "queue", queue_name: "pgbus_test_gone_handler", msg_id: 3 }]
      )

      rows = data_source.event_rows

      expect(rows.map { |r| r.values_at(:handler_class, :pattern) })
        .to eq([%w[OrderHandler orders.#], %w[WebhookHandler webhook.*], [nil, nil]])
    end
  end

  describe "#event_state_counts" do
    it "counts only the handler queues" do
      allow(data_source).to receive(:job_state_counts).and_return(:counts)

      expect(data_source.event_state_counts(queue_name: "pgbus_test_order_handler")).to eq(:counts)
      expect(data_source).to have_received(:job_state_counts)
        .with(queue_name: "pgbus_test_order_handler", queues: %w[pgbus_test_order_handler pgbus_test_webhook_handler])
    end
  end

  describe "#events_ahead" do
    it "counts the events ahead the way the job list does" do
      allow(data_source).to receive(:jobs_ahead).with(:rows).and_return(:ahead)

      expect(data_source.events_ahead(:rows)).to eq(:ahead)
    end
  end

  describe "#event_list_context" do
    let(:processes) { [] }

    before do
      allow(data_source).to receive_messages(processes: processes, paused_queue_names: [],
                                             drained_queue_names: nil)
    end

    def consumer(topics, healthy: true, kind: "consumer")
      { kind: kind, healthy: healthy, metadata: topics && { "topics" => topics, "threads" => 2 } }
    end

    it "wraps the job context with the claim window" do
      context = data_source.event_list_context(now: now)

      expect(context.jobs.now).to eq(now)
      expect(context.claim_window).to eq(Pgbus::EventBus::Handler.claim_ownership_window)
    end

    context "with a healthy consumer subscribed to webhook.*" do
      let(:processes) do
        [consumer(["webhook.*"]), consumer(["#"], healthy: false),
         { kind: "worker", healthy: true, metadata: { "topics" => ["#"] } }]
      end

      it "covers only the queues a healthy consumer's topics overlap" do
        expect(data_source.event_list_context.covered_queues).to eq(Set["pgbus_test_webhook_handler"])
      end
    end

    context "with a healthy consumer whose topic ends in #" do
      let(:processes) { [consumer(["orders.#"])] }

      it "covers every handler queue, as Registry#queue_names_for_topics reads them all" do
        expect(data_source.event_list_context.covered_queues)
          .to eq(Set["pgbus_test_order_handler", "pgbus_test_webhook_handler"])
      end
    end

    context "without a healthy consumer" do
      let(:processes) { [consumer(["#"], healthy: false)] }

      it "covers no queue" do
        expect(data_source.event_list_context.covered_queues).to eq(Set.new)
      end
    end

    context "with a healthy consumer that reports no topics" do
      let(:processes) { [consumer(nil)] }

      it "does not know the coverage" do
        expect(data_source.event_list_context.covered_queues).to be_nil
      end
    end
  end

  describe "#event_replay_states" do
    let(:events) do
      [{ "id" => 1, "event_id" => "evt-1", "handler_class" => "OrderHandler", "processed_at" => now - 600 },
       { "id" => 2, "event_id" => "evt-2", "handler_class" => "OrderHandler", "processed_at" => now - 60 },
       { "id" => 3, "event_id" => "evt-3", "handler_class" => "RemovedHandler", "processed_at" => now }]
    end

    it "probes each handler's archive once for the page, bounded by the oldest claim" do
      queries = []
      allow(mock_connection).to receive(:select_values) do |sql, label, binds|
        queries << [sql, label, binds]
        ["evt-2"]
      end

      states = data_source.event_replay_states(events)

      expect(states).to eq(1 => :not_archived, 2 => :replayable, 3 => :no_handler)
      expect(queries.size).to eq(1)
      sql, label, binds = queries.first
      expect(label).to eq("Pgbus Archived Events")
      expect(sql).to include("FROM pgmq.a_pgbus_test_order_handler a")
      expect(sql).to include("a.archived_at >= $2::timestamptz", "a.message->>'event_id' = ANY($1::text[])")
      expect(sql).to include("COALESCE(a.message->'headers'->>'routing_key', a.message->>'routing_key') IS NOT NULL")
      expect(binds).to eq(["{evt-1,evt-2}", (now - 660).utc.iso8601(6)])
    end

    it "reports nothing archived when the archive cannot be read" do
      allow(mock_connection).to receive(:select_values).and_raise(StandardError, "no table")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.event_replay_states(events.first(1))).to eq(1 => :not_archived)
      expect(Pgbus.logger).to have_received(:error)
    end

    it "queries nothing for an empty page" do
      expect(data_source.event_replay_states([])).to eq({})
    end
  end

  describe "#replay_event" do
    let(:event) do
      { "id" => 7, "event_id" => "evt-7", "handler_class" => "OrderHandler", "processed_at" => now }
    end
    let(:archived) do
      { "message" => { "event_id" => "evt-7", "payload" => { "id" => 1 }, "routing_key" => "orders.created",
                       "published_at" => "2026-10-10T11:00:00Z", "pgbus_current" => { "Current" => {} } }.to_json,
        "headers" => '{"trace_id":"t"}' }
    end
    let(:txn) { double("txn", produce: 99) }

    before do
      allow(mock_client).to receive(:transaction).and_yield(txn)
      allow(SecureRandom).to receive(:uuid).and_return("evt-new")
    end

    it "re-delivers the archived envelope to that handler's queue under a new event_id" do
      allow(mock_connection).to receive(:select_one).and_return(archived)

      expect(data_source.replay_event(event)).to be(true)

      expect(txn).to have_received(:produce) do |queue, message, headers:|
        expect(queue).to eq("pgbus_test_order_handler")
        expect(JSON.parse(message)).to eq(
          "event_id" => "evt-new", "replayed_from" => "evt-7", "payload" => { "id" => 1 },
          "routing_key" => "orders.created", "published_at" => "2026-10-10T11:00:00Z",
          "pgbus_current" => { "Current" => {} }
        )
        expect(headers).to eq('{"trace_id":"t"}')
      end
    end

    it "reads the archive bounded by the claim time" do
      captured = nil
      allow(mock_connection).to receive(:select_one) do |sql, label, binds|
        captured = [sql, label, binds]
        archived
      end

      data_source.replay_event(event)

      expect(captured[0]).to include("FROM pgmq.a_pgbus_test_order_handler a", "a.message->>'event_id' = $1")
      expect(captured[1]).to eq("Pgbus Archived Event")
      expect(captured[2]).to eq(["evt-7", (now - 60).utc.iso8601(6)])
    end

    it "refuses when the archived message is gone" do
      allow(mock_connection).to receive(:select_one).and_return(nil)

      expect(data_source.replay_event(event)).to be(false)
      expect(mock_client).not_to have_received(:transaction)
    end

    it "refuses when no subscriber runs the handler any more" do
      allow(mock_connection).to receive(:select_one)

      expect(data_source.replay_event(event.merge("handler_class" => "RemovedHandler"))).to be(false)
      expect(mock_connection).not_to have_received(:select_one)
    end

    it "refuses an archived envelope without a routing key" do
      allow(mock_connection).to receive(:select_one)
        .and_return(archived.merge("message" => { "event_id" => "evt-7", "payload" => {} }.to_json))

      expect(data_source.replay_event(event)).to be(false)
    end

    it "logs and refuses when the produce raises" do
      allow(mock_connection).to receive(:select_one).and_return(archived)
      allow(txn).to receive(:produce).and_raise(StandardError, "db gone")
      allow(Pgbus.logger).to receive(:error)

      expect(data_source.replay_event(event)).to be(false)
      expect(Pgbus.logger).to have_received(:error)
    end
  end
end
