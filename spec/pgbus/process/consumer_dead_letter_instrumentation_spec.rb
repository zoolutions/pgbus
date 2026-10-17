# frozen_string_literal: true

require "spec_helper"
require "json"

# Issue #543: the consumer dead-lettered an event in silence. The executor
# emits pgbus.job_dead_lettered for a job; this is the event twin.
RSpec.describe Pgbus::Process::Consumer, "#handle_message emits pgbus.event_dead_lettered (issue #543)" do
  let(:mock_client) { build_mock_client }
  let(:mock_pool) do
    instance_double(
      Pgbus::ExecutionPools::ThreadPool,
      capacity: 1, available_capacity: 1, idle?: true,
      kill: nil, shutdown: nil, wait_for_termination: nil,
      metadata: { mode: :threads, capacity: 1, busy: 0 }
    )
  end
  let(:handler_class) { Class.new(Pgbus::EventBus::Handler) { def self.name = "NotifyHandler" } }
  let(:owner) do
    instance_double(Pgbus::EventBus::Subscriber, pattern: "orders.#", queue_name: "q_orders", handler_class: handler_class)
  end
  let(:registry) { instance_double(Pgbus::EventBus::Registry, subscribers: [owner], queue_names_for_topics: []) }
  let(:stat_buffer) { nil }
  let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer) }
  let(:last_error) do
    { error_class: "RuntimeError", error_message: "boom", backtrace: nil, retry_count: 4, failed_at: nil }
  end
  let(:envelope) do
    JSON.generate("event_id" => "e-1", "headers" => { "routing_key" => "orders.placed" }, "payload" => { "id" => 1 })
  end

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(mock_pool)
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil))
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
    allow(registry).to receive(:handlers_for).and_return([])
    allow(registry).to receive(:handlers_for).with("orders.placed", queue_name: "q_orders").and_return([owner])
    allow(Pgbus.configuration).to receive(:max_retries).and_return(5)
    allow(Pgbus::FailedEventRecorder).to receive_messages(last_error: last_error, clear!: nil, record!: nil)
  end

  # Every pgbus.event_dead_lettered payload emitted while the block runs.
  def capture_dead_letters(&)
    received = []
    callback = ->(_name, _start, _finish, _id, payload) { received << payload }
    ActiveSupport::Notifications.subscribed(callback, "pgbus.event_dead_lettered", &)
    received
  end

  def handle(body, read_ct: 6, msg_id: 7)
    message = build_message_double(msg_id: msg_id, message: body, read_ct: read_ct)
    consumer.send(:handle_message, message, "q_orders")
    message
  end

  it "emits once with the queue, routing key, owning handler, event id, attempts, message id and last error" do
    message = nil
    received = capture_dead_letters { message = handle(envelope) }

    expect(mock_client).to have_received(:move_to_dead_letter).with("q_orders", message, headers: kind_of(String))
    expect(received).to eq([
                             { queue_name: "q_orders", routing_key: "orders.placed", handler: "NotifyHandler",
                               event_id: "e-1", read_ct: 6, msg_id: 7, error: "RuntimeError" }
                           ])
  end

  it "emits after the move returns and after the failed-event row is cleared" do
    calls = []
    allow(mock_client).to receive(:move_to_dead_letter) { calls << :move }
    allow(Pgbus::FailedEventRecorder).to receive(:clear!) { calls << :clear }
    callback = ->(*) { calls << :notify }

    ActiveSupport::Notifications.subscribed(callback, "pgbus.event_dead_lettered") { handle(envelope) }

    expect(calls).to eq(%i[move clear notify])
  end

  it "carries error: nil when no handler failure was recorded" do
    allow(Pgbus::FailedEventRecorder).to receive(:last_error).and_return(nil)

    received = capture_dead_letters { handle(envelope) }

    expect(received.sole).to include(error: nil)
  end

  it "reads a routing key carried at the top level of the body" do
    body = JSON.generate("event_id" => "e-2", "routing_key" => "orders.placed")

    received = capture_dead_letters { handle(body) }

    expect(received.sole).to include(routing_key: "orders.placed", handler: "NotifyHandler", event_id: "e-2")
  end

  it "names no handler when no subscriber in this process owns the queue" do
    allow(registry).to receive(:handlers_for).with("orders.placed", queue_name: "q_orders").and_return([])

    received = capture_dead_letters { handle(envelope) }

    expect(received.sole).to include(routing_key: "orders.placed", handler: nil, event_id: "e-1")
  end

  # PGMQ stores jsonb, so a poison body is as often valid JSON that is not an
  # object (a raw scalar or array a producer wrote straight to the queue) as it
  # is unparseable text. The success path raises on each of these every read
  # until the message dies; the notification must still fire.
  context "when the body is not a JSON object" do
    {
      "unparseable text" => "not json",
      "a JSON number" => "42",
      "a JSON string" => '"orders.placed"',
      "a JSON array" => "[1,2]",
      "JSON null" => "null"
    }.each do |label, body|
      it "still dead-letters #{label} and emits with routing_key, handler and event_id nil" do
        message = nil
        received = nil

        expect { received = capture_dead_letters { message = handle(body) } }.not_to raise_error

        expect(mock_client).to have_received(:move_to_dead_letter).with("q_orders", message, headers: kind_of(String))
        expect(received).to eq([
                                 { queue_name: "q_orders", routing_key: nil, handler: nil, event_id: nil,
                                   read_ct: 6, msg_id: 7, error: "RuntimeError" }
                               ])
        expect(registry).not_to have_received(:handlers_for)
      end
    end
  end

  it "falls back to the top-level routing key when headers is not an object" do
    body = JSON.generate("event_id" => "e-3", "headers" => "oops", "routing_key" => "orders.placed")

    received = capture_dead_letters { handle(body) }

    expect(received.sole).to include(routing_key: "orders.placed", handler: "NotifyHandler", event_id: "e-3")
  end

  it "passes a non-String routing key through without a handler lookup" do
    body = JSON.generate("event_id" => "e-4", "headers" => { "routing_key" => 5 })

    received = capture_dead_letters { handle(body) }

    expect(received.sole).to include(routing_key: 5, handler: nil, event_id: "e-4")
    expect(registry).not_to have_received(:handlers_for)
  end

  it "emits nothing when the move raises (the message stays for the next read)" do
    allow(mock_client).to receive(:move_to_dead_letter).and_raise(StandardError, "db gone")

    received = capture_dead_letters { handle(envelope) }

    expect(received).to be_empty
    expect(Pgbus::FailedEventRecorder).not_to have_received(:clear!)
  end

  context "when a subscriber raises" do
    let(:stat_buffer) { instance_double(Pgbus::StatBuffer, push: nil, flush: nil, flush_if_due: nil, stop: nil) }
    let(:raising) { ->(*) { raise "subscriber bug" } }
    let(:debug_lines) { [] }

    before do
      allow(Pgbus.configuration).to receive(:stats_enabled).and_return(true)
      allow(consumer.circuit_breaker).to receive(:record_failure)
      allow(Pgbus.logger).to receive(:debug) { |&blk| debug_lines << blk.call }
    end

    def handle_with_raising_subscriber
      ActiveSupport::Notifications.subscribed(raising, "pgbus.event_dead_lettered") { handle(envelope) }
    end

    it "leaves the message dead-lettered and does not raise" do
      expect { handle_with_raising_subscriber }.not_to raise_error

      expect(mock_client).to have_received(:move_to_dead_letter).once
      expect(Pgbus::FailedEventRecorder).to have_received(:clear!).with(queue_name: "q_orders", msg_id: 7)
      expect(Pgbus::FailedEventRecorder).not_to have_received(:record!)
    end

    it "records exactly one dead_lettered stat and no failed stat" do
      handle_with_raising_subscriber

      expect(stat_buffer).to have_received(:push).once
      expect(stat_buffer).to have_received(:push).with(hash_including(status: "dead_lettered"))
    end

    it "does not record a circuit-breaker failure for the queue" do
      handle_with_raising_subscriber

      expect(consumer.circuit_breaker).not_to have_received(:record_failure)
    end

    it "logs the subscriber error" do
      handle_with_raising_subscriber

      expect(debug_lines).to include(a_string_including("pgbus.event_dead_lettered", "subscriber bug"))
    end
  end

  # A handler double, so the only parse counted is the consumer's own
  # (Handler#process parses the envelope again on its side).
  describe "the success path" do
    let(:handled_owner) do
      instance_double(Pgbus::EventBus::Subscriber, handler_class: double("HandlerClass", new: double("handler", process: nil)))
    end

    before do
      allow(registry).to receive(:handlers_for).with("orders.placed", queue_name: "q_orders").and_return([handled_owner])
      allow(JSON).to receive(:parse).and_call_original
    end

    it "emits no dead-letter notification and parses the envelope once" do
      received = capture_dead_letters { handle(envelope, read_ct: 1) }

      expect(received).to be_empty
      expect(mock_client).not_to have_received(:move_to_dead_letter)
      expect(JSON).to have_received(:parse).with(envelope).once
    end
  end
end
