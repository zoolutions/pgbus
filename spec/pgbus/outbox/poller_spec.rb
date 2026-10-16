# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Outbox::Poller do
  let(:heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: true, stop: true) }
  let(:mock_client) { build_mock_client }
  let(:poller) { described_class.new }

  before do
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(heartbeat)
    allow(Pgbus).to receive(:client).and_return(mock_client)
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      poller.graceful_shutdown
      expect(poller.instance_variable_get(:@shutting_down)).to be true
    end
  end

  describe "#poll_and_publish" do
    let(:outbox_entry_class) { stub_const("Pgbus::OutboxEntry", Class.new) }
    let(:relation) { double("relation") }

    before do
      outbox_entry_class
      allow(Pgbus::OutboxEntry).to receive(:unpublished).and_return(relation)
      allow(Pgbus::OutboxEntry).to receive(:transaction).and_yield
      allow(relation).to receive_messages(order: relation, limit: relation, lock: relation)
    end

    context "with direct-queue entries for the same queue" do
      let(:first_entry) do
        double("first_entry", id: 1, queue_name: "default", routing_key: nil,
                              payload: { "data" => "one" }, headers: nil, delay: 0, priority: 1, update!: true)
      end
      let(:second_entry) do
        double("second_entry", id: 2, queue_name: "default", routing_key: nil,
                               payload: { "data" => "two" }, headers: nil, delay: 0, priority: 1, update!: true)
      end

      before { allow(relation).to receive(:to_a).and_return([first_entry, second_entry], []) }

      it "batch-publishes via send_batch" do
        result = poller.poll_and_publish

        expect(mock_client).to have_received(:send_batch).with(
          "default", [{ "data" => "one" }, { "data" => "two" }], headers: nil, delay: 0, priority: 1
        )
        expect(first_entry).to have_received(:update!).with(published_at: a_kind_of(Time))
        expect(second_entry).to have_received(:update!).with(published_at: a_kind_of(Time))
        expect(result).to eq(2)
      end
    end

    it "publishes topic-routed entries individually" do
      entry = double("entry",
                     id: 3,
                     queue_name: nil,
                     routing_key: "orders.created",
                     payload: { "event" => "data" },
                     headers: { "trace" => "id" },
                     delay: nil,
                     priority: 0,
                     update!: true)
      allow(relation).to receive(:to_a).and_return([entry], [])

      result = poller.poll_and_publish

      expect(mock_client).to have_received(:publish_to_topic)
        .with("orders.created", { "event" => "data" }, headers: { "trace" => "id" }, delay: 0)
      expect(result).to eq(1)
    end

    it "groups direct-queue entries by queue, priority, and delay" do
      entry_a = double("entry_a", id: 4, queue_name: "default", routing_key: nil,
                                  payload: "a", headers: nil, delay: 0, priority: 1, update!: true)
      entry_b = double("entry_b", id: 5, queue_name: "urgent", routing_key: nil,
                                  payload: "b", headers: nil, delay: 0, priority: 0, update!: true)
      allow(relation).to receive(:to_a).and_return([entry_a, entry_b], [])

      poller.poll_and_publish

      expect(mock_client).to have_received(:send_batch).with("default", ["a"], headers: nil, delay: 0, priority: 1)
      expect(mock_client).to have_received(:send_batch).with("urgent", ["b"], headers: nil, delay: 0, priority: 0)
    end

    it "falls back to individual sends when batch fails" do
      entry = double("entry",
                     id: 6,
                     queue_name: "default",
                     routing_key: nil,
                     payload: { "data" => "test" },
                     headers: nil,
                     delay: 0,
                     priority: 1,
                     update!: true)
      allow(relation).to receive(:to_a).and_return([entry], [])
      allow(mock_client).to receive(:send_batch).and_raise(StandardError, "batch failed")
      allow(mock_client).to receive(:send_message).and_return(1)

      result = poller.poll_and_publish

      # Fallback carries the same priority as the batch path
      expect(mock_client).to have_received(:send_message).with(
        "default", { "data" => "test" }, headers: nil, delay: 0, priority: 1
      )
      expect(result).to eq(1)
    end

    it "returns 0 when no entries" do
      allow(relation).to receive(:to_a).and_return([])

      result = poller.poll_and_publish
      expect(result).to eq(0)
    end

    it "rescues errors gracefully" do
      allow(relation).to receive(:to_a).and_raise(StandardError, "db error")

      expect { poller.poll_and_publish }.not_to raise_error
      expect(poller.poll_and_publish).to eq(0)
    end
  end

  # Issue #493: the Processes page states the poller's uptime.
  describe "#start_heartbeat (private)" do
    it "registers its pid, start time and parent pid (the supervisor forks it)" do
      poller.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new).with(
        kind: "outbox_poller",
        config: kind_of(Pgbus::Configuration),
        metadata: { pid: Process.pid, started_at: a_value_within(5).of(Time.now.to_f), supervisor_pid: Process.ppid }
      )
    end
  end
end
