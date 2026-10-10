# frozen_string_literal: true

require_relative "../integration_helper"

# The Events page against a real PGMQ (issue #494): a failed handler is
# recorded by the consumer, so the list reads the event as retrying with its
# error; a successful redelivery and a dead-letter move clear the row; a
# processed event replays from its archived message; and the Jobs list leaves
# the handler queue out.
RSpec.describe "Dashboard event list (integration)", :integration do
  let(:handler_class) do
    Class.new(Pgbus::EventBus::Handler) do
      idempotent!

      class << self
        attr_accessor :failures_left

        def handled = (@handled ||= [])
        def name = "DashboardEventListSpec::FlakyHandler"
      end

      def handle(event)
        if self.class.failures_left.to_i.positive?
          self.class.failures_left -= 1
          raise KeyError, "key not found: :email"
        end
        self.class.handled << event
      end
    end
  end

  let(:registry) { Pgbus::EventBus::Registry.instance }
  let(:consumer) { Pgbus::Process::Consumer.new(topics: ["orders.#"], threads: 1) }
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue) { "dashboard_event_list" }
  let(:physical) { Pgbus.configuration.queue_name(queue) }

  before do
    stub_const("DashboardEventListSpec", Module.new)
    stub_const("DashboardEventListSpec::FlakyHandler", handler_class)
    registry.clear!
    registry.subscribe("orders.created", handler_class, queue_name: queue)
    registry.setup_all!
    handler_class.dedup_cache.clear!
    handler_class.failures_left = 0
    consumer.send(:setup_subscriptions)
  end

  after { registry.clear! }

  # vt: 0 so the next read redelivers at once, as an expired lease would.
  def publish_and_handle
    Pgbus::EventBus::Publisher.publish("orders.created", { "order_id" => 7 })
    deliver
  end

  # A real redelivery waits out the visibility timeout, which outlasts the
  # claim ownership window; vt: 0 does not, so age the failed attempt's
  # pending claim the way that wait would.
  def deliver
    Pgbus::ProcessedEvent.where(completed_at: nil).update_all(processed_at: Time.now.utc - 3600)
    message = client.read_message(queue, vt: 0)
    expect(message).not_to be_nil
    consumer.send(:handle_message, message, queue)
    message
  end

  def failed_rows = Pgbus::BusRecord.connection.select_all("SELECT * FROM pgbus_failed_events").to_a

  it "lists a failed event as retrying with its error, and a successful redelivery clears it" do
    handler_class.failures_left = 1
    message = publish_and_handle

    rows = data_source.event_rows
    expect(rows.size).to eq(1)
    expect(rows.first).to include(state: "retrying", msg_id: message.msg_id.to_i, error_class: "KeyError",
                                  handler_class: "DashboardEventListSpec::FlakyHandler", pattern: "orders.created")
    expect(data_source.event_state_counts["retrying"]).to eq(1)
    expect(failed_rows.first).to include("queue_name" => queue, "retry_count" => 0)

    deliver

    expect(data_source.event_rows).to be_empty
    expect(failed_rows).to be_empty
    expect(handler_class.handled.size).to eq(1)
  end

  it "clears the failed row when the event is dead-lettered, carrying the error into the DLQ" do
    allow(Pgbus.configuration).to receive(:max_retries).and_return(1)
    handler_class.failures_left = 5
    publish_and_handle
    deliver # read_ct 2 > max_retries 1: dead-lettered

    expect(failed_rows).to be_empty
    dlq = client.read_message("#{queue}#{Pgbus::DEAD_LETTER_SUFFIX}", vt: 30)
    expect(Pgbus::DeadLetterHeader.parse(dlq.headers)).to include("source" => "consumer", "error_class" => "KeyError")
  end

  it "leaves the handler queue off the Jobs list" do
    handler_class.failures_left = 1
    publish_and_handle

    expect(data_source.job_rows(queue_name: physical).size).to eq(1)
    jobs = data_source.job_rows(exclude: data_source.handler_queue_physical_names)
    expect(jobs.map { |r| r[:queue_name] }).not_to include(physical, queue)
    expect(data_source.job_state_counts(exclude: data_source.handler_queue_physical_names)["retrying"]).to eq(0)
  end

  it "replays a processed event from its archived message under a new event_id" do
    publish_and_handle
    processed = data_source.processed_events.first
    original = handler_class.handled.first

    expect(data_source.event_replay_states([processed])).to eq(processed["id"] => :replayable)
    expect(data_source.replay_event(processed)).to be(true)

    deliver

    replayed = handler_class.handled.last
    expect(handler_class.handled.size).to eq(2)
    expect(replayed.event_id).not_to eq(original.event_id)
    expect(replayed.payload).to eq(original.payload)
    expect(Pgbus::ProcessedEvent.where(handler_class: handler_class.name).count).to eq(2)
  end

  it "does not offer a replay once the archive no longer holds the event" do
    publish_and_handle
    processed = data_source.processed_events.first
    Pgbus::BusRecord.connection.execute("DELETE FROM pgmq.a_#{physical}")

    expect(data_source.event_replay_states([processed])).to eq(processed["id"] => :not_archived)
    expect(data_source.replay_event(processed)).to be(false)
  end
end
