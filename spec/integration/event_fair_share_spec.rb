# frozen_string_literal: true

require_relative "../integration_helper"

# End-to-end contract of fair share for event-bus consumers (issue #427):
# config.event_fair_share tags the event envelope at publish, the tag fans out
# to every bound subscriber queue, the consumer's fair fetch interleaves across
# tenants within each queue, and handlers see an unchanged event payload. The
# scheduling rule itself is pinned in fair_share_read_spec; this spec proves
# the event path feeds it correctly, including through the outbox.
RSpec.describe "Fair share for event-bus consumers (issue #427)", :integration do
  let(:handler_class) do
    # A spec double: built per example and driven from the example's thread.
    # rubocop:disable-next ThreadSafety/ClassInstanceVariable
    Class.new(Pgbus::EventBus::Handler) do
      def self.handled
        @handled ||= []
      end

      def self.name
        "EventFairShareSpec::RecordingHandler"
      end

      def handle(event)
        self.class.handled << event.payload
      end
    end
  end
  let(:audit_class) do
    Class.new(Pgbus::EventBus::Handler) do
      def self.name
        "EventFairShareSpec::AuditHandler"
      end

      def handle(_event); end
    end
  end

  let(:registry) { Pgbus::EventBus::Registry.instance }
  let(:client) { Pgbus.client }
  let(:queue) { "fair_events_q" }
  let(:audit_queue) { "fair_events_audit_q" }
  let(:consumer) { Pgbus::Process::Consumer.new(topics: ["orders.#"], threads: 1) }

  def publish(tenant, count, weight: nil)
    count.times do |i|
      Pgbus.publish("orders.created", { "tenant" => tenant, "n" => i, "w" => weight })
    end
  end

  def tenants_of(messages)
    messages.map { |m| JSON.parse(m.message)["pgbus_fair_key"] }.tally
  end

  before do
    stub_const("EventFairShareSpec", Module.new)
    stub_const("EventFairShareSpec::RecordingHandler", handler_class)
    stub_const("EventFairShareSpec::AuditHandler", audit_class)

    Pgbus.configuration.event_fair_share = ->(e) { [e.payload["tenant"], e.payload["w"] || 1] }

    registry.clear!
    registry.subscribe("orders.created", handler_class, queue_name: queue)
    registry.subscribe("orders.#", audit_class, queue_name: audit_queue)
    registry.setup_all!
    [queue, audit_queue].each { |q| client.purge_queue(q) }

    consumer.send(:setup_subscriptions)
    consumer.send(:ensure_fair_indexes)
  end

  after do
    Pgbus.configuration.event_fair_share = nil
    [queue, audit_queue].each { |q| client.purge_queue(q) }
    registry.clear!
  end

  it "builds the fair index for subscriber queues at setup" do
    exists = client.pgmq.with_connection do |conn|
      conn.exec_params(
        "SELECT 1 FROM pg_indexes WHERE schemaname = 'pgmq' AND indexname = $1",
        ["q_#{Pgbus.configuration.queue_name(queue)}_fair_idx"]
      ).ntuples
    end
    expect(exists).to eq(1)
  end

  it "tags the envelope (not the payload), fans the tag out to every subscriber queue, and fair-reads it" do
    publish("acme", 20)
    publish("globex", 4)

    # Both subscriber queues received every event with the key in the envelope.
    [queue, audit_queue].each do |q|
      messages = client.read_batch_fair(q, qty: 8, vt: 30)
      expect(messages.size).to eq(8)
      expect(tenants_of(messages)).to eq("acme" => 4, "globex" => 4)
      messages.each do |m|
        raw = JSON.parse(m.message)
        expect(raw["payload"]).not_to have_key("pgbus_fair_key")
        expect(raw["payload"]["tenant"]).to eq(raw["pgbus_fair_key"])
      end
    end
  end

  it "honours weight" do
    publish("heavy", 20, weight: 3)
    publish("light", 20)

    messages = client.read_batch_fair(queue, qty: 8, vt: 30)

    expect(tenants_of(messages)).to eq("heavy" => 6, "light" => 2)
    raw = messages.map { |m| JSON.parse(m.message) }
    expect(raw.find { |r| r["pgbus_fair_key"] == "heavy" }["pgbus_fair_weight"]).to eq(3)
    expect(raw.find { |r| r["pgbus_fair_key"] == "light" }).not_to have_key("pgbus_fair_weight")
  end

  it "the consumer fetches fairly across its subscriber queues and the handler sees a clean payload" do
    publish("acme", 10)
    publish("globex", 10)

    tagged = consumer.send(:fetch_messages, 4)

    expect(tagged.size).to eq(4)
    # Strict list order across queues: the first subscriber queue fills the
    # capacity before the audit queue is consulted.
    expect(tagged.map(&:first).uniq).to eq([queue])
    expect(tenants_of(tagged.map(&:last))).to eq("acme" => 2, "globex" => 2)

    tagged.each { |queue_name, message| consumer.send(:handle_message, message, queue_name) }

    expect(handler_class.handled.size).to eq(4)
    expect(handler_class.handled.map { |p| p.keys.sort }.uniq).to eq([%w[n tenant w]])
    expect(handler_class.handled.map { |p| p["tenant"] }.tally).to eq("acme" => 2, "globex" => 2)
    # Archived: a second fair read of the same capacity returns fresh messages only.
    expect(client.read_batch_fair(queue, qty: 20, vt: 30).size).to eq(16)
  end

  it "carries the key across the outbox hop" do
    Pgbus::OutboxEntry.transaction do
      Pgbus::Outbox.publish_event("orders.created", { "tenant" => "outboxed", "n" => 0 })
    end

    expect(Pgbus::Outbox::Poller.new.poll_and_publish).to eq(1)

    message = client.read_batch_fair(queue, qty: 1, vt: 30).first
    raw = JSON.parse(message.message)
    expect(raw["pgbus_fair_key"]).to eq("outboxed")
    expect(raw["payload"]).to eq("tenant" => "outboxed", "n" => 0)
  end
end
