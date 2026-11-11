# frozen_string_literal: true

require_relative "../integration_helper"

# Narrowing a subscription keeps the handler's queue, so the old pattern's
# pgmq.topic_bindings row stays and keeps copying events into it (issue #657).
# Client#unbind_topic and Registry#prune_stale_bindings! remove it, against a
# real PGMQ.
RSpec.describe "Topic binding prune (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:registry) { Pgbus::EventBus::Registry.instance }
  let(:audit_queue) { "prune_audit" }
  let(:billing_queue) { "prune_billing" }
  let(:bystander_queue) { "prune_bystander" }

  let(:audit_handler) { Class.new(Pgbus::EventBus::Handler) { def handle(_event) = nil } }
  let(:billing_handler) { Class.new(Pgbus::EventBus::Handler) { def handle(_event) = nil } }

  def clear_bindings
    ActiveRecord::Base.connection.execute(
      "DELETE FROM pgmq.topic_bindings WHERE queue_name LIKE 'pgbus_int_prune_%'"
    )
  end

  before do
    clear_bindings
    registry.clear!
  end

  after do
    registry.clear!
    clear_bindings
  end

  describe "Client#unbind_topic" do
    it "removes the binding and returns true, then false when there is none" do
      client.bind_topic("orders.#", audit_queue)

      expect(client.topic_patterns(audit_queue)).to eq(["orders.#"])
      expect(client.unbind_topic("orders.#", audit_queue)).to be(true)
      expect(client.topic_patterns(audit_queue)).to eq([])
      expect(client.unbind_topic("orders.#", audit_queue)).to be(false)
    end
  end

  describe "Registry#prune_stale_bindings!" do
    before do
      # Release 1: the audit handler takes every event.
      registry.subscribe("#", audit_handler, queue_name: audit_queue)
      registry.subscribe("invoice.paid", billing_handler, queue_name: billing_queue)
      registry.setup_all!
      client.bind_topic("#", bystander_queue)

      # Release 2: the same handler, narrowed.
      registry.clear!
      registry.subscribe("order.created", audit_handler, queue_name: audit_queue)
      registry.subscribe("invoice.paid", billing_handler, queue_name: billing_queue)
      registry.setup_all!
    end

    it "removes the old pattern, keeps the new one and leaves other queues alone" do
      expect(client.topic_patterns(audit_queue)).to contain_exactly("#", "order.created")

      expect(registry.prune_stale_bindings!).to eq(audit_queue => ["#"])

      expect(client.topic_patterns(audit_queue)).to eq(["order.created"])
      expect(client.topic_patterns(billing_queue)).to eq(["invoice.paid"])
      expect(client.topic_patterns(bystander_queue)).to eq(["#"])
    end

    it "returns an empty hash on a second run" do
      registry.prune_stale_bindings!

      expect(registry.prune_stale_bindings!).to eq({})
    end

    it "stops an event only the old pattern matched from reaching the queue" do
      registry.prune_stale_bindings!

      Pgbus::EventBus::Publisher.publish("widget.updated", {})
      Pgbus::EventBus::Publisher.publish("order.created", { "id" => 1 })

      expect(client.read_batch(audit_queue, qty: 10).size).to eq(1)
    end

    it "keeps both patterns when two subscribers spell one queue differently" do
      registry.subscribe("payments.#", billing_handler, queue_name: "Prune-Audit")
      registry.setup_all!

      expect(registry.prune_stale_bindings!).to eq(audit_queue => ["#"])
      expect(client.topic_patterns(audit_queue)).to contain_exactly("order.created", "payments.#")
    end

    it "with queue_names:, prunes only the named queue" do
      client.bind_topic("#", billing_queue)

      expect(registry.prune_stale_bindings!(queue_names: [billing_queue])).to eq(billing_queue => ["#"])

      expect(client.topic_patterns(billing_queue)).to eq(["invoice.paid"])
      expect(client.topic_patterns(audit_queue)).to contain_exactly("#", "order.created")
    end

    it "with queue_names:, raises for a queue no subscriber is registered on and unbinds nothing" do
      expect { registry.prune_stale_bindings!(queue_names: [audit_queue, bystander_queue]) }
        .to raise_error(ArgumentError, /prune_bystander/)

      expect(client.topic_patterns(bystander_queue)).to eq(["#"])
      expect(client.topic_patterns(audit_queue)).to contain_exactly("#", "order.created")
    end
  end
end
