# frozen_string_literal: true

require "spec_helper"

require "singleton"

RSpec.describe Pgbus::EventBus::Registry do
  subject(:registry) { described_class.instance }

  # Use a test double for handler class
  let(:handler_class) do
    klass = Class.new(Pgbus::EventBus::Handler)
    stub_const("TestHandler", klass)
    klass
  end

  before { registry.clear! }

  describe "#subscribe" do
    it "registers a subscriber" do
      registry.subscribe("orders.#", handler_class)
      expect(registry.subscribers.size).to eq(1)
    end

    it "returns a Subscriber" do
      subscriber = registry.subscribe("orders.created", handler_class)
      expect(subscriber).to be_a(Pgbus::EventBus::Subscriber)
      expect(subscriber.pattern).to eq("orders.created")
    end
  end

  describe "#subscribers_matching" do
    before do
      registry.subscribe("orders.#", handler_class)
    end

    it "matches exact routing keys" do
      handlers = registry.subscribers_matching("orders.created")
      expect(handlers.size).to eq(1)
    end

    it "matches wildcard patterns" do
      handlers = registry.subscribers_matching("orders.updated.shipping")
      expect(handlers.size).to eq(1)
    end

    it "does not match unrelated routing keys" do
      handlers = registry.subscribers_matching("users.created")
      expect(handlers).to be_empty
    end
  end

  # Owner-only dispatch (issue #469): each subscriber has its own queue, so a
  # message read from queue Q belongs to Q's owner(s) alone. Pattern-only
  # selection fanned every queue copy out to every matching handler, running
  # each handler once per subscriber on the topic.
  describe "#handlers_for" do
    let(:other_handler_class) do
      klass = Class.new(Pgbus::EventBus::Handler)
      stub_const("OtherTestHandler", klass)
      klass
    end
    let(:third_handler_class) do
      klass = Class.new(Pgbus::EventBus::Handler)
      stub_const("ThirdTestHandler", klass)
      klass
    end

    it "returns only the subscriber owning the queue, though other patterns also match" do
      owner = registry.subscribe("orders.#", handler_class, queue_name: "q_orders")
      registry.subscribe("#", other_handler_class, queue_name: "q_audit")
      registry.subscribe("orders.created", third_handler_class, queue_name: "q_billing")

      expect(registry.handlers_for("orders.created", queue_name: "q_orders")).to eq([owner])
    end

    it "returns both subscribers registered against the same explicit queue_name" do
      first = registry.subscribe("orders.#", handler_class, queue_name: "q_shared")
      second = registry.subscribe("orders.created", other_handler_class, queue_name: "q_shared")

      expect(registry.handlers_for("orders.created", queue_name: "q_shared"))
        .to contain_exactly(first, second)
    end

    it "returns [] when the owner's pattern does not match the routing key (stale binding)" do
      registry.subscribe("orders.#", handler_class, queue_name: "q_orders")

      expect(registry.handlers_for("users.created", queue_name: "q_orders")).to be_empty
    end

    it "returns [] for a queue no subscriber in this process owns" do
      registry.subscribe("orders.#", handler_class, queue_name: "q_orders")

      expect(registry.handlers_for("orders.created", queue_name: "q_ghost")).to be_empty
    end

    it "requires the queue name — a keyword-less call must not fall back to fan-out" do
      registry.subscribe("orders.#", handler_class, queue_name: "q_orders")

      expect { registry.handlers_for("orders.created") }.to raise_error(ArgumentError)
    end
  end

  describe "#event_queue_names" do
    it "returns the prefixed physical queue name for each subscriber (issue #333)" do
      registry.subscribe("orders.#", handler_class, queue_name: "orders_handler")
      prefix = Pgbus.configuration.queue_prefix

      expect(registry.event_queue_names).to contain_exactly("#{prefix}_orders_handler")
    end

    it "returns an empty set when no subscribers are registered" do
      expect(registry.event_queue_names).to be_empty
    end
  end

  describe "#clear!" do
    it "removes all subscribers" do
      registry.subscribe("orders.#", handler_class)
      registry.clear!
      expect(registry.subscribers).to be_empty
    end
  end

  describe "#setup_all! (issue #334)" do
    let(:subscriber) do
      registry.subscribe("orders.#", handler_class, queue_name: "orders_handler")
    end

    it "sets up every subscriber by default" do
      allow(subscriber).to receive(:setup!)
      registry.setup_all!
      expect(subscriber).to have_received(:setup!)
    end

    context "with safe: true" do
      it "swallows a connection error instead of crashing boot" do
        allow(subscriber).to receive(:setup!).and_raise(real_pgmq_connection_error, "db down")
        allow(Pgbus.logger).to receive(:warn)

        expect { registry.setup_all!(safe: true) }.not_to raise_error
        expect(Pgbus.logger).to have_received(:warn)
      end

      it "does NOT swallow a non-connection PG::Error (a real setup bug still surfaces)" do
        # safe: only tolerates "database isn't up yet" — a syntax/permission/
        # missing-table error is a real bug and must propagate (review of #334).
        real_pgmq_connection_error # loads PGMQ::Errors (referenced in the rescue)
        require "pg"
        syntax_error = Class.new(PG::Error)
        allow(subscriber).to receive(:setup!).and_raise(syntax_error, "syntax error")

        expect { registry.setup_all!(safe: true) }.to raise_error(syntax_error)
      end

      it "skips entirely during a schema/db: rake context (no connection opened)" do
        allow(registry).to receive(:schema_task_context?).and_return(true)
        allow(subscriber).to receive(:setup!)

        registry.setup_all!(safe: true)

        expect(subscriber).not_to have_received(:setup!)
      end

      it "still sets up normally outside a schema context" do
        allow(registry).to receive(:schema_task_context?).and_return(false)
        allow(subscriber).to receive(:setup!)

        registry.setup_all!(safe: true)

        expect(subscriber).to have_received(:setup!)
      end
    end
  end

  # Narrowing a handler's subscription keeps its queue (named after the
  # handler), so the old pattern's pgmq.topic_bindings row stays and keeps
  # copying events the consumer archives as unrouted (issue #657).
  describe "#prune_stale_bindings!" do
    let(:client) { instance_double(Pgbus::Client, bind_topic: nil, topic_patterns: [], unbind_topic: true) }
    let(:other_handler_class) do
      klass = Class.new(Pgbus::EventBus::Handler)
      stub_const("OtherTestHandler", klass)
      klass
    end
    let(:bound) { Hash.new { |h, k| h[k] = [] } }

    before do
      allow(Pgbus).to receive(:client).and_return(client)
      allow(Pgbus.logger).to receive(:info)
      allow(client).to receive(:bind_topic) { |pattern, queue| bound[queue] |= [pattern] }
      allow(client).to receive(:topic_patterns) { |queue| bound[queue].dup }
      allow(client).to receive(:unbind_topic) { |pattern, queue| !bound[queue].delete(pattern).nil? }
    end

    it "unbinds a pattern no subscriber of the queue declares and keeps the declared one" do
      bound["q_audit"] = ["#"]
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")

      expect(registry.prune_stale_bindings!).to eq("q_audit" => ["#"])
      expect(bound["q_audit"]).to eq(["order.created"])
      expect(client).to have_received(:unbind_topic).with("#", "q_audit").once
      expect(client).not_to have_received(:unbind_topic).with("order.created", "q_audit")
    end

    it "binds the declared patterns before it unbinds anything" do
      bound["q_audit"] = ["#"]
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")

      registry.prune_stale_bindings!

      expect(client).to have_received(:bind_topic).with("order.created", "q_audit").ordered
      expect(client).to have_received(:unbind_topic).with("#", "q_audit").ordered
    end

    it "keeps every pattern declared by any subscriber sharing the queue" do
      bound["q_shared"] = ["#", "orders.#", "payments.#"]
      registry.subscribe("orders.#", handler_class, queue_name: "q_shared")
      registry.subscribe("payments.#", other_handler_class, queue_name: "q_shared")

      expect(registry.prune_stale_bindings!).to eq("q_shared" => ["#"])
      expect(bound["q_shared"]).to contain_exactly("orders.#", "payments.#")
    end

    it "leaves out queues with nothing stale, so a second run returns an empty hash" do
      bound["q_audit"] = ["#"]
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")
      registry.subscribe("payments.#", other_handler_class, queue_name: "q_payments")

      expect(registry.prune_stale_bindings!).to eq("q_audit" => ["#"])
      expect(registry.prune_stale_bindings!).to eq({})
    end

    it "does not report a pattern another process unbound first" do
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")
      allow(client).to receive(:topic_patterns).with("q_audit").and_return(["#", "order.created"])
      allow(client).to receive(:unbind_topic).with("#", "q_audit").and_return(false)

      expect(registry.prune_stale_bindings!).to eq({})
    end

    it "never touches a queue no subscriber is registered on" do
      bound["q_unregistered"] = ["#"]
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")

      registry.prune_stale_bindings!

      expect(client).not_to have_received(:topic_patterns).with("q_unregistered")
      expect(bound["q_unregistered"]).to eq(["#"])
    end

    it "returns an empty hash and makes no call with no subscriber registered" do
      expect(registry.prune_stale_bindings!).to eq({})
      expect(client).not_to have_received(:topic_patterns)
    end

    it "logs each queue it pruned" do
      bound["q_audit"] = ["#"]
      registry.subscribe("order.created", handler_class, queue_name: "q_audit")

      registry.prune_stale_bindings!

      expect(Pgbus.logger).to have_received(:info) do |&block|
        expect(block.call).to include("q_audit", '"#"')
      end
    end

    context "with queue_names:" do
      it "prunes only the named queues" do
        bound["q_audit"] = ["#"]
        bound["q_payments"] = ["#"]
        registry.subscribe("order.created", handler_class, queue_name: "q_audit")
        registry.subscribe("payments.#", other_handler_class, queue_name: "q_payments")

        expect(registry.prune_stale_bindings!(queue_names: ["q_audit"])).to eq("q_audit" => ["#"])
        expect(bound["q_payments"]).to eq(["#"])
        expect(client).not_to have_received(:topic_patterns).with("q_payments")
      end

      it "prunes nothing for an empty list" do
        bound["q_audit"] = ["#"]
        registry.subscribe("order.created", handler_class, queue_name: "q_audit")

        expect(registry.prune_stale_bindings!(queue_names: [])).to eq({})
        expect(client).not_to have_received(:topic_patterns)
        expect(client).not_to have_received(:bind_topic)
      end

      it "unbinds every pattern of a named queue no subscriber declares any more (a removed handler)" do
        bound["q_removed"] = ["orders.#", "#"]

        expect(registry.prune_stale_bindings!(queue_names: ["q_removed"])).to eq("q_removed" => ["orders.#", "#"])
        expect(bound["q_removed"]).to eq([])
        expect(client).not_to have_received(:bind_topic)
      end
    end
  end

  describe "#queue_names_for_topics" do
    # The topic → queue derivation the Consumer uses in setup_subscriptions,
    # exposed on the registry so the supervisor-owned NotifyHub (issue #381)
    # computes the same LISTEN set the consumer forks read from.
    let(:orders_handler) do
      klass = Class.new(Pgbus::EventBus::Handler)
      stub_const("OrdersHandler", klass)
      klass
    end
    let(:payments_handler) do
      klass = Class.new(Pgbus::EventBus::Handler)
      stub_const("PaymentsHandler", klass)
      klass
    end

    before do
      registry.subscribe("orders.#", orders_handler)
      registry.subscribe("payments.captured", payments_handler, queue_name: "payments_q")
    end

    it "matches by exact pattern equality" do
      expect(registry.queue_names_for_topics(["payments.captured"])).to eq(%w[payments_q])
    end

    it "matches subscriptions the topic filter prefixes" do
      expect(registry.queue_names_for_topics(["orders"])).to eq(%w[orders_handler])
    end

    # The overlap check is deliberately coarse (Consumer#pattern_overlaps?
    # behavior, preserved verbatim by the extraction): ANY topic filter ending
    # in "#" claims every subscriber. The consumer reads more queues than
    # strictly necessary rather than risking an uncovered subscriber.
    it "matches every subscriber for a topic filter ending in #" do
      expect(registry.queue_names_for_topics(["orders.#"])).to contain_exactly("orders_handler", "payments_q")
    end

    it "returns an empty array when nothing overlaps" do
      expect(registry.queue_names_for_topics(["inventory.restocked"])).to eq([])
    end

    it "de-duplicates queue names shared by multiple matching subscribers" do
      registry.subscribe("orders.created", orders_handler, queue_name: "orders_handler")

      expect(registry.queue_names_for_topics(["orders"])).to eq(%w[orders_handler])
    end
  end

  describe "#pattern_matches?" do
    it "matches * to one word and # to any" do
      registry = described_class.instance
      expect(registry.pattern_matches?("orders.*", "orders.created")).to be(true)
      expect(registry.pattern_matches?("orders.*", "orders.created.v2")).to be(false)
      expect(registry.pattern_matches?("orders.#", "orders.created.v2")).to be(true)
      expect(registry.pattern_matches?("orders.#", "webhook.sent")).to be(false)
    end
  end
end
