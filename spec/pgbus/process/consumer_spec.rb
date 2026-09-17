# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Pgbus::Process::Consumer do
  let(:mock_client) { build_mock_client }
  let(:mock_pool) do
    instance_double(
      Pgbus::ExecutionPools::ThreadPool,
      capacity: 3, available_capacity: 3, idle?: true,
      kill: nil, shutdown: nil, wait_for_termination: nil,
      metadata: { mode: :threads, capacity: 3, busy: 0 }
    )
  end
  # NotifyListener#start returns self so callers can chain `.new(...).start`.
  let(:fake_listener) do
    instance_double(
      Pgbus::Process::NotifyListener,
      stop: nil, running?: true, delivering?: true,
      listening_to: [], add_queue: nil, remove_queue: nil
    ).tap { |l| allow(l).to receive(:start).and_return(l) }
  end
  let(:mock_heartbeat) { instance_double(Pgbus::Process::Heartbeat, start: nil, stop: nil) }
  let(:subscriber_a) { instance_double(Pgbus::EventBus::Subscriber, pattern: "orders.#", queue_name: "q_orders") }
  let(:subscriber_b) { instance_double(Pgbus::EventBus::Subscriber, pattern: "payments.completed", queue_name: "q_payments") }
  let(:subscriber_c) { instance_double(Pgbus::EventBus::Subscriber, pattern: "shipping.label", queue_name: "q_shipping") }
  let(:registry) do
    instance_double(Pgbus::EventBus::Registry,
                    subscribers: [subscriber_a, subscriber_b, subscriber_c],
                    queue_names_for_topics: [])
  end

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::ExecutionPools).to receive(:build).and_return(mock_pool)
    allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
    allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(registry)
  end

  describe "#initialize" do
    it "stores topics, threads, and config" do
      consumer = described_class.new(topics: ["orders.#"], threads: 5)

      expect(consumer.topics).to eq(["orders.#"])
      expect(consumer.threads).to eq(5)
      expect(consumer.config).to eq(Pgbus.configuration)
    end

    it "wraps a single topic into an array" do
      consumer = described_class.new(topics: "orders.#")

      expect(consumer.topics).to eq(["orders.#"])
    end

    it "defaults threads to 3" do
      consumer = described_class.new(topics: ["orders.#"])

      expect(consumer.threads).to eq(3)
    end
  end

  describe "#graceful_shutdown" do
    it "sets shutting_down flag" do
      consumer = described_class.new(topics: ["orders.#"])
      consumer.graceful_shutdown

      expect(consumer.shutting_down?).to be true
    end
  end

  describe "#immediate_shutdown" do
    it "sets shutting_down flag and kills the thread pool" do
      consumer = described_class.new(topics: ["orders.#"])
      consumer.immediate_shutdown

      expect(consumer.shutting_down?).to be true
      expect(mock_pool).to have_received(:kill)
    end
  end

  describe "setup_subscriptions (private)" do
    # Overlap semantics are pinned in registry_spec — the consumer only
    # delegates, so the hub (issue #381) and the fork derive the same set.
    it "delegates queue-name derivation to the registry" do
      allow(registry).to receive(:queue_names_for_topics)
        .with(["payments.completed"]).and_return(["q_payments"])

      consumer = described_class.new(topics: ["payments.completed"])
      consumer.send(:setup_subscriptions)

      expect(consumer.queue_names).to eq(["q_payments"])
    end

    it "preserves an injected queue_names seed instead of re-deriving" do
      consumer = described_class.new(topics: ["payments.completed"], queue_names: %w[seeded])
      consumer.send(:setup_subscriptions)

      expect(consumer.queue_names).to eq(%w[seeded])
      expect(registry).not_to have_received(:queue_names_for_topics)
    end
  end

  describe "handle_message (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }
    let(:handler_instance) { double("handler", process: nil) }
    let(:handler_class) { double("HandlerClass", new: handler_instance) }
    let(:matching_subscriber) { instance_double(Pgbus::EventBus::Subscriber, handler_class: handler_class) }
    let(:message_body) { JSON.generate("headers" => { "routing_key" => "orders.created" }, "data" => { "id" => 42 }) }
    let(:message) { build_message_double(msg_id: 7, message: message_body) }

    before do
      allow(registry).to receive(:handlers_for)
        .with("orders.created", queue_name: "q_orders").and_return([matching_subscriber])
    end

    it "parses routing_key, finds handlers, processes, and archives" do
      consumer.send(:handle_message, message, "q_orders")

      expect(registry).to have_received(:handlers_for).with("orders.created", queue_name: "q_orders")
      expect(handler_instance).to have_received(:process).with(message)
      expect(mock_client).to have_received(:archive_message).with("q_orders", 7)
    end

    it "rescues errors gracefully and logs them" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))

      expect { consumer.send(:handle_message, message, "q_orders") }.not_to raise_error
    end

    context "when routing_key is in the message body (not headers)" do
      let(:message_body) { JSON.generate("routing_key" => "orders.shipped", "payload" => { "id" => 99 }) }
      let(:message) { build_message_double(msg_id: 8, message: message_body) }

      before do
        allow(registry).to receive(:handlers_for)
          .with("orders.shipped", queue_name: "q_orders").and_return([matching_subscriber])
      end

      it "extracts routing_key from the top-level body field" do
        consumer.send(:handle_message, message, "q_orders")

        expect(registry).to have_received(:handlers_for).with("orders.shipped", queue_name: "q_orders")
        expect(handler_instance).to have_received(:process).with(message)
        expect(mock_client).to have_received(:archive_message).with("q_orders", 8)
      end
    end

    # Owner-only dispatch (issue #469), against the real registry: each
    # subscriber owns its own queue, so a topic with N matching subscribers
    # produces N queue copies of the event. Dispatching each copy by pattern ran
    # every handler N times per event.
    context "with two subscribers whose patterns both match" do
      # A private-new Registry rather than the singleton: the outer before hook
      # has already stubbed .instance, and a fresh instance keeps the global
      # subscriber list untouched.
      let(:real_registry) { Pgbus::EventBus::Registry.send(:new) }
      let(:invocations) { [] }

      before do
        calls = invocations
        first = Class.new do
          define_method(:process) { |_message| calls << :first }
        end
        second = Class.new do
          define_method(:process) { |_message| calls << :second }
        end
        stub_const("FirstOrdersHandler", first)
        stub_const("SecondOrdersHandler", second)
        real_registry.subscribe("orders.#", first, queue_name: "q_first")
        real_registry.subscribe("orders.#", second, queue_name: "q_second")
        allow(Pgbus::EventBus::Registry).to receive(:instance).and_return(real_registry)
      end

      it "invokes only the handler owning the queue the message was read from" do
        consumer = described_class.new(topics: ["orders.#"], queue_names: %w[q_first q_second])

        consumer.send(:handle_message, message, "q_first")

        expect(invocations).to eq([:first])
      end
    end

    context "when no subscriber in this process owns the queue" do
      before do
        allow(registry).to receive(:handlers_for).and_return([])
        allow(Pgbus::Instrumentation).to receive(:instrument)
        allow(Pgbus.logger).to receive(:warn)
      end

      it "archives the message so it cannot loop through VT redelivery into the DLQ" do
        consumer.send(:handle_message, message, "q_orders")

        expect(mock_client).to have_received(:archive_message).with("q_orders", 7)
      end

      it "logs a warning naming the queue and the routing key" do
        consumer.send(:handle_message, message, "q_orders")

        expect(Pgbus.logger).to have_received(:warn) do |&block|
          expect(block.call).to include("q_orders").and include("orders.created")
        end
      end

      it "emits pgbus.event_unrouted with the queue name and routing key" do
        consumer.send(:handle_message, message, "q_orders")

        expect(Pgbus::Instrumentation).to have_received(:instrument)
          .with("pgbus.event_unrouted", hash_including(queue_name: "q_orders", routing_key: "orders.created"))
      end

      it "warns once per queue per process but instruments every unrouted message" do
        consumer.send(:handle_message, message, "q_orders")
        consumer.send(:handle_message, build_message_double(msg_id: 8, message: message_body), "q_orders")

        expect(Pgbus.logger).to have_received(:warn).once
        expect(Pgbus::Instrumentation).to have_received(:instrument)
          .with("pgbus.event_unrouted", anything).twice
      end

      it "warns again for a different queue" do
        consumer.send(:handle_message, message, "q_orders")
        consumer.send(:handle_message, build_message_double(msg_id: 8, message: message_body), "q_audit")

        expect(Pgbus.logger).to have_received(:warn).twice
      end

      it "records a breaker success — an unowned queue is not a queue failure" do
        allow(consumer.circuit_breaker).to receive(:record_success)

        consumer.send(:handle_message, message, "q_orders")

        expect(consumer.circuit_breaker).to have_received(:record_success).with("q_orders")
      end
    end
  end

  describe "fetch_multi_consumer (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: %w[q_orders q_payments q_shipping]) }

    it "caps the total across queues at qty so the execution pool cannot overflow (issue #123)" do
      allow(mock_client).to receive(:read_multi).and_return([])
      consumer.send(:fetch_multi_consumer, %w[q_orders q_payments q_shipping], 3)
      expect(mock_client).to have_received(:read_multi)
        .with(%w[q_orders q_payments q_shipping], qty: 3, limit: 3)
    end
  end

  describe "#recycle_needed? (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "returns false when no limits are configured" do
      expect(consumer.send(:recycle_needed?)).to be false
    end

    context "when max_jobs_per_worker is exceeded" do
      before { consumer.config.max_jobs_per_worker = 100 }
      after { consumer.config.max_jobs_per_worker = nil }

      it "returns true when jobs_processed reaches the limit" do
        consumer.jobs_processed = 100
        expect(consumer.send(:recycle_needed?)).to be true
      end

      it "returns false when below the limit" do
        consumer.jobs_processed = 50
        expect(consumer.send(:recycle_needed?)).to be false
      end
    end

    context "when max_worker_lifetime is exceeded" do
      before { consumer.config.max_worker_lifetime = 60 }
      after { consumer.config.max_worker_lifetime = nil }

      it "returns true when lifetime is exceeded" do
        # config.max_worker_lifetime = 60 is set by the enclosing before hook.
        aged = described_class.new(
          topics: ["orders.#"],
          started_at_monotonic: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 120
        )
        expect(aged.send(:recycle_needed?)).to be true
      end
    end

    context "when max_memory_mb is exceeded" do
      before do
        consumer.config.max_memory_mb = 256
        Pgbus::Process::MemoryUsage.reset!
      end

      after do
        consumer.config.max_memory_mb = nil
        Pgbus::Process::MemoryUsage.reset!
      end

      it "returns true when memory exceeds the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(512)
        expect(consumer.send(:recycle_needed?)).to be true
      end

      it "returns false when memory is within the limit" do
        allow(Pgbus::Process::MemoryUsage).to receive(:current_mb).and_return(128)
        expect(consumer.send(:recycle_needed?)).to be false
      end
    end
  end

  describe "#check_recycle (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }
    let(:wake_signal) { consumer.wake_signal }

    before { allow(wake_signal).to receive(:notify!) }

    it "does nothing when no recycle is needed" do
      consumer.send(:check_recycle)
      expect(consumer.shutting_down?).to be false
    end

    context "when a limit is exceeded" do
      before do
        consumer.config.max_jobs_per_worker = 10
        consumer.jobs_processed = 10
      end

      after { consumer.config.max_jobs_per_worker = nil }

      it "sets shutting_down so the loop exits cleanly" do
        consumer.send(:check_recycle)
        expect(consumer.shutting_down?).to be true
      end

      it "instruments pgbus.consumer.recycle with the reason" do
        allow(Pgbus::Instrumentation).to receive(:instrument)
        consumer.send(:check_recycle)
        expect(Pgbus::Instrumentation).to have_received(:instrument)
          .with("pgbus.consumer.recycle", hash_including(reason: :max_jobs))
      end

      it "wakes the idle wait so the loop notices the shutdown immediately" do
        consumer.send(:check_recycle)
        expect(wake_signal).to have_received(:notify!)
      end

      it "recycles only once even if called repeatedly" do
        allow(Pgbus::Instrumentation).to receive(:instrument)
        consumer.send(:check_recycle)
        consumer.send(:check_recycle)
        expect(Pgbus::Instrumentation).to have_received(:instrument).once
      end
    end
  end

  describe "recycle counting in handle_message" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }
    let(:handler_instance) { double("handler", process: nil) }
    let(:handler_class) { double("HandlerClass", new: handler_instance) }
    let(:matching_subscriber) { instance_double(Pgbus::EventBus::Subscriber, handler_class: handler_class) }
    let(:message_body) { JSON.generate("headers" => { "routing_key" => "orders.created" }) }
    let(:message) { build_message_double(msg_id: 7, message: message_body) }

    before do
      allow(registry).to receive(:handlers_for)
        .with("orders.created", queue_name: "q_orders").and_return([matching_subscriber])
    end

    it "increments jobs_processed after a successful handle" do
      expect { consumer.send(:handle_message, message, "q_orders") }
        .to change(consumer, :jobs_processed).by(1)
    end

    it "increments jobs_processed when a handler raises (so recycling still counts failures)" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))

      expect { consumer.send(:handle_message, message, "q_orders") }
        .to change(consumer, :jobs_processed).by(1)
    end

    it "increments jobs_processed when a message is routed to the DLQ (poison queue still recycles)" do
      dlq_message = build_message_double(msg_id: 9, message: message_body, read_ct: 99)
      allow(consumer.config).to receive(:max_retries).and_return(5)

      expect { consumer.send(:handle_message, dlq_message, "q_orders") }
        .to change(consumer, :jobs_processed).by(1)
      expect(mock_client).to have_received(:move_to_dead_letter).with("q_orders", dlq_message)
    end
  end

  describe "#wake_timeout (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "returns polling_interval when notify_wakeup? is off" do
      allow(consumer).to receive(:notify_wakeup?).and_return(false)
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end

    it "returns polling_interval when wakeup is on but no listener attached yet" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      consumer.notify_listener = nil
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end

    it "raises the wait to NOTIFY_FALLBACK_POLL_SECONDS when a listener is active" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      consumer.notify_listener = fake_listener
      expect(consumer.send(:wake_timeout))
        .to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
    end

    it "returns polling_interval when the listener thread has died" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(fake_listener).to receive(:running?).and_return(false)
      consumer.notify_listener = fake_listener
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end

    it "returns polling_interval when a live listener is not delivering (issue #332 parity with Worker)" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(fake_listener).to receive_messages(running?: true, delivering?: false)
      consumer.notify_listener = fake_listener
      expect(consumer.send(:wake_timeout)).to eq(consumer.config.polling_interval)
    end
  end

  describe ":supervisor scope (supervisor-mediated wakes, issue #381)" do
    let(:pipe_ios) { IO.pipe }
    let(:piped_consumer) do
      described_class.new(topics: ["orders.#"], queue_names: ["orders"], wake_pipe: pipe_ios[0])
    end

    after do
      piped_consumer.wake_pipe&.stop
      pipe_ios.each { |io| io.close unless io.closed? }
    end

    def wait_for(timeout: 2)
      deadline = Time.now + timeout
      until yield
        raise "timed out waiting for condition" if Time.now > deadline

        sleep 0.01
      end
    end

    it "does not construct a local NotifyListener" do
      allow(piped_consumer).to receive(:notify_wakeup?).and_return(true)
      allow(Pgbus::Process::NotifyListener).to receive(:new)

      piped_consumer.send(:start_wake_source)

      expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
    end

    it "skips the per-fork listener self-healing loop" do
      allow(piped_consumer).to receive(:notify_wakeup?).and_return(true)
      allow(piped_consumer).to receive(:start_notify_listener)
      piped_consumer.notify_retry_at = 0.0

      piped_consumer.send(:ensure_notify_listener)

      expect(piped_consumer).not_to have_received(:start_notify_listener)
    end

    it "wakes the loop on a W byte from the supervisor" do
      piped_consumer.send(:start_wake_source)

      pipe_ios[1].write(Pgbus::Process::WakePipe::WAKE)

      expect(piped_consumer.wake_signal.wait(timeout: 2)).to be true
    end

    it "uses the NOTIFY ceiling while the hub reports delivering" do
      allow(piped_consumer).to receive(:notify_wakeup?).and_return(true)
      piped_consumer.send(:start_wake_source)

      expect(piped_consumer.send(:wake_timeout)).to eq(described_class::NOTIFY_FALLBACK_POLL_SECONDS)
    end

    it "falls back to fast polling when the hub broadcasts degraded" do
      allow(piped_consumer).to receive(:notify_wakeup?).and_return(true)
      piped_consumer.send(:start_wake_source)

      pipe_ios[1].write(Pgbus::Process::WakePipe::DEGRADED)
      wait_for { !piped_consumer.wake_pipe.delivering? }

      expect(piped_consumer.send(:wake_timeout)).to eq(piped_consumer.config.polling_interval)
    end
  end

  describe ":supervisor scope WITHOUT a wake pipe (hub failed to start)" do
    # Poll-only guarantee, mirroring Worker: a hub outage must not balloon
    # the host back to one direct LISTEN connection per fork (issue #381
    # review).
    let(:bare_consumer) { described_class.new(topics: ["orders.#"], queue_names: ["orders"]) }

    before do
      allow(bare_consumer).to receive(:notify_wakeup?).and_return(true)
      allow(bare_consumer.config).to receive(:worker_notify_scope).and_return(:supervisor)
    end

    it "start_wake_source builds no local listener" do
      allow(Pgbus::Process::NotifyListener).to receive(:new)

      bare_consumer.send(:start_wake_source)

      expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
    end

    it "self-healing does not resurrect a local listener either" do
      allow(bare_consumer).to receive(:start_notify_listener)
      bare_consumer.notify_retry_at = 0.0

      bare_consumer.send(:ensure_notify_listener)

      expect(bare_consumer).not_to have_received(:start_notify_listener)
    end
  end

  describe "#start_notify_listener (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["orders"]) }

    it "does not construct a listener when notify_wakeup? is off" do
      allow(consumer).to receive(:notify_wakeup?).and_return(false)
      allow(Pgbus::Process::NotifyListener).to receive(:new)
      consumer.send(:start_notify_listener)
      expect(Pgbus::Process::NotifyListener).not_to have_received(:new)
      expect(consumer.notify_listener).to be_nil
    end

    it "constructs and starts a listener over the physical queue names when wakeup is on" do
      prefix = consumer.config.queue_prefix
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(consumer.config).to receive(:worker_notify_connection_options).and_return({ dbname: "test" })
      allow(Pgbus::Process::NotifyListener).to receive(:new).and_return(fake_listener)

      consumer.send(:start_notify_listener)

      expect(Pgbus::Process::NotifyListener).to have_received(:new).with(
        physical_queues: ["#{prefix}_orders"],
        on_wake: an_instance_of(Proc),
        connection_options: { dbname: "test" },
        health_check_ms: an_instance_of(Integer),
        logger: Pgbus.logger
      )
      expect(fake_listener).to have_received(:start)
      expect(consumer.notify_listener).to be(fake_listener)
    end

    it "swallows listener startup errors and falls back to polling without crashing" do
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
      allow(consumer.config).to receive(:worker_notify_connection_options)
        .and_raise(StandardError, "no direct port configured")
      allow(Pgbus.logger).to receive(:error)

      expect { consumer.send(:start_notify_listener) }.not_to raise_error
      expect(consumer.notify_listener).to be_nil
      expect(Pgbus.logger).to have_received(:error)
    end
  end

  describe "#ensure_notify_listener (private)" do
    # The self-healing loop: a listener that never started (nil) or whose thread
    # died (running? false) must be retried from the consumer loop, throttled by
    # exponential backoff so a persistent outage doesn't spin start attempts
    # every tick. Mirrors Worker#ensure_notify_listener coverage.
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["orders"]) }
    let(:base) { described_class::NOTIFY_RETRY_BASE_SECONDS }
    # A retry time already in the past — the backoff window has elapsed.
    let(:elapsed_retry_at) { consumer.send(:monotonic_now) - 1 }

    before do
      # The local-listener lifecycle exists only under :fork scope; the
      # default is :supervisor (issue #381).
      allow(consumer.config).to receive(:worker_notify_scope).and_return(:fork)
      allow(consumer).to receive(:notify_wakeup?).and_return(true)
    end

    it "is a no-op when notify_wakeup? is off" do
      allow(consumer).to receive(:notify_wakeup?).and_return(false)
      allow(consumer).to receive(:start_notify_listener)
      consumer.send(:ensure_notify_listener)
      expect(consumer).not_to have_received(:start_notify_listener)
    end

    it "is a no-op while a healthy listener is running" do
      consumer.notify_listener = fake_listener
      allow(consumer).to receive(:start_notify_listener)
      consumer.send(:ensure_notify_listener)
      expect(consumer).not_to have_received(:start_notify_listener)
    end

    it "does not retry before the backoff window elapses" do
      future = described_class.new(
        topics: ["orders.#"], queue_names: ["orders"],
        notify_retry_at: Process.clock_gettime(Process::CLOCK_MONOTONIC) + base
      )
      allow(future).to receive(:notify_wakeup?).and_return(true)
      allow(future).to receive(:start_notify_listener)
      future.send(:ensure_notify_listener)
      expect(future).not_to have_received(:start_notify_listener)
    end

    it "retries start once the backoff window has elapsed" do
      consumer.notify_retry_at = elapsed_retry_at
      allow(consumer).to receive(:start_notify_listener) { consumer.notify_listener = fake_listener }

      consumer.send(:ensure_notify_listener)
      expect(consumer).to have_received(:start_notify_listener)
    end

    it "doubles the backoff each time the restart fails" do
      consumer.notify_retry_at = elapsed_retry_at
      allow(consumer).to receive(:start_notify_listener) # leaves notify_listener nil

      consumer.send(:ensure_notify_listener)
      expect(consumer.notify_retry_backoff).to eq(base * 2)

      consumer.notify_retry_at = consumer.send(:monotonic_now) - 1
      consumer.send(:ensure_notify_listener)
      expect(consumer.notify_retry_backoff).to eq(base * 4)
    end

    it "caps the backoff at NOTIFY_RETRY_MAX_SECONDS" do
      capped = described_class.new(
        topics: ["orders.#"], queue_names: ["orders"],
        notify_retry_backoff: described_class::NOTIFY_RETRY_MAX_SECONDS,
        notify_retry_at: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1
      )
      allow(capped).to receive(:notify_wakeup?).and_return(true)
      allow(capped).to receive(:start_notify_listener)

      capped.send(:ensure_notify_listener)
      expect(capped.notify_retry_backoff).to eq(described_class::NOTIFY_RETRY_MAX_SECONDS)
    end

    it "resets the backoff after a successful restart" do
      inflated = described_class.new(
        topics: ["orders.#"], queue_names: ["orders"],
        notify_retry_backoff: described_class::NOTIFY_RETRY_BASE_SECONDS * 8,
        notify_retry_at: Process.clock_gettime(Process::CLOCK_MONOTONIC) - 1
      )
      allow(inflated).to receive(:notify_wakeup?).and_return(true)
      allow(inflated).to receive(:start_notify_listener) { inflated.notify_listener = fake_listener }

      inflated.send(:ensure_notify_listener)
      expect(inflated.notify_retry_backoff).to eq(base)
    end

    it "stops a dead listener before restarting it" do
      dead = fake_listener
      allow(dead).to receive(:running?).and_return(false)
      consumer.notify_listener = dead
      consumer.notify_retry_at = elapsed_retry_at
      allow(consumer).to receive(:start_notify_listener)

      consumer.send(:ensure_notify_listener)
      expect(dead).to have_received(:stop)
    end
  end

  describe "#stop_dead_notify_listener (private)" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "is a no-op and leaves notify_listener nil when none is attached" do
      consumer.notify_listener = nil
      expect { consumer.send(:stop_dead_notify_listener) }.not_to raise_error
      expect(consumer.notify_listener).to be_nil
    end

    it "stops the listener and clears the reference" do
      consumer.notify_listener = fake_listener
      consumer.send(:stop_dead_notify_listener)
      expect(fake_listener).to have_received(:stop)
      expect(consumer.notify_listener).to be_nil
    end

    it "logs, swallows a stop error, and still clears the reference so the loop can't wedge" do
      allow(fake_listener).to receive(:stop).and_raise(StandardError, "listener gone")
      allow(Pgbus.logger).to receive(:warn)
      consumer.notify_listener = fake_listener

      expect { consumer.send(:stop_dead_notify_listener) }.not_to raise_error
      expect(Pgbus.logger).to have_received(:warn)
      expect(consumer.notify_listener).to be_nil
    end
  end

  describe "wake-on-notify integration" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }
    let(:wake_signal) { consumer.wake_signal }

    it "WakeSignal#notify! interrupts the idle wait immediately" do
      # A real WakeSignal: waiting with a long timeout returns instantly once
      # notify! fires from another thread (what NotifyListener's on_wake does).
      waker = Thread.new do
        sleep 0.01
        wake_signal.notify!
      end
      expect(wake_signal.wait(timeout: 5)).to be true
      waker.join
    end

    it "consume waits on the wake_signal (not interruptible_sleep) when idle" do
      allow(mock_client).to receive(:read_batch).and_return([])
      allow(wake_signal).to receive(:wait)

      consumer.send(:consume)

      expect(wake_signal).to have_received(:wait).with(timeout: a_kind_of(Numeric))
    end
  end

  describe "#shutdown stops the notify listener" do
    let(:consumer) { described_class.new(topics: ["orders.#"]) }

    it "stops the listener when one is attached" do
      consumer.notify_listener = fake_listener
      consumer.send(:shutdown)
      expect(fake_listener).to have_received(:stop)
    end

    it "bounds the drain wait by config.drain_timeout, not a hardcoded 30s (issue #386)" do
      config = Pgbus::Configuration.new
      config.drain_timeout = 42
      consumer = described_class.new(topics: ["orders.#"], config: config)

      consumer.send(:shutdown)

      expect(mock_pool).to have_received(:wait_for_termination).with(42)
    end
  end

  # Operational parity with Worker (issue #274). A consumer fork must be
  # visible to the supervisor watchdog, so it stamps a loop beacon each
  # iteration and persists it via the heartbeat's loop_tick_supplier.
  describe "loop-tick beacon (issue #274)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }

    it "starts with a nil last_loop_tick" do
      expect(consumer.last_loop_tick).to be_nil
    end

    it "stamps a wall-clock loop tick when stamp_loop_tick runs" do
      before_stamp = Time.now.to_f
      consumer.send(:stamp_loop_tick)
      expect(consumer.last_loop_tick).to be >= before_stamp
    end

    it "pokes the liveness pipe with a byte when the supervisor forked one" do
      reader, writer = IO.pipe
      piped = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], liveness_pipe: writer)

      piped.send(:stamp_loop_tick)

      expect(reader.read_nonblock(16)).to eq("\0")
    ensure
      reader.close unless reader.closed?
      writer.close unless writer.closed?
    end

    it "never raises from stamp_loop_tick when the pipe reader is gone" do
      reader, writer = IO.pipe
      reader.close
      piped = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], liveness_pipe: writer)

      expect { piped.send(:stamp_loop_tick) }.not_to raise_error
    ensure
      writer.close unless writer.closed?
    end

    it "wires the heartbeat with kind consumer, a loop_tick_supplier, and an on_beat hook" do
      allow(Pgbus::Process::Heartbeat).to receive(:new).and_return(mock_heartbeat)
      consumer.send(:stamp_loop_tick)
      tick = consumer.last_loop_tick

      consumer.send(:start_heartbeat)

      expect(Pgbus::Process::Heartbeat).to have_received(:new) do |kwargs|
        expect(kwargs[:kind]).to eq("consumer")
        expect(kwargs[:on_beat]).to be_a(Proc)
        expect(kwargs[:loop_tick_supplier].call).to eq(tick)
      end
    end
  end

  # Consumer consults the circuit breaker before fetching so a poison queue
  # that keeps tripping the breaker is skipped instead of hammered forever.
  describe "circuit breaker (issue #274)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }
    let(:message) do
      build_message_double(msg_id: 7, message: JSON.generate("headers" => { "routing_key" => "orders.created" }))
    end

    before do
      allow(registry).to receive(:handlers_for).and_return([])
    end

    it "skips a queue whose breaker is tripped and reads nothing" do
      allow(consumer.circuit_breaker).to receive(:paused?).with("q_orders").and_return(true)

      consumer.send(:fetch_messages, 3)

      expect(mock_client).not_to have_received(:read_batch)
    end

    it "reads a queue whose breaker is closed" do
      allow(consumer.circuit_breaker).to receive(:paused?).with("q_orders").and_return(false)
      allow(mock_client).to receive(:read_batch).with("q_orders", qty: 3).and_return([])

      consumer.send(:fetch_messages, 3)

      expect(mock_client).to have_received(:read_batch).with("q_orders", qty: 3)
    end

    it "records a breaker success on a handled message" do
      allow(consumer.circuit_breaker).to receive(:record_success)
      consumer.send(:handle_message, message, "q_orders")
      expect(consumer.circuit_breaker).to have_received(:record_success).with("q_orders")
    end

    it "records a breaker failure when the handler raises" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))
      allow(consumer.circuit_breaker).to receive(:record_failure)

      consumer.send(:handle_message, message, "q_orders")

      expect(consumer.circuit_breaker).to have_received(:record_failure).with("q_orders")
    end

    it "idles the loop WITHOUT an ErrorReporter call when the connection breaker is open" do
      allow(mock_client).to receive(:read_batch).and_raise(Pgbus::ConnectionCircuitOpenError)
      allow(consumer.circuit_breaker).to receive(:paused?).and_return(false)
      allow(Pgbus::ErrorReporter).to receive(:report)

      expect(consumer.send(:fetch_messages, 3)).to eq([])
      expect(Pgbus::ErrorReporter).not_to have_received(:report)
    end

    it "reports a generic fetch error to the ErrorReporter" do
      allow(mock_client).to receive(:read_batch).and_raise(StandardError.new("db gone"))
      allow(consumer.circuit_breaker).to receive(:paused?).and_return(false)
      allow(Pgbus::ErrorReporter).to receive(:report)

      expect(consumer.send(:fetch_messages, 3)).to eq([])
      expect(Pgbus::ErrorReporter).to have_received(:report)
    end
  end

  describe "fair share for consumers (issue #427)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: %w[q_orders q_payments]) }
    let(:orders_msgs) { [build_message_double(msg_id: 1), build_message_double(msg_id: 2)] }
    let(:payments_msgs) { [build_message_double(msg_id: 3)] }

    before do
      Pgbus.configuration.event_fair_share = ->(_event) { "tenant" }
      allow(mock_client).to receive(:ensure_fair_index)
      allow(mock_client).to receive(:read_batch_fair) do |queue, **_kwargs|
        case queue
        when "q_orders" then orders_msgs
        when "q_payments" then payments_msgs
        else []
        end
      end
    end

    after { Pgbus.configuration.event_fair_share = nil }

    it "fair-reads each active queue in list order with the remaining capacity" do
      results = consumer.send(:fetch_messages, 5)

      expect(mock_client).to have_received(:read_batch_fair).with("q_orders", qty: 5).ordered
      expect(mock_client).to have_received(:read_batch_fair).with("q_payments", qty: 3).ordered
      expect(mock_client).not_to have_received(:read_multi)
      expect(mock_client).not_to have_received(:read_batch)
      expect(results).to eq([["q_orders", orders_msgs[0]], ["q_orders", orders_msgs[1]], ["q_payments", payments_msgs[0]]])
    end

    it "stops reading once capacity is filled" do
      consumer.send(:fetch_messages, 2)

      expect(mock_client).to have_received(:read_batch_fair).with("q_orders", qty: 2)
      expect(mock_client).not_to have_received(:read_batch_fair).with("q_payments", qty: anything)
    end

    it "uses the fair read on the single-queue path too" do
      single = described_class.new(topics: ["orders.#"], queue_names: %w[q_orders])
      single.send(:fetch_messages, 4)

      expect(mock_client).to have_received(:read_batch_fair).with("q_orders", qty: 4)
      expect(mock_client).not_to have_received(:read_batch)
    end

    it "skips queues paused by the circuit breaker" do
      allow(consumer.circuit_breaker).to receive(:paused?) { |q| q == "q_orders" }

      consumer.send(:fetch_messages, 5)

      expect(mock_client).not_to have_received(:read_batch_fair).with("q_orders", qty: anything)
      expect(mock_client).to have_received(:read_batch_fair).with("q_payments", qty: 5)
    end

    it "does not fair-read when only config.fair_share (jobs) is set" do
      Pgbus.configuration.event_fair_share = nil
      Pgbus.configuration.fair_share = ->(_job) { "tenant" }

      consumer.send(:fetch_messages, 5)

      expect(mock_client).not_to have_received(:read_batch_fair)
      expect(mock_client).to have_received(:read_multi)
    ensure
      Pgbus.configuration.fair_share = nil
    end

    it "ensures the fair index for every subscriber queue it reads" do
      consumer.send(:ensure_fair_indexes)

      expect(mock_client).to have_received(:ensure_fair_index).with("q_orders")
      expect(mock_client).to have_received(:ensure_fair_index).with("q_payments")
    end

    it "ensures nothing when event fair share is off" do
      Pgbus.configuration.event_fair_share = nil

      consumer.send(:ensure_fair_indexes)

      expect(mock_client).not_to have_received(:ensure_fair_index)
    end

    it "ensures the indexes on run, after subscriptions are resolved" do
      allow(registry).to receive(:queue_names_for_topics).with(["orders.#"]).and_return(%w[q_orders])
      runner = described_class.new(topics: ["orders.#"])
      allow(runner).to receive(:setup_signals)
      allow(runner).to receive(:consume) { runner.graceful_shutdown }
      allow(runner).to receive(:shutdown)

      runner.run

      expect(mock_client).to have_received(:ensure_fair_index).with("q_orders")
    end
  end

  # StatBuffer parity: the consumer records success / failure / DLQ outcomes
  # and flushes at drain entry, on recycle, and on stop, mirroring Worker.
  describe "stat buffer (issue #274)" do
    let(:message) do
      build_message_double(msg_id: 7, message: JSON.generate("headers" => { "routing_key" => "orders.created" }))
    end
    let(:handler_instance) { double("handler", process: nil) }
    let(:handler_class) { double("HandlerClass", new: handler_instance) }
    let(:matching_subscriber) { instance_double(Pgbus::EventBus::Subscriber, handler_class: handler_class) }
    let(:stat_buffer) { instance_double(Pgbus::StatBuffer, push: nil, flush: nil, flush_if_due: nil, stop: nil) }

    before do
      allow(registry).to receive(:handlers_for)
        .with("orders.created", queue_name: "q_orders").and_return([matching_subscriber])
    end

    it "builds a StatBuffer when stats_enabled is on" do
      allow(Pgbus.configuration).to receive(:stats_enabled).and_return(true)
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"])
      expect(consumer.stat_buffer).to be_a(Pgbus::StatBuffer)
    end

    it "builds no StatBuffer when stats_enabled is off" do
      allow(Pgbus.configuration).to receive(:stats_enabled).and_return(false)
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"])
      expect(consumer.stat_buffer).to be_nil
    end

    it "records a success stat on a handled message" do
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      consumer.send(:handle_message, message, "q_orders")
      expect(stat_buffer).to have_received(:push).with(hash_including(status: "success", queue_name: "q_orders"))
    end

    it "records a failed stat when the handler raises" do
      allow(registry).to receive(:handlers_for).and_raise(StandardError.new("boom"))
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      consumer.send(:handle_message, message, "q_orders")
      expect(stat_buffer).to have_received(:push).with(hash_including(status: "failed"))
    end

    it "records a dead_lettered stat when the message is routed to the DLQ" do
      dlq_message = build_message_double(msg_id: 9, message: message.message, read_ct: 99)
      allow(Pgbus.configuration).to receive(:max_retries).and_return(5)
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      consumer.send(:handle_message, dlq_message, "q_orders")
      expect(stat_buffer).to have_received(:push).with(hash_including(status: "dead_lettered"))
    end

    it "flushes buffered stats on graceful_shutdown (drain entry)" do
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      allow(consumer.wake_signal).to receive(:notify!)
      consumer.graceful_shutdown
      expect(stat_buffer).to have_received(:flush)
    end

    it "flushes buffered stats when a recycle is triggered" do
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      allow(consumer.wake_signal).to receive(:notify!)
      allow(Pgbus::Instrumentation).to receive(:instrument)
      consumer.config.max_jobs_per_worker = 5
      consumer.jobs_processed = 5

      consumer.send(:check_recycle)

      expect(stat_buffer).to have_received(:flush)
    ensure
      consumer.config.max_jobs_per_worker = nil
    end

    it "stops the StatBuffer on shutdown" do
      consumer = described_class.new(topics: ["orders.#"], queue_names: ["q_orders"], stat_buffer: stat_buffer)
      consumer.send(:shutdown)
      expect(stat_buffer).to have_received(:stop)
    end
  end

  # Pool-stats heartbeat parity: on_beat emits pgbus.client.pool once per beat.
  describe "pool-stats heartbeat (issue #274)" do
    let(:consumer) { described_class.new(topics: ["orders.#"], queue_names: ["q_orders"]) }

    it "emits pgbus.client.pool with the pool stats" do
      allow(Pgbus::Instrumentation).to receive(:instrument)
      consumer.send(:on_heartbeat)
      expect(Pgbus::Instrumentation).to have_received(:instrument)
        .with("pgbus.client.pool", { size: 5, available: 5, pool_timeout: 5 })
    end

    it "does not emit when pool_stats is empty" do
      allow(mock_client).to receive(:pool_stats).and_return({})
      allow(Pgbus::Instrumentation).to receive(:instrument)
      consumer.send(:on_heartbeat)
      expect(Pgbus::Instrumentation).not_to have_received(:instrument)
    end

    it "never lets a heartbeat hook error crash the beat" do
      allow(mock_client).to receive(:pool_stats).and_raise(StandardError.new("pool boom"))
      expect { consumer.send(:on_heartbeat) }.not_to raise_error
    end
  end
end
