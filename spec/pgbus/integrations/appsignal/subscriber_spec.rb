# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pgbus::Integrations::Appsignal::Subscriber" do
  # We stub the entire ::Appsignal module so the spec can run without the
  # real appsignal gem on the load path. The integration only references
  # public Appsignal API: Transaction, Probes, set_gauge, increment_counter,
  # add_distribution_value, set_error.

  let(:appsignal_class) do
    # A spec double: built per example and driven from the example's thread.
    # rubocop:disable-next ThreadSafety/ClassAndModuleAttributes
    Class.new do
      class << self
        attr_accessor :counters, :gauges, :distributions, :errors
      end
      self.counters = []
      self.gauges = []
      self.distributions = []
      self.errors = []

      def self.increment_counter(name, value, tags = {})
        counters << [name, value, tags]
      end

      def self.set_gauge(name, value, tags = {})
        gauges << [name, value, tags]
      end

      def self.add_distribution_value(name, value, tags = {})
        distributions << [name, value, tags]
      end

      def self.set_error(err) # rubocop:disable Naming/AccessorMethodName
        errors << err
      end
    end
  end

  let(:transaction_double) do
    double("Appsignal::Transaction").tap do |dbl|
      allow(dbl).to receive(:set_action_if_nil)
      allow(dbl).to receive(:add_tags)
      allow(dbl).to receive(:set_queue_start)
      allow(dbl).to receive(:set_error)
      allow(dbl).to receive(:add_params_if_nil).and_yield
    end
  end

  let(:completed_counter) { [0] }
  let(:transaction_class) do
    txn = transaction_double
    counter = completed_counter
    klass = Class.new
    klass.define_singleton_method(:create) { |_kind| txn }
    klass.define_singleton_method(:complete_current!) { counter[0] += 1 }
    klass.define_singleton_method(:completed_count) { counter[0] }
    klass
  end

  before do
    stub_const("Appsignal", appsignal_class)
    stub_const("Appsignal::Transaction", transaction_class)
    stub_const("Appsignal::Transaction::BACKGROUND_JOB", "background_job")
    require "pgbus/integrations/appsignal/subscriber"
    Pgbus::Integrations::Appsignal::Subscriber.reset!
    Pgbus::Integrations::Appsignal::Subscriber.install!
  end

  after do
    Pgbus::Integrations::Appsignal::Subscriber.reset!
  end

  describe ".install!" do
    it "subscribes once when two threads install at once", :first_call_race do
      Pgbus::Integrations::Appsignal::Subscriber.reset!
      hold_first_call(ActiveSupport::Notifications, :subscribe)

      race { Pgbus::Integrations::Appsignal::Subscriber.install! }

      expect(ActiveSupport::Notifications).to have_received(:subscribe).with("pgbus.job_completed").once
    end
  end

  describe "pgbus.executor.execute" do
    it "creates a background-job transaction with action and tags" do
      ActiveSupport::Notifications.instrument(
        "pgbus.executor.execute",
        queue: "default",
        job_class: "TestJob",
        job_id: "abc-123",
        provider_job_id: "42",
        arguments: [1, "hi"],
        enqueued_at: "2026-05-05T10:00:00Z",
        read_ct: 1,
        msg_id: 9
      ) { :ok }

      expect(transaction_double).to have_received(:set_action_if_nil).with("TestJob#perform")
      expect(transaction_double).to have_received(:set_queue_start)
      expect(transaction_double).to have_received(:add_tags).with(
        hash_including("queue" => "default", "job_class" => "TestJob", "active_job_id" => "abc-123")
      )
      expect(transaction_class.completed_count).to eq(1)
      distribution_names = appsignal_class.distributions.map(&:first)
      expect(distribution_names).to include("pgbus_job_duration_ms")
    end

    it "completes the transaction even when the inner block raises" do
      expect do
        ActiveSupport::Notifications.instrument(
          "pgbus.executor.execute",
          queue: "default",
          job_class: "BoomJob"
        ) { raise "boom" }
      end.to raise_error("boom")

      expect(transaction_class.completed_count).to eq(1)
    end
  end

  describe "pgbus.job_completed" do
    it "increments the processed counter" do
      ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "TestJob")

      expect(appsignal_class.counters).to include(
        ["pgbus_queue_job_count", 1, { queue: "default", job_class: "TestJob", status: "processed" }]
      )
    end
  end

  describe "pgbus.job_failed" do
    let(:err) { StandardError.new("nope") }

    it "increments the failed counter and reports the error" do
      ActiveSupport::Notifications.instrument(
        "pgbus.job_failed",
        queue: "default",
        job_class: "TestJob",
        error: "StandardError",
        exception_object: err
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_queue_job_count", 1, hash_including(status: "failed")]
      )
      expect(appsignal_class.errors).to include(err)
    end
  end

  describe "pgbus.job_dead_lettered" do
    it "increments the dead_lettered counter" do
      ActiveSupport::Notifications.instrument("pgbus.job_dead_lettered", queue: "default", job_class: "TestJob")

      expect(appsignal_class.counters).to include(
        ["pgbus_queue_job_count", 1, hash_including(status: "dead_lettered")]
      )
    end
  end

  describe "pgbus.event_processed" do
    it "creates a transaction and increments the processed counter" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_processed",
        event_id: "ev-1",
        handler: "MyHandler",
        routing_key: "orders.created",
        published_at: Time.utc(2026, 5, 5, 10, 0, 0),
        read_ct: 1,
        msg_id: 7
      ) { :ok }

      expect(transaction_double).to have_received(:set_action_if_nil).with("MyHandler#handle")
      expect(appsignal_class.counters).to include(
        ["pgbus_event_count", 1, hash_including(handler: "MyHandler", status: "processed")]
      )
    end
  end

  describe "pgbus.event_failed" do
    let(:err) { StandardError.new("handler bug") }

    it "increments the failed counter and reports the error" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_failed",
        handler: "MyHandler",
        routing_key: "orders.created",
        exception_object: err
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_event_count", 1, hash_including(status: "failed")]
      )
      expect(appsignal_class.errors).to include(err)
    end
  end

  describe "pgbus.client.send_message / send_batch / read_batch" do
    it "tracks counters and distributions" do
      ActiveSupport::Notifications.instrument("pgbus.client.send_message", queue: "default") { nil }
      ActiveSupport::Notifications.instrument("pgbus.client.send_batch", queue: "default", count: 5) { nil }
      ActiveSupport::Notifications.instrument("pgbus.client.read_batch", queue: "default", count: 3) { nil }

      counter_names = appsignal_class.counters.map(&:first)
      expect(counter_names).to include("pgbus_messages_sent", "pgbus_messages_read")

      send_distributions = appsignal_class.distributions.map(&:first)
      expect(send_distributions).to include("pgbus_send_duration_ms", "pgbus_send_batch_duration_ms")
    end
  end

  describe "pgbus.stream.broadcast" do
    it "tracks count and bytes" do
      ActiveSupport::Notifications.instrument(
        "pgbus.stream.broadcast",
        stream: "chat:42",
        deferred: false,
        bytes: 128
      ) { nil }

      expect(appsignal_class.counters).to include(
        ["pgbus_stream_broadcast_count", 1, hash_including(stream: "chat:42", deferred: "false")]
      )
      expect(appsignal_class.distributions).to include(
        ["pgbus_stream_broadcast_bytes", 128, { stream: "chat:42" }]
      )
    end
  end

  describe "pgbus.outbox.publish" do
    it "tracks count and duration" do
      ActiveSupport::Notifications.instrument("pgbus.outbox.publish", queue: "default", kind: :job) { nil }

      expect(appsignal_class.counters).to include(["pgbus_outbox_published", 1, { kind: :job }])
      expect(appsignal_class.distributions.map(&:first)).to include("pgbus_outbox_publish_duration_ms")
    end
  end

  describe "pgbus.recurring.enqueue" do
    it "increments the enqueued counter" do
      ActiveSupport::Notifications.instrument(
        "pgbus.recurring.enqueue",
        task: "send_digest",
        class_name: "DigestJob",
        queue: "default",
        run_at: Time.now
      ) { nil }

      expect(appsignal_class.counters).to include(
        ["pgbus_recurring_enqueued", 1, hash_including(task: "send_digest")]
      )
    end
  end

  describe "pgbus.worker.recycle" do
    it "increments the recycled counter with the reason and kind: worker" do
      ActiveSupport::Notifications.instrument(
        "pgbus.worker.recycle",
        reason: :max_jobs,
        jobs_processed: 10_000
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_worker_recycled", 1, hash_including(reason: :max_jobs, kind: "worker")]
      )
    end
  end

  describe "pgbus.consumer.recycle" do
    it "increments the recycled counter with the reason and kind: consumer" do
      ActiveSupport::Notifications.instrument(
        "pgbus.consumer.recycle",
        reason: :max_memory,
        jobs_processed: 500
      )

      expect(appsignal_class.counters).to include(
        ["pgbus_worker_recycled", 1, hash_including(reason: :max_memory, kind: "consumer")]
      )
    end
  end

  describe "pgbus.client.pool" do
    it "is not subscribed — the minutely probe already covers AppSignal pool gauges" do
      appsignal_class.gauges.clear

      ActiveSupport::Notifications.instrument("pgbus.client.pool", size: 10, available: 7, pool_timeout: 5)

      expect(appsignal_class.gauges).to be_empty
      expect(appsignal_class.counters).to be_empty
      expect(appsignal_class.distributions).to be_empty
    end
  end

  describe "subscriber resilience" do
    it "swallows errors and logs at warn level" do
      allow(appsignal_class).to receive(:increment_counter).and_raise(StandardError, "agent down")
      allow(Pgbus.logger).to receive(:warn).and_call_original

      expect do
        ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "TestJob")
      end.not_to raise_error

      expect(Pgbus.logger).to have_received(:warn)
    end
  end
end
