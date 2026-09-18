# frozen_string_literal: true

require "spec_helper"
require "socket"

RSpec.describe Pgbus::Metrics::Subscriber do
  # A recording backend that captures every call for assertion.
  let(:backend) do
    Class.new(Pgbus::Metrics::Backend) do
      attr_reader :counters, :gauges, :histograms

      def initialize
        super
        @counters = []
        @gauges = []
        @histograms = []
      end

      def increment(name, value = 1, tags = {})
        @counters << [name, value, tags]
      end

      def gauge(name, value, tags = {})
        @gauges << [name, value, tags]
      end

      def histogram(name, value, tags = {})
        @histograms << [name, value, tags]
      end
    end.new
  end

  before do
    described_class.reset!
    described_class.install!(backend: backend)
  end

  after { described_class.reset! }

  describe ".install!" do
    it "is idempotent" do
      expect(described_class.install!(backend: backend)).to be(false)
      expect(described_class).to be_installed
    end
  end

  describe "pgbus.job_completed" do
    it "increments queue_job_count with a processed status" do
      ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "TestJob")

      expect(backend.counters).to include(
        ["pgbus_queue_job_count", 1, { queue: "default", job_class: "TestJob", status: "processed" }]
      )
    end
  end

  describe "pgbus.job_failed" do
    it "increments queue_job_count with a failed status" do
      ActiveSupport::Notifications.instrument(
        "pgbus.job_failed", queue: "default", job_class: "TestJob", exception_object: StandardError.new
      )

      expect(backend.counters).to include(
        ["pgbus_queue_job_count", 1, hash_including(status: "failed")]
      )
    end
  end

  describe "pgbus.job_dead_lettered" do
    it "increments queue_job_count with a dead_lettered status" do
      ActiveSupport::Notifications.instrument("pgbus.job_dead_lettered", queue: "default", job_class: "TestJob")

      expect(backend.counters).to include(
        ["pgbus_queue_job_count", 1, hash_including(status: "dead_lettered")]
      )
    end
  end

  describe "pgbus.executor.execute" do
    it "records a job_duration_ms histogram" do
      ActiveSupport::Notifications.instrument(
        "pgbus.executor.execute", queue: "default", job_class: "TestJob"
      ) { :ok }

      names = backend.histograms.map(&:first)
      expect(names).to include("pgbus_job_duration_ms")
    end
  end

  describe "pgbus.event_processed" do
    it "records event_duration_ms and increments event_count as processed" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_processed", handler: "H", routing_key: "orders.created"
      ) { :ok }

      expect(backend.histograms.map(&:first)).to include("pgbus_event_duration_ms")
      expect(backend.counters).to include(
        ["pgbus_event_count", 1, hash_including(status: "processed")]
      )
    end
  end

  describe "pgbus.event_failed" do
    it "increments event_count as failed" do
      ActiveSupport::Notifications.instrument("pgbus.event_failed", handler: "H", routing_key: "k")

      expect(backend.counters).to include(
        ["pgbus_event_count", 1, hash_including(status: "failed")]
      )
    end
  end

  describe "pgbus.event_skipped" do
    it "increments event_count as skipped, tagged with the reason (issue #470)" do
      ActiveSupport::Notifications.instrument(
        "pgbus.event_skipped", handler: "H", routing_key: "k", reason: :owned
      )

      expect(backend.counters).to include(
        ["pgbus_event_count", 1, hash_including(status: "skipped", reason: :owned)]
      )
    end
  end

  describe "pgbus.client.send_message" do
    it "increments messages_sent by one" do
      ActiveSupport::Notifications.instrument("pgbus.client.send_message", queue: "default") { :ok }

      expect(backend.counters).to include(
        ["pgbus_messages_sent", 1, { queue: "default" }]
      )
    end
  end

  describe "pgbus.client.send_batch" do
    it "increments messages_sent by the batch count" do
      ActiveSupport::Notifications.instrument("pgbus.client.send_batch", queue: "default", count: 5) { :ok }

      expect(backend.counters).to include(
        ["pgbus_messages_sent", 5, { queue: "default" }]
      )
    end
  end

  describe "pgbus.client.read_batch" do
    it "increments messages_read by the fetched count" do
      ActiveSupport::Notifications.instrument("pgbus.client.read_batch", queue: "default", count: 3) { :ok }

      expect(backend.counters).to include(
        ["pgbus_messages_read", 3, { queue: "default" }]
      )
    end
  end

  describe "pgbus.stream.broadcast" do
    it "increments stream_broadcast_count" do
      ActiveSupport::Notifications.instrument("pgbus.stream.broadcast", stream: "chat", deferred: false)

      expect(backend.counters).to include(
        ["pgbus_stream_broadcast_count", 1, hash_including(stream: "chat")]
      )
    end
  end

  describe "pgbus.outbox.publish" do
    it "increments outbox_published" do
      ActiveSupport::Notifications.instrument("pgbus.outbox.publish", kind: "job") { :ok }

      expect(backend.counters).to include(
        ["pgbus_outbox_published", 1, { kind: "job" }]
      )
    end
  end

  describe "pgbus.recurring.enqueue" do
    it "increments recurring_enqueued" do
      ActiveSupport::Notifications.instrument("pgbus.recurring.enqueue", task: "cleanup", class_name: "CleanupJob")

      expect(backend.counters).to include(
        ["pgbus_recurring_enqueued", 1, hash_including(task: "cleanup")]
      )
    end
  end

  describe "pgbus.worker.recycle" do
    it "increments worker_recycled tagged kind: worker" do
      ActiveSupport::Notifications.instrument("pgbus.worker.recycle", reason: "max_jobs")

      expect(backend.counters).to include(
        ["pgbus_worker_recycled", 1, { reason: "max_jobs", kind: "worker" }]
      )
    end
  end

  describe "pgbus.consumer.recycle" do
    it "increments worker_recycled tagged kind: consumer" do
      ActiveSupport::Notifications.instrument("pgbus.consumer.recycle", reason: "max_memory")

      expect(backend.counters).to include(
        ["pgbus_worker_recycled", 1, { reason: "max_memory", kind: "consumer" }]
      )
    end
  end

  describe "pgbus.client.pool" do
    it "forwards size and available as gauges tagged with hostname" do
      ActiveSupport::Notifications.instrument("pgbus.client.pool", size: 10, available: 7, pool_timeout: 5)

      expect(backend.gauges).to include(
        ["pgbus_pool_size", 10, { hostname: Socket.gethostname }]
      )
      expect(backend.gauges).to include(
        ["pgbus_pool_available", 7, { hostname: Socket.gethostname }]
      )
    end
  end

  describe "backend failure isolation" do
    it "rescues and logs when the backend raises, never into the producer" do
      allow(backend).to receive(:increment).and_raise(StandardError, "backend down")
      allow(Pgbus.logger).to receive(:warn)

      expect do
        ActiveSupport::Notifications.instrument("pgbus.job_completed", queue: "default", job_class: "T")
      end.not_to raise_error

      expect(Pgbus.logger).to have_received(:warn)
    end
  end
end
