# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::ActiveJob::Adapter do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:job_id) { SecureRandom.uuid }
  let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
  let(:serialized_hash) { { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] } }

  # Shared concurrency fixtures — used by both the #enqueue and #enqueue_all
  # concurrency contexts so the two stay in sync.
  let(:concurrency_config) do
    { limit: 1, duration: 900, on_conflict: :block, key: ->(*) { "TestJob-42" } }
  end
  let(:concurrency_job_class) do
    double("JobClass", pgbus_concurrency: concurrency_config, name: "TestJob").tap do |klass|
      allow(klass).to receive(:respond_to?).and_return(false)
      allow(klass).to receive(:respond_to?).with(:pgbus_concurrency).and_return(true)
    end
  end
  let(:concurrency_payload) { serialized_hash.merge("pgbus_concurrency_key" => "TestJob-42") }

  before do
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
  end

  describe "#enqueue" do
    it "serializes the job, sends a message, sets provider_job_id, and returns the job" do
      allow(mock_client).to receive(:send_message).and_return(42)

      result = adapter.enqueue(job)

      expect(Pgbus::Serializer).to have_received(:serialize_job_hash).with(job)
      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: 0, priority: nil)
      expect(job).to have_received(:provider_job_id=).with(42)
      expect(result).to eq(job)
    end

    context "when queue_name is nil" do
      let(:job) { build_job_double(job_class: "TestJob", queue_name: nil, job_id: job_id) }

      before do
        allow(job).to receive(:queue_name).and_return(nil)
      end

      it "falls back to config.default_queue" do
        allow(mock_client).to receive(:send_message).and_return(1)

        adapter.enqueue(job)

        expect(mock_client).to have_received(:send_message).with("default", anything, delay: 0, priority: nil)
      end
    end
  end

  describe "#enqueue_at" do
    it "calculates delay and sends message with delay parameter" do
      future_time = Time.now.to_f + 60
      allow(mock_client).to receive(:send_message).and_return(99)

      result = adapter.enqueue_at(job, future_time)

      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: a_value_between(59, 61), priority: nil)
      expect(job).to have_received(:provider_job_id=).with(99)
      expect(result).to eq(job)
    end

    context "when timestamp is in the past" do
      it "uses delay 0" do
        past_time = Time.now.to_f - 100
        allow(mock_client).to receive(:send_message).and_return(7)

        adapter.enqueue_at(job, past_time)

        expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: 0, priority: nil)
      end
    end
  end

  describe "#enqueue with concurrency" do
    let(:job_class_double) { concurrency_job_class }

    before do
      allow(Pgbus::Concurrency).to receive_messages(inject_metadata: concurrency_payload, extract_key: "TestJob-42")
      allow(job).to receive(:class).and_return(job_class_double)
    end

    it "acquires semaphore and enqueues when under limit" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(mock_client).to receive(:send_message).and_return(42)

      adapter.enqueue(job)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire).with("TestJob-42", 1, 900)
      expect(mock_client).to have_received(:send_message).with("default", concurrency_payload, delay: 0, priority: nil)
      expect(job).to have_received(:provider_job_id=).with(42)
    end

    it "blocks when at concurrency limit with on_conflict: :block" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
      allow(job).to receive(:try).with(:priority).and_return(0)

      adapter.enqueue(job)

      expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).with(
        concurrency_key: "TestJob-42",
        queue_name: "default",
        payload: concurrency_payload,
        priority: 0,
        duration: 900
      )
      expect(mock_client).not_to have_received(:send_message)
    end

    it "discards when at concurrency limit with on_conflict: :discard" do
      allow(job_class_double).to receive(:pgbus_concurrency).and_return(
        concurrency_config.merge(on_conflict: :discard)
      )
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
    end

    it "raises when at concurrency limit with on_conflict: :raise" do
      allow(job_class_double).to receive(:pgbus_concurrency).and_return(
        concurrency_config.merge(on_conflict: :raise)
      )
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

      expect { adapter.enqueue(job) }.to raise_error(Pgbus::ConcurrencyLimitExceeded, /TestJob-42/)
    end

    context "when inside a batch context" do
      around do |example|
        Thread.current[:pgbus_batch_id] = "batch-1"
        Thread.current[:pgbus_batch_job_count] = 0
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
        Thread.current[:pgbus_batch_job_count] = nil
      end

      it "does not count a job discarded at the concurrency limit — it will never signal completion" do
        allow(job_class_double).to receive(:pgbus_concurrency).and_return(
          concurrency_config.merge(on_conflict: :discard)
        )
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

        adapter.enqueue(job)

        expect(Thread.current[:pgbus_batch_job_count]).to eq(0)
      end

      it "keeps a blocked job counted — it runs once the semaphore frees and signals then" do
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
        allow(job).to receive(:try).with(:priority).and_return(0)

        adapter.enqueue(job)

        expect(Thread.current[:pgbus_batch_job_count]).to eq(1)
      end
    end
  end

  describe "#enqueue with :until_executed uniqueness and retry_on (issue #333)" do
    let(:uniqueness_config) do
      { strategy: :until_executed, key: ->(*) { "UniqJob-42" }, explicit_key: true, on_conflict: :reject }
    end
    let(:job_class_double) do
      double("JobClass", pgbus_uniqueness: uniqueness_config, name: "UniqJob").tap do |klass|
        allow(klass).to receive(:respond_to?).and_return(false)
        allow(klass).to receive(:respond_to?).with(:pgbus_uniqueness).and_return(true)
      end
    end
    let(:uniqueness_payload) { serialized_hash.merge("pgbus_uniqueness_key" => "UniqJob-42") }

    before do
      allow(Pgbus::Uniqueness).to receive_messages(inject_metadata: uniqueness_payload, extract_key: "UniqJob-42")
      allow(Pgbus::Uniqueness).to receive(:uniqueness_config).and_return(uniqueness_config)
      allow(Pgbus::Uniqueness).to receive(:bind_lock)
      allow(Pgbus::Uniqueness).to receive(:release_lock)
      allow(job).to receive(:class).and_return(job_class_double)
      allow(mock_client).to receive(:send_message).and_return(42)
    end

    after do
      Thread.current[:pgbus_acquired_uniqueness_key] = nil
    end

    it "rejects a FRESH duplicate (executions == 0) whose key is already held" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:locked)

      expect { adapter.enqueue(job) }.to raise_error(Pgbus::JobNotUnique, /UniqJob/)
      expect(mock_client).not_to have_received(:send_message)
    end

    it "lets a RETRY re-enqueue (executions > 0) through against its own held key" do
      allow(job).to receive(:executions).and_return(1)
      # acquire_enqueue_lock must NOT even be consulted — the retry owns the key.
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock)

      adapter.enqueue(job)

      expect(mock_client).to have_received(:send_message)
      expect(Pgbus::Uniqueness).not_to have_received(:acquire_enqueue_lock)
      expect(Pgbus::Uniqueness).not_to have_received(:bind_lock)
    end

    it "acquires against the logical queue and binds msg_id after send (issue #418)" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:acquired)

      adapter.enqueue(job)

      expect(Pgbus::Uniqueness).to have_received(:acquire_enqueue_lock).with(
        "UniqJob-42", job, queue_name: "default"
      )
      expect(Pgbus::Uniqueness).to have_received(:bind_lock).with(
        "UniqJob-42", queue_name: "default", msg_id: 42
      )
      expect(job).to have_received(:provider_job_id=).with(42)
    end

    it "does not bind when send_message fails, and rolls back the lock" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:acquired)
      allow(mock_client).to receive(:send_message).and_raise(StandardError, "connection refused")

      expect { adapter.enqueue(job) }.to raise_error(StandardError, "connection refused")
      expect(Pgbus::Uniqueness).to have_received(:release_lock).with("UniqJob-42")
      expect(Pgbus::Uniqueness).not_to have_received(:bind_lock)
    end

    it "still enqueues when bind_lock raises" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:acquired)
      allow(Pgbus::Uniqueness).to receive(:bind_lock).and_raise(StandardError, "pooler timeout")
      allow(Pgbus.logger).to receive(:warn)

      result = adapter.enqueue(job)

      expect(Pgbus::Uniqueness).to have_received(:bind_lock)
      expect(result).to eq(job)
      expect(job).to have_received(:provider_job_id=).with(42)
    end

    context "when inside a batch context" do
      around do |example|
        Thread.current[:pgbus_batch_id] = "batch-1"
        Thread.current[:pgbus_batch_job_count] = 0
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
        Thread.current[:pgbus_batch_job_count] = nil
      end

      it "does not count a duplicate discarded at enqueue time into the batch" do
        allow(job).to receive(:executions).and_return(0)
        allow(Pgbus::Uniqueness).to receive_messages(acquire_enqueue_lock: :locked,
                                                     uniqueness_config: uniqueness_config.merge(on_conflict: :discard))

        adapter.enqueue(job)

        expect(mock_client).not_to have_received(:send_message)
        expect(Thread.current[:pgbus_batch_job_count]).to eq(0)
      end
    end
  end

  describe "#enqueue with uniqueness and concurrency" do
    let(:uniqueness_config) do
      { strategy: :until_executed, key: ->(*) { "UniqJob-42" }, explicit_key: true, on_conflict: :reject }
    end
    let(:job_class_double) do
      double("JobClass",
             pgbus_concurrency: concurrency_config.merge(on_conflict: :discard),
             pgbus_uniqueness: uniqueness_config,
             name: "TestJob").tap do |klass|
        allow(klass).to receive(:respond_to?).and_return(false)
        allow(klass).to receive(:respond_to?).with(:pgbus_concurrency).and_return(true)
        allow(klass).to receive(:respond_to?).with(:pgbus_uniqueness).and_return(true)
      end
    end
    let(:combined_payload) do
      serialized_hash.merge(
        "pgbus_concurrency_key" => "TestJob-42",
        "pgbus_uniqueness_key" => "UniqJob-42"
      )
    end

    before do
      allow(job).to receive_messages(class: job_class_double, executions: 0)
      allow(Pgbus::Concurrency).to receive_messages(inject_metadata: combined_payload, extract_key: "TestJob-42")
      allow(Pgbus::Uniqueness).to receive_messages(
        inject_metadata: combined_payload,
        extract_key: "UniqJob-42",
        uniqueness_config: uniqueness_config,
        acquire_enqueue_lock: :acquired
      )
      allow(Pgbus::Uniqueness).to receive(:release_lock)
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
    end

    after { Thread.current[:pgbus_acquired_uniqueness_key] = nil }

    it "releases the :until_executed lock when a concurrency :discard conflict drops the job" do
      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
      expect(Pgbus::Uniqueness).to have_received(:release_lock).with("UniqJob-42")
    end

    it "does not release the lock when the job is blocked — the stored payload runs later" do
      allow(job_class_double).to receive(:pgbus_concurrency).and_return(concurrency_config)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
      allow(job).to receive(:try).with(:priority).and_return(0)

      adapter.enqueue(job)

      expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
    end

    it "does not release a uniqueness lock it did not acquire (duplicate discarded)" do
      allow(Pgbus::Uniqueness).to receive_messages(
        acquire_enqueue_lock: :locked,
        uniqueness_config: uniqueness_config.merge(on_conflict: :discard)
      )

      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
      expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
    end
  end

  describe "#enqueue_all" do
    let(:second_job_id) { SecureRandom.uuid }
    let(:job2) { build_job_double(job_class: "OtherJob", queue_name: "default", job_id: second_job_id) }
    let(:second_serialized_hash) do
      { "job_class" => "OtherJob", "job_id" => second_job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(job).to receive(:scheduled_at).and_return(nil)
      allow(job2).to receive(:scheduled_at).and_return(nil)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job).and_return(serialized_hash)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job2).and_return(second_serialized_hash)
    end

    it "batches immediate jobs via send_batch" do
      allow(mock_client).to receive(:send_batch).and_return([1, 2])

      result = adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch).with("default", [serialized_hash, second_serialized_hash])
      expect(job).to have_received(:provider_job_id=).with(1)
      expect(job2).to have_received(:provider_job_id=).with(2)
      expect(result).to eq(2)
    end

    it "schedules future jobs individually via enqueue_at" do
      future_time = Time.now + 120
      allow(job).to receive(:scheduled_at).and_return(future_time)
      allow(job2).to receive(:scheduled_at).and_return(nil)

      # job is scheduled in the future -> enqueue_at individually
      # job2 is immediate -> send_batch
      allow(mock_client).to receive_messages(send_message: 10, send_batch: [20])

      adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: a_value > 0, priority: nil)
      expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash])
    end

    context "when batch response size mismatches" do
      it "raises an error" do
        allow(mock_client).to receive(:send_batch).and_return([1])

        expect { adapter.enqueue_all([job, job2]) }.to raise_error(Pgbus::EnqueueError, /batch enqueue failed/)
      end
    end

    context "when inside a batch context (issue #413)" do
      let(:batch_id) { SecureRandom.uuid }

      around do |example|
        Thread.current[:pgbus_batch_id] = batch_id
        Thread.current[:pgbus_batch_job_count] = 0
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
        Thread.current[:pgbus_batch_job_count] = nil
      end

      it "tags every bulk payload with the batch id and counts the jobs" do
        allow(mock_client).to receive(:send_batch).and_return([1, 2])

        adapter.enqueue_all([job, job2])

        expect(mock_client).to have_received(:send_batch).with(
          "default",
          [
            serialized_hash.merge(Pgbus::Batch::METADATA_KEY => batch_id),
            second_serialized_hash.merge(Pgbus::Batch::METADATA_KEY => batch_id)
          ]
        )
        expect(Thread.current[:pgbus_batch_job_count]).to eq(2)
      end
    end

    context "with concurrency-limited jobs (issue #413)" do
      before do
        allow(Pgbus::Concurrency).to receive_messages(inject_metadata: concurrency_payload, extract_key: "TestJob-42")
        allow(job).to receive(:class).and_return(concurrency_job_class)
      end

      it "routes the concurrency job through the individual enqueue path while plain jobs stay on the bulk path" do
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
        allow(mock_client).to receive_messages(send_message: 10, send_batch: [20])

        adapter.enqueue_all([job, job2])

        expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire).with("TestJob-42", 1, 900)
        expect(mock_client).to have_received(:send_message).with("default", concurrency_payload, delay: 0, priority: nil)
        expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash])
      end

      it "handles conflicts instead of silently bypassing the limit" do
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
        allow(job).to receive(:try).with(:priority).and_return(0)
        allow(mock_client).to receive(:send_batch).and_return([20])

        adapter.enqueue_all([job, job2])

        expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert)
        expect(mock_client).not_to have_received(:send_message)
      end
    end
  end
end
