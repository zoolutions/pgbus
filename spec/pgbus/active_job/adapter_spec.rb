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
    allow(Pgbus::Semaphore).to receive(:transaction).and_yield
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

  describe "#enqueue inside a batch (issue #423)" do
    around do |example|
      Thread.current[:pgbus_batch_id] = "batch-1"
      example.run
    ensure
      Thread.current[:pgbus_batch_id] = nil
    end

    it "counts the job into its batch before the message is sent" do
      allow(Pgbus::Batch).to receive(:track_enqueue)
      allow(mock_client).to receive(:send_message).and_return(7)

      adapter.enqueue(job)

      expect(Pgbus::Batch).to have_received(:track_enqueue)
        .with(hash_including(Pgbus::Batch::METADATA_KEY => "batch-1", "job_id" => anything)).ordered
      expect(mock_client).to have_received(:send_message).ordered
    end

    it "raises AlreadyFinished before sending when the batch has finished" do
      allow(Pgbus::Batch).to receive(:track_enqueue).and_raise(Pgbus::Batch::AlreadyFinished)

      expect { adapter.enqueue(job) }.to raise_error(Pgbus::Batch::AlreadyFinished)
      expect(mock_client).not_to have_received(:send_message)
    end
  end

  describe "#enqueue of a retry_on re-enqueue outside any batch block (issue #424)" do
    before do
      allow(Pgbus::Batch).to receive(:track_retry)
      allow(Pgbus::Batch).to receive(:track_enqueue)
      allow(Pgbus::Batch).to receive(:note_retry_reenqueued)
      allow(mock_client).to receive(:send_message).and_return(9)
    end

    it "re-tags a retry into its batch without counting it and remembers it re-enqueued" do
      allow(job).to receive_messages(batch_id: "b-1", executions: 1)

      adapter.enqueue(job)

      tagged = serialized_hash.merge(Pgbus::Batch::METADATA_KEY => "b-1")
      expect(Pgbus::Batch).to have_received(:track_retry).with(tagged)
      expect(Pgbus::Batch).not_to have_received(:track_enqueue)
      expect(mock_client).to have_received(:send_message).with("default", tagged, delay: 0, priority: nil)
      expect(Pgbus::Batch).to have_received(:note_retry_reenqueued).with(job_id)
    end

    it "leaves a first-attempt job with a batch_id alone — membership stays explicit" do
      allow(job).to receive_messages(batch_id: "b-1", executions: 0)

      adapter.enqueue(job)

      expect(Pgbus::Batch).not_to have_received(:track_retry)
      expect(mock_client).to have_received(:send_message).with("default", serialized_hash, delay: 0, priority: nil)
    end

    it "does not re-tag a job that only reports on a batch (callback_batch_id)" do
      allow(job).to receive_messages(batch_id: nil, callback_batch_id: "b-1", executions: 1)

      adapter.enqueue(job)

      expect(Pgbus::Batch).not_to have_received(:track_retry)
    end

    it "does not remember a retry that was discarded at the concurrency limit" do
      allow(job).to receive_messages(batch_id: "b-1", executions: 1)
      allow(job).to receive(:class).and_return(concurrency_job_class)
      allow(concurrency_job_class).to receive(:pgbus_concurrency).and_return(concurrency_config.merge(on_conflict: :discard))
      allow(Pgbus::Concurrency).to receive_messages(inject_metadata: concurrency_payload, extract_key: "TestJob-42")
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
      allow(Pgbus::Batch).to receive(:forget_retry_reenqueued)

      adapter.enqueue(job)

      expect(mock_client).not_to have_received(:send_message)
      expect(Pgbus::Batch).not_to have_received(:note_retry_reenqueued)
      expect(Pgbus::Batch).to have_received(:forget_retry_reenqueued).with(job_id)
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

    # rails/solid_queue#712: the semaphore check and the park must commit
    # together, holding the semaphore row lock in between, so a holder that
    # signals concurrently waits and then sees the parked job.
    it "parks the job inside the transaction that saw the semaphore full" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
      allow(job).to receive(:try).with(:priority).and_return(0)
      in_transaction = []
      allow(Pgbus::Semaphore).to receive(:transaction) do |&block|
        in_transaction << :open
        block.call.tap { in_transaction << :closed }
      end
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert) { in_transaction << :parked }

      adapter.enqueue(job)

      expect(in_transaction).to eq(%i[open parked closed])
    end

    # A crash between COMMIT and the PGMQ produce leaks a slot (recovered by
    # the sweep); a crash the other way round — message live, slot rolled
    # back — would let the next enqueue run beside it. Send after commit, so
    # the only reachable failure is the safe one.
    it "sends the message only after the slot transaction has committed" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(mock_client).to receive(:send_message).and_return(42)
      order = []
      allow(Pgbus::Semaphore).to receive(:transaction) do |&block|
        order << :begin
        block.call
        order << :commit
      end
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire) {
        order << :acquire
        :acquired
      }
      allow(mock_client).to receive(:send_message) {
        order << :send
        42
      }

      adapter.enqueue(job)

      expect(order).to eq(%i[begin acquire commit send])
    end

    it "releases the acquired slot when the send raises, instead of leaking it until expiry" do
      allow(Pgbus::Concurrency::Semaphore).to receive_messages(acquire: :acquired, release: nil)
      allow(mock_client).to receive(:send_message).and_raise(StandardError, "pgmq down")

      expect { adapter.enqueue(job) }.to raise_error(StandardError, "pgmq down")

      expect(Pgbus::Concurrency::Semaphore).to have_received(:release).with("TestJob-42")
    end

    it "does not release any slot when the job was parked rather than sent" do
      allow(Pgbus::Concurrency::Semaphore).to receive_messages(acquire: :blocked, release: nil)
      allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
      allow(job).to receive(:try).with(:priority).and_return(0)

      adapter.enqueue(job)

      expect(Pgbus::Concurrency::Semaphore).not_to have_received(:release)
    end

    # A delayed job holds its slot from enqueue, but the visibility heartbeat
    # only starts at dequeue — so the lease has to cover the delay too, or the
    # sweep expires it mid-wait and promotes a second job for the same key.
    it "covers the scheduled delay in the slot's lease" do
      allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
      allow(mock_client).to receive(:send_message).and_return(42)

      adapter.enqueue_at(job, Time.current.to_f + 3600)

      expect(Pgbus::Concurrency::Semaphore).to have_received(:acquire) do |_key, _limit, duration|
        expect(duration).to be >= 900 + 3600
      end
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
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
      end

      before do
        allow(Pgbus::Batch).to receive(:track_enqueue)
        allow(Pgbus::Batch).to receive(:untrack_enqueue)
      end

      it "uncounts a job discarded at the concurrency limit — it will never signal completion" do
        allow(job_class_double).to receive(:pgbus_concurrency).and_return(
          concurrency_config.merge(on_conflict: :discard)
        )
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)

        adapter.enqueue(job)

        expect(Pgbus::Batch).to have_received(:track_enqueue).with(hash_including(Pgbus::Batch::METADATA_KEY => "batch-1"))
        expect(Pgbus::Batch).to have_received(:untrack_enqueue).with(hash_including(Pgbus::Batch::METADATA_KEY => "batch-1"))
      end

      it "keeps a blocked job counted — it runs once the semaphore frees and signals then" do
        allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
        allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
        allow(job).to receive(:try).with(:priority).and_return(0)

        adapter.enqueue(job)

        expect(Pgbus::Batch).to have_received(:track_enqueue).once
        expect(Pgbus::Batch).not_to have_received(:untrack_enqueue)
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

    it "clears the uniqueness thread-local after send without releasing the live lock" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:acquired)
      allow(Pgbus::Uniqueness).to receive(:release_lock)
      allow(Pgbus::Batch).to receive(:backfill_execution).and_raise(StandardError, "backfill boom")

      expect { adapter.enqueue(job) }.to raise_error(StandardError, "backfill boom")
      expect(Pgbus::Uniqueness).to have_received(:bind_lock)
      expect(Pgbus::Uniqueness).not_to have_received(:release_lock)
      expect(Thread.current[:pgbus_acquired_uniqueness_key]).to be_nil
    end

    it "releases the uniqueness lock when send_message raises before a message exists" do
      allow(job).to receive(:executions).and_return(0)
      allow(Pgbus::Uniqueness).to receive(:acquire_enqueue_lock).and_return(:acquired)
      allow(Pgbus::Uniqueness).to receive(:release_lock)
      allow(mock_client).to receive(:send_message).and_raise(StandardError, "pg down")

      expect { adapter.enqueue(job) }.to raise_error(StandardError, "pg down")
      expect(Pgbus::Uniqueness).to have_received(:release_lock).with("UniqJob-42")
      expect(Thread.current[:pgbus_acquired_uniqueness_key]).to be_nil
    end

    context "when inside a batch context" do
      around do |example|
        Thread.current[:pgbus_batch_id] = "batch-1"
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
      end

      before do
        allow(Pgbus::Batch).to receive(:track_enqueue)
        allow(Pgbus::Batch).to receive(:untrack_enqueue)
      end

      it "uncounts a duplicate discarded at enqueue time from the batch" do
        allow(job).to receive(:executions).and_return(0)
        allow(Pgbus::Uniqueness).to receive_messages(acquire_enqueue_lock: :locked,
                                                     uniqueness_config: uniqueness_config.merge(on_conflict: :discard))

        adapter.enqueue(job)

        expect(mock_client).not_to have_received(:send_message)
        expect(Pgbus::Batch).to have_received(:untrack_enqueue).with(hash_including("job_id" => anything))
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

      expect(mock_client).to have_received(:send_batch).with("default", [serialized_hash, second_serialized_hash], priority: nil)
      expect(job).to have_received(:provider_job_id=).with(1)
      expect(job2).to have_received(:provider_job_id=).with(2)
      expect(result).to eq(2)
    end

    it "sends one batch per priority level so priority routing is preserved" do
      allow(job).to receive(:try).with(:priority).and_return(0)
      allow(job2).to receive(:try).with(:priority).and_return(2)
      allow(mock_client).to receive(:send_batch).and_return([1])

      adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch).with("default", [serialized_hash], priority: 0)
      expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash], priority: 2)
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
      expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash], priority: nil)
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
        example.run
      ensure
        Thread.current[:pgbus_batch_id] = nil
      end

      before { allow(Pgbus::Batch).to receive(:track_enqueue) }

      it "tags every bulk payload with the batch id and counts them once, before the send" do
        allow(mock_client).to receive(:send_batch).and_return([1, 2])
        tagged = [
          serialized_hash.merge(Pgbus::Batch::METADATA_KEY => batch_id),
          second_serialized_hash.merge(Pgbus::Batch::METADATA_KEY => batch_id)
        ]

        adapter.enqueue_all([job, job2])

        expect(Pgbus::Batch).to have_received(:track_enqueue).with(tagged).once.ordered
        expect(mock_client).to have_received(:send_batch).with("default", tagged, priority: nil).ordered
      end

      it "raises AlreadyFinished before sending anything when the batch has finished" do
        allow(Pgbus::Batch).to receive(:track_enqueue).and_raise(Pgbus::Batch::AlreadyFinished)

        expect { adapter.enqueue_all([job, job2]) }.to raise_error(Pgbus::Batch::AlreadyFinished)
        expect(mock_client).not_to have_received(:send_batch)
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
        expect(mock_client).to have_received(:send_batch).with("default", [second_serialized_hash], priority: nil)
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

  describe "fair share metadata (issue #426)" do
    let(:second_job_id) { SecureRandom.uuid }
    let(:job2) { build_job_double(job_class: "OtherJob", queue_name: "default", job_id: second_job_id) }
    let(:second_serialized_hash) do
      { "job_class" => "OtherJob", "job_id" => second_job_id, "queue_name" => "default", "arguments" => [] }
    end
    let(:tagged) { serialized_hash.merge("pgbus_fair_key" => "tenant-7", "pgbus_fair_weight" => 3) }
    let(:second_tagged) { second_serialized_hash.merge("pgbus_fair_key" => "tenant-7", "pgbus_fair_weight" => 3) }

    before do
      Pgbus.configuration.fair_share = ->(_job) { ["tenant-7", 3] }
      allow(job).to receive(:scheduled_at).and_return(nil)
      allow(job2).to receive(:scheduled_at).and_return(nil)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job).and_return(serialized_hash)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).with(job2).and_return(second_serialized_hash)
    end

    after { Pgbus.configuration.fair_share = nil }

    it "#enqueue sends the key and weight inside the payload" do
      allow(mock_client).to receive(:send_message).and_return(1)

      adapter.enqueue(job)

      expect(mock_client).to have_received(:send_message).with("default", tagged, delay: 0, priority: nil)
    end

    it "#enqueue_at sends the key and weight inside the payload" do
      allow(mock_client).to receive(:send_message).and_return(1)

      adapter.enqueue_at(job, Time.now.to_f + 60)

      expect(mock_client).to have_received(:send_message).with("default", tagged, delay: anything, priority: nil)
    end

    it "#enqueue_all tags every payload on the bulk path" do
      allow(mock_client).to receive(:send_batch).and_return([1, 2])

      adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch).with("default", [tagged, second_tagged], priority: nil)
    end

    it "leaves payloads untouched when the callable returns nil for a job" do
      Pgbus.configuration.fair_share = ->(j) { j.equal?(job2) ? nil : "tenant-7" }
      allow(mock_client).to receive(:send_batch).and_return([1, 2])

      adapter.enqueue_all([job, job2])

      expect(mock_client).to have_received(:send_batch)
        .with("default", [serialized_hash.merge("pgbus_fair_key" => "tenant-7"), second_serialized_hash], priority: nil)
    end
  end
end
