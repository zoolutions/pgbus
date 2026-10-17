# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Batch do
  let(:batch_entry_double) do
    double(
      "BatchEntry",
      id: 1,
      attributes: { "batch_id" => "abc" },
      status: "processing",
      on_finish_class: nil,
      on_success_class: nil,
      on_discard_class: nil,
      properties: "{}"
    )
  end

  before do
    allow(Pgbus::BatchEntry).to receive_message_chain(:where, :update_all).and_return(1) # rubocop:disable RSpec/MessageChain
    allow(Pgbus::BatchEntry).to receive_messages(create!: batch_entry_double, find_by: batch_entry_double,
                                                 check_finished!: { just_finished: false, record: nil })
  end

  describe "#initialize" do
    it "generates a UUID batch_id" do
      batch = described_class.new
      expect(batch.batch_id).to match(/\A[0-9a-f-]{36}\z/)
    end

    it "stores callback classes and properties" do
      callback_class = Class.new
      batch = described_class.new(
        on_complete: callback_class,
        description: "test batch",
        properties: { user_id: 1 }
      )
      expect(batch.on_complete).to eq(callback_class)
      expect(batch.on_finish).to eq(callback_class)
      expect(batch.description).to eq("test batch")
      expect(batch.properties[:user_id]).to eq(1)
    end

    describe "the deprecated on_finish:" do
      let(:logger) { instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil) }

      before do
        allow(Pgbus).to receive(:logger).and_return(logger)
        described_class.reset_executions_migrated_cache!
      end

      after { described_class.reset_executions_migrated_cache! }

      it "maps onto on_complete and warns once per process" do
        callback_class = Class.new

        batch = described_class.new(on_finish: callback_class)
        described_class.new(on_finish: callback_class)

        expect(batch.on_complete).to eq(callback_class)
        expect(logger).to have_received(:warn).with(no_args).once
      end

      it "cannot be combined with on_complete:" do
        expect { described_class.new(on_complete: Class.new, on_finish: Class.new) }
          .to raise_error(ArgumentError, /on_complete/)
      end
    end

    it "stores on_complete in the on_finish columns" do
      callback_job = Class.new(ActiveJob::Base) do
        def self.name = "BatchSpec::CompleteJob"
        def perform(*); end
      end
      stub_const("BatchSpec::CompleteJob", callback_job)

      described_class.new(on_complete: callback_job).enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:create!).with(hash_including(on_finish_class: "BatchSpec::CompleteJob"))
    end

    it "accepts on_failure: as the canonical failure callback" do
      callback_class = Class.new
      batch = described_class.new(on_failure: callback_class)
      expect(batch.on_failure).to eq(callback_class)
      expect(batch.on_discard).to eq(callback_class)
    end

    it "maps deprecated on_discard: onto on_failure and warns" do
      callback_class = Class.new
      logger = instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)

      batch = described_class.new(on_discard: callback_class)

      expect(batch.on_failure).to eq(callback_class)
      expect(logger).to have_received(:warn)
    end
  end

  describe "#enqueue" do
    it "creates a batch record in the database" do
      batch = described_class.new(description: "test")
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:create!).with(
        hash_including(batch_id: batch.batch_id, description: "test", status: "pending")
      )
    end

    # Jobs count themselves into total_jobs as they are enqueued (issue #423),
    # so the block's end only flips pending -> processing and re-checks.
    it "flips the batch to processing without touching total_jobs, then re-checks finish" do
      relation = double("relation", update_all: 1)
      allow(Pgbus::BatchEntry).to receive(:where).with(batch_id: anything, status: "pending").and_return(relation)
      batch = described_class.new

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(relation).to have_received(:update_all).with(status: "processing")
      expect(Pgbus::BatchEntry).not_to have_received(:where).with(batch_id: batch.batch_id)
      expect(Pgbus::BatchEntry).to have_received(:check_finished!).with(batch.batch_id)
    end

    it "does not bump total_jobs itself at the end of the block" do
      allow(Pgbus::BatchEntry).to receive(:increment_total_jobs!)
      batch = described_class.new

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).not_to have_received(:increment_total_jobs!)
    end

    it "sets thread-local batch_id during block execution" do
      captured_batch_id = nil
      batch = described_class.new

      batch.enqueue do
        captured_batch_id = Thread.current[:pgbus_batch_id]
      end

      expect(captured_batch_id).to eq(batch.batch_id)
      expect(Thread.current[:pgbus_batch_id]).to be_nil
    end

    # Issue #553: inside a transaction, jobs that enqueue after commit count
    # themselves in from after_commit callbacks registered during the block,
    # so the start has to queue up behind them.
    describe "inside an open transaction" do
      let(:relation) { double("relation", update_all: 1) }
      let(:deferred) { [] }

      before do
        allow(Pgbus::BatchEntry).to receive(:where).with(batch_id: anything, status: "pending").and_return(relation)
        allow(ActiveRecord).to receive(:after_all_transactions_commit) { |&block| deferred << block }
      end

      it "starts processing only once the transactions commit" do
        batch = described_class.new

        batch.enqueue {} # rubocop:disable Lint/EmptyBlock

        expect(relation).not_to have_received(:update_all)
        expect(Pgbus::BatchEntry).not_to have_received(:check_finished!)

        deferred.each(&:call)

        expect(relation).to have_received(:update_all).with(status: "processing")
        expect(Pgbus::BatchEntry).to have_received(:check_finished!).with(batch.batch_id)
      end

      it "holds the trailing finish check of a re-opened block until the commit" do
        batch = described_class.new
        batch.enqueue {} # rubocop:disable Lint/EmptyBlock
        deferred.shift.call

        batch.enqueue {} # rubocop:disable Lint/EmptyBlock

        expect(Pgbus::BatchEntry).to have_received(:check_finished!).once
        deferred.each(&:call)
        expect(Pgbus::BatchEntry).to have_received(:check_finished!).twice
      end
    end

    it "starts at once on Active Record 7.1, which has no after_all_transactions_commit" do
      allow(ActiveRecord).to receive(:respond_to?).and_call_original
      allow(ActiveRecord).to receive(:respond_to?).with(:after_all_transactions_commit).and_return(false)
      batch = described_class.new

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:check_finished!).with(batch.batch_id)
    end
  end

  describe "#enqueue on an already-started batch (reopen)" do
    let(:finished_double) do
      double("BatchEntry", status: "finished", total_jobs: 1, completed_jobs: 1, failed_jobs: 0,
                           on_finish_class: nil, on_success_class: nil, properties: "{}")
    end

    it "re-reads the row before deciding the batch is still open" do
      batch = described_class.new
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock
      allow(Pgbus::BatchEntry).to receive(:find_by).and_return(finished_double)

      expect { batch.enqueue {} }.to raise_error(Pgbus::Batch::AlreadyFinished) # rubocop:disable Lint/EmptyBlock
    end

    it "does not bump total_jobs at the end of a re-opened block either" do
      allow(Pgbus::BatchEntry).to receive(:increment_total_jobs!)
      batch = described_class.new
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).not_to have_received(:increment_total_jobs!)
      expect(Pgbus::BatchEntry).to have_received(:check_finished!).with(batch.batch_id).twice
    end
  end

  describe "#add" do
    let(:work_job) do
      Class.new(ActiveJob::Base) do
        def self.name = "BatchSpec::WorkJob"
        def perform(*); end
      end
    end

    before do
      stub_const("BatchSpec::WorkJob", work_job)
      allow(ActiveJob).to receive(:perform_all_later)
    end

    it "enqueues job instances in one bulk call inside the batch tag" do
      jobs = [work_job.new(1), work_job.new(2)]
      tag = nil
      allow(ActiveJob).to receive(:perform_all_later) { tag = Thread.current[:pgbus_batch_id] }
      batch = described_class.new

      expect(batch.add(*jobs)).to eq(jobs)
      expect(ActiveJob).to have_received(:perform_all_later).with(jobs).once
      expect(tag).to eq(batch.batch_id)
    end

    it "tags each instance with the batch, so an enqueue deferred past the block still joins it" do
      tagged_job = Class.new(work_job) { include Pgbus::ActiveJob::BatchId }
      jobs = [tagged_job.new(1), tagged_job.new(2)]
      batch = described_class.new

      batch.add(*jobs)

      expect(jobs.map(&:enqueue_batch_id)).to eq([batch.batch_id, batch.batch_id])
    end

    it "adds to an already-started batch through the reopen path" do
      batch = described_class.new
      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      batch.add(work_job.new)

      expect(Pgbus::BatchEntry).to have_received(:create!).once
      expect(Pgbus::BatchEntry).to have_received(:check_finished!).with(batch.batch_id).twice
    end

    it "yields a block inside the batch tag" do
      tag = nil
      batch = described_class.new

      batch.add { tag = Thread.current[:pgbus_batch_id] }

      expect(tag).to eq(batch.batch_id)
    end

    it "rejects anything that is not a job instance" do
      expect { described_class.new.add(work_job) }.to raise_error(ArgumentError, /job instance/)
    end

    it "is a no-op without jobs or a block" do
      described_class.new.add

      expect(Pgbus::BatchEntry).not_to have_received(:create!)
    end
  end

  describe "uniqueness (run-scoped lock)" do
    let(:logger) { instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil) }
    let(:release_scope) { double("scope", delete_all: 1) }

    before do
      allow(Pgbus).to receive(:logger).and_return(logger)
      allow(Pgbus::UniquenessKey).to receive_messages(acquire!: true, clear_bind_stamp!: nil)
      allow(Pgbus::UniquenessKey).to receive(:where).and_return(release_scope)
    end

    def finished_result(batch_id)
      attrs = {
        batch_id: batch_id, status: "finished", total_jobs: 1, completed_jobs: 1, discarded_jobs: 0,
        on_finish_class: nil, on_success_class: nil, on_discard_class: nil, properties: "{}"
      }
      record = double("BatchEntry", **attrs, presence: "{}")
      { record: record, just_finished: true }
    end

    it "rejects an unknown on_conflict" do
      expect { described_class.new(uniqueness_key: "k", on_conflict: :explode) }
        .to raise_error(ArgumentError, /on_conflict must be one of/)
    end

    it "takes no lock without a key" do
      described_class.new.enqueue { nil }

      expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
    end

    it "takes the lock at enqueue, keyed by the batch that owns it, and runs the block" do
      batch = described_class.new(uniqueness_key: "perfecta-daily")
      ran = false

      batch.enqueue { ran = true }

      expect(Pgbus::UniquenessKey).to have_received(:acquire!)
        .with("batch:perfecta-daily", queue_name: "batch:#{batch.batch_id}", msg_id: 0)
      expect(Pgbus::UniquenessKey).to have_received(:clear_bind_stamp!).with("batch:perfecta-daily")
      expect(ran).to be(true)
      expect(batch).not_to be_discarded
      expect(batch.lock_key).to eq("batch:perfecta-daily")
    end

    it "raises AlreadyRunning and creates nothing when the key is held (:reject)" do
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
      batch = described_class.new(uniqueness_key: "perfecta-daily")

      expect { batch.enqueue { raise "block must not run" } }.to raise_error(Pgbus::Batch::AlreadyRunning, /perfecta-daily/)
      expect(Pgbus::BatchEntry).not_to have_received(:create!)
    end

    it "skips the block quietly when the key is held (:discard)" do
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
      batch = described_class.new(uniqueness_key: "perfecta-daily", on_conflict: :discard)

      result = batch.enqueue { raise "block must not run" }

      expect(result).to be(batch)
      expect(batch).to be_discarded
      expect(Pgbus::BatchEntry).not_to have_received(:create!)
      expect(logger).to have_received(:info)
      expect(logger).not_to have_received(:warn)
    end

    it "skips the block and warns when the key is held (:log)" do
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)
      batch = described_class.new(uniqueness_key: "perfecta-daily", on_conflict: :log)

      batch.enqueue { raise "block must not run" }

      expect(batch).to be_discarded
      expect(logger).to have_received(:warn)
    end

    it "gives the lock back if the batch row cannot be created" do
      allow(Pgbus::BatchEntry).to receive(:create!).and_raise(ActiveRecord::StatementInvalid, "down")
      batch = described_class.new(uniqueness_key: "perfecta-daily")

      expect { batch.enqueue { nil } }.to raise_error(ActiveRecord::StatementInvalid)

      expect(Pgbus::UniquenessKey).to have_received(:where).with(queue_name: "batch:#{batch.batch_id}")
      expect(release_scope).to have_received(:delete_all)
    end

    it "releases the lock in the finish that fires the callbacks" do
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(finished_result("abc"))

      described_class.job_completed("abc")

      expect(Pgbus::UniquenessKey).to have_received(:where).with(queue_name: "batch:abc")
      expect(release_scope).to have_received(:delete_all)
    end

    it "does not release anything while the batch is still running" do
      allow(Pgbus::BatchEntry).to receive(:increment_counter!)
        .and_return(finished_result("abc").merge(just_finished: false))

      described_class.job_completed("abc")

      expect(Pgbus::UniquenessKey).not_to have_received(:where)
    end

    describe ".lock_orphaned?" do
      it "treats a missing batch as an orphan" do
        allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "gone").and_return(nil)

        expect(described_class.lock_orphaned?("batch:gone")).to be(true)
      end

      it "treats a finished batch as an orphan" do
        allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "done").and_return(double(status: "finished"))

        expect(described_class.lock_orphaned?("batch:done")).to be(true)
      end

      it "keeps the lock of a running batch, however old" do
        allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "live").and_return(double(status: "processing"))

        expect(described_class.lock_orphaned?("batch:live")).to be(false)
      end

      it "keeps the lock when the lookup fails" do
        allow(Pgbus::BatchEntry).to receive(:find_by).and_raise(ActiveRecord::StatementInvalid, "down")

        expect(described_class.lock_orphaned?("batch:live")).to be(false)
      end
    end
  end

  describe ".track_enqueue" do
    let(:payload) { { "job_id" => "j-1", "queue_name" => "default", Pgbus::Batch::METADATA_KEY => "b-1" } }

    before do
      allow(described_class).to receive(:executions_migrated?).and_return(true)
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:increment_total_jobs!).and_return(true)
      allow(Pgbus::BatchExecution).to receive(:insert_for!)
    end

    it "increments total_jobs before inserting the execution row, inside one transaction" do
      described_class.track_enqueue(payload)

      expect(Pgbus::BatchEntry).to have_received(:transaction).ordered
      expect(Pgbus::BatchEntry).to have_received(:increment_total_jobs!).with("b-1", 1).ordered
      expect(Pgbus::BatchExecution).to have_received(:insert_for!)
        .with(batch_id: "b-1", job_id: "j-1", queue_name: "default").ordered
    end

    it "increments once by N for a bulk of payloads" do
      second = payload.merge("job_id" => "j-2")

      described_class.track_enqueue([payload, second])

      expect(Pgbus::BatchEntry).to have_received(:increment_total_jobs!).with("b-1", 2).once
      expect(Pgbus::BatchExecution).to have_received(:insert_for!).twice
    end

    it "inserts no row before the executions migration but still counts" do
      allow(described_class).to receive(:executions_migrated?).and_return(false)

      described_class.track_enqueue(payload)

      expect(Pgbus::BatchEntry).to have_received(:increment_total_jobs!).with("b-1", 1)
      expect(Pgbus::BatchExecution).not_to have_received(:insert_for!)
    end

    it "propagates AlreadyFinished without inserting a row" do
      allow(Pgbus::BatchEntry).to receive(:increment_total_jobs!).and_raise(Pgbus::Batch::AlreadyFinished)

      expect { described_class.track_enqueue(payload) }.to raise_error(Pgbus::Batch::AlreadyFinished)
      expect(Pgbus::BatchExecution).not_to have_received(:insert_for!)
    end
  end

  describe ".untrack_enqueue" do
    let(:payload) { { "job_id" => "j-1", Pgbus::Batch::METADATA_KEY => "b-1" } }

    it "decrements total_jobs and deletes the row in one transaction" do
      allow(described_class).to receive(:executions_migrated?).and_return(true)
      allow(Pgbus::BatchEntry).to receive(:transaction).and_yield
      allow(Pgbus::BatchEntry).to receive(:decrement_total_jobs!)
      rows = double("rows", delete_all: 1)
      allow(Pgbus::BatchExecution).to receive(:where).with(job_id: "j-1").and_return(rows)

      described_class.untrack_enqueue(payload)

      expect(Pgbus::BatchEntry).to have_received(:decrement_total_jobs!).with("b-1")
      expect(rows).to have_received(:delete_all)
    end
  end

  describe ".job_completed" do
    def build_batch_result(overrides = {})
      attrs = {
        status: "processing",
        total_jobs: 3,
        completed_jobs: 1,
        discarded_jobs: 0,
        on_finish_class: nil,
        on_success_class: nil,
        on_discard_class: nil,
        properties: "{}"
      }.merge(overrides)

      record = double("BatchEntry", **attrs, presence: attrs[:properties])
      allow(record).to receive(:properties).and_return(attrs[:properties])
      { record: record, just_finished: overrides.fetch(:just_finished, false) }
    end

    it "increments completed_jobs counter" do
      result = build_batch_result
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(result)

      described_class.job_completed("batch-123")

      expect(Pgbus::BatchEntry).to have_received(:increment_counter!).with("batch-123", "completed_jobs")
    end

    it "fires on_finish callback when batch finishes" do
      result = build_batch_result(
        status: "finished", total_jobs: 2, completed_jobs: 2,
        on_finish_class: "BatchCallbackJob", properties: '{"user_id":1}',
        just_finished: true
      )
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(result)

      callback_job = Class.new(ActiveJob::Base) { def perform(*); end }
      stub_const("BatchCallbackJob", callback_job)
      allow(callback_job).to receive(:perform_later)

      described_class.job_completed("batch-123")

      expect(callback_job).to have_received(:perform_later).with({ "user_id" => 1 })
    end

    it "fires on_success callback when all jobs succeed" do
      result = build_batch_result(
        status: "finished", total_jobs: 1, completed_jobs: 1,
        on_success_class: "SuccessJob",
        just_finished: true
      )
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(result)

      callback_job = Class.new(ActiveJob::Base) { def perform(*); end }
      stub_const("SuccessJob", callback_job)
      allow(callback_job).to receive(:perform_later)

      described_class.job_completed("batch-123")

      expect(callback_job).to have_received(:perform_later)
    end

    it "fires on_discard callback when some jobs were discarded" do
      result = build_batch_result(
        status: "finished", total_jobs: 2, completed_jobs: 1, discarded_jobs: 1,
        on_discard_class: "DiscardJob",
        just_finished: true
      )
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(result)

      callback_job = Class.new(ActiveJob::Base) { def perform(*); end }
      stub_const("DiscardJob", callback_job)
      allow(callback_job).to receive(:perform_later)

      described_class.job_discarded("batch-123")

      expect(callback_job).to have_received(:perform_later)
    end

    it "returns nil when batch not found" do
      allow(Pgbus::BatchEntry).to receive(:increment_counter!).and_return(nil)

      expect(described_class.job_completed("nonexistent")).to be_nil
    end
  end

  describe ".find" do
    # BREAKING (pre-1.0): used to return the raw attributes Hash.
    let(:record) do
      double(
        "BatchEntry",
        batch_id: "abc", description: "nightly", status: "processing",
        properties: '{"tenant":"acme"}', total_jobs: 4, completed_jobs: 1,
        failed_jobs: 1, finished_at: nil,
        on_finish_class: nil, on_success_class: nil, on_failure_class: nil
      )
    end

    before do
      allow(record).to receive(:has_attribute?).with(:failed_jobs).and_return(true)
      allow(record).to receive(:has_attribute?).with(:discarded_jobs).and_return(false)
      allow(Pgbus::BatchEntry).to receive(:find_by).with(batch_id: "abc").and_return(record)
      allow(described_class).to receive(:executions_migrated?).and_return(true)
    end

    it "returns a rehydrated Batch handle" do
      expect(described_class.find("abc")).to be_a(described_class)
    end

    it "exposes the stored description and parsed properties" do
      batch = described_class.find("abc")

      expect(batch.description).to eq("nightly")
      expect(batch.properties).to eq({ "tenant" => "acme" })
    end

    it "delegates the counters and status to the row" do
      batch = described_class.find("abc")

      expect(batch.status).to eq("processing")
      expect(batch.total_jobs).to eq(4)
      expect(batch.completed_jobs).to eq(1)
      expect(batch.failed_jobs).to eq(1)
      expect(batch.progress_percentage).to eq(50)
      expect(batch).not_to be_finished
    end

    it "returns nil when not found" do
      allow(Pgbus::BatchEntry).to receive(:find_by).and_return(nil)

      expect(described_class.find("missing")).to be_nil
    end
  end

  describe "configured callback instances" do
    let(:callback_job_class) do
      Class.new(ActiveJob::Base) do
        include Pgbus::ActiveJob::BatchId

        def self.name = "BatchSpec::CallbackJob"
        def perform(*); end
      end
    end

    before do
      stub_const("BatchSpec::CallbackJob", callback_job_class)
      allow(described_class).to receive(:callback_jobs_migrated?).and_return(true)
    end

    it "serializes a configured instance at batch-creation time" do
      callback = callback_job_class.new.set(queue: :critical)
      batch = described_class.new(on_complete: callback)

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:create!).with(
        hash_including(
          on_finish_class: nil,
          on_finish_job: hash_including("job_class" => "BatchSpec::CallbackJob", "queue_name" => "critical")
        )
      )
    end

    it "degrades a configured instance to its class and warns when the jsonb columns are missing" do
      allow(described_class).to receive(:callback_jobs_migrated?).and_return(false)
      logger = instance_double(Logger, warn: nil, error: nil, info: nil, debug: nil)
      allow(Pgbus).to receive(:logger).and_return(logger)
      batch = described_class.new(on_complete: callback_job_class.new.set(queue: :critical))

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:create!).with(
        hash_including(on_finish_class: "BatchSpec::CallbackJob")
      )
      expect(Pgbus::BatchEntry).to have_received(:create!).with(hash_excluding(:on_finish_job))
      expect(logger).to have_received(:warn)
    end

    it "keeps storing a bare class in the legacy column" do
      batch = described_class.new(on_success: callback_job_class)

      batch.enqueue {} # rubocop:disable Lint/EmptyBlock

      expect(Pgbus::BatchEntry).to have_received(:create!).with(
        hash_including(on_success_class: "BatchSpec::CallbackJob", on_success_job: nil)
      )
    end
  end

  describe ".cleanup" do
    it "deletes finished batches older than threshold" do
      scope = double("scope", delete_all: 3)
      allow(Pgbus::BatchEntry).to receive(:stale).and_return(scope)

      expect(described_class.cleanup(older_than: Time.now - 86_400)).to eq(3)
    end
  end
end
