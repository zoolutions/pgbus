# frozen_string_literal: true

require "spec_helper"
require "active_job"

RSpec.describe Pgbus::Uniqueness do
  let(:uniqueness_key_class) { stub_const("Pgbus::UniquenessKey", Class.new) }

  before do
    uniqueness_key_class
    allow(Pgbus::UniquenessKey).to receive_messages(acquire!: true, release!: 1, locked?: false, bind!: 1)
  end

  describe ".ensures_uniqueness" do
    it "stores uniqueness config on the class" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(id) { "test-#{id}" }
      end

      config = job_class.pgbus_uniqueness
      expect(config[:strategy]).to eq(:until_executed)
      expect(config[:key]).to respond_to(:call)
    end

    it "defaults to :until_executed strategy" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness key: ->(id) { "job-#{id}" }
      end

      expect(job_class.pgbus_uniqueness[:strategy]).to eq(:until_executed)
    end

    it "permits omitting key: for :until_executed at definition time (the guard is at resolve time)" do
      # A no-key :until_executed job is legal to DECLARE — a no-argument job
      # (e.g. a recurring task) legitimately uses the class-name default. The
      # per-record collapse footgun is caught at resolve time only when such a
      # job is enqueued WITH arguments (see #333).
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :until_executed
        end
      end.not_to raise_error
    end

    it "rejects a non-nil, non-callable key (including false) at definition time" do
      # `key: false` used to slip through the truthiness guard, record
      # explicit_key: true, and crash at enqueue with NoMethodError instead
      # of the documented ArgumentError here.
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :until_executed, key: false
        end
      end.to raise_error(ArgumentError, /callable/)
    end

    it "records explicit_key: false when key is omitted" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed
      end
      expect(job_class.pgbus_uniqueness[:explicit_key]).to be(false)
    end

    it "records explicit_key: true when a key is given" do
      job_class = Class.new do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(id) { "job-#{id}" }
      end
      expect(job_class.pgbus_uniqueness[:explicit_key]).to be(true)
    end

    it "raises ArgumentError when the removed lock_ttl: keyword is passed" do
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness lock_ttl: 600
        end
      end.to raise_error(ArgumentError, /lock_ttl.*removed.*upgrading-pgbus/im)
    end

    it "rejects invalid strategies" do
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :bogus
        end
      end.to raise_error(ArgumentError, /strategy/)
    end

    it "rejects invalid on_conflict" do
      expect do
        Class.new do
          include Pgbus::Uniqueness

          ensures_uniqueness on_conflict: :bogus
        end
      end.to raise_error(ArgumentError, /on_conflict/)
    end
  end

  describe ".resolve_key" do
    it "resolves key from job arguments" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness key: ->(order_id) { "import-#{order_id}" }
      end
      job = job_class.new(42)

      expect(described_class.resolve_key(job)).to eq("import-42")
    end

    it "returns nil for jobs without uniqueness" do
      job_class = Class.new(ActiveJob::Base)
      job = job_class.new
      expect(described_class.resolve_key(job)).to be_nil
    end

    it "uses class name as default key for :while_executing (per-invocation acquire)" do
      # :while_executing acquires the lock at execution start, per invocation, so
      # the class-name default is acceptable there (it means "one at a time by
      # class"). See #333.
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :while_executing
      end
      stub_const("MyUniqueJob", job_class)
      job = MyUniqueJob.new

      expect(described_class.resolve_key(job)).to eq("MyUniqueJob")
    end

    context "with the class-name default collapse guard for :until_executed (issue #333)" do
      it "uses the class-name default for a NO-argument :until_executed job (legit — e.g. a recurring task)" do
        job_class = Class.new(ActiveJob::Base) do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :until_executed
        end
        stub_const("CleanupJob", job_class)

        expect(described_class.resolve_key(CleanupJob.new)).to eq("CleanupJob")
      end

      it "RAISES when a no-key :until_executed job is enqueued WITH arguments (the collapse footgun)" do
        job_class = Class.new(ActiveJob::Base) do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :until_executed
        end
        stub_const("ImportOrderJob", job_class)

        expect { described_class.resolve_key(ImportOrderJob.new(42)) }
          .to raise_error(ArgumentError, /class name.*collapse|collapse.*singleton/im)
      end

      it "does NOT raise when an explicit key is given even with arguments" do
        job_class = Class.new(ActiveJob::Base) do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :until_executed, key: ->(id) { "order-#{id}" }
        end
        stub_const("ImportOrderJob2", job_class)

        expect(described_class.resolve_key(ImportOrderJob2.new(42))).to eq("order-42")
      end

      it "does NOT raise for :while_executing with arguments and no key" do
        job_class = Class.new(ActiveJob::Base) do
          include Pgbus::Uniqueness

          ensures_uniqueness strategy: :while_executing
        end
        stub_const("WhileExecJob", job_class)

        expect { described_class.resolve_key(WhileExecJob.new(42)) }.not_to raise_error
      end
    end

    it "automatically serializes GlobalID-compatible objects returned by the key lambda" do
      global_id = instance_double(GlobalID, to_s: "gid://app/Order/42")
      record = double("Order", to_global_id: global_id)

      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness key: ->(order:, **) { order }
      end
      job = job_class.new(order: record)

      expect(described_class.resolve_key(job)).to eq("gid://app/Order/42")
    end

    it "does not modify key values that are not GlobalID-compatible" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness key: ->(id) { "order-#{id}" }
      end
      job = job_class.new(42)

      expect(described_class.resolve_key(job)).to eq("order-42")
    end
  end

  describe ".inject_metadata" do
    it "adds uniqueness key and strategy to payload" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :while_executing, key: ->(*) { "test-key" }
      end
      job = job_class.new

      result = described_class.inject_metadata(job, { "job_class" => "Test" })
      expect(result[described_class::METADATA_KEY]).to eq("test-key")
      expect(result[described_class::STRATEGY_KEY]).to eq("while_executing")
    end

    it "returns payload unchanged for jobs without uniqueness" do
      job_class = Class.new(ActiveJob::Base)
      job = job_class.new
      payload = { "job_class" => "Test" }

      result = described_class.inject_metadata(job, payload)
      expect(result).to eq(payload)
    end
  end

  describe ".acquire_enqueue_lock" do
    it "acquires lock for :until_executed strategy" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(*) { "k" }
      end
      stub_const("LockJob", job_class)
      job = LockJob.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:acquired)
      expect(Pgbus::UniquenessKey).to have_received(:acquire!).with(
        "test-key", queue_name: described_class::PLACEHOLDER_QUEUE, msg_id: 0
      )
    end

    it "uses the provided queue_name and leaves msg_id as 0 before produce" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(*) { "k" }
      end
      stub_const("QueuedLockJob", job_class)
      job = QueuedLockJob.new

      result = described_class.acquire_enqueue_lock("test-key", job, queue_name: "critical")
      expect(result).to eq(:acquired)
      expect(Pgbus::UniquenessKey).to have_received(:acquire!).with(
        "test-key", queue_name: "critical", msg_id: 0
      )
    end

    it "returns :no_lock for :while_executing strategy" do
      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :while_executing
      end
      job = job_class.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:no_lock)
      expect(Pgbus::UniquenessKey).not_to have_received(:acquire!)
    end

    it "returns :locked when lock is already held" do
      allow(Pgbus::UniquenessKey).to receive(:acquire!).and_return(false)

      job_class = Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(*) { "k" }
      end
      job = job_class.new

      result = described_class.acquire_enqueue_lock("test-key", job)
      expect(result).to eq(:locked)
    end
  end

  describe ".release_lock" do
    it "releases the lock" do
      described_class.release_lock("test-key")
      expect(Pgbus::UniquenessKey).to have_received(:release!).with("test-key")
    end

    it "does nothing for nil key" do
      described_class.release_lock(nil)
      expect(Pgbus::UniquenessKey).not_to have_received(:release!)
    end
  end

  describe ".bind_lock" do
    it "binds the lock to the produced queue and msg_id" do
      described_class.bind_lock("test-key", queue_name: "critical", msg_id: 42)
      expect(Pgbus::UniquenessKey).to have_received(:bind!).with(
        "test-key", queue_name: "critical", msg_id: 42
      )
    end

    it "does nothing for nil key" do
      described_class.bind_lock(nil, queue_name: "critical", msg_id: 42)
      expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
    end

    it "does nothing when msg_id is not positive" do
      described_class.bind_lock("test-key", queue_name: "critical", msg_id: 0)
      expect(Pgbus::UniquenessKey).not_to have_received(:bind!)
    end
  end

  describe ".placeholder?" do
    it "is true for the synthetic pending queue regardless of msg_id" do
      expect(described_class.placeholder?(queue_name: "pending", msg_id: 0)).to be(true)
      expect(described_class.placeholder?(queue_name: "pending", msg_id: 9)).to be(true)
    end

    it "is true for msg_id 0 on a real queue" do
      expect(described_class.placeholder?(queue_name: "default", msg_id: 0)).to be(true)
    end

    it "is false for a bound lock" do
      expect(described_class.placeholder?(queue_name: "default", msg_id: 42)).to be(false)
    end
  end

  describe "inheritance of base-class declarations (issue #357)" do
    let(:base_class) do
      Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, on_conflict: :discard
      end
    end

    before do
      stub_const("RecurringBaseJob", base_class)
      stub_const("NightlyCleanupJob", Class.new(RecurringBaseJob))
    end

    it "makes the base-class config visible to subclasses" do
      config = NightlyCleanupJob.pgbus_uniqueness
      expect(config).to be_present
      expect(config[:strategy]).to eq(:until_executed)
      expect(config[:explicit_key]).to be(false)
    end

    it "resolves the SUBCLASS name for the class-name default, not the declaring class" do
      # The old default proc (`->(*) { name }`) captured the declaring class,
      # so a naive inheritance fix would collapse every subclass into ONE
      # "RecurringBaseJob" singleton lock — worse than the inert lookup it
      # replaced. The default key must come from the enqueued job's class.
      expect(described_class.resolve_key(NightlyCleanupJob.new)).to eq("NightlyCleanupJob")
    end

    it "fires the #333 no-key guard for a subclass enqueued WITH arguments" do
      expect { described_class.resolve_key(NightlyCleanupJob.new(42)) }
        .to raise_error(ArgumentError, /collapse/i)
    end

    it "lets a subclass's own declaration override the inherited one" do
      stub_const("KeyedChildJob", Class.new(RecurringBaseJob) do
        ensures_uniqueness strategy: :until_executed, key: ->(id) { "child-#{id}" }
      end)

      expect(described_class.resolve_key(KeyedChildJob.new(7))).to eq("child-7")
      expect(described_class.resolve_key(NightlyCleanupJob.new)).to eq("NightlyCleanupJob")
    end

    it "uses an inherited explicit key: proc with the subclass's arguments" do
      stub_const("ExplicitBaseJob", Class.new(ActiveJob::Base) do
        include Pgbus::Uniqueness

        ensures_uniqueness strategy: :until_executed, key: ->(id) { "record-#{id}" }
      end)
      stub_const("ExplicitChildJob", Class.new(ExplicitBaseJob))

      expect(described_class.resolve_key(ExplicitChildJob.new(9))).to eq("record-9")
    end

    it "still returns nil for a class with no declaration anywhere in its ancestry" do
      stub_const("PlainJob", Class.new(ActiveJob::Base) { include Pgbus::Uniqueness })

      expect(PlainJob.pgbus_uniqueness).to be_nil
      expect(described_class.resolve_key(PlainJob.new)).to be_nil
    end
  end
end
