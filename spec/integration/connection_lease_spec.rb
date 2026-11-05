# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #649: pgbus's own ActiveRecord work on the job path — the uniqueness,
# concurrency, batch and failed-event bookkeeping — runs after the Rails
# executor wrap returned the job's lease, so a bare `Model.connection` there
# pinned a connection to the pool thread (threads mode) or left it on a dead
# fiber for the reaper to reset (async mode). Every such call now runs inside
# `connection_pool.with_connection`: an owner that held no lease before holds
# none after, and a caller's own lease is kept. River's spec shape
# (riverqueue/river#1574): each operation in a fresh owner, no lease inside it
# afterwards, no dead connection once it has ended.
RSpec.describe "pgbus bookkeeping returns its connection lease (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:pool) { Pgbus::BusRecord.connection_pool }
  let(:queue) { "lease_work" }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }

  let(:unique_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      self.queue_adapter = :pgbus
      queue_as(queue_name)
      ensures_uniqueness strategy: :until_executed, key: ->(id) { "lease-unique-#{id}" }
      def self.name = "ConnectionLeaseSpec::UniqueJob"
      def perform(*); end
    end
  end

  let(:while_executing_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      self.queue_adapter = :pgbus
      queue_as(queue_name)
      ensures_uniqueness strategy: :while_executing, key: ->(id) { "lease-while-#{id}" }
      def self.name = "ConnectionLeaseSpec::WhileExecutingJob"
      def perform(*); end
    end
  end

  let(:limited_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      self.queue_adapter = :pgbus
      queue_as(queue_name)
      limits_concurrency to: 1, key: ->(*) { "lease-limited" }, on_conflict: :block
      def self.name = "ConnectionLeaseSpec::LimitedJob"
      def perform(*); end
    end
  end

  let(:plain_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue_name)
      def self.name = "ConnectionLeaseSpec::PlainJob"
      def perform(*); end
    end
  end

  let(:failing_job) do
    queue_name = queue
    Class.new(ActiveJob::Base) do
      self.queue_adapter = :pgbus
      queue_as(queue_name)
      def self.name = "ConnectionLeaseSpec::FailingJob"
      def perform(*) = raise(ArgumentError, "card declined")
    end
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    stub_const("ConnectionLeaseSpec", Module.new)
    stub_const("ConnectionLeaseSpec::UniqueJob", unique_job)
    stub_const("ConnectionLeaseSpec::WhileExecutingJob", while_executing_job)
    stub_const("ConnectionLeaseSpec::LimitedJob", limited_job)
    stub_const("ConnectionLeaseSpec::PlainJob", plain_job)
    stub_const("ConnectionLeaseSpec::FailingJob", failing_job)
    client.ensure_queue(queue)
    pool.release_connection
    pool.reap
  end

  after do
    ActiveSupport::IsolatedExecutionState.isolation_level = :thread
  end

  # Runs the block in a fresh owner — a Thread, or a Fiber under :fiber
  # isolation, as execution_mode: :async runs a job — and returns whether that
  # owner still held a lease when the block returned. The spec process's own
  # lease (setup ran on this thread) is returned first, so `busy` counts only
  # what the owner left behind.
  def in_fresh_owner(kind, &block)
    pool.release_connection
    case kind
    when :thread
      Thread.new do # rubocop:disable ThreadSafety/NewThread -- joined by #value
        block.call
        pool.active_connection?
      end.value
    when :fiber
      ActiveSupport::IsolatedExecutionState.isolation_level = :fiber
      begin
        Fiber.new do
          block.call
          pool.active_connection?
        end.resume
      ensure
        ActiveSupport::IsolatedExecutionState.isolation_level = :thread
      end
    end
  end

  def expect_no_lease_left(kind, &)
    held = in_fresh_owner(kind, &)
    expect(held).to be_falsey, "the #{kind} still held an ActiveRecord lease after the call"
    expect(pool.stat[:dead]).to eq(0), "a connection was left on the ended #{kind} for the reaper"
    expect(pool.stat[:busy]).to eq(0)
  end

  def read(queue_name = queue)
    message = client.read_message(queue_name, vt: 30)
    raise "no message on #{queue_name}" unless message

    message
  end

  def enqueued_at = Time.current.utc.iso8601(6)

  %i[thread fiber].each do |kind|
    context "when the owner is a #{kind}" do
      describe "UniquenessKey" do
        it "returns the lease after acquire!, bind!, bound_to?, release_if_bound! and release_held_by!" do
          expect_no_lease_left(kind) do
            Pgbus::UniquenessKey.acquire!("lease-k1", queue_name: "pending", msg_id: 0)
            Pgbus::UniquenessKey.bind!("lease-k1", queue_name: "lease_work", msg_id: 7)
            Pgbus::UniquenessKey.bound_to?("lease-k1", queue_name: "lease_work", msg_id: 7)
            Pgbus::UniquenessKey.release_if_bound!("lease-k1", queue_name: "lease_work", msg_id: 7)
            Pgbus::UniquenessKey.acquire!("lease-k2", queue_name: "lease_work", msg_id: 8)
            Pgbus::UniquenessKey.release_held_by!("lease-k2", queue_name: "lease_work", msg_id: 8,
                                                              enqueued_at: enqueued_at)
          end
        end

        it "returns the lease after unbind!, release!, locked? and release_if_unbound!" do
          expect_no_lease_left(kind) do
            Pgbus::UniquenessKey.acquire!("lease-k3", queue_name: "lease_work", msg_id: 9)
            Pgbus::UniquenessKey.unbind!("lease-k3", from_queue_name: "lease_work", from_msg_id: 9,
                                                     from_enqueued_at: enqueued_at, queue_name: "lease_work")
            Pgbus::UniquenessKey.locked?("lease-k3")
            Pgbus::UniquenessKey.release_if_unbound!("lease-k3", acquired_before: Time.current + 60)
            Pgbus::UniquenessKey.acquire!("lease-k4", queue_name: "lease_work", msg_id: 10)
            Pgbus::UniquenessKey.release!("lease-k4")
            Pgbus::UniquenessKey.release_all_held_by!([["lease-k5", "lease_work", 11, enqueued_at]])
          end
        end
      end

      describe "concurrency semaphores" do
        it "returns the lease after acquire, signal (promoting a parked job), touch and expire_stale" do
          limited_job.perform_later
          limited_job.perform_later
          expect(Pgbus::BlockedExecution.count).to eq(1)

          expect_no_lease_left(kind) do
            Pgbus::Concurrency::Semaphore.signal("lease-limited", client: client)
            Pgbus::Concurrency::Semaphore.touch("lease-limited", 60)
            Pgbus::Concurrency::Semaphore.acquire("lease-other", 1, 60)
            Pgbus::Concurrency::Semaphore.release("lease-other")
            Pgbus::Concurrency::Semaphore.expire_stale
            Pgbus::Concurrency::BlockedExecution.release_next("lease-none")
          end
          expect(Pgbus::BlockedExecution.count).to eq(0)
        end
      end

      describe "batches" do
        it "returns the lease after job_completed and job_discarded" do
          batch = Pgbus::Batch.new
          jobs = []
          batch.enqueue { jobs = Array.new(2) { plain_job.perform_later } }

          expect_no_lease_left(kind) do
            Pgbus::Batch.job_completed(batch.batch_id, job_id: jobs[0].job_id)
            Pgbus::Batch.job_discarded(batch.batch_id, job_id: jobs[1].job_id)
          end
          expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
        end
      end

      describe "FailedEventRecorder" do
        it "returns the lease after record!, exists?, last_error and clear!" do
          scope = { queue_name: queue, msg_id: 42, source_queue: client.target_queue(queue, nil) }
          expect_no_lease_left(kind) do
            Pgbus::FailedEventRecorder.record!(**scope, payload: {}, headers: nil,
                                                        error: ArgumentError.new("boom"), retry_count: 1)
            Pgbus::FailedEventRecorder.exists?(**scope)
            Pgbus::FailedEventRecorder.last_error(**scope)
            Pgbus::FailedEventRecorder.clear!(**scope)
          end
        end
      end

      describe "Executor#execute" do
        it "takes no lease for a plain job, the stat table's first probe included" do
          # The memoized probe runs once per process; forget it so this
          # example covers it whatever ran before.
          Pgbus::JobStat.remove_instance_variable(:@table_exists) if Pgbus::JobStat.instance_variable_defined?(:@table_exists)
          plain_job.perform_later
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:success) }
        end

        it "returns the lease after an :until_executed job" do
          unique_job.perform_later(1)
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:success) }
          expect(Pgbus::UniquenessKey.count).to eq(0)
        end

        it "returns the lease after a limits_concurrency job that promotes the next" do
          limited_job.perform_later
          limited_job.perform_later
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:success) }
          expect(Pgbus::BlockedExecution.count).to eq(0)
        end

        it "returns the lease after a batch member" do
          batch = Pgbus::Batch.new
          batch.enqueue { plain_job.perform_later }
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:success) }
          expect(Pgbus::BatchEntry.find_by(batch_id: batch.batch_id).status).to eq("finished")
        end

        it "returns the lease after a failure (record! and the retry backoff)" do
          failing_job.perform_later
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:failed) }
          expect(Pgbus::FailedEventRecorder.exists?(queue_name: queue, msg_id: message.msg_id.to_i,
                                                    source_queue: client.target_queue(queue, nil))).to be(true)
        end

        it "returns the lease after a dead-letter (last_error, clear! and bound_to?)" do
          original = config.max_retries
          config.max_retries = 0
          while_executing_job.perform_later(1)
          message = read
          expect_no_lease_left(kind) { expect(executor.execute(message, queue)).to eq(:dead_lettered) }
        ensure
          config.max_retries = original
        end
      end
    end
  end

  describe "a caller's own lease" do
    it "is kept: pgbus reuses it and leaves it in place" do
      pool.with_connection do |conn|
        Pgbus::UniquenessKey.release_held_by!("lease-kept", queue_name: queue, msg_id: 1, enqueued_at: enqueued_at)
        Pgbus::Concurrency::Semaphore.signal("lease-kept", client: client)
        Pgbus::FailedEventRecorder.clear!(queue_name: queue, msg_id: 1, source_queue: queue)
        expect(pool.active_connection?).to be(conn)
      end
    end
  end
end
