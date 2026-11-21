# frozen_string_literal: true

require "spec_helper"
require "active_job"

# Issue #679: ActiveJob.perform_all_later returns nil and reads each job's
# outcome from the job: an adapter's enqueue_all sets successfully_enqueued
# (and enqueue_error) on the instances it was given.
RSpec.describe Pgbus::ActiveJob::Adapter, "#enqueue_all enqueue status" do
  subject(:adapter) { described_class.new }

  let(:mock_client) { build_mock_client }
  let(:held_keys) { Set.new }
  let(:next_id) { [100] }

  let(:plain_class) do
    Class.new(ActiveJob::Base) do
      def self.name = "EnqueueStatusSpec::PlainJob"
      def perform(*) = nil
    end
  end

  def unique_class(on_conflict)
    Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      def self.name = "EnqueueStatusSpec::UniqueJob"
      def perform(*) = nil

      ensures_uniqueness strategy: :until_executing, key: ->(id) { "status-#{id}" }, on_conflict: on_conflict
    end
  end

  def limited_class(on_conflict)
    Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      def self.name = "EnqueueStatusSpec::LimitedJob"
      def perform(*) = nil

      limits_concurrency to: 1, key: ->(*) { "status-limit" }, on_conflict: on_conflict
    end
  end

  def take_id
    next_id[0] += 1
  end

  def statuses(jobs)
    jobs.map(&:successfully_enqueued?)
  end

  before do
    stub_const("EnqueueStatusSpec::PlainJob", plain_class)
    stub_bus_record_pool
    allow(Pgbus).to receive(:client).and_return(mock_client)
    allow(mock_client).to receive(:send_message) { take_id }
    allow(mock_client).to receive(:send_batch) { |_queue, payloads, **| payloads.map { take_id } }
    allow(Pgbus::Semaphore).to receive(:transaction).and_yield
    allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:acquired)
    allow(Pgbus::Concurrency::BlockedExecution).to receive(:insert)
    allow(Pgbus::UniquenessKey).to receive(:acquire!) { |key, **| !held_keys.add?(key).nil? }
    allow(Pgbus::UniquenessKey).to receive(:bind!)
    allow(Pgbus.logger).to receive(:info)
  end

  after { Thread.current[:pgbus_acquired_uniqueness_key] = nil }

  it "marks every job of a plain bulk enqueued, each with its provider_job_id" do
    jobs = Array.new(3) { |i| plain_class.new(i) }

    count = adapter.enqueue_all(jobs)

    expect(statuses(jobs)).to eq([true, true, true])
    expect(jobs.map(&:provider_job_id)).to eq([101, 102, 103])
    expect(count).to eq(3)
  end

  it "reports what perform_all_later reads: the flags and the notification's enqueued_count" do
    plain_class.queue_adapter = adapter
    jobs = Array.new(3) { |i| plain_class.new(i) }
    counts = []
    subscriber = ActiveSupport::Notifications.subscribe("enqueue_all.active_job") do |*, payload|
      counts << payload[:enqueued_count]
    end

    begin
      expect(ActiveJob.perform_all_later(jobs)).to be_nil
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    expect(statuses(jobs)).to eq([true, true, true])
    expect(counts).to eq([3])
  end

  it "marks a uniqueness-dropped job false and a concurrency-parked job true beside plain jobs" do
    stub_const("EnqueueStatusSpec::UniqueJob", unique_class(:discard))
    stub_const("EnqueueStatusSpec::LimitedJob", limited_class(:block))
    allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
    first = EnqueueStatusSpec::UniqueJob.new(1)
    duplicate = EnqueueStatusSpec::UniqueJob.new(1)
    parked = EnqueueStatusSpec::LimitedJob.new
    plain = Array.new(2) { |i| plain_class.new(i) }
    jobs = [first, duplicate, parked, *plain]

    count = adapter.enqueue_all(jobs)

    expect(statuses(jobs)).to eq([true, false, true, true, true])
    expect(Pgbus::Concurrency::BlockedExecution).to have_received(:insert).once
    expect(parked.provider_job_id).to be_nil
    expect(count).to eq(4)
  end

  it "marks a job its concurrency limit discards false" do
    stub_const("EnqueueStatusSpec::LimitedJob", limited_class(:discard))
    allow(Pgbus::Concurrency::Semaphore).to receive(:acquire).and_return(:blocked)
    dropped = EnqueueStatusSpec::LimitedJob.new
    plain = plain_class.new

    count = adapter.enqueue_all([dropped, plain])

    expect(statuses([dropped, plain])).to eq([false, true])
    expect(count).to eq(1)
  end

  it "marks a future job of the bulk enqueued once it is sent with its delay" do
    later = plain_class.new.tap { |j| j.scheduled_at = Time.current + 120 }
    now = plain_class.new

    count = adapter.enqueue_all([later, now])

    expect(statuses([later, now])).to eq([true, true])
    expect(mock_client).to have_received(:send_message).with("default", anything, delay: a_value > 0, priority: nil)
    expect(count).to eq(2)
  end

  it "resets a reused instance's earlier outcome before enqueueing it" do
    stub_const("EnqueueStatusSpec::UniqueJob", unique_class(:discard))
    held_keys.add("status-1")
    job = EnqueueStatusSpec::UniqueJob.new(1)
    job.successfully_enqueued = true
    job.enqueue_error = StandardError.new("earlier")

    adapter.enqueue_all([job])

    expect(job.successfully_enqueued?).to be(false)
    expect(job.enqueue_error).to be_nil
  end

  it "raises on a short send_batch and marks no job of that bulk enqueued" do
    allow(mock_client).to receive(:send_batch).and_return([1])
    jobs = Array.new(2) { |i| plain_class.new(i) }

    expect { adapter.enqueue_all(jobs) }.to raise_error(Pgbus::EnqueueError, /batch enqueue failed/)

    expect(statuses(jobs)).to eq([false, false])
    expect(jobs.map(&:provider_job_id)).to eq([nil, nil])
  end

  it "keeps the jobs sent before a raise marked, and the ones it never reached false" do
    stub_const("EnqueueStatusSpec::UniqueJob", unique_class(:reject))
    first = EnqueueStatusSpec::UniqueJob.new(1)
    duplicate = EnqueueStatusSpec::UniqueJob.new(1)
    plain = plain_class.new

    expect { adapter.enqueue_all([first, duplicate, plain]) }.to raise_error(Pgbus::JobNotUnique)

    expect(statuses([first, duplicate, plain])).to eq([true, false, false])
  end

  describe "the single-job path perform_later takes" do
    it "leaves successfully_enqueued to ActiveJob: enqueue and enqueue_at never set it" do
      now = plain_class.new
      later = plain_class.new

      adapter.enqueue(now)
      adapter.enqueue_at(later, Time.current.to_f + 60)

      expect(statuses([now, later])).to eq([nil, nil])
    end

    it "still returns the job from enqueue and enqueue_at, a dropped one included" do
      stub_const("EnqueueStatusSpec::UniqueJob", unique_class(:discard))
      held_keys.add("status-1")
      dropped = EnqueueStatusSpec::UniqueJob.new(1)
      later = plain_class.new

      expect(adapter.enqueue(dropped)).to be(dropped)
      expect(adapter.enqueue_at(later, Time.current.to_f + 60)).to be(later)
    end

    it "is marked enqueued by ActiveJob itself through perform_later, as before" do
      plain_class.queue_adapter = adapter

      job = plain_class.perform_later(1)

      expect(job.successfully_enqueued?).to be(true)
      expect(job.provider_job_id).to eq(101)
    end
  end
end
