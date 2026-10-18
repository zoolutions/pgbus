# frozen_string_literal: true

require "spec_helper"
require "json"
require "active_job"

# Unit tests for timezone consistency. These verify the code uses the correct
# time source (Time.current vs Time.now.utc) by checking the actual values
# passed to dependencies. The integration tests in spec/integration/timezone_spec.rb
# exercise the full AR round-trip with a real database and default_timezone = :local.
RSpec.describe Pgbus do
  # Set Time.zone to a timezone with a significant UTC offset so that
  # any confusion between Time.now (system) and Time.current (Rails zone)
  # produces a measurable difference.
  around do |example|
    original_zone = Time.zone
    Time.zone = "Pacific/Auckland" # UTC+12/+13
    example.run
  ensure
    Time.zone = original_zone
  end

  describe "Adapter#enqueue_at delay calculation" do
    subject(:adapter) { Pgbus::ActiveJob::Adapter.new }

    let(:mock_client) { build_mock_client }
    let(:job_id) { SecureRandom.uuid }
    let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
    let(:serialized_hash) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(described_class).to receive(:client).and_return(mock_client)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
      allow(mock_client).to receive(:send_message).and_return(1)
    end

    it "calculates correct delay from a UTC epoch timestamp" do
      future_timestamp = Time.current.to_f + 60
      adapter.enqueue_at(job, future_timestamp)

      expect(mock_client).to have_received(:send_message).with(
        "default", serialized_hash, delay: a_value_between(59, 61), priority: nil
      )
    end
  end

  describe "Adapter#enqueue_all scheduled_at comparison" do
    subject(:adapter) { Pgbus::ActiveJob::Adapter.new }

    let(:mock_client) { build_mock_client }
    let(:job_id) { SecureRandom.uuid }
    let(:job) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }
    let(:serialized_hash) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end

    before do
      allow(described_class).to receive(:client).and_return(mock_client)
      allow(Pgbus::Serializer).to receive(:serialize_job_hash).and_return(serialized_hash)
      allow(mock_client).to receive_messages(send_message: 1, send_batch: [1])
    end

    it "correctly identifies future jobs when scheduled_at is a TimeWithZone" do
      future_time = Time.current + 60
      allow(job).to receive(:scheduled_at).and_return(future_time)

      adapter.enqueue_all([job])

      expect(mock_client).to have_received(:send_message).with(
        "default", serialized_hash, delay: a_value_between(59, 61), priority: nil
      )
    end
  end

  describe "Executor#compute_enqueue_latency with non-UTC timezone" do
    subject(:executor) { Pgbus::ActiveJob::Executor.new(client: mock_client, config: config) }

    let(:mock_client) { build_mock_client }
    let(:config) { described_class.configuration }
    let(:job_id) { SecureRandom.uuid }
    let(:job_payload) do
      { "job_class" => "TestJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [] }
    end
    let(:message_json) { JSON.generate(job_payload) }
    let(:job_double) { build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id) }

    before do
      allow(ActiveJob::Base).to receive(:deserialize).and_return(job_double)
      allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
      # Make stats observable regardless of global config state — record_stat
      # returns early unless stats_enabled, so assert on a known-true value.
      allow(config).to receive(:stats_enabled).and_return(true)
      stub_const("Pgbus::JobStat", Class.new)
      allow(Pgbus::JobStat).to receive(:record!)
    end

    it "computes correct latency from an ISO8601 enqueued_at with offset" do
      enqueued_at = (Time.current - 1).utc.iso8601(6)
      message = build_message_double(msg_id: 1, message: message_json, read_ct: 1, enqueued_at: enqueued_at)

      executor.execute(message, "default")

      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(800, 2000))
      )
    end

    it "computes correct latency from a bare timestamp (no timezone offset)" do
      # This is the critical test: a timestamp string WITHOUT timezone info.
      # When ENV['TZ'] is non-UTC, Time.parse interprets it in system local
      # time, which can produce latency off by hours.
      one_second_ago_utc = (Time.now.utc - 1).strftime("%Y-%m-%d %H:%M:%S.%6N")
      message = build_message_double(msg_id: 2, message: message_json, read_ct: 1, enqueued_at: one_second_ago_utc)

      executor.execute(message, "default")

      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(500, 5000))
      )
    end
  end

  describe "BlockedExecution.insert expires_at" do
    it "sets expires_at relative to current time, not offset by timezone" do
      allow(Pgbus::BlockedExecution).to receive(:insert!)

      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "k", queue_name: "q",
        payload: { "job_class" => "T" }, duration: 900
      )

      expect(Pgbus::BlockedExecution).to have_received(:insert!).with(
        hash_including(:expires_at), record_timestamps: false
      ) do |args, **|
        # expires_at should be ~900 seconds from now
        delta = args[:expires_at] - Time.current
        expect(delta).to be_within(5).of(900)
      end
    end
  end

  describe "Semaphore.acquire expires_at" do
    it "sets expires_at relative to current time, not offset by timezone" do
      allow(Pgbus::Semaphore).to receive(:acquire!).and_return(:acquired)

      Pgbus::Concurrency::Semaphore.acquire("test-key", 1, 900)

      expect(Pgbus::Semaphore).to have_received(:acquire!) do |_key, _max, expires_at|
        delta = expires_at - Time.current
        expect(delta).to be_within(5).of(900)
      end
    end
  end

  describe "Recurring::Schedule timezone awareness" do
    it "evaluates cron schedules with TimeWithZone objects" do
      schedule = Pgbus::Recurring::Schedule.new(config: described_class.configuration)
      expect { schedule.due_tasks(Time.current) }.not_to raise_error
    end
  end
end
