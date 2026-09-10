# frozen_string_literal: true

require_relative "../integration_helper"
require_relative "../support/pgmq_doubles"
require "json"
require "active_job"
require "active_support/core_ext/time/zones"

# Reproduces timezone-related issues when Rails is configured with:
# - ActiveRecord::Base.default_timezone = :local
# - Time.zone set to a non-UTC timezone (e.g. Africa/Johannesburg, UTC+2)
# - System timezone (ENV['TZ']) set to the same non-UTC timezone
#
# This combination is common in apps that store timestamps in local time
# (e.g. an app using Africa/Casablanca for both DB and process timezones).
RSpec.describe "Timezone handling (integration)", :integration do
  # Rails 8.1 moved default_timezone from ActiveRecord::Base to ActiveRecord module.
  def ar_default_timezone
    if ActiveRecord.respond_to?(:default_timezone)
      ActiveRecord.default_timezone
    else
      ActiveRecord::Base.default_timezone
    end
  end

  def ar_default_timezone=(value)
    if ActiveRecord.respond_to?(:default_timezone=)
      ActiveRecord.default_timezone = value
    else
      ActiveRecord::Base.default_timezone = value
    end
  end

  around do |example|
    unless PGBUS_DATABASE_URL
      example.run
      next
    end

    original_env_tz = ENV.fetch("TZ", nil)
    original_ar_tz = ar_default_timezone
    original_zone = Time.zone

    ENV["TZ"] = "Africa/Johannesburg"
    self.ar_default_timezone = :local
    Time.zone = "Africa/Johannesburg"

    ActiveRecord::Base.connection.execute(
      "SET SESSION timezone = 'Africa/Johannesburg'"
    )

    example.run
  ensure
    if PGBUS_DATABASE_URL
      ENV["TZ"] = original_env_tz
      self.ar_default_timezone = original_ar_tz
      Time.zone = original_zone
      ActiveRecord::Base.connection.execute("SET SESSION timezone = 'UTC'")
    end
  end

  describe "Semaphore expiration" do
    before { Pgbus::Semaphore.delete_all }

    it "correctly expires semaphores with non-UTC timezone" do
      Pgbus::Concurrency::Semaphore.acquire("tz-expired-key", 1, -1)
      Pgbus::Concurrency::Semaphore.acquire("tz-active-key", 1, 300)

      expired = Pgbus::Concurrency::Semaphore.expire_stale
      expired_keys = expired.map { |r| r["key"] }

      expect(expired_keys).to include("tz-expired-key")
      expect(expired_keys).not_to include("tz-active-key")
    end

    it "does not prematurely expire active semaphores" do
      Pgbus::Concurrency::Semaphore.acquire("tz-test-key", 1, 60)

      expired = Pgbus::Concurrency::Semaphore.expire_stale
      expired_keys = expired.map { |r| r["key"] }

      expect(expired_keys).not_to include("tz-test-key")
    end

    it "round-trips expires_at correctly through AR" do
      Pgbus::Concurrency::Semaphore.acquire("tz-rt", 1, 120)
      record = Pgbus::Semaphore.find_by(key: "tz-rt")

      delta = record.expires_at - Time.current
      expect(delta).to be_within(5).of(120)
    end
  end

  describe "Blocked execution expiration" do
    before { Pgbus::BlockedExecution.delete_all }

    # A parked job never ages out: `expires_at` orders the sweep, it does
    # not drop rows. Whatever the clock says, the row is still released.
    it "still releases a blocked execution whose expires_at is in the past" do
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-key", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: -1
      )

      released = Pgbus::BlockedExecution.release_next!("tz-key")
      expect(released).not_to be_nil
      expect(Pgbus::BlockedExecution.where(concurrency_key: "tz-key").count).to eq(0)
    end

    it "round-trips expires_at correctly through AR" do
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-rt", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: 120
      )

      record = Pgbus::BlockedExecution.find_by(concurrency_key: "tz-rt")
      delta = record.expires_at - Time.current
      expect(delta).to be_within(5).of(120)
    end

    it "releases non-expired blocked executions" do
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: "tz-release", queue_name: "default",
        payload: { "job_class" => "TestJob" }, duration: 300
      )

      released = Pgbus::BlockedExecution.release_next!("tz-release")
      expect(released).not_to be_nil
      expect(released[:queue_name]).to eq("default")
    end
  end

  describe "Job enqueue with delay" do
    let(:client) { Pgbus.client }

    before { client.ensure_queue("default") }

    it "delays message correctly with non-UTC timezone" do
      payload = { "job_class" => "TestJob", "job_id" => SecureRandom.uuid,
                  "queue_name" => "default", "arguments" => [] }

      # Send with 5-second delay via PGMQ directly
      client.send_message("default", payload, delay: 5)

      # Message should be invisible (delayed)
      messages = client.read_batch("default", qty: 1, vt: 1)
      expect(messages || []).to be_empty
    end
  end

  describe "Enqueue latency computation" do
    it "computes correct latency from bare timestamp" do
      mock_client = build_mock_client
      executor = Pgbus::ActiveJob::Executor.new(client: mock_client, config: Pgbus.configuration)
      job_id = SecureRandom.uuid
      payload = { "job_class" => "TestJob", "job_id" => job_id,
                  "queue_name" => "default", "arguments" => [] }
      job_double = build_job_double(job_class: "TestJob", queue_name: "default", job_id: job_id)

      allow(ActiveJob::Base).to receive(:deserialize).and_return(job_double)
      allow(Pgbus::Concurrency).to receive(:extract_key).and_return(nil)
      stub_const("Pgbus::JobStat", Class.new)
      allow(Pgbus::JobStat).to receive(:record!)

      enqueued_at = (Time.now.utc - 1).strftime("%Y-%m-%d %H:%M:%S.%6N")
      message = build_message_double(msg_id: 1, message: JSON.generate(payload),
                                     read_ct: 1, enqueued_at: enqueued_at)
      executor.execute(message, "default")

      expect(Pgbus::JobStat).to have_received(:record!).with(
        hash_including(enqueue_latency_ms: a_value_between(500, 5_000))
      )
    end
  end

  describe "Circuit breaker resume_at" do
    it "auto-resumes when resume_at is in the past" do
      skip "pgbus_queue_states table not available" unless ActiveRecord::Base.connection.table_exists?("pgbus_queue_states")

      Pgbus::QueueState.delete_all
      breaker = Pgbus::CircuitBreaker.new(config: Pgbus.configuration)

      Pgbus::QueueState.create!(
        queue_name: "tz-test-queue",
        paused: true,
        paused_reason: "circuit_breaker: test",
        paused_at: Time.current - 120,
        circuit_breaker_trip_count: 1,
        circuit_breaker_resume_at: Time.current - 60
      )

      expect(breaker.paused?("tz-test-queue")).to be false
    end
  end
end
