# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Job lifecycle (integration)", :integration do
  let(:client) { Pgbus.client }

  before do
    client.ensure_queue("default")
  end

  describe "enqueue and read" do
    it "sends a message to PGMQ and reads it back" do
      payload = { "job_class" => "TestJob", "job_id" => SecureRandom.uuid, "arguments" => [1, 2] }
      msg_id = client.send_message("default", payload)
      expect(msg_id.to_i).to be_positive

      messages = client.read_batch("default", qty: 1)
      expect(messages).not_to be_empty
      expect(messages.first.msg_id.to_i).to eq(msg_id.to_i)

      parsed = JSON.parse(messages.first.message)
      expect(parsed["job_class"]).to eq("TestJob")
    end

    it "respects visibility timeout — message is invisible after read" do
      client.send_message("default", { "test" => true })
      messages = client.read_batch("default", qty: 1, vt: 30)
      expect(messages.size).to eq(1)

      # Message was read with VT=30s — reading again immediately should return empty
      # because the message is invisible until VT expires
      second_read = client.read_batch("default", qty: 1, vt: 30)
      expect(second_read || []).to be_empty
    end
  end

  describe "archive and delete" do
    it "archives a message after processing" do
      msg_id = client.send_message("default", { "test" => "archive" })
      messages = client.read_batch("default", qty: 1)
      expect(messages.size).to eq(1)

      client.archive_message("default", msg_id)

      # Queue should be empty now
      remaining = client.read_batch("default", qty: 1)
      expect(remaining).to be_empty
    end
  end

  # Issue #484: the executor skips the failed-event DELETE on a first delivery,
  # so the redelivery path is the one that must still clear the row the failed
  # attempt wrote.
  describe "failed-event bookkeeping across a redelivery" do
    let(:queue) { "lifecycle_failed_events" }
    let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
    let(:flaky_job) do
      Class.new(ActiveJob::Base) do
        self.queue_adapter = :inline
        def self.name = "JobLifecycleSpec::FlakyJob"
        cattr_accessor :attempts, default: 0
        def perform(*)
          self.class.attempts += 1
          raise "boom on first attempt" if self.class.attempts == 1
        end
      end
    end

    before do
      require "active_job"
      ActiveJob::Base.logger = Logger.new(IO::NULL)
      stub_const("JobLifecycleSpec", Module.new)
      stub_const("JobLifecycleSpec::FlakyJob", flaky_job)
      client.ensure_queue(queue)
      client.purge_queue(queue)
      ActiveRecord::Base.connection.execute("DELETE FROM pgbus_failed_events WHERE queue_name = '#{queue}'")
    end

    def failed_event_count(msg_id)
      ActiveRecord::Base.connection.select_value(
        "SELECT COUNT(*) FROM pgbus_failed_events WHERE queue_name = '#{queue}' AND msg_id = #{msg_id.to_i}"
      ).to_i
    end

    it "records the failure, then clears it when the redelivered job succeeds" do
      msg_id = client.send_message(queue, flaky_job.new(1).serialize)

      first = client.read_batch(queue, qty: 1, vt: 30).first
      expect(executor.execute(first, queue)).to eq(:failed)
      expect(failed_event_count(msg_id)).to eq(1)

      client.set_visibility_timeout(queue, msg_id.to_i, vt: 0)
      second = client.read_batch(queue, qty: 1, vt: 30).first
      expect(second.read_ct.to_i).to eq(2)
      expect(executor.execute(second, queue)).to eq(:success)

      expect(failed_event_count(msg_id)).to eq(0)
    end
  end

  describe "dead letter queue" do
    it "moves a message to DLQ" do
      client.send_message("default", { "dlq_test" => true })
      messages = client.read_batch("default", qty: 1)
      expect(messages).not_to be_empty

      client.move_to_dead_letter("default", messages.first)

      # Original queue should be empty
      remaining = client.read_batch("default", qty: 1)
      expect(remaining).to be_empty
    end
  end
end
