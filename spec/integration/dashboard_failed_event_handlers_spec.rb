# frozen_string_literal: true

require_relative "../integration_helper"

# Regression coverage for the dashboard's failed event handlers.
#
# History: discard_failed_event/discard_all_failed deleted the row from
# pgbus_failed_events and released the uniqueness lock — but never touched
# the actual message in the PGMQ queue. The message stayed there, vt expired,
# the worker re-read it, the job failed again, a NEW pgbus_failed_events row
# was created. Meanwhile the lock was gone, so the recurring scheduler could
# also enqueue duplicates. Same problem for retry_failed_event: it sent a
# NEW message via send_message and deleted the row, but the original was
# still in the queue.
#
# The fix: discard archives the message from the queue. Retry resets the
# message's visibility timeout to 0 so the worker picks it up immediately
# (no new message produced).
RSpec.describe "Dashboard failed event handlers (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue_name) { "default" }
  let(:full_queue) { Pgbus.configuration.queue_name(queue_name) }
  let(:lock_key) { "TestJob:42" }

  let(:payload) do
    {
      "job_class" => "TestJob",
      "arguments" => [42],
      Pgbus::Uniqueness::METADATA_KEY => lock_key,
      Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
    }
  end

  before do
    Pgbus::UniquenessKey.delete_all
    client.ensure_queue(queue_name)
  end

  # Helper: acquire the uniqueness lock, enqueue the message and bind the lock
  # to it (the adapter's order), then register a failed_event row pointing to
  # it — mimicking what happens after a job fails. A lock acquired after the
  # send would be a successor's, which a discard leaves alone (issue #564).
  def setup_failed_message
    Pgbus::UniquenessKey.acquire!(lock_key, queue_name: queue_name, msg_id: 0)
    msg_id = client.send_message(queue_name, payload)
    Pgbus::UniquenessKey.bind!(lock_key, queue_name: queue_name, msg_id: msg_id)
    Pgbus::FailedEventRecorder.record!(
      queue_name: queue_name,
      msg_id: msg_id,
      payload: payload,
      headers: nil,
      error: StandardError.new("boom"),
      retry_count: 1
    )
    msg_id
  end

  def message_in_queue?(msg_id)
    ActiveRecord::Base.connection.select_value(
      "SELECT 1 FROM pgmq.q_#{full_queue} WHERE msg_id = $1 LIMIT 1", "test", [msg_id]
    ).present?
  end

  describe "#discard_failed_event" do
    it "archives the message from the queue" do
      msg_id = setup_failed_message
      expect(message_in_queue?(msg_id)).to be(true)

      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(message_in_queue?(msg_id)).to be(false)
    end

    it "releases the uniqueness lock" do
      setup_failed_message
      expect(Pgbus::UniquenessKey.locked?(lock_key)).to be(true)

      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(Pgbus::UniquenessKey.locked?(lock_key)).to be(false)
    end

    it "deletes the failed_event row" do
      setup_failed_message
      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(data_source.failed_events).to be_empty
    end

    it "is idempotent when the message is already gone" do
      msg_id = setup_failed_message
      client.archive_message(queue_name, msg_id)
      expect(message_in_queue?(msg_id)).to be(false)

      event = data_source.failed_events.first
      result = data_source.discard_failed_event(event["id"])

      expect(result).to be(true)
      expect(data_source.failed_events).to be_empty
    end
  end

  describe "#discard_all_failed" do
    it "archives all messages from the queue and releases their locks" do
      msg_ids = 3.times.map do |i|
        key = "TestJob:#{i}"
        msg_payload = payload.merge(Pgbus::Uniqueness::METADATA_KEY => key, "arguments" => [i])
        Pgbus::UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: 0)
        msg_id = client.send_message(queue_name, msg_payload)
        Pgbus::FailedEventRecorder.record!(
          queue_name: queue_name, msg_id: msg_id, payload: msg_payload, headers: nil,
          error: StandardError.new("boom #{i}"), retry_count: 0
        )
        msg_id
      end

      data_source.discard_all_failed

      msg_ids.each { |id| expect(message_in_queue?(id)).to be(false) }
      expect(Pgbus::UniquenessKey.count).to eq(0)
      expect(data_source.failed_events).to be_empty
    end
  end

  describe "#retry_failed_event" do
    it "resets the existing message vt to 0 (does NOT enqueue a duplicate)" do
      msg_id = setup_failed_message

      # Read+lock the message so vt is in the future
      client.read_message(queue_name, vt: 600)

      # Verify vt is set in the future
      vt_before = ActiveRecord::Base.connection.select_value(
        "SELECT vt FROM pgmq.q_#{full_queue} WHERE msg_id = $1", "test", [msg_id]
      )
      expect(Time.parse(vt_before.to_s)).to be > Time.current

      event = data_source.failed_events.first
      data_source.retry_failed_event(event["id"])

      # The original message should still be the only one in the queue
      count = ActiveRecord::Base.connection.select_value(
        "SELECT COUNT(*) FROM pgmq.q_#{full_queue}", "test"
      )
      expect(count.to_i).to eq(1)

      # And its vt should be at-or-before now (immediately visible)
      vt_after = ActiveRecord::Base.connection.select_value(
        "SELECT vt FROM pgmq.q_#{full_queue} WHERE msg_id = $1", "test", [msg_id]
      )
      expect(Time.parse(vt_after.to_s)).to be <= Time.current + 1
    end

    it "deletes the failed_event row" do
      setup_failed_message
      event = data_source.failed_events.first
      data_source.retry_failed_event(event["id"])

      expect(data_source.failed_events).to be_empty
    end

    it "does NOT release the uniqueness lock (job is being retried)" do
      setup_failed_message
      event = data_source.failed_events.first
      data_source.retry_failed_event(event["id"])

      expect(Pgbus::UniquenessKey.locked?(lock_key)).to be(true)
    end

    it "falls back to send_message when the original is gone (e.g. moved to DLQ)" do
      msg_id = setup_failed_message
      client.archive_message(queue_name, msg_id)
      expect(message_in_queue?(msg_id)).to be(false)

      event = data_source.failed_events.first
      result = data_source.retry_failed_event(event["id"])

      expect(result).to be(true)
      # A fresh copy was enqueued
      count = ActiveRecord::Base.connection.select_value(
        "SELECT COUNT(*) FROM pgmq.q_#{full_queue}", "test"
      )
      expect(count.to_i).to eq(1)
    end
  end
end
