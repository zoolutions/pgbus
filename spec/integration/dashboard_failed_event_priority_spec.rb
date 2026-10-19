# frozen_string_literal: true

require_relative "../integration_helper"

# Issue #577: a failed event records the logical queue only. Under priority
# routing the message sits in one of the `_pN` sub-queues, never in the base
# table, and each sub-queue numbers its messages from its own sequence, so
# the same msg_id can name another job's message in a sibling sub-queue.
# Retry and Discard act on the sub-queue whose message carries the job's
# job_id, and leave a same-numbered message elsewhere alone.
RSpec.describe "Failed-job Retry and Discard under priority routing (integration)", :integration do
  let(:queue) { "prio_failed_retry" }
  let(:data_source) { Pgbus::Web::DataSource.new(client: Pgbus.client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:msg_id) { 9_000_001 }
  let(:payload) { { "job_class" => "PrioJob", "job_id" => "failed-job", "arguments" => [1] } }

  around do |example|
    original = Pgbus.configuration.priority_levels
    Pgbus.configuration.priority_levels = 3
    Pgbus.reset_client!
    example.run
  ensure
    Pgbus.client.physical_queue_names(queue).each { |name| Pgbus.client.drop_queue(name, prefixed: false) }
    Pgbus.configuration.priority_levels = original
    Pgbus.reset_client!
  end

  before { Pgbus.client.ensure_queue(queue) }

  def physical(level)
    Pgbus.client.physical_queue_names(queue).fetch(level)
  end

  # A message with a chosen msg_id, invisible for an hour, the way two
  # sub-queues' sequences collide.
  def plant(level, message)
    conn.exec_insert(
      "INSERT INTO pgmq.q_#{physical(level)} (msg_id, vt, message) OVERRIDING SYSTEM VALUE " \
      "VALUES ($1, NOW() + interval '1 hour', $2::jsonb)",
      "spec", [msg_id, JSON.generate(message)]
    )
  end

  def record_failure(id = msg_id)
    Pgbus::FailedEventRecorder.record!(queue_name: queue, msg_id: id, payload: payload, headers: nil,
                                       error: StandardError.new("boom"), retry_count: 1)
    data_source.failed_events.find { |e| e["msg_id"].to_i == id.to_i }
  end

  def visible?(level, id = msg_id)
    vt = conn.select_value("SELECT vt FROM pgmq.q_#{physical(level)} WHERE msg_id = $1", "spec", [id])
    Time.parse(vt.to_s) <= Time.current + 1
  end

  def queued?(level, id = msg_id)
    conn.select_value("SELECT 1 FROM pgmq.q_#{physical(level)} WHERE msg_id = $1", "spec", [id]).present?
  end

  def archived?(level, id = msg_id)
    conn.select_value("SELECT 1 FROM pgmq.a_#{physical(level)} WHERE msg_id = $1", "spec", [id]).present?
  end

  def queued_total
    Pgbus.client.physical_queue_names(queue).sum do |name|
      conn.select_value("SELECT COUNT(*) FROM pgmq.q_#{name}", "spec").to_i
    end
  end

  describe "#retry_failed_event" do
    it "resets the vt of the message in the sub-queue it was sent to" do
      sent = Pgbus.client.send_message(queue, payload, priority: 1)
      Pgbus.client.read_batch_prioritized(queue, qty: 1, vt: 600)
      expect(visible?(1, sent)).to be(false)

      expect(data_source.retry_failed_event(record_failure(sent)["id"])).to be(true)

      expect(visible?(1, sent)).to be(true)
      expect(queued_total).to eq(1)
      expect(data_source.failed_events).to be_empty
    end

    it "leaves a same-numbered message of another job in a sibling sub-queue alone" do
      plant(0, payload.merge("job_id" => "someone-else"))
      plant(1, payload)

      expect(data_source.retry_failed_event(record_failure["id"])).to be(true)

      expect(visible?(1)).to be(true)
      expect(visible?(0)).to be(false)
      expect(queued_total).to eq(2)
    end

    it "enqueues a fresh copy when no sub-queue holds the job's message any more" do
      plant(0, payload.merge("job_id" => "someone-else"))

      expect(data_source.retry_failed_event(record_failure["id"])).to be(true)

      expect(visible?(0)).to be(false)
      expect(queued_total).to eq(2)
    end
  end

  describe "#discard_failed_event" do
    it "archives the job's message from its sub-queue and keeps the sibling's" do
      plant(0, payload.merge("job_id" => "someone-else"))
      plant(1, payload)

      expect(data_source.discard_failed_event(record_failure["id"])).to be(true)

      expect(queued?(1)).to be(false)
      expect(archived?(1)).to be(true)
      expect(queued?(0)).to be(true)
    end
  end

  describe "#discard_all_failed" do
    it "archives every failed job's message from its sub-queue and keeps the sibling's" do
      plant(0, payload.merge("job_id" => "someone-else"))
      plant(1, payload)
      record_failure

      data_source.discard_all_failed

      expect(queued?(1)).to be(false)
      expect(archived?(1)).to be(true)
      expect(queued?(0)).to be(true)
      expect(data_source.failed_events).to be_empty
    end
  end
end
