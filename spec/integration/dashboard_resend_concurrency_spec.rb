# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #636: the dashboard puts a job payload back on its queue as a new
# message — DLQ Retry (and Retry all, and `pgbus dlq retry`), and failed-job
# Retry when the original message is gone. The payload still names its
# limits_concurrency key, so the copy signals the semaphore when it finishes:
# re-sent without a slot of its own, that signal gave back a slot another copy
# held and promoted a parked job beside it. The re-send now takes its slot the
# way an enqueue does: acquired before the send, parked behind the limit under
# on_conflict: :block, refused (the source stays) under :discard and :raise.
RSpec.describe "Dashboard re-send of a limits_concurrency payload (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:queue) { "resend_slot" }
  let(:dlq_name) { config.dead_letter_queue_name(queue) }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:slot) { "resend-slot-42" }
  let(:outcome) { { fail: true } }

  around do |example|
    original = config.max_retries
    config.max_retries = 1
    example.run
  ensure
    config.max_retries = original
  end

  before do
    ActiveJob::Base.logger = Logger.new(IO::NULL)
    ActiveJob::Base.queue_adapter = :pgbus
    client.ensure_queue(queue)
  end

  after { ActiveJob::Base.queue_adapter = :test }

  def define_job(on_conflict, uniqueness: nil)
    state = outcome
    queue_name = queue
    stub_const("ResendSlotJob", Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness
      include Pgbus::Concurrency

      queue_as(queue_name)
      ensures_uniqueness strategy: uniqueness, on_conflict: :discard, key: ->(id) { "resend-#{id}" } if uniqueness
      limits_concurrency to: 1, key: ->(id) { "resend-slot-#{id}" }, on_conflict: on_conflict

      define_method(:perform) { |_id| raise ArgumentError, "boom" if state[:fail] }
    end)
  end

  # Read → execute → make visible again, until the executor dead-letters it.
  # Dead-lettering signals the job's slot back.
  def drive_to_dlq
    5.times do
      message = client.read_message(queue, vt: 30)
      raise "no message on #{queue}" unless message
      return message if executor.execute(message, queue) == :dead_lettered

      client.set_visibility_timeout(queue, message.msg_id.to_i, vt: 0)
    end
    raise "never dead-lettered"
  end

  def dlq_rows
    data_source.reset_cache!
    data_source.dlq_messages(dlq: dlq_name)
  end

  def queued_msg_ids
    conn.select_values("SELECT msg_id FROM pgmq.q_#{config.queue_name(queue)} ORDER BY msg_id", "spec").map(&:to_i)
  end

  def slots_held(key = slot) = Pgbus::Concurrency::Semaphore.current_value(key)
  def parked(key = slot) = Pgbus::BlockedExecution.where(concurrency_key: key)

  # Another copy of the job runs, holding the only slot.
  def hold_slot(key = slot)
    expect(Pgbus::Concurrency::Semaphore.acquire(key, 1, 600)).to eq(:acquired)
  end

  def run_queued_copy
    outcome[:fail] = false
    message = client.read_message(queue, vt: 30)
    expect(executor.execute(message, queue)).to eq(:success)
  end

  shared_examples "a re-send that takes a slot" do
    it "takes the free slot before the send, and the copy's completion gives back only that slot" do
      define_job(:block)
      dead_letter_job

      expect(resend).to be(true)

      expect(queued_msg_ids.size).to eq(1)
      expect(slots_held).to eq(1)
      ResendSlotJob.perform_later(42)
      expect(parked.count).to eq(1) # a fresh enqueue waits behind the re-sent copy

      run_queued_copy
      expect(parked.count).to eq(0)
      expect(queued_msg_ids.size).to eq(1) # the parked job was promoted into the freed slot
      expect(slots_held).to eq(1)
    end

    # The issue's repro: one copy runs, one is parked behind it.
    it "parks the copy behind a full limit under on_conflict: :block, and the holder's signal promotes in order" do
      define_job(:block)
      dead_letter_job
      hold_slot
      ResendSlotJob.perform_later(42)

      expect(resend).to be(true)

      expect(queued_msg_ids).to be_empty
      expect(parked.count).to eq(2)
      expect(slots_held).to eq(1)
      expect(source_still_there?).to be(false)

      Pgbus::Concurrency::Semaphore.signal(slot, client: client) # the running copy finishes
      expect(queued_msg_ids.size).to eq(1)
      expect(parked.count).to eq(1)
      expect(slots_held).to eq(1)
    end

    %i[discard raise].each do |on_conflict|
      it "refuses under on_conflict: :#{on_conflict} at the limit and leaves the source in place" do
        define_job(on_conflict)
        dead_letter_job
        hold_slot

        expect(resend).to be(false)

        expect(queued_msg_ids).to be_empty
        expect(parked.count).to eq(0)
        expect(slots_held).to eq(1)
        expect(source_still_there?).to be(true)
      end
    end

    it "parks an :until_executed copy with its uniqueness row unbound, and the promoted copy releases it" do
      define_job(:block, uniqueness: :until_executed)
      dead_letter_job
      hold_slot

      expect(resend).to be(true)

      expect(parked.count).to eq(1)
      expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-42")).to have_attributes(msg_id: 0)

      Pgbus::Concurrency::Semaphore.signal(slot, client: client)
      run_queued_copy
      expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-42")).to be_nil
      expect(slots_held).to eq(0)
    end

    it "gives the uniqueness key back when the limit refuses the copy" do
      define_job(:discard, uniqueness: :until_executed)
      dead_letter_job
      hold_slot

      expect(resend).to be(false)

      expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-42")).to be_nil
    end
  end

  describe "DLQ Retry" do
    def dead_letter_job
      ResendSlotJob.perform_later(42)
      drive_to_dlq
      expect(slots_held).to eq(0) # dead-lettering signalled the slot back
    end

    def resend = data_source.retry_dlq_message(dlq_name, dlq_rows.sole[:msg_id])
    def source_still_there? = dlq_rows.size == 1

    it_behaves_like "a re-send that takes a slot"

    # The park commits on the bus connection and the DLQ copy is deleted on
    # PGMQ's, so the two cannot share a transaction: a failed delete withdraws
    # the park while it still waits, and nothing changes.
    it "withdraws the park and keeps the DLQ copy when deleting it fails" do
      define_job(:block, uniqueness: :until_executed)
      dead_letter_job
      hold_slot
      allow(client).to receive(:delete_message).and_raise(StandardError, "delete failed")

      expect(resend).to be(false)

      expect(parked.count).to eq(0)
      expect(source_still_there?).to be(true)
      expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-42")).to be_nil
      expect(slots_held).to eq(1)
    end

    it "promotes a parked copy with the DLQ retry count a sent copy carries" do
      define_job(:block)
      dead_letter_job
      hold_slot

      expect(resend).to be(true)
      Pgbus::Concurrency::Semaphore.signal(slot, client: client)

      message = client.read_message(queue, vt: 30)
      headers = message.headers.is_a?(String) ? JSON.parse(message.headers) : message.headers
      expect(headers).to include(Pgbus::DeadLetterHeader::RETRIES_KEY => 1)
      expect(JSON.parse(message.message)).not_to have_key(Pgbus::Concurrency::BlockedExecution::HEADERS_KEY)
    end

    describe "Retry all" do
      it "sends the first copy into the free slot and parks the second behind it" do
        define_job(:block)
        ResendSlotJob.perform_later(42)
        drive_to_dlq
        ResendSlotJob.perform_later(42)
        drive_to_dlq

        expect(data_source.retry_all_dlq).to eq(2)

        expect(dlq_rows).to be_empty
        expect(queued_msg_ids.size).to eq(1)
        expect(parked.count).to eq(1)
        expect(slots_held).to eq(1)
      end
    end
  end

  describe "failed-job Retry with the original message gone" do
    # The failed row of a message that no longer exists in its queue: the
    # job dead-lettered (its slot signalled back), and the row recorded here
    # stands for one whose message is gone the same way.
    def dead_letter_job
      ResendSlotJob.perform_later(42)
      drive_to_dlq
      job = ResendSlotJob.new(42)
      payload = Pgbus::Uniqueness.inject_metadata(job, Pgbus::Concurrency.inject_metadata(job, job.serialize))
      Pgbus::FailedEventRecorder.record!(queue_name: queue, msg_id: 9_000_042, source_queue: config.queue_name(queue),
                                         payload: payload, headers: nil, error: StandardError.new("boom"),
                                         retry_count: 1)
      conn.exec_delete("DELETE FROM pgmq.q_#{dlq_name}", "spec")
      Pgbus::UniquenessKey.delete_all
      @event = data_source.failed_events.sole
    end

    def resend = data_source.retry_failed_event(@event["id"])
    def source_still_there? = conn.select_value("SELECT COUNT(*) FROM pgbus_failed_events", "spec").to_i == 1

    it_behaves_like "a re-send that takes a slot"

    describe "Retry all" do
      it "counts a copy parked behind a full limit as retried, and deletes its failed row" do
        define_job(:block)
        dead_letter_job
        hold_slot

        expect(data_source.retry_all_failed).to eq(1)

        expect(source_still_there?).to be(false)
        expect(parked.count).to eq(1)
        expect(queued_msg_ids).to be_empty
      end
    end

    # A failed attempt keeps its slot until it succeeds or dead-letters, so a
    # message still queued goes back with the slot it holds: no second one.
    it "resets the vt of a message still queued without taking another slot" do
      define_job(:block)
      ResendSlotJob.perform_later(42)
      message = client.read_message(queue, vt: 30)
      expect(executor.execute(message, queue)).to eq(:failed)
      event = data_source.failed_events.sole

      expect(data_source.retry_failed_event(event["id"])).to be(true)

      expect(slots_held).to eq(1)
      expect(queued_msg_ids).to eq([message.msg_id.to_i])
      expect(parked.count).to eq(0)
    end
  end
end
