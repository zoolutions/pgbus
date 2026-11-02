# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# Issue #631: the dashboard puts a job payload back on its queue as a new
# message — DLQ Retry (and Retry all), and failed-job Retry when the original
# message is gone (and Retry all). The payload still names its uniqueness key
# and strategy, so the copy releases "its" row when it starts
# (:until_executing) or finishes (:until_executed). Re-sent without a row of
# its own, that release could match a newer copy's unbound row — one parked
# behind limits_concurrency on_conflict: :block — and free the key while that
# copy still waits. The re-send now acquires the key like an enqueue does:
# taken before the send, bound to the new message after it, and refused (the
# source row stays) while another copy holds it.
RSpec.describe "Dashboard re-send of a uniqueness payload (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:config) { Pgbus.configuration }
  let(:queue) { "resend_unique" }
  let(:dlq_name) { config.dead_letter_queue_name(queue) }
  let(:lock_queue) { Pgbus::Uniqueness.lock_queue_name(queue) }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:conn) { ActiveRecord::Base.connection }
  let(:key) { "resend-42" }
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
    Pgbus::UniquenessKey.delete_all
    conn.exec_delete("DELETE FROM pgbus_failed_events", "spec")
  end

  after { ActiveJob::Base.queue_adapter = :test }

  def define_job(strategy)
    state = outcome
    queue_name = queue
    stub_const("ResendUniqueJob", Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness

      queue_as(queue_name)
      ensures_uniqueness strategy: strategy, on_conflict: :discard, key: ->(id) { "resend-#{id}" }

      define_method(:perform) { |_id| raise ArgumentError, "boom" if state[:fail] }
    end)
  end

  # Read → execute → make visible again, until the executor dead-letters it.
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

  def lock_row = Pgbus::UniquenessKey.find_by(lock_key: key)

  # A newer copy parked behind its concurrency limit: its row is unbound
  # (msg_id 0) and acquired after the dead-lettered message was sent.
  def park_newer_copy
    Pgbus::UniquenessKey.acquire!(key, queue_name: lock_queue, msg_id: 0)
    Pgbus::UniquenessKey.clear_bind_stamp!(key)
    lock_row.created_at
  end

  def run_queued_copy
    outcome[:fail] = false
    message = client.read_message(queue, vt: 30)
    expect(executor.execute(message, queue)).to eq(:success)
  end

  shared_examples "a re-send that holds the key" do |strategy|
    before { define_job(strategy) }

    it "binds the key to the re-sent message, and the copy releases it" do
      ResendUniqueJob.perform_later(42)
      drive_to_dlq
      expect(lock_row).to be_nil # the dead-lettered copy released its row

      expect(resend_dead_letter).to be(true)

      expect(queued_msg_ids.size).to eq(1)
      expect(lock_row).to have_attributes(queue_name: lock_queue, msg_id: queued_msg_ids.first)
      ResendUniqueJob.perform_later(42)
      expect(queued_msg_ids.size).to eq(1) # a fresh enqueue is deduped against the re-sent copy

      run_queued_copy
      expect(lock_row).to be_nil
    end

    it "refuses while a newer parked copy holds the key, and leaves that copy's row alone" do
      ResendUniqueJob.perform_later(42)
      drive_to_dlq
      parked_at = park_newer_copy

      expect(resend_dead_letter).to be(false)

      expect(queued_msg_ids).to be_empty
      expect(lock_row).to have_attributes(msg_id: 0, created_at: parked_at)
      expect(source_still_there?).to be(true)
    end
  end

  describe "DLQ Retry" do
    def resend_dead_letter = data_source.retry_dlq_message(dlq_name, dlq_rows.sole[:msg_id])
    def source_still_there? = dlq_rows.size == 1

    it_behaves_like "a re-send that holds the key", :until_executed
    it_behaves_like "a re-send that holds the key", :until_executing

    describe "Retry all" do
      before { define_job(:until_executed) }

      it "re-sends what it can lock and leaves the held one in the DLQ" do
        ResendUniqueJob.perform_later(42)
        drive_to_dlq
        ResendUniqueJob.perform_later(43)
        drive_to_dlq
        park_newer_copy

        expect(data_source.retry_all_dlq).to eq(1)

        expect(dlq_rows.map { |row| JSON.parse(row[:message])["arguments"] }).to eq([[42]])
        expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-43").msg_id).to eq(queued_msg_ids.sole)
        expect(lock_row.msg_id).to eq(0)
      end
    end

    # The issue's repro end to end: the newer copy is really parked behind
    # limits_concurrency on_conflict: :block, and still runs after the
    # refused retry.
    describe "with a copy parked behind limits_concurrency on_conflict: :block" do
      let(:slot) { "resend-slot-42" }

      before do
        Pgbus::Semaphore.delete_all
        Pgbus::BlockedExecution.delete_all
        state = outcome
        queue_name = queue
        stub_const("ResendUniqueJob", Class.new(ActiveJob::Base) do
          include Pgbus::Uniqueness
          include Pgbus::Concurrency

          queue_as(queue_name)
          ensures_uniqueness strategy: :until_executing, on_conflict: :discard, key: ->(id) { "resend-#{id}" }
          limits_concurrency to: 1, key: ->(id) { "resend-slot-#{id}" }, on_conflict: :block

          define_method(:perform) { |_id| raise ArgumentError, "boom" if state[:fail] }
        end)
      end

      it "refuses the retry and leaves the parked copy runnable, holding its key" do
        ResendUniqueJob.perform_later(42)
        drive_to_dlq # dead-lettering signals the slot back
        expect(Pgbus::Concurrency::Semaphore.acquire(slot, 1, 600)).to eq(:acquired) # another copy runs
        ResendUniqueJob.perform_later(42)
        expect(Pgbus::BlockedExecution.count).to eq(1)
        parked_at = lock_row.created_at
        expect(lock_row.msg_id).to eq(0)

        expect(data_source.retry_dlq_message(dlq_name, dlq_rows.sole[:msg_id])).to be(false)

        expect(dlq_rows.size).to eq(1)
        expect(queued_msg_ids).to be_empty
        expect(Pgbus::BlockedExecution.count).to eq(1)
        expect(lock_row).to have_attributes(msg_id: 0, created_at: parked_at)

        Pgbus::Concurrency::Semaphore.signal(slot, client: client) # the running copy finishes
        run_queued_copy
        expect(lock_row).to be_nil
      end
    end

    # The executor's release at dead-letter time failed: the row is still
    # bound to the message the DLQ copy came from. Nobody live holds it, so
    # the copy takes it over (the row Discard would release).
    it "takes over a row still bound to the dead-lettered message" do
      define_job(:until_executed)
      ResendUniqueJob.perform_later(42)
      source = drive_to_dlq
      Pgbus::UniquenessKey.acquire!(key, queue_name: lock_queue, msg_id: source.msg_id.to_i)

      expect(data_source.retry_dlq_message(dlq_name, dlq_rows.sole[:msg_id])).to be(true)

      expect(lock_row).to have_attributes(queue_name: lock_queue, msg_id: queued_msg_ids.sole)
      run_queued_copy
      expect(lock_row).to be_nil
    end

    # A retry_on re-enqueue of an :until_executing job went out with no row
    # (the attempt's was released at its start), so its copy never releases
    # one: the re-send takes none either, and a parked copy's row survives it.
    it "re-sends an :until_executing retry (executions > 0) without a row, and the copy leaves a parked row alone" do
      define_job(:until_executing)
      payload = ResendUniqueJob.new(42).serialize.merge(
        "executions" => 1, Pgbus::Uniqueness::METADATA_KEY => key, Pgbus::Uniqueness::STRATEGY_KEY => "until_executing"
      )
      client.send_message(queue, payload)
      drive_to_dlq
      parked_at = park_newer_copy

      expect(data_source.retry_dlq_message(dlq_name, dlq_rows.sole[:msg_id])).to be(true)
      expect(lock_row).to have_attributes(msg_id: 0, created_at: parked_at)

      run_queued_copy
      expect(lock_row).to have_attributes(msg_id: 0, created_at: parked_at)
    end
  end

  describe "failed-job Retry with the original message gone" do
    # The failed row of a message that no longer exists in its queue.
    def record_gone_failure(id)
      job = ResendUniqueJob.new(id)
      payload = Pgbus::Uniqueness.inject_metadata(job, job.serialize)
      Pgbus::FailedEventRecorder.record!(queue_name: queue, msg_id: 9_000_000 + id, source_queue: config.queue_name(queue),
                                         payload: payload, headers: nil, error: StandardError.new("boom"),
                                         retry_count: 1)
      data_source.failed_events.find { |event| event["msg_id"].to_i == 9_000_000 + id }
    end

    def failed_count = conn.select_value("SELECT COUNT(*) FROM pgbus_failed_events", "spec").to_i

    describe "Retry" do
      def resend_dead_letter = data_source.retry_failed_event(@event["id"])
      def source_still_there? = failed_count == 1

      # The executor dead-lettered the job and cleared its failed row; the
      # row recorded here stands for one whose message is gone the same way.
      def drive_to_dlq
        super.tap { @event = record_gone_failure(42) }
      end

      it_behaves_like "a re-send that holds the key", :until_executed
      it_behaves_like "a re-send that holds the key", :until_executing
    end

    describe "Retry of a job whose row is still bound to its gone message" do
      before { define_job(:until_executed) }

      it "takes the row over for the copy" do
        event = record_gone_failure(42)
        Pgbus::UniquenessKey.acquire!(key, queue_name: lock_queue, msg_id: event["msg_id"].to_i)

        expect(data_source.retry_failed_event(event["id"])).to be(true)

        expect(lock_row).to have_attributes(queue_name: lock_queue, msg_id: queued_msg_ids.sole)
        expect(failed_count).to eq(0)
      end
    end

    describe "Retry all" do
      before { define_job(:until_executed) }

      # A failed job's message waits in its queue for its next read: Retry
      # all makes it visible, as Retry does, instead of sending a second copy
      # beside it (which would also have taken its row).
      it "resets the vt of a message still queued and re-sends only the gone one" do
        Pgbus::UniquenessKey.acquire!(key, queue_name: lock_queue, msg_id: 0)
        job = ResendUniqueJob.new(42)
        live_id = client.send_message(queue, Pgbus::Uniqueness.inject_metadata(job, job.serialize))
        Pgbus::UniquenessKey.bind!(key, queue_name: lock_queue, msg_id: live_id)
        client.set_visibility_timeout(queue, live_id.to_i, vt: 600)
        Pgbus::FailedEventRecorder.record!(queue_name: queue, msg_id: live_id, source_queue: config.queue_name(queue),
                                           payload: { "job_class" => "ResendUniqueJob" }, headers: nil,
                                           error: StandardError.new("boom"), retry_count: 1)
        record_gone_failure(43)

        expect(data_source.retry_all_failed).to eq(2)

        expect(failed_count).to eq(0)
        expect(queued_msg_ids.size).to eq(2)
        expect(lock_row.msg_id).to eq(live_id.to_i)
        vt = conn.select_value("SELECT vt FROM pgmq.q_#{config.queue_name(queue)} WHERE msg_id = $1", "spec", [live_id])
        expect(Time.parse(vt.to_s)).to be <= Time.current + 1
      end

      it "re-sends what it can lock and keeps the held one's failed row" do
        record_gone_failure(42)
        record_gone_failure(43)
        park_newer_copy

        expect(data_source.retry_all_failed).to eq(1)

        expect(failed_count).to eq(1)
        expect(Pgbus::UniquenessKey.find_by(lock_key: "resend-43")).to have_attributes(
          queue_name: lock_queue, msg_id: queued_msg_ids.sole
        )
        expect(lock_row.msg_id).to eq(0)
      end
    end
  end
end
