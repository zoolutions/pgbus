# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Dashboard discard operations clear locks (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue_name) { "default" }
  let(:full_queue) { Pgbus.configuration.queue_name(queue_name) }

  # A discard releases only the lock the discarded message holds (issue
  # #564): the lock is acquired before its message is sent, so these
  # fixtures acquire first, as the adapter does. A lock acquired after the
  # message was sent is a successor's and stays.
  before do
    Pgbus::UniquenessKey.delete_all
    client.ensure_queue(queue_name)
  end

  def unique_payload(key, job_class: "ImportJob")
    { "job_class" => job_class, "arguments" => [key],
      Pgbus::Uniqueness::METADATA_KEY => key, Pgbus::Uniqueness::STRATEGY_KEY => "until_executed" }
  end

  describe "#discard_job clears associated lock" do
    it "releases the uniqueness lock when archiving a single enqueued message" do
      # The lock acquired at enqueue time, then the message itself
      Pgbus::UniquenessKey.acquire!("import-42", queue_name: queue_name, msg_id: 0)
      payload = {
        "job_class" => "ImportJob",
        "arguments" => [42],
        Pgbus::Uniqueness::METADATA_KEY => "import-42",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
      }
      client.send_message(queue_name, payload)
      expect(Pgbus::UniquenessKey.locked?("import-42")).to be true

      # Read message to get msg_id
      msg = client.read_batch(queue_name, qty: 1, vt: 0).first

      # Discard via dashboard
      data_source.discard_job(full_queue, msg.msg_id)

      # Lock should be released
      expect(Pgbus::UniquenessKey.locked?("import-42")).to be false
    end

    it "does nothing for messages without uniqueness metadata" do
      payload = { "job_class" => "PlainJob", "arguments" => [] }
      client.send_message(queue_name, payload)

      msg = client.read_batch(queue_name, qty: 1, vt: 0).first

      expect { data_source.discard_job(full_queue, msg.msg_id) }.not_to raise_error
    end

    it "keeps the lock its job handed to a retry that is still waiting" do
      attempt_id = enqueue_unique("import-7")
      retry_id = hand_to_waiting_retry("import-7", attempt_id)

      data_source.discard_job(full_queue, attempt_id)

      expect(Pgbus::UniquenessKey.find_by(lock_key: "import-7")&.msg_id).to eq(retry_id)
    end

    it "keeps a successor's lock taken after the discarded message was sent" do
      old_id = client.send_message(queue_name, unique_payload("import-8"))
      Pgbus::UniquenessKey.acquire!("import-8", queue_name: queue_name, msg_id: 0)

      data_source.discard_job(full_queue, old_id)

      expect(Pgbus::UniquenessKey.locked?("import-8")).to be(true)
    end
  end

  describe "#discard_all_enqueued clears all associated locks" do
    it "archives all messages from all queues and releases their locks" do
      # Enqueue messages with uniqueness locks
      3.times do |i|
        payload = {
          "job_class" => "ImportJob",
          "arguments" => [i],
          Pgbus::Uniqueness::METADATA_KEY => "import-#{i}",
          Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
        }
        Pgbus::UniquenessKey.acquire!("import-#{i}", queue_name: queue_name, msg_id: 0)
        client.send_message(queue_name, payload)
      end

      expect(Pgbus::UniquenessKey.count).to eq(3)

      count = data_source.discard_all_enqueued
      expect(count).to eq(3)

      # All locks should be released
      expect(Pgbus::UniquenessKey.count).to eq(0)

      # Queue should be empty
      messages = client.read_batch(queue_name, qty: 10, vt: 0)
      expect(messages).to be_empty
    end

    it "returns 0 when no messages exist" do
      count = data_source.discard_all_enqueued
      expect(count).to eq(0)
    end

    it "only clears locks for discarded messages, not unrelated locks" do
      # Enqueue one message with a lock
      payload = {
        "job_class" => "ImportJob",
        "arguments" => [1],
        Pgbus::Uniqueness::METADATA_KEY => "import-1",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
      }
      Pgbus::UniquenessKey.acquire!("import-1", queue_name: queue_name, msg_id: 0)
      client.send_message(queue_name, payload)

      # Create an unrelated lock (e.g., from a different system)
      Pgbus::UniquenessKey.acquire!("unrelated-lock", queue_name: "other", msg_id: 0)

      data_source.discard_all_enqueued

      # The enqueued message's lock should be cleared
      expect(Pgbus::UniquenessKey.locked?("import-1")).to be false
      # The unrelated lock should remain
      expect(Pgbus::UniquenessKey.locked?("unrelated-lock")).to be true
    end
  end

  # The lifecycle the adapter and the executor give a job's lock: acquired
  # unbound before the send, bound after it.
  def enqueue_unique(key, job_class: "ImportJob", bind: true)
    Pgbus::UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: 0)
    msg_id = client.send_message(queue_name, unique_payload(key, job_class: job_class)).to_i
    Pgbus::UniquenessKey.bind!(key, queue_name: queue_name, msg_id: msg_id) if bind
    msg_id
  end

  def sent_at(msg_id)
    ActiveRecord::Base.connection.select_value(
      "SELECT enqueued_at FROM pgmq.q_#{full_queue} WHERE msg_id = $1", "spec", [msg_id]
    )
  end

  # The running attempt hands its lock to a retry that parks behind its
  # concurrency slot: unbound, re-stamped after the attempt was sent.
  def hand_to_parked_retry(key, attempt_id)
    stamp = Pgbus::UniquenessKey.unbind!(key, from_queue_name: queue_name, from_msg_id: attempt_id,
                                              from_enqueued_at: sent_at(attempt_id), queue_name: queue_name)
    expect(stamp).not_to be_nil
    stamp
  end

  # The same handoff to a retry that was sent with a wait: unbound and
  # re-stamped, then bound to the delayed retry with that stamp.
  def hand_to_waiting_retry(key, attempt_id, job_class: "ImportJob")
    stamp = hand_to_parked_retry(key, attempt_id)
    retry_id = client.send_message(queue_name, unique_payload(key, job_class: job_class), delay: 300).to_i
    expect(Pgbus::UniquenessKey.bind!(key, queue_name: queue_name, msg_id: retry_id, acquired_at: stamp)).to eq(1)
    retry_id
  end

  # As the executor records it: under the logical queue, with the msg_id.
  def record_failure(key, msg_id)
    Pgbus::FailedEventRecorder.record!(
      queue_name: queue_name, msg_id: msg_id, payload: unique_payload(key, job_class: "FailedJob"),
      headers: nil, error: RuntimeError.new("boom"), retry_count: 0
    )
  end

  def failed_event_for(msg_id)
    data_source.failed_events.find { |e| e["msg_id"].to_i == msg_id }
  end

  # A failed job's message is still in its queue (it waits for its next
  # read), so its own send time bounds which unbound lock is its own.
  describe "#discard_failed_event clears associated lock" do
    it "releases the lock bound to the failed job's message" do
      msg_id = enqueue_unique("failed-1", job_class: "FailedJob")
      record_failure("failed-1", msg_id)

      data_source.discard_failed_event(failed_event_for(msg_id)["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be false
    end

    it "releases an unbound lock acquired before the failed job's message was sent" do
      msg_id = enqueue_unique("failed-2", job_class: "FailedJob", bind: false)
      record_failure("failed-2", msg_id)

      data_source.discard_failed_event(failed_event_for(msg_id)["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-2")).to be false
    end

    # The attempt handed its lock to a parked retry, then its archive failed
    # and the failure was recorded after the handoff.
    it "keeps the lock the failed attempt handed to a parked retry" do
      msg_id = enqueue_unique("failed-3", job_class: "FailedJob")
      hand_to_parked_retry("failed-3", msg_id)
      record_failure("failed-3", msg_id)

      data_source.discard_failed_event(failed_event_for(msg_id)["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-3")).to be true
    end

    it "keeps a successor's unbound lock taken after the failed job's message was sent" do
      msg_id = client.send_message(queue_name, unique_payload("failed-4", job_class: "FailedJob")).to_i
      record_failure("failed-4", msg_id)
      Pgbus::UniquenessKey.acquire!("failed-4", queue_name: queue_name, msg_id: 0)

      data_source.discard_failed_event(failed_event_for(msg_id)["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-4")).to be true
    end

    it "leaves an unbound lock alone once the failed job's message is gone" do
      msg_id = enqueue_unique("failed-5", job_class: "FailedJob", bind: false)
      record_failure("failed-5", msg_id)
      client.archive_message(queue_name, msg_id)

      data_source.discard_failed_event(failed_event_for(msg_id)["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-5")).to be true
    end
  end

  describe "#discard_all_failed clears associated locks" do
    it "releases each lock a failed job's message holds and keeps one handed to a parked retry" do
      ids = %w[failed-a failed-b].to_h { |key| [key, enqueue_unique(key, job_class: "FailedJob")] }
      handed = enqueue_unique("failed-handed", job_class: "FailedJob")
      hand_to_parked_retry("failed-handed", handed)
      ids.merge("failed-handed" => handed).each { |key, msg_id| record_failure(key, msg_id) }

      data_source.discard_all_failed

      expect(Pgbus::UniquenessKey.pluck(:lock_key)).to eq(["failed-handed"])
    end
  end

  describe "dead-lettered copies" do
    let(:full_dlq) { Pgbus.configuration.dead_letter_queue_name(queue_name) }

    before { client.ensure_dead_letter_queue(queue_name) }

    # As the executor moves it, except that its own release failed: the copy's
    # header names the original and when it was sent.
    def dead_letter(msg_id)
      original = client.read_batch(queue_name, qty: 20, vt: 30).find { |m| m.msg_id.to_i == msg_id }
      headers = Pgbus::DeadLetterHeader.build(
        existing: nil, reason: Pgbus::DeadLetterHeader::REASON_MAX_RETRIES, source: "worker",
        source_queue: full_queue, source_msg_id: msg_id, source_enqueued_at: original.enqueued_at,
        attempts: 6, max_retries: 5
      )
      client.move_to_dead_letter(queue_name, original, headers: headers)
      latest_copy_id
    end

    def latest_copy_id
      ActiveRecord::Base.connection.select_value("SELECT max(msg_id) FROM pgmq.q_#{full_dlq}")
    end

    describe "#discard_dlq_message" do
      it "releases the lock still bound to the message the copy was moved from" do
        copy = dead_letter(enqueue_unique("dlq-1", job_class: "DlqJob"))

        data_source.discard_dlq_message(full_dlq, copy)

        expect(Pgbus::UniquenessKey.locked?("dlq-1")).to be false
      end

      it "releases an unbound lock acquired before the original was sent" do
        copy = dead_letter(enqueue_unique("dlq-2", job_class: "DlqJob", bind: false))

        data_source.discard_dlq_message(full_dlq, copy)

        expect(Pgbus::UniquenessKey.locked?("dlq-2")).to be false
      end

      # The attempt handed its lock to a parked retry, was redelivered after
      # its heartbeat lapsed, and dead-lettered; the copy is younger than the
      # handoff, the original is not.
      it "keeps the lock the attempt handed to a parked retry before it was dead-lettered" do
        attempt = enqueue_unique("dlq-3", job_class: "DlqJob")
        hand_to_parked_retry("dlq-3", attempt)
        copy = dead_letter(attempt)

        data_source.discard_dlq_message(full_dlq, copy)

        expect(Pgbus::UniquenessKey.locked?("dlq-3")).to be true
      end

      it "keeps the lock a waiting retry holds once it was bound to the retry" do
        attempt = enqueue_unique("dlq-4", job_class: "DlqJob")
        retry_id = hand_to_waiting_retry("dlq-4", attempt, job_class: "DlqJob")
        copy = dead_letter(attempt)

        data_source.discard_dlq_message(full_dlq, copy)

        expect(Pgbus::UniquenessKey.find_by(lock_key: "dlq-4")&.msg_id).to eq(retry_id)
      end

      # A copy dead-lettered by an earlier release names neither, and that
      # release already freed its job's lock: a lock for the key now is
      # another job's.
      it "leaves an unbound lock alone for a copy whose header does not name its original" do
        Pgbus::UniquenessKey.acquire!("dlq-5", queue_name: queue_name, msg_id: 0)
        ActiveRecord::Base.connection.execute(
          "INSERT INTO pgmq.q_#{full_dlq} (vt, message) " \
          "VALUES (NOW(), '#{JSON.generate(unique_payload("dlq-5", job_class: "DlqJob"))}')"
        )

        data_source.discard_dlq_message(full_dlq, latest_copy_id)

        expect(Pgbus::UniquenessKey.locked?("dlq-5")).to be true
      end
    end

    describe "#discard_all_dlq" do
      it "releases each lock still held by a copy's original and keeps one handed to a parked retry" do
        %w[dlq-a dlq-b].each { |key| dead_letter(enqueue_unique(key, job_class: "DlqJob")) }
        handed = enqueue_unique("dlq-handed", job_class: "DlqJob")
        hand_to_parked_retry("dlq-handed", handed)
        dead_letter(handed)

        data_source.discard_all_dlq

        expect(Pgbus::UniquenessKey.pluck(:lock_key)).to eq(["dlq-handed"])
      end
    end
  end
end
