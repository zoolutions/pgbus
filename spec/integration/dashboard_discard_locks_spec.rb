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
      Pgbus::UniquenessKey.acquire!("import-7", queue_name: queue_name, msg_id: 0)
      attempt_id = client.send_message(queue_name, unique_payload("import-7"))
      retry_id = client.send_message(queue_name, unique_payload("import-7"), delay: 300)
      Pgbus::UniquenessKey.where(lock_key: "import-7").update_all(msg_id: retry_id)

      data_source.discard_job(full_queue, attempt_id)

      expect(Pgbus::UniquenessKey.find_by(lock_key: "import-7")&.msg_id).to eq(retry_id.to_i)
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

  # A failed job's lock was acquired before its message was sent, so before
  # the failure was recorded: failed_at bounds which unbound lock is its own.
  def record_failure(key, arguments: [1])
    ActiveRecord::Base.connection.execute(<<~SQL)
      INSERT INTO pgbus_failed_events (queue_name, payload, headers, error_class, error_message, failed_at)
      VALUES ('#{full_queue}',
              '{"job_class":"FailedJob","arguments":#{arguments.to_json},"pgbus_uniqueness_key":"#{key}","pgbus_uniqueness_strategy":"until_executed"}',
              '{}', 'RuntimeError', 'boom', NOW())
    SQL
  end

  describe "#discard_failed_event clears associated lock" do
    it "releases the lock when discarding a failed event" do
      Pgbus::UniquenessKey.acquire!("failed-1", queue_name: queue_name, msg_id: 0)
      record_failure("failed-1")
      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be true

      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be false
    end

    it "keeps a successor's unbound lock taken after the failure was recorded" do
      record_failure("failed-1")
      Pgbus::UniquenessKey.acquire!("failed-1", queue_name: queue_name, msg_id: 0)

      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be true
    end
  end

  describe "#discard_all_failed clears associated locks" do
    before do
      3.times do |i|
        Pgbus::UniquenessKey.acquire!("failed-#{i}", queue_name: queue_name, msg_id: 0)
        record_failure("failed-#{i}", arguments: [i])
      end
    end

    it "keeps a successor's unbound lock taken after the failure was recorded" do
      record_failure("failed-late", arguments: [9])
      Pgbus::UniquenessKey.acquire!("failed-late", queue_name: queue_name, msg_id: 0)

      data_source.discard_all_failed

      expect(Pgbus::UniquenessKey.pluck(:lock_key)).to eq(["failed-late"])
    end

    it "releases all locks for discarded failed events" do
      expect(Pgbus::UniquenessKey.count).to eq(3)

      data_source.discard_all_failed

      expect(Pgbus::UniquenessKey.count).to eq(0)
    end
  end

  describe "#discard_dlq_message clears associated lock" do
    let(:dlq_name) { Pgbus.configuration.dead_letter_queue_name(queue_name) }
    let(:full_dlq) { dlq_name }

    before do
      client.ensure_dead_letter_queue(queue_name)
    end

    it "releases the lock when discarding a DLQ message" do
      # A lock left from the job's enqueue, then its copy in the DLQ
      Pgbus::UniquenessKey.acquire!("dlq-1", queue_name: queue_name, msg_id: 0)
      payload = {
        "job_class" => "DlqJob",
        "arguments" => [1],
        Pgbus::Uniqueness::METADATA_KEY => "dlq-1",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
      }
      ActiveRecord::Base.connection.execute(
        "INSERT INTO pgmq.q_#{full_dlq} (vt, message) VALUES (NOW(), '#{JSON.generate(payload)}')"
      )
      expect(Pgbus::UniquenessKey.locked?("dlq-1")).to be true

      msg = ActiveRecord::Base.connection.select_one("SELECT msg_id FROM pgmq.q_#{full_dlq} LIMIT 1")
      data_source.discard_dlq_message(full_dlq, msg["msg_id"])

      expect(Pgbus::UniquenessKey.locked?("dlq-1")).to be false
    end

    # The executor's own release failed when it dead-lettered the job, so the
    # lock is still bound to the original message. The copy's header names it.
    it "releases the lock still bound to the message the DLQ copy was moved from" do
      Pgbus::UniquenessKey.acquire!("dlq-3", queue_name: queue_name, msg_id: 0)
      original_id = client.send_message(queue_name, unique_payload("dlq-3", job_class: "DlqJob")).to_i
      Pgbus::UniquenessKey.bind!("dlq-3", queue_name: queue_name, msg_id: original_id)
      original = client.read_batch(queue_name, qty: 1, vt: 30).first
      headers = Pgbus::DeadLetterHeader.build(
        existing: nil, reason: Pgbus::DeadLetterHeader::REASON_MAX_RETRIES, source: "worker",
        source_queue: full_queue, source_msg_id: original_id, attempts: 6, max_retries: 5
      )
      client.move_to_dead_letter(queue_name, original, headers: headers)
      dead = ActiveRecord::Base.connection.select_one("SELECT msg_id FROM pgmq.q_#{full_dlq} ORDER BY msg_id DESC LIMIT 1")

      data_source.discard_dlq_message(full_dlq, dead["msg_id"])

      expect(Pgbus::UniquenessKey.locked?("dlq-3")).to be(false)
    end

    # The attempt was redelivered after it had handed its lock to a retry,
    # and then dead-lettered: the retry is still waiting and holds the key.
    it "keeps the lock a waiting retry holds when discarding the attempt's DLQ copy" do
      Pgbus::UniquenessKey.acquire!("dlq-2", queue_name: queue_name, msg_id: 0)
      retry_id = client.send_message(queue_name, unique_payload("dlq-2", job_class: "DlqJob"), delay: 300)
      Pgbus::UniquenessKey.where(lock_key: "dlq-2").update_all(msg_id: retry_id)
      ActiveRecord::Base.connection.execute(
        "INSERT INTO pgmq.q_#{full_dlq} (vt, message) " \
        "VALUES (NOW(), '#{JSON.generate(unique_payload("dlq-2", job_class: "DlqJob"))}')"
      )
      dead = ActiveRecord::Base.connection.select_one("SELECT msg_id FROM pgmq.q_#{full_dlq} ORDER BY msg_id DESC LIMIT 1")

      data_source.discard_dlq_message(full_dlq, dead["msg_id"])

      expect(Pgbus::UniquenessKey.find_by(lock_key: "dlq-2")&.msg_id).to eq(retry_id.to_i)
    end
  end

  describe "#discard_all_dlq clears associated locks" do
    let(:dlq_name) { Pgbus.configuration.dead_letter_queue_name(queue_name) }
    let(:full_dlq) { dlq_name }

    before do
      client.ensure_dead_letter_queue(queue_name)
    end

    it "releases all locks for discarded DLQ messages" do
      3.times do |i|
        payload = {
          "job_class" => "DlqJob",
          "arguments" => [i],
          Pgbus::Uniqueness::METADATA_KEY => "dlq-#{i}",
          Pgbus::Uniqueness::STRATEGY_KEY => "until_executed"
        }
        Pgbus::UniquenessKey.acquire!("dlq-#{i}", queue_name: queue_name, msg_id: 0)
        ActiveRecord::Base.connection.execute(
          "INSERT INTO pgmq.q_#{full_dlq} (vt, message) VALUES (NOW(), '#{JSON.generate(payload)}')"
        )
      end

      expect(Pgbus::UniquenessKey.count).to eq(3)

      data_source.discard_all_dlq

      expect(Pgbus::UniquenessKey.count).to eq(0)
    end
  end
end
