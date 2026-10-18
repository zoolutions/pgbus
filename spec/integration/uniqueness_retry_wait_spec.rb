# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"
require "active_job/queue_adapters/pgbus_adapter"

# An :until_executed job that reschedules itself (retry_job(wait:) or
# retry_on ... wait:) keeps its key for the whole wait (issue #564). The retry
# enqueues through its own held key (#333); the row then has to follow it, or
# the archiving attempt's release deletes it and a fresh enqueue runs beside
# the retry.
RSpec.describe "Uniqueness across a delayed retry (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:executor) { Pgbus::ActiveJob::Executor.new(client: client) }
  let(:key) { "poll-item-1" }
  let(:attempts) { [] }

  before do
    client.ensure_queue("default")
    Pgbus::UniquenessKey.delete_all
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    ActiveJob::Base.queue_adapter = :pgbus
  end

  after do
    ActiveJob::Base.queue_adapter = :test
  end

  def define_job(on_conflict: :discard, concurrency: nil, &perform)
    tracker = attempts
    stub_const("PollItemJob", Class.new(ActiveJob::Base) do
      include Pgbus::Uniqueness
      include Pgbus::Concurrency

      ensures_uniqueness strategy: :until_executed, on_conflict: on_conflict, key: ->(id) { "poll-item-#{id}" }
      limits_concurrency(**concurrency) if concurrency

      define_method(:perform) do |item_id|
        tracker << item_id
        instance_exec(tracker.size, &perform)
      end
    end)
  end

  def read_one
    messages = client.read_batch("default", qty: 5, vt: 30)
    expect(messages.size).to eq(1)
    messages.first
  end

  def lock_row
    Pgbus::UniquenessKey.find_by(lock_key: key)
  end

  # Every message in the queue that carries the key, delayed ones included
  # (a read only returns the visible ones).
  def queued_msg_ids_for_key
    physical = Pgbus.configuration.queue_name("default")
    ActiveRecord::Base.connection.select_values(
      "SELECT msg_id FROM pgmq.q_#{physical} WHERE message->>'pgbus_uniqueness_key' = $1 ORDER BY msg_id",
      "spec", [key]
    ).map(&:to_i)
  end

  def run_retry_to_success(row)
    client.set_visibility_timeout("default", row.msg_id, vt: 0)
    retry_message = read_one
    expect(retry_message.msg_id.to_i).to eq(row.msg_id)
    expect(executor.execute(retry_message, "default")).to eq(:success)
  end

  shared_examples "a key held through the retry's wait" do
    it "points the row at the delayed retry once the first attempt archives" do
      PollItemJob.perform_later(1)
      first = read_one

      expect(executor.execute(first, "default")).to eq(:success)

      row = lock_row
      expect(row).not_to be_nil
      expect(row.msg_id).not_to eq(first.msg_id.to_i)
      expect(client.message_exists?("default", msg_id: row.msg_id, uniqueness_key: key)).to be(true)
    end

    it "discards a fresh enqueue during the wait, then releases the key when the retry succeeds" do
      PollItemJob.perform_later(1)
      executor.execute(read_one, "default")
      row = lock_row

      PollItemJob.perform_later(1)

      expect(queued_msg_ids_for_key).to eq([row.msg_id])

      run_retry_to_success(row)

      expect(attempts).to eq([1, 1])
      expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    end
  end

  context "with retry_job(wait: 30) inside perform" do
    before { define_job { |attempt| retry_job(wait: 30) if attempt == 1 } }

    it_behaves_like "a key held through the retry's wait"
  end

  context "with retry_on ... wait: 30" do
    before do
      stub_const("PollNotReady", Class.new(StandardError))
      define_job { |attempt| raise PollNotReady if attempt == 1 }
      PollItemJob.retry_on PollNotReady, wait: 30, attempts: 3
    end

    it_behaves_like "a key held through the retry's wait"
  end

  context "with on_conflict: :reject" do
    before { define_job(on_conflict: :reject) { |attempt| retry_job(wait: 30) if attempt == 1 } }

    it "raises JobNotUnique for a fresh enqueue during the wait" do
      PollItemJob.perform_later(1)
      executor.execute(read_one, "default")

      expect { PollItemJob.perform_later(1) }.to raise_error(Pgbus::JobNotUnique)
      expect(Pgbus::UniquenessKey.locked?(key)).to be(true)
    end
  end

  # With limits_concurrency to: 1, on_conflict: :block the running attempt
  # holds the slot, so its retry always parks: it has no msg_id until the
  # attempt archives and signals.
  context "when the retry parks behind the attempt's own concurrency slot" do
    before do
      define_job(concurrency: { to: 1, key: ->(id) { "poll-slot-#{id}" }, on_conflict: :block }) do |attempt|
        retry_job(wait: 30) if attempt == 1
      end
    end

    it "keeps the key through the park and the promoted retry's wait, and releases it on the retry's success" do
      PollItemJob.perform_later(1)
      executor.execute(read_one, "default")

      # The attempt archived and signalled: the parked retry was promoted.
      expect(Pgbus::BlockedExecution.count).to eq(0)
      retried = queued_msg_ids_for_key
      expect(retried.size).to eq(1)
      expect(Pgbus::UniquenessKey.locked?(key)).to be(true)

      PollItemJob.perform_later(1)

      expect(Pgbus::BlockedExecution.count).to eq(0)
      expect(queued_msg_ids_for_key).to eq(retried)

      client.set_visibility_timeout("default", retried.first, vt: 0)
      expect(executor.execute(read_one, "default")).to eq(:success)

      expect(attempts).to eq([1, 1])
      expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    end
  end

  # A parked job is promoted without a bind, so the first attempt runs
  # holding an unbound row; its sent retry has to take that row too.
  context "when the first attempt was parked and promoted, so it runs holding an unbound row" do
    let(:slot) { "poll-slot-1" }

    before do
      define_job(concurrency: { to: 2, key: ->(id) { "poll-slot-#{id}" }, on_conflict: :block }) do |attempt|
        retry_job(wait: 30) if attempt == 1
      end
    end

    it "moves that row onto the sent retry and keeps the key through the retry's wait" do
      2.times { Pgbus::Concurrency::Semaphore.acquire(slot, 2, 900) }
      PollItemJob.perform_later(1)
      expect(Pgbus::BlockedExecution.count).to eq(1)
      expect(lock_row.msg_id).to eq(0)

      Pgbus::Concurrency::Semaphore.signal(slot, client: client) # promotes the parked first attempt
      Pgbus::Concurrency::Semaphore.release(slot) # room for its retry
      expect(executor.execute(read_one, "default")).to eq(:success)

      row = lock_row
      expect(row).not_to be_nil
      expect(queued_msg_ids_for_key).to eq([row.msg_id])

      PollItemJob.perform_later(1)

      expect(Pgbus::BlockedExecution.count).to eq(0)
      expect(queued_msg_ids_for_key).to eq([row.msg_id])

      run_retry_to_success(row)

      expect(attempts).to eq([1, 1])
      expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    end
  end

  # The retry is sent before the row is bound to it; with no wait, another
  # worker can run it to the end in between. Its release must still take the
  # row, and the late bind must not bring it back.
  context "when the retry finishes before the row is bound to it" do
    before { define_job { |attempt| retry_job if attempt == 1 } }

    it "releases the key and leaves nothing behind" do
      raced = false
      allow(Pgbus::UniquenessKey).to receive(:bind!).and_wrap_original do |original, lock_key, **kwargs|
        if kwargs[:acquired_at] && !raced
          raced = true
          expect(executor.execute(read_one, "default")).to eq(:success)
        end
        original.call(lock_key, **kwargs)
      end
      PollItemJob.perform_later(1)

      expect(executor.execute(read_one, "default")).to eq(:success)

      expect(raced).to be(true)
      expect(attempts).to eq([1, 1])
      expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
    end
  end

  describe "a successor that took the key while an older message ran" do
    let(:successor_msg_id) { 9_999_999 }

    def take_key_as_successor
      Pgbus::UniquenessKey.release!(key)
      Pgbus::UniquenessKey.acquire!(key, queue_name: "default", msg_id: successor_msg_id)
    end

    it "keeps the successor's row when the older message succeeds" do
      take_key = method(:take_key_as_successor)
      define_job { |_attempt| take_key.call }
      PollItemJob.perform_later(1)

      expect(executor.execute(read_one, "default")).to eq(:success)

      expect(lock_row&.msg_id).to eq(successor_msg_id)
    end

    it "keeps the successor's row when the older message dead-letters" do
      define_job { |_attempt| nil }
      PollItemJob.perform_later(1)
      exhausted = read_one.with(read_ct: Pgbus.configuration.max_retries + 1)
      take_key_as_successor

      expect(executor.execute(exhausted, "default")).to eq(:dead_lettered)

      expect(lock_row&.msg_id).to eq(successor_msg_id)
    end

    it "keeps a successor's unbound row acquired after the older message was sent" do
      define_job { |_attempt| nil }
      PollItemJob.perform_later(1)
      Pgbus::UniquenessKey.release!(key)
      Pgbus::UniquenessKey.acquire!(key, queue_name: "default", msg_id: 0)

      expect(executor.execute(read_one, "default")).to eq(:success)

      expect(lock_row&.msg_id).to eq(0)
    end
  end

  it "keeps the #333 behaviour: a retry_on wait: 0 retry runs once to success and the key is cleaned up" do
    stub_const("PollNotReady", Class.new(StandardError))
    define_job { |attempt| raise PollNotReady if attempt == 1 }
    PollItemJob.retry_on PollNotReady, wait: 0, attempts: 3

    PollItemJob.perform_later(1)
    3.times do
      messages = client.read_batch("default", qty: 5, vt: 30)
      break if messages.empty?

      messages.each { |m| executor.execute(m, "default") }
    end

    expect(attempts).to eq([1, 1])
    expect(Pgbus::UniquenessKey.locked?(key)).to be(false)
  end
end
