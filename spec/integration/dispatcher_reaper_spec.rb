# frozen_string_literal: true

require_relative "../integration_helper"

# Regression coverage for the dispatcher's uniqueness key reaper.
#
# History: the reaper deleted any uniqueness key whose row was older than
# `visibility_timeout * 2` (60 seconds default), regardless of whether the
# associated message was still in the queue. For a recurring job that failed
# repeatedly and stayed in the queue waiting for retry, this released the
# uniqueness lock, allowing the next scheduler tick to enqueue a fresh copy
# on top of the existing one. After ~5 minutes (the reap interval), a new
# duplicate appeared. Six copies in 29 minutes matched the symptom exactly.
#
# The fix: only reap a lock if the referenced message no longer exists in
# the PGMQ queue. The lock row stores queue_name + msg_id, so this is a
# single EXISTS check per row.
RSpec.describe "Dispatcher uniqueness key reaper (integration)", :integration do
  let(:dispatcher) { Pgbus::Process::Dispatcher.new }
  let(:queue_name) { "default" }

  before do
    Pgbus.client.ensure_queue(queue_name)
  end

  describe "#reap_orphaned_uniqueness_keys" do
    context "when the message is still in the queue" do
      it "does NOT reap the lock even when older than visibility_timeout * 2" do
        msg_id = Pgbus.client.send_message(
          queue_name,
          { "job_class" => "TestJob",
            Pgbus::Uniqueness::METADATA_KEY => "TestJob:still-running" }
        )
        Pgbus::UniquenessKey.acquire!("TestJob:still-running", queue_name: queue_name, msg_id: msg_id)

        # Backdate the lock to simulate a long-running/failing job
        Pgbus::UniquenessKey.where(lock_key: "TestJob:still-running")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(0)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "TestJob:still-running")).to be(true)
      end

      it "does NOT reap msg_id=0 placeholder locks while a queue message exists for the same key" do
        # Recurring scheduler creates locks with msg_id=0 (it doesn't know
        # the msg_id at lock time). The lock should outlive the visibility
        # timeout for as long as ANY message exists carrying the same key
        # in its pgbus_uniqueness_key payload field.
        msg_id = Pgbus.client.send_message(
          queue_name,
          { "job_class" => "RecurringJob",
            Pgbus::Uniqueness::METADATA_KEY => "RecurringJob" }
        )
        Pgbus::UniquenessKey.acquire!("RecurringJob", queue_name: queue_name, msg_id: 0)

        # Backdate
        Pgbus::UniquenessKey.where(lock_key: "RecurringJob")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(0)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "RecurringJob")).to be(true)

        # Cleanup
        Pgbus.client.archive_message(queue_name, msg_id)
      end
    end

    context "when the lock is the synthetic pending/msg_id=0 shape (issue #418)" do
      it "reaps an aged pending lock when no live queue carries the uniqueness key" do
        Pgbus::UniquenessKey.acquire!("ERP::Manager", queue_name: "pending", msg_id: 0)
        Pgbus::UniquenessKey.where(lock_key: "ERP::Manager")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(1)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "ERP::Manager")).to be(false)
      end

      it "does NOT reap an aged pending lock while a message on a real queue still holds the key" do
        # In-flight :until_executed jobs currently look identical to leaked
        # rows (pending/0). The message lives on the real queue (e.g. critical),
        # not on pgmq.q_<prefix>_pending — treating UndefinedTable on that
        # synthetic table as "gone" would unlock a running job.
        Pgbus.client.ensure_queue("critical")
        msg_id = Pgbus.client.send_message(
          "critical",
          { "job_class" => "HeartbeatJob",
            Pgbus::Uniqueness::METADATA_KEY => "ERP::Manager" }
        )
        Pgbus::UniquenessKey.acquire!("ERP::Manager", queue_name: "pending", msg_id: 0)
        Pgbus::UniquenessKey.where(lock_key: "ERP::Manager")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(0)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "ERP::Manager")).to be(true)

        Pgbus.client.archive_message("critical", msg_id)
      end
    end

    context "when the message is gone but the lock remains" do
      it "reaps the lock for a real msg_id when the queue row no longer exists" do
        # Simulate a true orphan: lock exists for a msg_id that's not in the queue
        Pgbus::UniquenessKey.acquire!("Orphan:gone", queue_name: queue_name, msg_id: 999_999_999)
        Pgbus::UniquenessKey.where(lock_key: "Orphan:gone")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(1)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "Orphan:gone")).to be(false)
      end

      it "reaps msg_id=0 placeholder locks when no message exists for that lock_key payload" do
        # Recurring lock with msg_id=0, no corresponding message in any queue
        Pgbus::UniquenessKey.acquire!("RecurringJob:abandoned", queue_name: queue_name, msg_id: 0)
        Pgbus::UniquenessKey.where(lock_key: "RecurringJob:abandoned")
                            .update_all(created_at: 10.minutes.ago)

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(1)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "RecurringJob:abandoned")).to be(false)
      end
    end

    context "when locks are recent" do
      it "does not reap locks newer than the threshold even if the message is gone" do
        Pgbus::UniquenessKey.acquire!("Recent:lock", queue_name: queue_name, msg_id: 999_999_998)
        # created_at is now (default), well within threshold

        reaped = dispatcher.send(:reap_orphaned_uniqueness_keys)

        expect(reaped).to eq(0)
        expect(Pgbus::UniquenessKey.exists?(lock_key: "Recent:lock")).to be(true)
      end
    end

    context "with the original duplicate-enqueue regression" do
      it "does NOT reap the recurring scheduler's lock while the failing job message is in the queue" do
        # This test reproduces the production scenario:
        # 1. Recurring scheduler enqueues a job with uniqueness lock (msg_id=0 placeholder)
        # 2. Job fails repeatedly, message stays in queue with growing read_ct
        # 3. Time passes (> visibility_timeout * 2)
        # 4. Reaper runs — must NOT delete the lock
        # 5. Next scheduler tick must still see :already_locked
        #
        # NOTE: production stores the LOGICAL queue name (e.g. "default") on
        # uniqueness key rows, not the prefixed physical name. The reaper
        # must handle both — that is what Pgbus::Client#message_exists? does.

        Pgbus.client.send_message(
          queue_name,
          { "job_class" => "FetchTransactionsJob",
            Pgbus::Uniqueness::METADATA_KEY => "FetchTransactionsJob" }
        )
        Pgbus::UniquenessKey.acquire!("FetchTransactionsJob", queue_name: queue_name, msg_id: 0)

        # Simulate 10 minutes of failing retries
        Pgbus::UniquenessKey.where(lock_key: "FetchTransactionsJob")
                            .update_all(created_at: 10.minutes.ago)

        # Reaper runs (this is the bug — it used to delete the lock here)
        dispatcher.send(:reap_orphaned_uniqueness_keys)

        # The lock MUST still be present
        expect(Pgbus::UniquenessKey.exists?(lock_key: "FetchTransactionsJob")).to be(true)

        # Sanity check: message_exists? routes via Pgbus::Client (no raw SQL)
        expect(Pgbus.client.message_exists?(queue_name, uniqueness_key: "FetchTransactionsJob")).to be(true)

        # And the next acquire! attempt should fail (lock is held)
        expect(
          Pgbus::UniquenessKey.acquire!("FetchTransactionsJob", queue_name: queue_name, msg_id: 0)
        ).to be(false)
      end
    end
  end
end
