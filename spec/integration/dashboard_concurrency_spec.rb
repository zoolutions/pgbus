# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# The Locks page's Concurrency section against the real tables: the FULL OUTER
# join's row shape, and the two guarded actions. Release promotes through
# Concurrency::BlockedExecution.promote_next, so the limit still holds; discard
# resolves the bookkeeping a parked job will never resolve itself.
RSpec.describe "Dashboard concurrency section (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:key) { "dashboard-concurrency-#{SecureRandom.hex(4)}" }

  before do
    client.ensure_queue("default")
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    Pgbus::UniquenessKey.delete_all
  end

  def payload(job_id: SecureRandom.uuid, extra: {})
    { "job_class" => "ConcurrencyVisibilityJob", "job_id" => job_id, "queue_name" => "default",
      "arguments" => [], Pgbus::Concurrency::METADATA_KEY => key }.merge(extra)
  end

  def park(extra: {}, job_id: SecureRandom.uuid)
    Pgbus::Concurrency::BlockedExecution.insert(
      concurrency_key: key, queue_name: "default",
      payload: payload(job_id: job_id, extra: extra), duration: 900
    )
  end

  def row_for(stats, wanted = key)
    stats[:keys].find { |k| k[:key] == wanted }
  end

  describe "#concurrency_stats" do
    it "reports a key at its limit with its parked jobs" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      2.times { park }

      stats = data_source.concurrency_stats

      expect(stats).to include(parked_total: 2, slots_held: 1, keys_at_limit: 1)
      expect(stats[:oldest_parked_age_sec]).to be >= 0
      expect(row_for(stats)).to include(value: 1, max_value: 1, parked_count: 2, lease_fresh: true)
      expect(row_for(stats)[:oldest_parked_age_sec]).to be >= 0
    end

    it "still lists a key whose semaphore is gone but whose jobs are parked" do
      park

      stats = data_source.concurrency_stats

      row = row_for(stats)
      expect(row).not_to be_nil
      expect(row).to include(value: nil, max_value: nil, parked_count: 1, lease_fresh: false)
    end

    it "lists a held key with nothing parked" do
      Pgbus::Concurrency::Semaphore.acquire(key, 3, 900)

      row = row_for(data_source.concurrency_stats)

      expect(row).to include(value: 1, max_value: 3, parked_count: 0, oldest_parked_age_sec: nil)
      expect(data_source.concurrency_stats[:keys_at_limit]).to eq(0)
    end

    it "marks an expired lease as stale" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, -60)

      expect(row_for(data_source.concurrency_stats)[:lease_fresh]).to be(false)
    end

    it "returns zeros and no keys on an empty install" do
      expect(data_source.concurrency_stats).to eq(
        parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0, keys: []
      )
    end

    it "merges the aggregates into summary_stats" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      park

      expect(data_source.summary_stats).to include(parked_total: 1, slots_held: 1, keys_at_limit: 1)
    end
  end

  describe "#release_concurrency_key" do
    it "promotes one parked job onto the queue and leaves the key at its limit" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      promoted_id = SecureRandom.uuid
      park(job_id: promoted_id)
      park

      expect(data_source.release_concurrency_key(key)).to eq(1)

      expect(Pgbus::BlockedExecution.where(concurrency_key: key).count).to eq(1)
      expect(Pgbus::Concurrency::Semaphore.current_value(key)).to eq(1)
      job_ids = Array(client.read_batch("default", qty: 10, vt: 0)).map { |m| JSON.parse(m.message)["job_id"] }
      expect(job_ids).to eq([promoted_id])
    end

    it "empties a stranded semaphore and leaves it for the sweep" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)

      expect(data_source.release_concurrency_key(key)).to eq(0)

      # Zeroed, not deleted: the row is where the limit is recorded, and the
      # dispatcher's expired-semaphore sweep reaps a value <= 0 row.
      expect(Pgbus::Semaphore.find_by(key: key).value).to eq(0)
      expect(Pgbus::Semaphore.expired).to include(Pgbus::Semaphore.find_by(key: key))
    end

    it "keeps the key's recorded limit so a payload with no resolvable class is judged against it" do
      # limit 3 recorded by the holder; the parked payload names a class that no
      # longer exists, so promotion has no limit of its own to use and must fall
      # back to the row's max_value rather than a guessed 1.
      Pgbus::Concurrency::Semaphore.acquire(key, 3, 900)
      3.times { park(extra: { "job_class" => "LongGoneJob" }) }

      promoted = data_source.release_concurrency_key(key)

      expect(Pgbus::Semaphore.find_by(key: key).max_value).to eq(3)
      expect(promoted).to eq(3)
      expect(Pgbus::Concurrency::Semaphore.current_value(key)).to eq(3)
    end
  end

  describe "#discard_parked_jobs" do
    it "deletes the parked rows and leaves the semaphore alone" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      3.times { park }

      expect(data_source.discard_parked_jobs(key)).to eq(3)

      expect(Pgbus::BlockedExecution.where(concurrency_key: key).count).to eq(0)
      expect(Pgbus::Concurrency::Semaphore.current_value(key)).to eq(1)
      expect(client.read_batch("default", qty: 10, vt: 0)).to be_empty
    end

    it "resolves a parked batch child as failed so the batch stops waiting" do
      batch_id = SecureRandom.uuid
      job_id = SecureRandom.uuid
      # A parked batch job: a batch row still waiting on it, and an execution
      # row with no msg_id (the adapter parked it instead of sending).
      Pgbus::BatchEntry.create!(batch_id: batch_id, total_jobs: 1, status: "processing")
      Pgbus::BatchExecution.create!(batch_id: batch_id, job_id: job_id, msg_id: nil, queue_name: "default")
      park(job_id: job_id, extra: { Pgbus::Batch::METADATA_KEY => batch_id })

      data_source.discard_parked_jobs(key)

      record = Pgbus::BatchEntry.find_by(batch_id: batch_id)
      expect(record.failed_jobs).to eq(1)
      expect(record.status).to eq("finished")
      expect(Pgbus::BatchExecution.where(batch_id: batch_id).count).to eq(0)
    end

    it "frees the uniqueness key a parked until_executed job still holds" do
      Pgbus::UniquenessKey.acquire!("unique-#{key}", queue_name: "default", msg_id: 0)
      park(extra: { Pgbus::Uniqueness::METADATA_KEY => "unique-#{key}",
                    Pgbus::Uniqueness::STRATEGY_KEY => "until_executed" })

      data_source.discard_parked_jobs(key)

      expect(Pgbus::UniquenessKey.locked?("unique-#{key}")).to be(false)
    end

    it "leaves a successor's bound lock alone when the reaper already took the parked job's" do
      # The reaper removed the parked job's own (unbound) lock, then a successor
      # acquired the same key and was actually sent, binding its lock to a live
      # msg_id. Discarding the parked job must not drop that — doing so would
      # admit a duplicate beside the running successor.
      lock_key = "unique-#{key}"
      Pgbus::UniquenessKey.acquire!(lock_key, queue_name: "default", msg_id: 4242)
      park(extra: { Pgbus::Uniqueness::METADATA_KEY => lock_key,
                    Pgbus::Uniqueness::STRATEGY_KEY => "until_executed" })

      data_source.discard_parked_jobs(key)

      expect(Pgbus::UniquenessKey.locked?(lock_key)).to be(true)
    end

    it "returns 0 when nothing is parked" do
      expect(data_source.discard_parked_jobs(key)).to eq(0)
    end
  end
end
