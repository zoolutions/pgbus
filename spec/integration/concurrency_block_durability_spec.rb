# frozen_string_literal: true

require_relative "../integration_helper"
require "active_job"

# `limits_concurrency ... on_conflict: :block` is the one way to constrain a
# job WITHOUT losing it. These examples pin the guarantees that make that
# true, mirroring the fixes solid_queue shipped for the same bug class
# (rails/solid_queue#712 enqueue/unblock race, #761 lock leaks → duplicates):
#
#   * a parked job is never dropped, however long it waits;
#   * a parked job is never promoted beside the job holding its slot;
#   * a job parked while the holder is releasing is promoted, not stranded;
#   * a promoted job always holds a tracked semaphore slot;
#   * a running job keeps its semaphore alive for as long as it runs.
RSpec.describe "Concurrency :block durability (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:key) { "durability-#{SecureRandom.hex(4)}" }
  let(:job_id) { SecureRandom.uuid }
  let(:payload) do
    { "job_class" => "DurabilityJob", "job_id" => job_id, "queue_name" => "default", "arguments" => [],
      Pgbus::Concurrency::METADATA_KEY => key }
  end

  before do
    client.ensure_queue("default")
    Pgbus::Semaphore.delete_all
    Pgbus::BlockedExecution.delete_all
    stub_const("DurabilityJob", Class.new(ActiveJob::Base) do
      include Pgbus::Concurrency

      limits_concurrency to: 1, key: ->(*) { "unused" }, duration: 120
    end)
  end

  def park(duration: 900, job_id: self.job_id)
    Pgbus::Concurrency::BlockedExecution.insert(concurrency_key: key, queue_name: "default",
                                                payload: payload.merge("job_id" => job_id), duration: duration)
  end

  def parked_count
    Pgbus::BlockedExecution.where(concurrency_key: key).count
  end

  def queued_job_ids
    Array(client.read_batch("default", qty: 20, vt: 0)).map { |m| JSON.parse(m.message)["job_id"] }
  end

  def value
    Pgbus::Concurrency::Semaphore.current_value(key)
  end

  describe "a parked job outliving its duration" do
    it "is promoted when the holder signals, not dropped" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      park(duration: -1)

      Pgbus::Concurrency::Semaphore.signal(key, client: client)

      expect(parked_count).to eq(0)
      expect(queued_job_ids).to include(job_id)
      expect(value).to eq(1)
    end

    it "survives the dispatcher sweep while its slot is still held" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      park(duration: -1)

      Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)

      expect(parked_count).to eq(1)
      expect(queued_job_ids).not_to include(job_id)
    end
  end

  describe "the limit" do
    it "is never exceeded by a promotion" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      park

      expect(Pgbus::Concurrency::BlockedExecution.promote_next(key, client: client)).to be(false)
      expect(parked_count).to eq(1)
      expect(value).to eq(1)
    end

    it "gives a job promoted after its semaphore was swept a tracked slot" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, -1)
      park
      expect(Pgbus::Concurrency::Semaphore.expire_stale.map { |r| r["key"] }).to include(key)

      expect(Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)).to eq(1)

      expect(parked_count).to eq(0)
      expect(queued_job_ids).to include(job_id)
      expect(value).to eq(1)
    end

    it "promotes up to the free slots and no further in one sweep" do
      stub_const("DurabilityJob", Class.new(ActiveJob::Base) do
        include Pgbus::Concurrency

        limits_concurrency to: 2, key: ->(*) { "unused" }
      end)
      Pgbus::Concurrency::Semaphore.acquire(key, 2, 900)
      3.times { |i| park(job_id: "#{job_id}-#{i}") }

      expect(Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)).to eq(1)

      expect(parked_count).to eq(2)
      expect(value).to eq(2)
    end
  end

  describe "the sweep's key scan" do
    # pending_keys was capped at 1000 keys ordered by age. A thousand keys
    # whose slots are held sat at the head of that window forever, so a key
    # behind them whose holder had died was never serviced.
    it "skips keys whose slots are all held, so a promotable key behind them is still serviced" do
      held = "held-#{SecureRandom.hex(4)}"
      Pgbus::Concurrency::Semaphore.acquire(held, 1, 900)
      Pgbus::Concurrency::BlockedExecution.insert(concurrency_key: held, queue_name: "default",
                                                  payload: payload.merge("job_id" => "starved"), duration: 900)
      park(job_id: "promotable")

      expect(Pgbus::BlockedExecution.promotable_keys).to eq([key])

      expect(Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)).to eq(1)
      expect(queued_job_ids).to include("promotable")
      expect(Pgbus::BlockedExecution.where(concurrency_key: held).count).to eq(1)
    end
  end

  describe "a parked job whose class no longer resolves" do
    # Forcing limit 1 for an unresolved class refused every promotion while
    # a `to: 3` semaphore still held 2 slots, so the parked jobs could never
    # reach the executor — which is what dead-letters a missing class.
    it "is promoted against the limit the semaphore row already records" do
      Pgbus::Concurrency::Semaphore.acquire(key, 3, 900)
      Pgbus::Concurrency::Semaphore.acquire(key, 3, 900)
      Pgbus::Concurrency::BlockedExecution.insert(
        concurrency_key: key, queue_name: "default",
        payload: payload.merge("job_class" => "NoSuchJobAnyMore", "job_id" => "orphan"), duration: 900
      )

      expect(Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)).to eq(1)

      expect(queued_job_ids).to include("orphan")
      expect(value).to eq(3)
      expect(Pgbus::Semaphore.find_by(key: key).max_value).to eq(3)
    end
  end

  describe "a job parked while the holder is releasing (rails/solid_queue#712)" do
    it "is promoted by that release instead of being stranded" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      parked = Concurrent::Event.new

      enqueuer = Thread.new do
        Pgbus::BusRecord.connection_pool.with_connection do
          # The adapter's enqueue transaction: see the semaphore full, park,
          # commit. The upsert holds the semaphore row lock until commit.
          Pgbus::Semaphore.transaction do
            expect(Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)).to eq(:blocked)
            park
            parked.set
            sleep 0.8
          end
        end
      end

      parked.wait(5)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      Pgbus::Concurrency::Semaphore.signal(key, client: client)
      elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
      enqueuer.join(5)

      expect(elapsed).to be >= 0.5
      expect(parked_count).to eq(0)
      expect(queued_job_ids).to include(job_id)
      expect(value).to eq(1)
    end
  end

  # The parked payload was handed to the jsonb column already serialized, so
  # ActiveRecord encoded it a second time: every row was a jsonb *string*.
  # Promotion still worked because the client passes a String body through,
  # but everything that reads the payload as a document — the job-class
  # lookup, `scheduled_at`, the batch backfill, the batch sweep's
  # `payload->>'job_id'` — saw a string and silently did the wrong thing.
  describe "the parked payload" do
    it "is stored as a JSON object, not a JSON string" do
      park

      expect(Pgbus::BlockedExecution.pluck(Arel.sql("jsonb_typeof(payload)"))).to eq(["object"])
      expect(Pgbus::BlockedExecution.pluck(Arel.sql("payload->>'job_id'"))).to eq([job_id])
      expect(Pgbus::BlockedExecution.release_next!(key)[:payload]).to include("job_class" => "DurabilityJob")
    end

    it "keeps a scheduled job's delay when it is promoted" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 900)
      park(job_id: "later").tap do
        Pgbus::BlockedExecution.where(concurrency_key: key)
                               .update_all(["payload = payload || ?::jsonb", { scheduled_at: (Time.current + 120).iso8601 }.to_json])
      end

      Pgbus::Concurrency::Semaphore.signal(key, client: client)

      expect(parked_count).to eq(0)
      expect(queued_job_ids).not_to include("later")
    end

    it "heals rows a previous release double-encoded and promotes them" do
      legacy = JSON.generate(payload.merge("job_id" => "legacy"))
      Pgbus::BlockedExecution.connection.exec_query(
        "INSERT INTO pgbus_blocked_executions (concurrency_key, queue_name, payload, priority, expires_at) " \
        "VALUES ($1, 'default', to_jsonb($2::text), 0, now())", "legacy", [key, legacy]
      )
      expect(Pgbus::BlockedExecution.pluck(Arel.sql("jsonb_typeof(payload)"))).to eq(["string"])

      expect(Pgbus::Concurrency::BlockedExecution.promote_pending(client: client)).to eq(1)

      expect(queued_job_ids).to include("legacy")
      expect(value).to eq(1)
    end
  end

  describe "a running job" do
    it "keeps its semaphore alive through the heartbeat touch" do
      Pgbus::Concurrency::Semaphore.acquire(key, 1, 1)

      Pgbus::Concurrency::Semaphore.touch(key, 600)

      expect(Pgbus::Semaphore.find_by(key: key).expires_at).to be > Time.current + 500
      sleep 1.1
      expect(Pgbus::Concurrency::Semaphore.expire_stale.map { |r| r["key"] }).not_to include(key)
    end
  end
end
