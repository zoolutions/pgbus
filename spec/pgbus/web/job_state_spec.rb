# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::JobState do
  let(:now) { Time.utc(2026, 10, 9, 12, 0, 0) }
  let(:paused) { Set.new }
  let(:drained) { Set["pgbus_default"] }
  let(:workers_alive) { true }
  let(:consumers_alive) { true }
  let(:context) do
    described_class::Context.new(now: now, max_retries: 5, paused: paused, drained: drained,
                                 workers_alive: workers_alive, handler_queues: Set["pgbus_events"],
                                 consumers_alive: consumers_alive)
  end

  def queue_row(**attrs)
    { source: "queue", queue_name: "pgbus_default", logical_queue: "default", msg_id: 42,
      read_ct: 0, enqueued_at: now - 60, last_read_at: nil, vt: now - 1 }.merge(attrs)
  end

  def present(row, ahead: nil)
    described_class.present(row, context, ahead: ahead)
  end

  describe "scheduled" do
    it "runs at its visibility time" do
      result = present(queue_row(state: "scheduled", vt: now + 7200))

      expect(result).to have_attributes(state: "scheduled", reason_key: "scheduled",
                                        next_run_at: now + 7200, badge_tone: :gray)
      expect(result.reason_args).to eq(time: now + 7200)
    end
  end

  describe "ready" do
    it "says how many jobs are ahead" do
      result = present(queue_row(state: "ready"), ahead: 3)

      expect(result).to have_attributes(state: "ready", reason_key: "waiting", badge_tone: :blue)
      expect(result.reason_args).to eq(count: 3)
    end

    it "caps the ahead count at 10k+" do
      result = present(queue_row(state: "ready"), ahead: described_class::AHEAD_CAP)

      expect(result.reason_args).to eq(count: "10k+")
    end

    it "labels a priority sub-table count as within its priority" do
      row = queue_row(state: "ready", queue_name: "pgbus_default_p1")
      context_with_p1 = described_class::Context.new(now: now, max_retries: 5, paused: paused,
                                                     drained: Set["pgbus_default_p1"], workers_alive: true,
                                                     handler_queues: Set.new, consumers_alive: true)

      result = described_class.present(row, context_with_p1, ahead: 2)

      expect(result.reason_key).to eq("waiting_priority")
    end

    it "falls back to a plain waiting reason when no ahead count is known" do
      expect(present(queue_row(state: "ready")).reason_key).to eq("waiting_unknown")
    end

    it "explains a redelivery after an expired lease" do
      result = present(queue_row(state: "ready", read_ct: 2, last_read_at: now - 120), ahead: 0)

      expect(result.reason_key).to eq("lease_expired")
      expect(result.reason_args).to eq(attempt: 3)
    end

    context "with overlays in precedence order" do
      let(:paused) { Set["default"] }
      let(:drained) { Set["pgbus_other"] }
      let(:workers_alive) { false }

      it "reports a paused queue first" do
        expect(present(queue_row(state: "ready"), ahead: 1).reason_key).to eq("paused")
      end

      it "reports an undrained queue before missing workers" do
        paused.clear

        expect(present(queue_row(state: "ready"), ahead: 1).reason_key).to eq("not_drained")
      end

      it "reports missing workers once the queue is drained" do
        paused.clear
        drained << "pgbus_default"

        expect(present(queue_row(state: "ready"), ahead: 1).reason_key).to eq("no_workers")
      end

      it "overlays a lease-expired redelivery too" do
        expect(present(queue_row(state: "ready", read_ct: 1), ahead: 1).reason_key).to eq("paused")
      end
    end

    context "with an event-handler queue" do
      let(:drained) { Set["pgbus_default", "pgbus_events"] }
      let(:handler_row) { queue_row(state: "ready", queue_name: "pgbus_events", logical_queue: "events") }

      context "when only consumers are alive" do
        let(:workers_alive) { false }

        it "does not blame missing workers for a queue consumers drain" do
          expect(present(handler_row, ahead: 0).reason_key).to eq("waiting")
        end

        it "still blames missing workers for a job queue" do
          expect(present(queue_row(state: "ready"), ahead: 0).reason_key).to eq("no_workers")
        end
      end

      context "when no consumer is alive" do
        let(:consumers_alive) { false }

        it "reports the missing consumer as no healthy worker" do
          expect(present(handler_row, ahead: 0).reason_key).to eq("no_workers")
        end
      end
    end

    context "when a wildcard capsule drains every queue" do
      let(:drained) { nil }

      it "treats the queue as drained" do
        expect(present(queue_row(state: "ready"), ahead: 0).reason_key).to eq("waiting")
      end
    end
  end

  describe "running" do
    it "says when it was claimed and when the lease expires" do
      result = present(queue_row(state: "running", read_ct: 1, last_read_at: now - 12, vt: now + 48))

      expect(result).to have_attributes(state: "running", reason_key: "running", badge_tone: :indigo,
                                        next_run_at: nil)
      expect(result.reason_args).to eq(ago: now - 12, time: now + 48)
    end
  end

  describe "retrying" do
    let(:row) do
      queue_row(state: "retrying", read_ct: 2, last_read_at: now - 30, vt: now + 40,
                error_class: "Net::ReadTimeout", failed_event_id: 7)
    end

    it "names the failed attempt, the error and the next attempt" do
      result = present(row)

      expect(result).to have_attributes(state: "retrying", reason_key: "retrying", badge_tone: :yellow,
                                        next_run_at: now + 40)
      expect(result.reason_args).to eq(attempt: 2, max: 5, error: "Net::ReadTimeout", time: now + 40)
    end

    it "says the retry is due now once the backoff has passed" do
      result = present(row.merge(vt: now - 5))

      expect(result.reason_key).to eq("retrying_due")
      expect(result.reason_args).to eq(attempt: 2, max: 5, error: "Net::ReadTimeout")
    end

    it "warns that the next read dead-letters the job at max_retries" do
      result = present(row.merge(read_ct: 5))

      expect(result.reason_key).to eq("retrying_dlq")
      expect(result.reason_args).to eq(attempt: 5, max: 5, error: "Net::ReadTimeout")
    end

    it "reports an orphaned failed row whose message left the queue" do
      orphan = { source: "failed", state: "retrying", queue_name: "default", logical_queue: "default",
                 msg_id: 9, failed_event_id: 3, error_class: "RuntimeError", read_ct: nil, vt: nil }

      result = present(orphan)

      expect(result).to have_attributes(state: "retrying", reason_key: "orphaned", next_run_at: nil)
      expect(result.reason_args).to eq(error: "RuntimeError")
    end
  end

  describe "blocked" do
    let(:row) do
      { source: "blocked", state: "blocked", queue_name: "default", logical_queue: "default",
        concurrency_key: "Import:42", slots_held: 2, slots_max: 2, enqueued_at: now - 300 }
    end

    it "names the concurrency key, its slot usage and how long it has been parked" do
      result = present(row)

      expect(result).to have_attributes(state: "blocked", reason_key: "blocked", badge_tone: :purple,
                                        next_run_at: nil)
      expect(result.reason_args).to eq(key: "Import:42", held: 2, limit: 2, ago: now - 300)
    end

    it "omits slot usage when the key has no semaphore row" do
      result = present(row.merge(slots_held: nil, slots_max: nil))

      expect(result.reason_key).to eq("blocked_no_slots")
      expect(result.reason_args).to eq(key: "Import:42", ago: now - 300)
    end
  end

  it "parses string timestamps from the database" do
    result = present(queue_row(state: "scheduled", vt: "2026-10-09 14:00:00+00"))

    expect(result.next_run_at).to eq(Time.utc(2026, 10, 9, 14))
  end
end
