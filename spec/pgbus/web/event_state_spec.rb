# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::EventState do
  let(:now) { Time.utc(2026, 10, 9, 12, 0, 0) }
  let(:paused) { Set.new }
  let(:consumers_alive) { true }
  let(:covered_queues) { nil }
  let(:jobs) do
    Pgbus::Web::JobState::Context.new(now: now, max_retries: 5, paused: paused, drained: Set["pgbus_orders_handler"],
                                      workers_alive: false, handler_queues: Set["pgbus_orders_handler"],
                                      consumers_alive: consumers_alive)
  end
  let(:context) { described_class::Context.new(jobs: jobs, covered_queues: covered_queues, claim_window: 60) }

  def event_row(**attrs)
    { source: "queue", queue_name: "pgbus_orders_handler", logical_queue: "orders_handler", msg_id: 42,
      read_ct: 0, enqueued_at: now - 60, last_read_at: nil, vt: now - 1,
      handler_class: "OrderHandler", pattern: "orders.#" }.merge(attrs)
  end

  def present(row, ahead: nil)
    described_class.present(row, context, ahead: ahead)
  end

  describe "scheduled" do
    it "runs at its visibility time" do
      result = present(event_row(state: "scheduled", vt: now + 600))

      expect(result).to have_attributes(state: "scheduled", reason_key: "scheduled", next_run_at: now + 600,
                                        badge_tone: :gray, handler_class: "OrderHandler")
      expect(result.reason_args).to eq(time: now + 600, handler: "OrderHandler")
    end
  end

  describe "ready" do
    it "names the handler and how many events are ahead" do
      result = present(event_row(state: "ready"), ahead: 3)

      expect(result).to have_attributes(state: "ready", reason_key: "waiting", badge_tone: :blue)
      expect(result.reason_args).to eq(count: 3, handler: "OrderHandler")
    end

    it "waits to be picked up when the ahead count is unknown" do
      expect(present(event_row(state: "ready")).reason_key).to eq("waiting_unknown")
    end

    it "says a paused queue first" do
      paused << "orders_handler"

      expect(present(event_row(state: "ready"), ahead: 0).reason_key).to eq("paused")
    end

    context "when no running consumer subscribes to the pattern" do
      let(:covered_queues) { Set["pgbus_other_handler"] }

      it "names the pattern and the handler" do
        result = present(event_row(state: "ready"), ahead: 0)

        expect(result.reason_key).to eq("no_consumer_for_queue")
        expect(result.reason_args).to eq(pattern: "orders.#", handler: "OrderHandler")
      end

      it "outranks a redelivery after an expired lease" do
        expect(present(event_row(state: "ready", read_ct: 2)).reason_key).to eq("no_consumer_for_queue")
      end

      it "still says a paused queue first" do
        paused << "orders_handler"

        expect(present(event_row(state: "ready"), ahead: 0).reason_key).to eq("paused")
      end
    end

    context "when no consumer is healthy and coverage is unknown" do
      let(:consumers_alive) { false }

      it "says no healthy consumer runs" do
        result = present(event_row(state: "ready"), ahead: 0)

        expect(result.reason_key).to eq("no_consumers")
        expect(result.reason_args).to eq(handler: "OrderHandler")
      end
    end

    it "says the consumer likely died when a delivered event is ready again" do
      result = present(event_row(state: "ready", read_ct: 2))

      expect(result.reason_key).to eq("lease_expired")
      expect(result.reason_args).to eq(attempt: 3, handler: "OrderHandler")
    end
  end

  describe "running" do
    it "is being handled, with the claim age and the lease expiry" do
      result = present(event_row(state: "running", read_ct: 1, last_read_at: now - 12, vt: now + 18))

      expect(result).to have_attributes(state: "running", reason_key: "handling", badge_tone: :indigo)
      expect(result.reason_args).to eq(ago: now - 12, time: now + 18, handler: "OrderHandler")
    end
  end

  describe "retrying" do
    let(:failed) { { state: "retrying", read_ct: 2, last_read_at: now - 30, error_class: "Net::ReadTimeout" } }

    it "says which attempt failed with which error and when the next one is due" do
      result = present(event_row(**failed, vt: now + 40))

      expect(result).to have_attributes(reason_key: "retrying", next_run_at: now + 40, badge_tone: :yellow)
      expect(result.reason_args).to eq(attempt: 2, max: 5, error: "Net::ReadTimeout", time: now + 40,
                                       handler: "OrderHandler")
    end

    it "is due now once the visibility time has passed" do
      expect(present(event_row(**failed, vt: now - 5)).reason_key).to eq("retrying_due")
    end

    it "says the next read dead-letters it at max retries" do
      expect(present(event_row(**failed, read_ct: 5, vt: now + 40)).reason_key).to eq("retrying_dlq")
    end

    it "says the message is gone for an orphaned failed row" do
      result = present(event_row(source: "failed", state: "retrying", error_class: "KeyError", vt: nil))

      expect(result.reason_key).to eq("orphaned")
      expect(result.reason_args).to eq(error: "KeyError", handler: "OrderHandler")
    end
  end

  it "falls back to the queue name when no subscriber owns the queue any more" do
    result = present(event_row(state: "ready", handler_class: nil, pattern: nil), ahead: 1)

    expect(result.reason_args).to eq(count: 1, handler: "pgbus_orders_handler")
  end

  describe ".processed" do
    def processed(row) = described_class.processed(row, now: now, claim_window: 60)

    it "is completed when the claim has a completion stamp" do
      result = processed("processed_at" => now - 300, "completed_at" => now - 299)

      expect(result).to have_attributes(state: "completed", reason_key: "completed", badge_tone: :green)
      expect(result.reason_args).to eq(ago: now - 299)
    end

    it "is being handled while the pending claim was refreshed inside the window" do
      result = processed("processed_at" => (now - 3).iso8601, "completed_at" => nil)

      expect(result).to have_attributes(state: "handling", reason_key: "handling", badge_tone: :indigo)
      expect(result.reason_args).to eq(ago: now - 3)
    end

    it "went silent once the pending claim is older than the window" do
      result = processed("processed_at" => now - 600, "completed_at" => nil)

      expect(result).to have_attributes(state: "abandoned", reason_key: "abandoned", badge_tone: :yellow)
      expect(result.reason_args).to eq(ago: now - 600)
    end

    it "degrades to a single-phase record on a legacy schema without completed_at" do
      result = processed("processed_at" => now - 120)

      expect(result).to have_attributes(state: "completed", reason_key: "completed_legacy", badge_tone: :green)
      expect(result.reason_args).to eq(ago: now - 120)
    end
  end
end
