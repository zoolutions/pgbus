# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::QueueSummary do
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }
  let(:name) { "pgbus_default" }
  let(:detail) { { name: name, queue_visible_length: 3, parked_length: 2, oldest_claimable_age_sec: 240 } }
  let(:pause_state) { { paused: false, reason: nil, paused_at: nil, resumes_at: nil, trip_count: 0 } }
  let(:drainers) do
    { logical: "default", capsules: ["default"], wildcard: false, handler: false, stream: false,
      live_workers: 2, live_consumers: 0, siblings: [] }
  end

  def present(detail: self.detail, pause: {}, drain: {})
    described_class.present(detail, pause_state.merge(pause), drainers.merge(drain), max_retries: 5)
  end

  def keys(lines) = lines.map(&:key)
  def line(lines, key) = lines.find { |l| l.key == key }

  describe "pause line" do
    it "is omitted while the queue runs" do
      expect(keys(present)).to eq(%w[drained_by backlog])
    end

    it "comes first for an operator pause, with the reason" do
      lines = present(pause: { paused: true, reason: "maintenance", paused_at: now - 300 })

      expect(lines.first).to have_attributes(key: "paused_operator", tone: :yellow,
                                             args: { ago: now - 300, reason: "maintenance" })
    end

    it "has its own wording for an operator pause without a reason" do
      lines = present(pause: { paused: true, paused_at: now - 300 })

      expect(lines.first).to have_attributes(key: "paused_operator_no_reason", args: { ago: now - 300 })
    end

    it "explains a circuit-breaker pause with the failures, resume time and trip" do
      lines = present(pause: { paused: true, reason: "circuit_breaker: 7 consecutive failures", paused_at: now - 30,
                               resumes_at: now + 90, trip_count: 2 })

      expect(lines.first).to have_attributes(key: "paused_circuit_breaker", tone: :yellow,
                                             args: { ago: now - 30, failures: 7, time: now + 90, trip: 2 })
    end
  end

  describe "drain line" do
    it "names the capsules and the healthy workers" do
      expect(line(present, "drained_by")).to have_attributes(tone: :gray,
                                                             args: { capsules: "default", count: 2 })
    end

    it "joins several capsules" do
      lines = present(drain: { capsules: %w[default bulk] })

      expect(line(lines, "drained_by").args).to include(capsules: "default, bulk")
    end

    it "credits a wildcard capsule" do
      lines = present(drain: { capsules: [], wildcard: true })

      expect(line(lines, "drained_wildcard").args).to eq(count: 2)
    end

    it "warns in red when nothing is configured to drain the queue and no worker listens" do
      lines = present(drain: { capsules: [], live_workers: 0 })

      expect(line(lines, "not_drained")).to have_attributes(tone: :red)
    end

    it "warns in red when no healthy worker runs, with the claimable count" do
      lines = present(drain: { live_workers: 0 })

      expect(line(lines, "no_workers")).to have_attributes(tone: :red, args: { count: 3 })
    end

    # Capsules started with `pgbus --queues …` are not in the web process's
    # configuration; a live heartbeat for the queue is the better evidence.
    it "trusts healthy worker heartbeats when the web process sees no capsule config" do
      lines = present(drain: { capsules: [], live_workers: 2 })

      expect(line(lines, "drained_live")).to have_attributes(tone: :gray, args: { count: 2 })
    end

    it "prefers not_drained over no_workers" do
      expect(keys(present(drain: { capsules: [], live_workers: 0 }))).to eq(%w[not_drained backlog])
    end

    it "explains a handler queue by its consumers" do
      lines = present(drain: { capsules: [], handler: true, live_consumers: 1 })

      expect(line(lines, "handler").args).to eq(count: 1)
    end

    it "warns in red when a handler queue has no healthy consumer" do
      lines = present(drain: { capsules: [], handler: true, live_consumers: 0 })

      expect(line(lines, "no_consumers")).to have_attributes(tone: :red, args: { count: 3 })
    end

    it "explains a stream queue and skips the backlog" do
      expect(keys(present(drain: { capsules: [], stream: true }))).to eq(%w[stream])
    end

    it "explains a dead-letter queue above every drain line and skips the backlog" do
      lines = present(detail: detail.merge(name: "pgbus_default_dlq"), drain: { capsules: [], live_workers: 0 })

      expect(keys(lines)).to eq(%w[dlq])
      expect(lines.first.args).to eq(logical: "default", max: 5)
    end
  end

  describe "backlog line" do
    it "counts claimable and parked jobs and the oldest wait" do
      expect(line(present, "backlog").args).to eq(visible: 3, age: 240, parked: 2)
    end

    it "says only parked jobs remain" do
      lines = present(detail: detail.merge(queue_visible_length: 0))

      expect(line(lines, "backlog_parked_only").args).to eq(parked: 2)
    end

    it "says nothing is waiting" do
      lines = present(detail: detail.merge(queue_visible_length: 0, parked_length: 0))

      expect(keys(lines)).to include("empty")
    end
  end

  describe "priority line" do
    it "names the level and the logical queue of a priority sub-table" do
      lines = present(detail: detail.merge(name: "pgbus_default_p1"), drain: { priority_level: 1 })

      expect(lines.last).to have_attributes(key: "priority_level", tone: :gray, args: { level: 1, logical: "default" })
    end

    it "is omitted for a plain queue" do
      expect(keys(present)).not_to include("priority_level")
    end

    # `_pN` is legal in an ordinary queue name; only the queue strategy knows.
    it "is omitted for a plain queue whose name merely ends in _pN" do
      lines = present(detail: detail.merge(name: "pgbus_mailers_p1"), drain: { priority_level: nil })

      expect(keys(lines)).not_to include("priority_level")
    end
  end
end
