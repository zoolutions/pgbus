# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowTimeline do
  let(:now) { Time.utc(2026, 10, 17, 12, 0, 0) }

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    I18n.load_path |= Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.backend.reload!
  end

  around { |example| I18n.with_locale(:en) { example.run } }

  def step(**attrs)
    { step_name: "capture", attempt: 1, status: "completed", outcome: "completed", created_at: now - 600,
      scheduled_at: now - 500, started_at: now - 480, finished_at: now - 400, waiting_since: nil, batch_id: nil,
      error_class: nil }.merge(attrs)
  end

  def kinds(segments) = segments.map { |s| [s[:kind], s[:to] - s[:from]] }

  describe ".segments" do
    it "splits a finished attempt into waited, queued and ran" do
      expect(kinds(described_class.segments(step, now: now))).to eq([[:waited, 100], [:queued, 20], [:ran, 80]])
    end

    it "runs an in-progress attempt up to now" do
      expect(kinds(described_class.segments(step(status: "in_progress", finished_at: nil), now: now)).last).to eq([:ran, 480])
    end

    it "queues a due attempt that never started up to now" do
      segments = described_class.segments(step(status: "scheduled", started_at: nil, finished_at: nil), now: now)

      expect(kinds(segments)).to eq([[:waited, 100], [:queued, 500]])
    end

    it "keeps the planned wait of an attempt scheduled in the future" do
      segments = described_class.segments(step(status: "scheduled", scheduled_at: now + 3600, started_at: nil, finished_at: nil),
                                          now: now)

      expect(kinds(segments)).to eq([[:waited, 4200]])
    end

    it "shows a waiting attempt's wait up to now" do
      segments = described_class.segments(step(status: "waiting", finished_at: nil, waiting_since: now - 470), now: now)

      expect(kinds(segments)).to eq([[:waited, 100], [:queued, 20], [:ran, 10], [:waiting, 470]])
    end

    it "orders a woken signal step: it waited, was scheduled again and ran" do
      woken = step(waiting_since: now - 590, scheduled_at: now - 300, started_at: now - 290, finished_at: now - 280)

      expect(kinds(described_class.segments(woken, now: now))).to eq([[:waited, 10], [:waiting, 290], [:queued, 10], [:ran, 10]])
    end

    it "clips a cancelled attempt at its finished_at" do
      cancelled = step(status: "cancelled", outcome: "cancelled", scheduled_at: now + 3600, started_at: nil, finished_at: now - 60)

      expect(kinds(described_class.segments(cancelled, now: now))).to eq([[:waited, 540]])
    end
  end

  describe ".durations" do
    it "sums each kind in seconds" do
      expect(described_class.durations(step(status: "waiting", finished_at: nil, waiting_since: now - 470), now: now))
        .to eq(waited: 100.0, queued: 20.0, ran: 10.0, waiting: 470.0)
    end

    it "leaves a kind the attempt never reached nil" do
      expect(described_class.durations(step(status: "scheduled", scheduled_at: now + 60, started_at: nil, finished_at: nil), now: now))
        .to eq(waited: 660.0, queued: nil, ran: nil, waiting: nil)
    end
  end

  describe ".paused_periods" do
    def event(name, time) = { event: name, created_at: time }

    it "pairs each pause with what ended it, oldest first" do
      events = [event("resumed", now - 100), event("paused", now - 300), event("started", now - 900)]

      expect(described_class.paused_periods(events, status: "ready", now: now)).to eq([{ from: now - 300, to: now - 100 }])
    end

    it "runs a pause that has not ended up to now" do
      events = [event("paused", now - 300)]

      expect(described_class.paused_periods(events, status: "paused", now: now)).to eq([{ from: now - 300, to: now }])
    end
  end

  describe ".chart" do
    it "gives one row per attempt, oldest first, each segment with its series, duration and the attempt's outcome" do
      steps = [step(step_name: "receipt", attempt: 1, created_at: now - 390, scheduled_at: now - 390, started_at: now - 389,
                    finished_at: now - 388),
               step(status: "failed", outcome: "paused", error_class: "Timeout::Error")]

      chart = described_class.chart(steps, [], status: "finished", now: now)

      expect(chart[:rows].map { |r| r[:label] }).to eq(["capture #1", "receipt #1"])
      ran = chart[:rows].first[:segments].last
      expect(ran).to include(kind: "ran", series: "ran_failed", duration: "1m 20s", from: (now - 480).to_i * 1000,
                             to: (now - 400).to_i * 1000)
      expect(chart[:rows].first).to include(status: "failed", error_class: "Timeout::Error")
      expect(chart[:paused]).to eq([])
    end

    it "colours ran by how the attempt ended" do
      series = %w[completed skipped failed in_progress cancelled].map do |status|
        row = described_class.chart([step(status: status, finished_at: (now - 400 unless status == "in_progress"))], [],
                                    status: "ready", now: now)[:rows].first
        row[:segments].last[:series]
      end

      expect(series).to eq(%w[ran_ok ran_ok ran_failed ran_open ran_cancelled])
    end
  end
end
