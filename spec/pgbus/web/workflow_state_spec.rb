# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowState do
  let(:now) { Time.utc(2026, 10, 17, 12, 0, 0) }
  let(:threshold) { 3600 }

  def present(**row)
    described_class.present({ current_step: "capture", workflow_class: "OrderWorkflow" }.merge(row), threshold: threshold, now: now)
  end

  def said(**row)
    result = present(**row)
    [result.badge, result.next_key, result.next_args]
  end

  it "says what a performing run does, with the attempt past the first" do
    expect(said(status: "performing", current_attempt: 1)).to eq(["performing", "performing", { step: "capture" }])
    expect(said(status: "performing", current_attempt: 2))
      .to eq(["performing", "performing_attempt", { step: "capture", attempt: 2 }])
  end

  it "says what a waiting run waits for: a batch, a signal (with its deadline) or its step" do
    expect(said(status: "waiting", batch_id: "b-1", batch_progress: { done: 7, total: 12 }))
      .to eq(["waiting", "waiting_batch", { batch: "OrderWorkflow#capture", done: 7, total: 12 }])
    expect(said(status: "waiting", batch_id: "b-1", batch_progress: nil).last).to include(done: 0, total: 0)
    expect(said(status: "waiting", waiting_for: "shipped", waiting_since: now - 60))
      .to eq(["waiting", "waiting_signal", { signal: "shipped", ago: now - 60 }])
    expect(said(status: "waiting", waiting_for: "shipped", waiting_since: now - 60, timeout_at: now + 60))
      .to eq(["waiting", "waiting_signal_timeout", { signal: "shipped", ago: now - 60, time: now + 60 }])
    expect(said(status: "waiting")).to eq(["waiting", "waiting_step", { step: "capture" }])
  end

  it "calls an on_timeout: :notify deadline a late notice, and says since when a wait it fired on is late (issue #565)" do
    waiting = { status: "waiting", waiting_for: "approved", on_timeout: "notify" }
    expect(said(**waiting, waiting_since: now - 60, timeout_at: now + 60))
      .to eq(["waiting", "waiting_signal_notify", { signal: "approved", ago: now - 60, time: now + 60 }])
    expect(said(**waiting, waiting_since: now - 10_800, timeout_at: now + 3600, late_since: now - 7200))
      .to eq(["waiting", "waiting_signal_late", { signal: "approved", ago: now - 10_800, late: now - 7200, time: now + 3600 }])
    expect(said(**waiting, on_timeout: "pause!", waiting_since: now - 60, timeout_at: now + 60).second)
      .to eq("waiting_signal_timeout")
    # A deploy switched the step to :pause! after a late notice: the next deadline pauses.
    expect(said(**waiting, on_timeout: "pause!", waiting_since: now - 60, timeout_at: now + 60, late_since: now - 30).second)
      .to eq("waiting_signal_timeout")
    expect(said(**waiting, on_timeout: nil, waiting_since: now - 60, timeout_at: now + 60, late_since: now - 30).second)
      .to eq("waiting_signal_late")
  end

  it "says why a run is paused" do
    expect(said(status: "paused", error_class: "Timeout::Error"))
      .to eq(["paused", "paused_error", { step: "capture", error: "Timeout::Error" }])
    expect(said(status: "paused")).to eq(["paused", "paused_manual", { step: "capture" }])
  end

  it "tells a scheduled step from a due and an overdue one" do
    expect(said(status: "ready", scheduled_at: now + 86_400)).to eq(["scheduled", "scheduled", { step: "capture", time: now + 86_400 }])
    expect(said(status: "ready", scheduled_at: (now - 60).iso8601)).to eq(["ready", "due", { step: "capture", ago: now - 60 }])
    expect(said(status: "ready", scheduled_at: now - 7200)).to eq(["ready", "overdue", { step: "capture", ago: now - 7200 }])
    expect(said(status: "ready")).to eq(["ready", "ready", { step: "capture" }])
  end

  it "says when a run ended" do
    expect(said(status: "finished", finished_at: now - 60)).to eq(["finished", "finished", { ago: now - 60 }])
    expect(said(status: "cancelled", cancelled_at: nil, updated_at: now - 5)).to eq(["cancelled", "cancelled", { ago: now - 5 }])
  end

  it "badges each state in the batch colours, scheduled and ready in gray" do
    tones = %w[ready performing waiting paused finished cancelled].map { |status| present(status: status).badge_tone }

    expect(tones).to eq(%i[gray blue yellow yellow green red])
    expect(present(status: "ready", scheduled_at: now + 60).badge_tone).to eq(:gray)
  end

  it "keeps an unknown status readable" do
    expect(said(status: "archived")).to eq(["archived", "unknown", { status: "archived" }])
  end
end
