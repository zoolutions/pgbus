# frozen_string_literal: true

require_relative "support"
require "securerandom"
require "active_support/testing/time_helpers"

# Web::DataSource's workflow insights (issue #549) against a real database:
# the percentiles, pickup latency and rates per step, the live counts, the
# end-to-end durations and their histogram, the throughput buckets, the
# funnel and the per-revision rows, all on hand-built rows with known
# timestamps (insert_all: no StepJob is sent).
RSpec.describe "Pgbus::Web::DataSource workflow insights", :integration do
  include WorkflowIntegration
  include ActiveSupport::Testing::TimeHelpers

  subject(:data_source) { Pgbus::Web::DataSource.new }

  let(:now) { Time.current.change(usec: 0) }
  let(:window) { Pgbus::Web::WorkflowWindow.parse("24h", retention: nil) }

  before do
    Pgbus::Workflow::Registry.reset!
    stub_const("CheckoutWorkflow", Class.new(Pgbus::Workflow) do
      step(:reserve) { nil }
      step(:capture) { nil }
      removed_step :nudge
      step(:receipt) { nil }
    end)
  end

  after { Pgbus::Workflow::Registry.reset! }

  def run!(klass = "CheckoutWorkflow", status: "finished", created_at: now - 3600, current_step: "receipt", **attrs)
    ended = attrs[:finished_at] || attrs[:cancelled_at]
    attrs[:finished_at] = created_at + 10 if status == "finished" && !attrs.key?(:finished_at)
    ended ||= attrs[:finished_at]
    Pgbus::WorkflowEntry.create!(workflow_id: SecureRandom.uuid, workflow_class: klass, status: status,
                                 current_step: current_step, definition_digest: "spec", arguments: {},
                                 created_at: created_at, updated_at: ended || created_at, **attrs)
  end

  # One attempt row. +run_ms+ / +pickup_ms+ place started_at and finished_at
  # after +scheduled_at+; started: false leaves started_at empty.
  def attempt!(run, step_name, status: "completed", scheduled_at: run.created_at, pickup_ms: 0, run_ms: 0,
               started: true, finished: true, attempt: nil, **attrs)
    started_at = scheduled_at + Rational(pickup_ms, 1000) if started
    finished_at = (started_at || scheduled_at) + Rational(run_ms, 1000) if finished
    @attempts ||= Hash.new(0)
    number = attempt || (@attempts[[run.workflow_id, step_name]] += 1)
    Pgbus::WorkflowStep.insert_all!([{ workflow_id: run.workflow_id, workflow_class: run.workflow_class, step_name: step_name,
                                       attempt: number, status: status, scheduled_at: scheduled_at, started_at: started_at,
                                       finished_at: finished_at, created_at: scheduled_at,
                                       outcome: attrs.delete(:outcome) || (status if finished), **attrs }])
  end

  def definition(name = "CheckoutWorkflow", win = window) = data_source.workflow_definition(name, window: win)

  def step_of(result, name) = result[:steps].find { |s| s[:name] == name }

  describe "#workflow_definition" do
    it "gives each step's run-time and pickup percentiles over the attempts that started in the window" do
      20.times do |i|
        attempt!(run!, "capture", scheduled_at: now - 7200 + i, pickup_ms: 10 * (i + 1), run_ms: 100 * (i + 1))
      end
      attempt!(run!(status: "cancelled", cancelled_at: now - 60, current_step: "capture"), "capture",
               status: "cancelled", started: false, scheduled_at: now - 120)
      attempt!(run!(created_at: now - (3 * 86_400), finished_at: now - (2 * 86_400)), "capture",
               scheduled_at: now - (2 * 86_400) - 5, run_ms: 999_999)

      capture = step_of(definition, "capture")

      expect(capture[:run_ms]).to eq(p50: 1050.0, p95: 1905.0, p99: 1981.0, samples: 20)
      expect(capture[:pickup_ms]).to eq(p50: 105.0, p95: 190.5, samples: 20)
      expect(capture[:sample_size]).to eq(21)
      expect(capture[:outcomes]).to include(completed: 20, cancelled: 1)
    end

    it "gives the failure, reattempt and skip rates of the attempts that finished in the window" do
      run = run!(status: "paused", current_step: "receipt", paused_at: now - 60)
      attempt!(run, "receipt", run_ms: 5)
      attempt!(run, "receipt", status: "failed", outcome: "reattempted", error_class: "Timeout::Error", run_ms: 5)
      attempt!(run, "receipt", status: "failed", outcome: "paused", error_class: "Timeout::Error", run_ms: 5)
      attempt!(run, "receipt", status: "failed", outcome: "reattempted", run_ms: 5) # reattempt! without an error
      attempt!(run, "receipt", status: "skipped", run_ms: 0)

      receipt = step_of(definition, "receipt")

      expect(receipt[:rates]).to eq(failed: 0.4, reattempted: 0.4, skipped: 0.2)
      expect(receipt[:run_ms][:samples]).to eq(4)
      expect(receipt[:sample_size]).to eq(5)
    end

    it "lists the steps in declaration order with the live count of runs sitting at each, by status" do
      2.times { run!(status: "ready", current_step: "capture") }
      run!(status: "performing", current_step: "capture")
      run!(status: "paused", current_step: "capture", paused_at: now)
      run!(status: "waiting", current_step: "receipt")
      run!(status: "performing", current_step: "nudge")

      result = definition

      expect(result[:steps].map { |s| [s[:name], s[:kind], s[:removed]] })
        .to eq([["reserve", "step", false], ["capture", "step", false], ["nudge", "removed", true], ["receipt", "step", false]])
      expect(step_of(result, "capture")[:counts]).to eq(ready: 2, performing: 1, waiting: 0, paused: 1)
      expect(step_of(result, "receipt")[:counts]).to include(waiting: 1)
      expect(result).to include(loaded: true, ongoing: 6, paused: 1, removed_in_flight: { "nudge" => 1 })
    end

    it "gives the end-to-end durations of the runs that finished in the window, and their distribution" do
      [500, 5_000, 30_000, 300_000, 7_200_000].each do |ms|
        run!(created_at: now - 20_000, finished_at: now - 20_000 + Rational(ms, 1000))
      end
      run!(status: "cancelled", created_at: now - 600, cancelled_at: now - 300, current_step: "capture")
      run!(created_at: now - (3 * 86_400), finished_at: now - (2 * 86_400))

      result = definition

      expect(result).to include(finished: 5, cancelled: 1)
      expect(result[:e2e_ms]).to include(p50: 30_000.0, samples: 5)
      expect(result[:duration_histogram].map { |bin| bin[:count] }).to eq([1, 1, 1, 1, 0, 1, 0, 0, 0])
      expect(result[:duration_histogram].first).to include(from_ms: 0, to_ms: 1_000)
      expect(result[:duration_histogram].last).to include(from_ms: 604_800_000, to_ms: nil)
    end

    it "buckets the runs started, finished and cancelled across the whole window" do
      run!(created_at: now - 7200, finished_at: now - 3600)
      run!(status: "cancelled", created_at: now - 7200, cancelled_at: now - 100, current_step: "capture")
      run!(status: "ready", created_at: now - 100, current_step: "reserve")
      run!(created_at: now - (3 * 86_400), finished_at: now - 100) # started before the window

      buckets = definition[:throughput]

      expect(buckets.size).to be_between(24, 25)
      expect(buckets.map { |b| b[:from] }).to eq(buckets.map { |b| b[:from] }.sort)
      expect(buckets.sum { |b| b[:started] }).to eq(3)
      expect(buckets.sum { |b| b[:finished] }).to eq(2)
      expect(buckets.sum { |b| b[:cancelled] }).to eq(1)
      latest = buckets.find { |b| (b[:from]...(b[:from] + 3600)).cover?(now - 100) }
      expect(latest[:started]).to eq(1)
      expect(definition[:bucket_seconds]).to eq(3_600)
    end

    it "starts the buckets at the zone's midnight" do
      run!(created_at: now - 3600, finished_at: now - 1800)
      day = Pgbus::Web::WorkflowWindow.parse("30d", retention: nil)

      Time.use_zone("Asia/Kolkata") do
        buckets = data_source.workflow_definition("CheckoutWorkflow", window: day)[:throughput]

        expect(buckets.map { |b| b[:from].in_time_zone.strftime("%H:%M") }.uniq).to eq(["00:00"])
        expect(buckets.sum { |b| b[:started] }).to eq(1)
        expect(buckets.sum { |b| b[:finished] }).to eq(1)
      end
    end

    it "starts each day bucket at the zone's midnight on both sides of a DST change" do
      travel_to(Time.utc(2026, 11, 5, 12)) do
        # 00:30 on 21 October in Stockholm, summer time (UTC+2)
        run!(created_at: Time.utc(2026, 10, 20, 22, 30), finished_at: Time.utc(2026, 10, 20, 22, 31))
        day = Pgbus::Web::WorkflowWindow.parse("30d", retention: nil)

        Time.use_zone("Europe/Stockholm") do
          buckets = data_source.workflow_definition("CheckoutWorkflow", window: day)[:throughput]
          busy = buckets.find { |b| b[:started].positive? }

          expect(busy[:from]).to eq(Time.utc(2026, 10, 20, 22))
          expect(busy[:finished]).to eq(1)
        end
      end
    end

    it "builds the funnel of the runs started in the window: how many reached each step and where the rest are" do
      3.times do
        run = run!(created_at: now - 600)
        %w[reserve capture receipt].each { |name| attempt!(run, name, scheduled_at: now - 590) }
      end
      paused = run!(status: "paused", created_at: now - 600, current_step: "capture", paused_at: now - 500)
      attempt!(paused, "reserve", scheduled_at: now - 590)
      attempt!(paused, "capture", status: "failed", error_class: "Timeout::Error", scheduled_at: now - 580)
      cancelled = run!(status: "cancelled", created_at: now - 600, current_step: "capture", cancelled_at: now - 400)
      attempt!(cancelled, "reserve", scheduled_at: now - 590)
      attempt!(cancelled, "capture", status: "cancelled", started: false, scheduled_at: now - 580)
      fresh = run!(status: "ready", created_at: now - 60, current_step: "reserve")
      attempt!(fresh, "reserve", status: "scheduled", started: false, finished: false, outcome: nil, scheduled_at: now - 60)
      old = run!(created_at: now - (3 * 86_400), finished_at: now - (3 * 86_400) + 30)
      attempt!(old, "reserve", scheduled_at: old.created_at)

      result = definition
      funnel = result[:funnel].to_h { |s| [s[:name], s] }

      expect(result[:started]).to eq(6)
      expect(funnel.transform_values { |s| s[:reached] }).to include("reserve" => 6, "capture" => 5, "receipt" => 3)
      expect(funnel["reserve"][:reached]).to eq(result[:started])
      expect(funnel["reserve"][:stopped]).to include(ready: 1)
      expect(funnel["capture"][:stopped]).to include(paused: 1, cancelled: 1)
      expect(funnel["receipt"][:stopped]).to include(finished: 3)
      expect(result[:funnel].sum { |s| s[:stopped].values.sum }).to eq(result[:started])
    end

    it "gives the numbers per app revision for the five revisions seen last, newest first" do
      %w[r1 r2 r3 r4 r5 r6].each_with_index do |revision, i|
        attempt!(run!, "capture", scheduled_at: now - 80_000 + (i * 3600), app_revision: revision, run_ms: 100)
      end
      latest = run!
      attempt!(latest, "capture", status: "failed", error_class: "Timeout::Error", app_revision: "r6",
                                  scheduled_at: now - 60, run_ms: 900)
      attempt!(latest, "reserve", app_revision: "r6", scheduled_at: now - 70, run_ms: 50)
      attempt!(run!, "capture", scheduled_at: now - 50, run_ms: 100) # no revision recorded

      revisions = definition[:by_revision]

      expect(revisions.map { |r| r[:revision] }).to eq(%w[r6 r5 r4 r3 r2])
      newest = revisions.first
      expect(newest).to include(attempts: 3)
      expect(newest[:rates][:failed]).to be_within(0.001).of(1.0 / 3)
      expect(newest[:steps].to_h { |s| [s[:name], s[:attempts]] }).to eq("capture" => 2, "reserve" => 1)
      expect(newest[:steps].find { |s| s[:name] == "capture" }[:rates][:failed]).to eq(0.5)
      expect(revisions.last[:rates][:failed]).to eq(0.0)
      expect(newest[:last_seen_at]).to be_within(2).of(now - 60 + 0.9)
    end

    it "orders the steps of a class that no longer loads by where they ran, and says the definition is not loaded" do
      2.times do
        run = run!("Gone::ImportWorkflow", created_at: now - 900, current_step: "load")
        %w[download parse load].each_with_index { |name, i| attempt!(run, name, scheduled_at: now - 890 + i) }
      end

      result = definition("Gone::ImportWorkflow")

      expect(result).to include(workflow_class: "Gone::ImportWorkflow", loaded: false, removed_in_flight: {})
      expect(result[:steps].map { |s| [s[:name], s[:kind], s[:declared]] })
        .to eq([["download", nil, false], ["parse", nil, false], ["load", nil, false]])
    end

    it "is nil for a class that is neither defined nor in any run, and never constantizes the name" do
      allow(ActiveSupport::Inflector).to receive(:safe_constantize).and_call_original

      expect(definition("Nope::MissingWorkflow")).to be_nil
      expect(ActiveSupport::Inflector).not_to have_received(:safe_constantize).with("Nope::MissingWorkflow")
    end

    it "treats a class name in the rows that resolves to something other than a workflow as not loaded" do
      run!("String", status: "ready", current_step: "reserve")

      expect(definition("String")).to include(loaded: false)
    end

    it "keeps to the window, capped at workflow_retention" do
      run!(created_at: now - 20_000, finished_at: now - 10_800)
      run!(created_at: now - 3_000, finished_at: now - 1_800)

      capped = Pgbus::Web::WorkflowWindow.parse("24h", retention: 7_200)

      expect(definition("CheckoutWorkflow", capped)).to include(finished: 1)
      expect(definition("CheckoutWorkflow", capped)[:window]).to include(name: "24h", seconds: 7_200, capped: true)
    end
  end

  describe "#workflow_definitions" do
    it "lists every class name in the rows, however many there are" do
      rows = Array.new(600) do |i|
        { workflow_id: SecureRandom.uuid, workflow_class: format("Many::Workflow%03d", i), status: "finished",
          current_step: "one", definition_digest: "spec", arguments: {}, created_at: now - 600, updated_at: now - 600 }
      end
      Pgbus::WorkflowEntry.insert_all!(rows)

      expect(data_source.workflow_definitions(window: window).count { |row| row[:workflow_class].start_with?("Many::") })
        .to eq(600)
    end

    it "lists every registered class and every class still in the rows, with counts and end-to-end percentiles" do
      stub_const("SignupWorkflow", Class.new(Pgbus::Workflow) { step(:welcome) { nil } })
      run!(created_at: now - 600, finished_at: now - 590)
      run!(created_at: now - 600, finished_at: now - 570)
      run!(status: "cancelled", cancelled_at: now - 100, current_step: "capture")
      paused = run!(status: "paused", current_step: "capture", paused_at: now - 50)
      attempt!(paused, "capture", status: "failed", error_class: "Timeout::Error", scheduled_at: now - 60)
      run!(status: "performing", current_step: "nudge")
      run!("Gone::ImportWorkflow", status: "ready", current_step: "load")
      run!("Old::ArchivedWorkflow", created_at: now - (5 * 86_400), finished_at: now - (5 * 86_400) + 1)

      rows = data_source.workflow_definitions(window: window).to_h { |row| [row[:workflow_class], row] }

      expect(rows.keys).to eq(%w[CheckoutWorkflow Gone::ImportWorkflow Old::ArchivedWorkflow SignupWorkflow])
      expect(rows["CheckoutWorkflow"]).to include(loaded: true, ongoing: 2, paused: 1, attention: 1, finished: 2,
                                                  cancelled: 1, removed_in_flight: { "nudge" => 1 })
      expect(rows["CheckoutWorkflow"][:e2e_ms]).to include(p50: 20_000.0, samples: 2)
      expect(rows["Gone::ImportWorkflow"]).to include(loaded: false, ongoing: 1, finished: 0)
      expect(rows["Old::ArchivedWorkflow"]).to include(loaded: false, ongoing: 0, finished: 0)
      expect(rows["SignupWorkflow"]).to include(loaded: true, ongoing: 0, finished: 0, attention: 0)
      expect(rows["SignupWorkflow"][:e2e_ms]).to eq(p50: nil, p95: nil, samples: 0)
    end
  end
end
