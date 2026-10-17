# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowInsightsReport do
  let(:klass) do
    Class.new(Pgbus::Workflow) do
      def self.name = "ReportSpecWorkflow"
      step(:reserve) { nil }
      step(:ship, wait_for: :shipped) { nil }
      removed_step :nudge
      step(:receipt) { nil }
    end
  end
  let(:window) { Pgbus::Web::WorkflowWindow.parse("1h", retention: nil) }
  let(:now) { Time.utc(2026, 10, 17, 12, 7, 30) }
  let(:stats) do
    { "reserve" => { attempts: 4, completed: 3, failed: 1, reattempted: 1, skipped: 0, cancelled: 0,
                     run_ms: { p50: 10.0, p95: 20.0, p99: 30.0, samples: 4 }, pickup_ms: { p50: 1.0, p95: 2.0, samples: 4 },
                     first_seen_at: now - 600 } }
  end

  let(:inputs) do
    { ongoing: [], ended: nil, attention: 0, stats: {}, histogram: {}, throughput: {}, reached: {}, stopped: {}, revisions: [] }
  end

  after { Pgbus::Workflow::Registry.reset! }

  def detail(klass: self.klass, **overrides)
    described_class.detail("ReportSpecWorkflow", klass, window: window, since: window.since(now), now: now,
                                                        **inputs, **overrides)
  end

  it "names each declared step's kind, gravestones included, then the names only the rows hold" do
    steps = detail(stats: { "legacy" => stats["reserve"] })[:steps]

    expect(steps.map { |s| s.values_at(:name, :kind, :removed, :declared) })
      .to eq([["reserve", "step", false, true], ["ship", "signal", false, true], ["nudge", "removed", true, true],
              ["receipt", "step", false, true], ["legacy", nil, false, false]])
  end

  it "gives a step's outcomes, rates and percentiles, and zeros for a step without attempts" do
    steps = detail(stats: stats)[:steps]

    expect(steps.first).to include(sample_size: 4, rates: { failed: 0.25, reattempted: 0.25, skipped: 0.0 },
                                   outcomes: { completed: 3, failed: 1, reattempted: 1, skipped: 0, cancelled: 0 })
    expect(steps.last).to include(sample_size: 0, rates: { failed: nil, reattempted: nil, skipped: nil },
                                  run_ms: { p50: nil, p95: nil, p99: nil, samples: 0 })
  end

  it "fills every bucket of the window, oldest first" do
    busy = Time.utc(2026, 10, 17, 11, 30).to_i
    buckets = detail(throughput: { busy => [2, 1, 0] })[:throughput]

    expect(buckets.size).to eq(13)
    expect(buckets.first[:from]).to eq(Time.utc(2026, 10, 17, 11, 5))
    expect(buckets.last[:from]).to eq(Time.utc(2026, 10, 17, 12, 5))
    expect(buckets.find { |b| b[:from].to_i == busy }).to include(started: 2, finished: 1, cancelled: 0)
  end

  it "starts day buckets at the zone's midnight, on both sides of a DST change" do
    zone = ActiveSupport::TimeZone["Europe/Stockholm"]
    month = Pgbus::Web::WorkflowWindow.parse("30d", retention: nil)
    after_dst = Time.utc(2026, 11, 5, 12)
    buckets = described_class.detail("ReportSpecWorkflow", klass, window: month, since: month.since(after_dst), now: after_dst,
                                                                  zone: zone, **inputs)[:throughput]

    expect(buckets.map { |b| b[:from].in_time_zone(zone).strftime("%H:%M") }.uniq).to eq(["00:00"])
    expect(buckets.map { |b| b[:from] }).to include(Time.utc(2026, 10, 20, 22), Time.utc(2026, 10, 26, 23))
    long_day = buckets.find { |b| b[:from] == Time.utc(2026, 10, 24, 22) } # 25 October, 25 hours long
    expect(long_day[:to] - long_day[:from]).to eq(25 * 3600)
  end

  it "skips the wall-clock buckets a spring-forward night does not have" do
    zone = ActiveSupport::TimeZone["Europe/Stockholm"]
    hour = Pgbus::Web::WorkflowWindow.parse("1h", retention: nil)
    after_jump = Time.utc(2026, 3, 29, 1, 30) # 03:30 CEST; 02:00-02:59 never happened
    froms = described_class.detail("ReportSpecWorkflow", klass, window: hour, since: hour.since(after_jump), now: after_jump,
                                                                zone: zone, **inputs)[:throughput].map { |b| b[:from] }

    expect(froms).to eq(froms.uniq.sort)
    expect(froms.map { |t| t.in_time_zone(zone).hour }.uniq).to eq([1, 3])
  end

  it "keeps every wall-clock bucket a fall-back night passes through, the repeated ones once" do
    zone = ActiveSupport::TimeZone["Europe/Stockholm"]
    hour = Pgbus::Web::WorkflowWindow.parse("1h", retention: nil)
    after_fall = Time.utc(2026, 10, 25, 1, 30) # 02:30 CET, the second 02:30 that night
    buckets = described_class.detail("ReportSpecWorkflow", klass, window: hour, since: hour.since(after_fall), now: after_fall,
                                                                  zone: zone, **inputs)[:throughput]
    walls = buckets.map { |b| b[:from].in_time_zone(zone).strftime("%H:%M") }

    expect(walls).to eq(%w[02:00 02:05 02:10 02:15 02:20 02:25 02:30 02:35 02:40 02:45 02:50 02:55])
  end

  it "starts a day bucket whose midnight a DST change skips at the first hour that day has" do
    zone = ActiveSupport::TimeZone["America/Santiago"] # 2026-09-06 00:00 jumps to 01:00
    month = Pgbus::Web::WorkflowWindow.parse("30d", retention: nil)
    froms = described_class.detail("ReportSpecWorkflow", klass, window: month, since: Time.utc(2026, 9, 4), now: Time.utc(2026, 9, 9),
                                                                zone: zone, **inputs)[:throughput].map { |b| b[:from] }

    expect(froms).to all(be_a(Time))
    expect(froms).to eq(froms.uniq.sort)
    expect(froms).to include(Time.utc(2026, 9, 6, 4)) # 01:00 -03, the first hour of 6 September
  end

  it "keys the buckets by the zone's wall clock, as the query does" do
    zone = ActiveSupport::TimeZone["Asia/Kolkata"]
    wall = described_class.wall_epoch(Time.utc(2026, 10, 17, 0, 0), zone)

    expect(wall).to eq(Time.utc(2026, 10, 17, 5, 30).to_i)
  end

  it "labels the duration bins with their bounds" do
    bins = detail(histogram: { 0 => 2, 8 => 1 })[:duration_histogram]

    expect(bins.size).to eq(9)
    expect(bins.first).to eq(from_ms: 0, to_ms: 1_000, count: 2)
    expect(bins.last).to eq(from_ms: 604_800_000, to_ms: nil, count: 1)
  end

  it "says where the runs started in the window are now, per step" do
    funnel = detail(reached: { "reserve" => { reached: 3, position: 1.0 } },
                    stopped: { %w[reserve ready] => 1, %w[receipt finished] => 2 })[:funnel]

    expect(funnel.first).to include(name: "reserve", reached: 3)
    expect(funnel.first[:stopped]).to include(ready: 1, finished: 0)
    expect(funnel.last[:stopped]).to include(finished: 2)
  end

  it "groups the revision rows, totals first, steps in pipeline order" do
    seen = now - 60
    row = lambda do |step, attempts|
      { revision: "abc", first_seen_at: seen, last_seen_at: seen, step: step, stats: stats["reserve"].merge(attempts: attempts) }
    end

    revision = detail(revisions: [row.call(nil, 6), row.call("receipt", 2), row.call("reserve", 4)])[:by_revision].sole

    expect(revision).to include(revision: "abc", attempts: 6, last_seen_at: seen)
    expect(revision[:steps].map { |s| s[:name] }).to eq(%w[reserve receipt])
  end

  it "counts the ongoing runs still on a gravestone" do
    ongoing = [{ workflow_class: "ReportSpecWorkflow", status: "performing", step: "nudge", runs: 2 },
               { workflow_class: "ReportSpecWorkflow", status: "paused", step: "receipt", runs: 1 }]

    expect(detail(ongoing: ongoing)).to include(removed_in_flight: { "nudge" => 2 }, ongoing: 3, paused: 1)
  end

  it "orders a class that no longer loads by the steps' positions in its runs" do
    reached = { "load" => { reached: 1, position: 3.0 }, "download" => { reached: 2, position: 1.0 } }

    expect(detail(klass: nil, reached: reached)[:steps].map { |s| s[:name] }).to eq(%w[download load])
  end
end
