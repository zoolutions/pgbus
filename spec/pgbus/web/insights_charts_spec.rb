# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::InsightsCharts do
  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }

  describe ".throughput" do
    let(:points) do
      [{ time: (now - 180).iso8601, count: 4 }, { time: now - 120, count: 9 }, { time: (now - 60).iso8601, count: 2 }]
    end

    it "totals the window and finds the busiest minute" do
      chart = described_class.throughput(points, minutes: 60)

      expect(chart).to have_attributes(total: 15, peak: 9, step_minutes: 1)
      expect(chart.peak_at).to eq(now - 120)
      expect(chart.rows).to eq([{ from: now - 180, count: 4 }, { from: now - 120, count: 9 }, { from: now - 60, count: 2 }])
    end

    # A 30-day window has up to 43,200 minute buckets: the data table rolls
    # them up so it never holds more than MAX_TABLE_ROWS rows.
    it "rolls minutes up into at most MAX_TABLE_ROWS table rows" do
      minutes = 43_200
      many = Array.new(minutes) { |i| { time: now - (i * 60), count: 1 } }

      chart = described_class.throughput(many, minutes: minutes)

      expect(chart.step_minutes).to eq(720)
      expect(chart.rows.size).to eq(described_class::MAX_TABLE_ROWS)
      expect(chart.rows.sum { |r| r[:count] }).to eq(minutes)
      expect(chart.total).to eq(minutes)
    end

    it "is empty without points" do
      expect(described_class.throughput([], minutes: 60)).to have_attributes(total: 0, rows: [], peak: nil, empty?: true)
    end
  end

  describe ".status" do
    it "lists every status with its share, known ones first" do
      chart = described_class.status({ "failed" => 5, "dead_lettered" => 2, "success" => 335 })

      expect(chart.total).to eq(342)
      expect(chart.rows.map { |r| r[:status] }).to eq(%w[success failed dead_lettered])
      expect(chart.rows.first[:share]).to be_within(0.0001).of(335 / 342.0)
      expect(chart[:failed]).to eq(5)
    end

    it "reads a status with no row as zero" do
      expect(described_class.status({ "success" => 3 })[:dead_lettered]).to eq(0)
    end

    # The sparse stub, and a fresh install, report every status at zero.
    it "is empty when every count is zero" do
      expect(described_class.status({ "success" => 0, "failed" => 0 })).to be_empty
    end
  end

  describe ".latency" do
    it "rolls minutes up into rows with the mean average and the highest p95" do
      points = [{ time: now - 120, avg_ms: 40, p95_ms: 200 }, { time: (now - 60).iso8601, avg_ms: 60, p95_ms: 300 }]

      chart = described_class.latency(points, minutes: 240)

      expect(chart.step_minutes).to eq(4)
      expect(chart.rows).to eq([{ from: now - 120, avg_ms: 50, p95_ms: 300 }])
    end

    it "is empty without points" do
      expect(described_class.latency([], minutes: 60)).to be_empty
    end
  end
end
