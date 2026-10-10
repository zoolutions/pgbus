# frozen_string_literal: true

require "spec_helper"
require "active_support/testing/time_helpers"

RSpec.describe Pgbus::Web::TimeFormat do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 0, 8, 54) }

  before(:all) do # rubocop:disable RSpec/BeforeAfterAll
    locales = Dir[File.expand_path("../../../config/locales/*.yml", __dir__)]
    I18n.load_path |= locales
    I18n.backend.reload!
  end

  around do |example|
    I18n.with_locale(:en) do
      Time.use_zone("Europe/Stockholm") { travel_to(now) { example.run } }
    end
  end

  describe ".coerce" do
    it "returns nil for nil and blank strings" do
      expect(described_class.coerce(nil)).to be_nil
      expect(described_class.coerce("  ")).to be_nil
    end

    it "puts a Time into Time.zone" do
      expect(described_class.coerce(now).time_zone.name).to eq("Europe/Stockholm")
    end

    it "accepts a TimeWithZone and a DateTime" do
      expect(described_class.coerce(now.in_time_zone("Asia/Tokyo"))).to eq(now)
      expect(described_class.coerce(now.to_datetime)).to eq(now)
    end

    it "parses ISO 8601 and Time#to_s strings, keeping their offset" do
      expect(described_class.coerce("2026-10-10T00:08:54Z")).to eq(now)
      expect(described_class.coerce("2026-10-10 00:08:54 +0000")).to eq(now)
    end

    it "parses a zone-less string in Time.zone, not the system zone" do
      expect(described_class.coerce("2026-08-23 10:00")).to eq(Time.utc(2026, 8, 23, 8, 0))
    end

    it "reads a Numeric as epoch seconds (ActiveJob's pre-7.1 scheduled_at)" do
      expect(described_class.coerce(now.to_f)).to eq(now)
    end

    it "returns nil for a string that is not a time" do
      expect(described_class.coerce("not a time")).to be_nil
    end
  end

  describe ".relative" do
    it "is 'now' within a second either way" do
      expect(described_class.relative(now - 0.4)).to eq("now")
      expect(described_class.relative(now + 0.4)).to eq("now")
    end

    it "uses the single largest unit in the past" do
      expect(described_class.relative(now - 30)).to eq("30s ago")
      expect(described_class.relative(now - 120)).to eq("2m ago")
      expect(described_class.relative(now - 7200)).to eq("2h ago")
      expect(described_class.relative(now - 172_800)).to eq("2d ago")
    end

    it "uses the future template for a future time" do
      expect(described_class.relative(now + 90)).to eq("in 1m")
      expect(described_class.relative(now + 5400)).to eq("in 1h")
    end

    it "never renders a future time as a negative age (the -1d ago defect)" do
      expect(described_class.relative((now + 30).utc.iso8601)).to eq("in 30s")
    end

    it "measures against an explicit now:" do
      expect(described_class.relative(now, now: now + 60)).to eq("1m ago")
    end

    it "returns nil for nil" do
      expect(described_class.relative(nil)).to be_nil
    end
  end

  describe ".absolute / .clock / .iso" do
    it "formats the absolute in Time.zone with the zone abbreviation" do
      expect(described_class.absolute(now)).to eq("2026-10-10 02:08:54 CEST")
    end

    it "formats a clock time in Time.zone" do
      expect(described_class.clock(now)).to eq("02:08")
    end

    it "formats the machine value as UTC ISO 8601" do
      expect(described_class.iso("2026-10-10 02:08:54 +0200")).to eq("2026-10-10T00:08:54Z")
    end
  end

  describe ".duration" do
    it "renders up to two units" do
      expect(described_class.duration(nil)).to eq("—")
      expect(described_class.duration(45)).to eq("45s")
      expect(described_class.duration(125)).to eq("2m 5s")
      expect(described_class.duration(3725)).to eq("1h 2m")
      expect(described_class.duration(90_000)).to eq("1d 1h")
    end
  end

  describe ".ms_duration" do
    it "keeps today's thresholds" do
      expect(described_class.ms_duration(nil)).to eq("—")
      expect(described_class.ms_duration(42)).to eq("42ms")
      expect(described_class.ms_duration(1500)).to eq("1.5s")
      expect(described_class.ms_duration(59_950)).to eq("60.0s")
      expect(described_class.ms_duration(120_000)).to eq("2.0m")
    end
  end

  describe ".range_label" do
    it "picks the largest whole unit, pluralised" do
      expect(described_class.range_label(1)).to eq("1 minute")
      expect(described_class.range_label(90)).to eq("90 minutes")
      expect(described_class.range_label(60)).to eq("1 hour")
      expect(described_class.range_label(120)).to eq("2 hours")
      expect(described_class.range_label(1440)).to eq("24 hours")
      expect(described_class.range_label(2880)).to eq("2 days")
      expect(described_class.range_label(0)).to eq("1 minute")
    end
  end

  context "with the ja locale" do
    around { |example| I18n.with_locale(:ja) { example.run } }

    it "takes every word from the locale" do
      expect(described_class.relative(now - 300)).to eq("5分前")
      expect(described_class.relative(now + 7200)).to eq("2時間後")
      expect(described_class.duration(125)).to eq("2分 5秒")
      expect(described_class.range_label(120)).to eq("2時間")
    end
  end
end
