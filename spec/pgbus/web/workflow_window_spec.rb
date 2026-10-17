# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::Web::WorkflowWindow do
  let(:day) { 86_400 }

  it "defaults to the last 24 hours, in hourly buckets" do
    window = described_class.parse(nil, retention: nil)

    expect([window.name, window.seconds, window.bucket_seconds, window.capped?]).to eq(["24h", day, 3_600, false])
  end

  it "reads a blank value as the default" do
    expect(described_class.parse("", retention: nil).name).to eq("24h")
  end

  it "knows each choice and its bucket" do
    expect(described_class::CHOICES.keys.map { |name| described_class.parse(name, retention: nil) }
             .map { |w| [w.name, w.seconds, w.bucket_seconds] })
      .to eq([["1h", 3_600, 300], ["24h", day, 3_600], ["7d", 7 * day, 21_600], ["30d", 30 * day, day]])
  end

  it "caps a window longer than workflow_retention at it, with that length's bucket" do
    window = described_class.parse("30d", retention: 7 * day)

    expect([window.name, window.seconds, window.bucket_seconds, window.capped?]).to eq(["30d", 7 * day, 21_600, true])
  end

  it "does not cap a window the retention covers" do
    expect(described_class.parse("7d", retention: 30 * day).capped?).to be(false)
  end

  it "accepts an ActiveSupport::Duration retention" do
    expect(described_class.parse("30d", retention: ActiveSupport::Duration.days(2)).seconds).to eq(2 * day)
  end

  it "starts the window seconds before now" do
    now = Time.utc(2026, 10, 17, 12)

    expect(described_class.parse("1h", retention: nil).since(now)).to eq(Time.utc(2026, 10, 17, 11))
  end

  it "says what it is, for an MCP answer" do
    expect(described_class.parse("30d", retention: 7 * day).to_h)
      .to eq(name: "30d", seconds: 7 * day, bucket_seconds: 21_600, capped: true)
  end

  [["1d"], [:"24h"], [%w[24h]], ["24H"]].each do |(value)|
    it "rejects #{value.inspect}" do
      expect { described_class.parse(value, retention: nil) }.to raise_error(described_class::Invalid, /window/)
    end
  end

  it "reads workflow_retention from the configuration by default" do
    allow(Pgbus.configuration).to receive(:workflow_retention).and_return(3_600)

    expect(described_class.parse("24h").seconds).to eq(3_600)
  end
end
