# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::ConcurrencyTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:count_class) { Pgbus::Web::DataSource::ListCounts::Count }
  let(:total) { 1 }
  let(:capped) { false }

  before do
    allow(data_source).to receive(:list_count).with(:concurrency_keys)
                                              .and_return(count_class.new(total: total, capped: capped))
    allow(data_source).to receive(:concurrency_stats).and_return(
      { parked_total: 7, oldest_parked_age_sec: 812, slots_held: 3, keys_at_limit: 1, keys: [{ key: "K" }] }
    )
  end

  def result(**args)
    JSON.parse(described_class.call(server_context: context, **args).content.first[:text])
  end

  it "has page and per_page integer properties with a minimum of 1" do
    props = described_class.input_schema.to_h[:properties]
    expect(props[:page]).to include(type: "integer", minimum: 1)
    expect(props[:per_page]).to include(type: "integer", minimum: 1)
  end

  it "keeps today's call and output shape when called with no arguments" do
    out = result

    expect(data_source).to have_received(:concurrency_stats).with(page: 1, per_page: 100)
    expect(out.keys).to include("parked_total", "oldest_parked_age_sec", "slots_held", "keys_at_limit", "keys",
                                "page", "per_page", "total", "has_more")
    expect(out).to include("parked_total" => 7, "page" => 1, "per_page" => 100, "has_more" => false)
  end

  it "passes page and per_page through" do
    result(page: 3, per_page: 20)

    expect(data_source).to have_received(:concurrency_stats).with(page: 3, per_page: 20)
  end

  it "clamps per_page to 100 and page to 1..1000" do
    out = result(page: 99_999, per_page: 5_000)

    expect(data_source).to have_received(:concurrency_stats).with(page: 1_000, per_page: 100)
    expect(out).to include("page" => 1_000, "per_page" => 100)
  end

  it "clamps zero and negative values up to 1" do
    out = result(page: 0, per_page: -5)

    expect(out).to include("page" => 1, "per_page" => 1)
  end

  context "when more rows exist beyond the page" do
    let(:total) { 250 }

    it "reports has_more and the total" do
      expect(result(page: 1, per_page: 100)).to include("total" => 250, "has_more" => true)
    end

    it "reports no more on the last page" do
      expect(result(page: 3, per_page: 100)).to include("has_more" => false)
    end
  end

  context "when the count is capped" do
    let(:total) { 10_000 }
    let(:capped) { true }

    it "reports has_more on a full page past the counted cap" do
      allow(data_source).to receive(:concurrency_stats).and_return(
        { parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0,
          keys: Array.new(100) { |i| { key: "K#{i}" } } }
      )

      expect(result(page: 100, per_page: 100)).to include("total" => 10_000, "has_more" => true)
    end

    it "stops reporting has_more at the page ceiling, which the next call could not get past" do
      expect(result(page: described_class::MAX_PAGE, per_page: 1)).to include("has_more" => false)
    end

    it "stops reporting has_more on a short page past the cap" do
      expect(result(page: 102, per_page: 100)).to include("has_more" => false)
    end
  end
end
