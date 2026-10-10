# frozen_string_literal: true

require "json"
require_relative "spec_helper"

RSpec.describe Pgbus::MCP::Tools::RecurringTool do
  let(:data_source) { instance_double(Pgbus::Web::DataSource) }
  let(:context) { { data_source: data_source, allow_payloads: false } }
  let(:count_class) { Pgbus::Web::DataSource::ListCounts::Count }
  let(:total) { 1 }
  let(:capped) { false }
  let(:tasks) { [{ key: "cleanup", schedule: "0 * * * *" }] }

  before do
    allow(data_source).to receive(:list_count).with(:recurring_tasks)
                                              .and_return(count_class.new(total: total, capped: capped))
    allow(data_source).to receive(:recurring_tasks).and_return(tasks)
  end

  def result(**args)
    JSON.parse(described_class.call(server_context: context, **args).content.first[:text])
  end

  it "has page and per_page integer properties with a minimum of 1" do
    props = described_class.input_schema.to_h[:properties]
    expect(props[:page]).to include(type: "integer", minimum: 1)
    expect(props[:per_page]).to include(type: "integer", minimum: 1)
  end

  context "with no arguments" do
    let(:tasks) { [{ key: "a" }, { key: "b" }] }

    it "returns every task, unpaginated, without counting" do
      out = result

      expect(data_source).to have_received(:recurring_tasks).with(no_args)
      expect(data_source).not_to have_received(:list_count)
      expect(out["recurring_tasks"].size).to eq(2)
      expect(out).to include("page" => 1, "per_page" => nil, "total" => 2, "has_more" => false)
    end
  end

  it "passes page and per_page through" do
    result(page: 3, per_page: 20)

    expect(data_source).to have_received(:recurring_tasks).with(page: 3, per_page: 20)
  end

  it "defaults per_page to 100 when only page is given" do
    expect(result(page: 2)).to include("page" => 2, "per_page" => 100)
    expect(data_source).to have_received(:recurring_tasks).with(page: 2, per_page: 100)
  end

  it "defaults page to 1 when only per_page is given" do
    expect(result(per_page: 10)).to include("page" => 1, "per_page" => 10)
  end

  it "clamps per_page to 100 and page to 1..1000" do
    out = result(page: 99_999, per_page: 5_000)

    expect(data_source).to have_received(:recurring_tasks).with(page: 1_000, per_page: 100)
    expect(out).to include("page" => 1_000, "per_page" => 100)
  end

  it "clamps zero and negative values up to 1" do
    expect(result(page: 0, per_page: -5)).to include("page" => 1, "per_page" => 1)
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
      allow(data_source).to receive(:recurring_tasks).and_return(Array.new(100) { |i| { key: "t#{i}" } })

      expect(result(page: 100, per_page: 100)).to include("total" => 10_000, "has_more" => true)
    end

    it "stops reporting has_more on a short page past the cap" do
      expect(result(page: 102, per_page: 100)).to include("has_more" => false)
    end
  end
end
