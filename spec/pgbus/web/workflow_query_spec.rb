# frozen_string_literal: true

require "spec_helper"
require "active_support/core_ext/object/blank"

RSpec.describe Pgbus::Web::WorkflowQuery do
  around { |example| Time.use_zone("Europe/Stockholm") { example.run } }

  it "defaults to the ongoing runs, with no filters" do
    query = described_class.new({})

    expect([query.status, query.filters, query.filtered?]).to eq(["ongoing", {}, false])
  end

  it "keeps each known status" do
    expect(Pgbus::Web::DataSource::WorkflowList::STATUSES.map { |s| described_class.new(status: s).status })
      .to eq(Pgbus::Web::DataSource::WorkflowList::STATUSES)
  end

  it "strips the text filters and drops blank ones" do
    query = described_class.new(workflow_class: " OrderWorkflow ", step: "", error_class: nil, subject_id: "42")

    expect(query.filters).to eq(workflow_class: "OrderWorkflow", subject_id: "42")
    expect(query.filtered?).to be(true)
  end

  it "reads a date as the start, or for created_to the end, of that day in Time.zone" do
    query = described_class.new(created_from: "2026-10-01", created_to: "2026-10-17")

    expect(query.filters[:created_from]).to eq(Time.zone.parse("2026-10-01 00:00"))
    expect(query.filters[:created_to]).to eq(Time.zone.parse("2026-10-17").end_of_day)
  end

  it "takes an ISO 8601 time as it is" do
    expect(described_class.new(created_to: "2026-10-17T10:00:00Z").filters[:created_to]).to eq(Time.utc(2026, 10, 17, 10))
  end

  it "gives the query back as URL params, merged" do
    query = described_class.new(status: "paused", workflow_class: "OrderWorkflow", created_from: "2026-10-01")

    expect(query.to_param(page: 2)).to eq(status: "paused", workflow_class: "OrderWorkflow", created_from: "2026-10-01", page: 2)
    expect(query.to_param(status: "all", workflow_class: nil)).to eq(status: "all", created_from: "2026-10-01")
  end

  {
    { status: "stuck" } => /unknown status/,
    { created_from: "last tuesday" } => /not a date/,
    { created_to: "2026-02-30" } => /not a date/,
    { workflow_class: %w[A B] } => /single value/,
    { error_class: "x" * 256 } => /longer than 255/
  }.each do |params, message|
    it "rejects #{params.keys.first} #{params.values.first.inspect[0, 20]}" do
      expect { described_class.new(params) }.to raise_error(described_class::Invalid, message)
    end
  end
end
