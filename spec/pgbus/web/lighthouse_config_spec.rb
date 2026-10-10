# frozen_string_literal: true

require "spec_helper"
require "json"

# lighthouserc.dashboard.json is the single list of audited dashboard pages:
# Lighthouse collects it, bin/lighthouse reads it, and the axe gate in
# spec/system/accessibility_spec.rb derives its paths from it. These examples
# keep the file's shape and budgets from drifting silently.
RSpec.describe "Lighthouse dashboard config" do # rubocop:disable RSpec/DescribeClass
  let(:path) { Pathname.new(__dir__).join("../../../lighthouserc.dashboard.json").expand_path }
  let(:ci) { JSON.parse(path.read).fetch("ci") }
  let(:collect) { ci.fetch("collect") }
  let(:assertions) { ci.dig("assert", "assertions") }
  let(:urls) { collect.fetch("url") }

  it "exists at the repo root" do
    expect(path).to exist
  end

  it "runs every URL at least three times so the median is stable" do
    expect(collect.fetch("numberOfRuns")).to be >= 3
  end

  it "audits exactly performance, accessibility and best-practices" do
    expect(collect.dig("settings", "onlyCategories")).to contain_exactly("performance", "accessibility", "best-practices")
  end

  it "skips the audits that always fail on http://localhost" do
    expect(collect.dig("settings", "skipAudits"))
      .to include("is-on-https", "redirects-http", "uses-http2", "uses-text-compression", "modern-http-insight")
  end

  it "asserts its own budgets rather than a preset" do
    expect(ci.fetch("assert")).not_to have_key("preset")
  end

  it "keeps the budgets from the 2026-10-10 baseline" do
    expect(assertions).to include(
      "categories:performance" => ["error", { "minScore" => 0.9 }],
      "categories:accessibility" => ["error", { "minScore" => 1 }],
      "categories:best-practices" => ["error", { "minScore" => 0.95 }],
      "largest-contentful-paint" => ["error", { "maxNumericValue" => 2000 }],
      "total-blocking-time" => ["error", { "maxNumericValue" => 300 }],
      "cumulative-layout-shift" => ["error", { "maxNumericValue" => 0.1 }],
      "resource-summary:script:size" => ["error", { "maxNumericValue" => 850_000 }],
      "total-byte-weight" => ["error", { "maxNumericValue" => 1_200_000 }],
      "color-contrast" => "error",
      "heading-order" => "error",
      "label-content-name-mismatch" => "error",
      "errors-in-console" => "error"
    )
  end

  it "only warns on the JavaScript-weight audits" do
    expect(assertions.values_at("unminified-javascript", "unused-javascript")).to all(eq("warn"))
  end

  it "points every URL at the dummy server's mounted engine" do
    expect(urls).to all(start_with("http://localhost:3003/pgbus"))
  end

  it "lists each page once" do
    paths = urls.map { |url| URI.parse(url).request_uri }
    expect(paths).to eq(paths.uniq)
  end
end
