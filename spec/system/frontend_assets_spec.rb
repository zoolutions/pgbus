# frozen_string_literal: true

require "system_helper"

# Issue #517: the dashboard loads minified modules, and ApexCharts (575 KB) only
# on Insights — charts.js imports it on demand through the importmap.
RSpec.describe "Dashboard frontend assets", type: :system do
  def fetched_scripts
    page.evaluate_script('performance.getEntriesByType("resource").map((e) => e.name)')
  end

  def importmap
    JSON.parse(page.find("script[type=importmap]", visible: false).text(:all))["imports"]
  end

  before { @stub_data_source.fill_sample_data! }

  it "never fetches ApexCharts off Insights, though the importmap knows it", :aggregate_failures do
    visit "/pgbus"

    expect(page).to have_css("h1")
    expect(page).to have_no_css("script[src*='apexcharts']", visible: false)
    expect(importmap).to include("apexcharts", "application", "turbo", "charts")
    expect(fetched_scripts).to include(a_string_matching(%r{/modules/[^/]+/application\.js\z}))
    expect(fetched_scripts).not_to include(a_string_matching(/apexcharts/))
  end

  it "loads ApexCharts once and draws every chart after a Turbo Drive visit to Insights", :aggregate_failures do
    visit "/pgbus"
    find("a[href='/pgbus/insights']", match: :first).click

    expect(page).to have_css(".apexcharts-canvas", count: 3)
    expect(page).to have_current_path("/pgbus/insights")
    expect(fetched_scripts.grep(/apexcharts/).size).to eq(1)
  end
end
