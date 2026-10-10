# frozen_string_literal: true

require "system_helper"

# Issue #492: each Insights chart says what it shows in a caption, carries its
# data as a table, is themed for both modes and re-themes on the toggle.
RSpec.describe "Insights charts", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }

  around { |example| travel_to(now) { example.run } }

  def chart_figure(id) = find("figure:has(##{id})")

  def toggle_dark_mode = first("[data-pgbus-action='toggle-dark-mode']").click

  context "with sample data" do
    before { @stub_data_source.fill_sample_data! }

    %i[light dark].each do |mode|
      context "with #{mode} mode" do
        before { mode == :dark ? visit_dark("/pgbus/insights") : visit_light("/pgbus/insights") }

        it "captions every chart in words", :aggregate_failures do
          expect(chart_figure("throughput-chart"))
            .to have_css("figcaption", text: "327 jobs in the last 1 hour, peak 34 jobs/min 45m ago (11:15)")
          expect(chart_figure("status-chart"))
            .to have_css("figcaption", text: "335 succeeded, 5 failed, 2 dead-lettered")
          expect(chart_figure("latency-chart"))
            .to have_css("figcaption", text: "Jobs waited 85ms on average before a worker picked them up, p95 310ms")
          expect(page).to have_css("#throughput-chart[role=img][aria-label^='327 jobs in the last 1 hour']")
        end

        it "draws each chart from the embedded data in the #{mode} theme" do
          expect(page).to have_css(".apexcharts-canvas.apexcharts-theme-#{mode}", count: 3)
        end

        it "carries each chart's data as a table", :aggregate_failures do
          within(chart_figure("throughput-chart")) do
            find("details summary", text: "Data table").click
            expect(page).to have_css("table.pgbus-table caption", text: "Jobs per minute", visible: :all)
            expect(page).to have_css("td[data-label='Jobs']", text: "34")
          end
          within(chart_figure("status-chart")) do
            find("details summary", text: "Data table").click
            expect(find("tr", text: "Succeeded")).to have_text("335").and have_text("98%")
          end
          within(chart_figure("latency-chart")) do
            find("details summary", text: "Data table").click
            expect(page).to have_css("td[data-label='Highest p95']", text: "327ms")
          end
        end
      end
    end

    it "re-themes the charts when dark mode is toggled, without a reload", :aggregate_failures do
      visit_light("/pgbus/insights")
      expect(page).to have_css(".apexcharts-canvas.apexcharts-theme-light", count: 3)
      page.execute_script("window.pgbusNoReload = true")

      toggle_dark_mode
      expect(page).to have_css(".apexcharts-canvas.apexcharts-theme-dark", count: 3)
      slice_stroke = page.evaluate_script("document.querySelector('#status-chart .apexcharts-pie-series path').getAttribute('stroke')")
      expect(slice_stroke).to eq("#1f2937")

      toggle_dark_mode
      expect(page).to have_css(".apexcharts-canvas.apexcharts-theme-light", count: 3)
      expect(page.evaluate_script("window.pgbusNoReload")).to be(true)
    end

    it "reads the series once per page view instead of fetching them again" do
      visit "/pgbus/insights"
      expect(page).to have_css(".apexcharts-canvas", count: 3)

      expect(@stub_data_source.calls[:job_throughput].size).to eq(1)
    end

    it "keeps units out of the latency table headers" do
      visit "/pgbus/insights"

      within(find("h2", text: "Latency by Queue").ancestor("div.rounded-lg")) do
        expect(page).to have_css("th", text: "AVG").and have_no_text("(ms)")
        expect(page).to have_css("td[data-label='Avg']", text: "64ms")
      end
    end
  end

  context "without job stats yet" do
    it "shows an empty state instead of each chart" do
      visit "/pgbus/insights"

      expect(page).to have_no_css("figure")
      expect(page).to have_css("[data-testid=chart-empty]", text: "No data yet", count: 2)
      expect(page).to have_no_css(".apexcharts-canvas")
    end
  end
end
