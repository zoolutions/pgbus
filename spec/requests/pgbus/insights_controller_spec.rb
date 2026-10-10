# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::InsightsController", type: :request do
  describe "GET /pgbus/insights" do
    def embedded_series
      script = Nokogiri::HTML(response.body).at_css("script#pgbus-insights-data[type='application/json']")
      JSON.parse(script.text)
    end

    context "when latency columns are unavailable" do
      it "renders the insights page" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end

    context "when latency columns are available" do
      before { @stub_data_source.insights_latency_available = true }

      it "renders the insights page" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end

    context "with chart series" do
      before { @stub_data_source.fill_sample_data! }

      # The page draws its charts from the series it embeds (issue #492)
      # instead of fetching /pgbus/api/insights.
      it "embeds the chart series as JSON", :aggregate_failures do
        get "/pgbus/insights"

        series = embedded_series
        expect(series.keys).to contain_exactly("throughput", "status_counts", "latency_trend")
        expect(series["throughput"].size).to eq(12)
        expect(series["status_counts"]).to eq("success" => 335, "failed" => 5, "dead_lettered" => 2)
        expect(response.body).not_to include("/pgbus/api/insights")
      end

      it "escapes the embedded JSON so a value cannot close the script tag" do
        @stub_data_source.insights_throughput = [{ time: "</script><script>alert(1)</script>", count: 1 }]

        get "/pgbus/insights"

        expect(response.body).not_to include("</script><script>alert(1)")
        expect(embedded_series["throughput"].first["time"]).to eq("</script><script>alert(1)</script>")
      end
    end

    context "when stream stats are available" do
      before { @stub_data_source.stream_stats_available = true }

      it "renders the insights page with stream stats" do
        get "/pgbus/insights"
        expect(response).to have_http_status(:ok)
      end
    end
  end
end
