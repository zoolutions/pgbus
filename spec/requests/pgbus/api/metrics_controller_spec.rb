# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::Api::MetricsController", type: :request do
  describe "GET /pgbus/api/metrics" do
    context "when metrics are disabled" do
      before { Pgbus.configuration.metrics_enabled = false }

      it "returns 404 Not Found" do
        get "/pgbus/api/metrics"
        expect(response).to have_http_status(:not_found)
      end
    end

    context "when metrics are enabled" do
      before { Pgbus.configuration.metrics_enabled = true }

      it "returns 200 OK" do
        get "/pgbus/api/metrics"
        expect(response).to have_http_status(:ok)
      end

      it "sets the Prometheus text exposition content type" do
        get "/pgbus/api/metrics"
        expect(response.headers["Content-Type"]).to eq("text/plain; version=0.0.4; charset=utf-8")
      end

      it "renders Prometheus metric lines from the data source" do
        get "/pgbus/api/metrics"
        expect(response.body).to include("# TYPE pgbus_queue_depth gauge")
        expect(response.body).to include('pgbus_queue_depth{queue="pgbus_default"}')
      end

      it "renders the concurrency gauges when jobs are parked" do
        @stub_data_source.stats[:parked_total] = 7
        @stub_data_source.stats[:oldest_parked_age_sec] = 812
        @stub_data_source.stats[:slots_held] = 3

        get "/pgbus/api/metrics"

        expect(response.body).to include("pgbus_concurrency_blocked_executions 7")
        expect(response.body).to include("pgbus_concurrency_blocked_oldest_age_seconds 812")
        expect(response.body).to include("pgbus_concurrency_slots_held 3")
      end
    end
  end
end
