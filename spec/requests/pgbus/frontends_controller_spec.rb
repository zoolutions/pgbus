# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::FrontendsController", type: :request do
  let(:version) { Pgbus::VERSION.tr(".", "-") }

  describe "GET /pgbus/frontend/static/:version/:id" do
    it "serves a known static asset for the current version" do
      get "/pgbus/frontend/static/#{version}/style.css"
      expect(response).to have_http_status(:ok)
    end

    it "returns 404 for an unknown asset id" do
      get "/pgbus/frontend/static/#{version}/nope"
      expect(response).to have_http_status(:not_found)
    end

    it "returns 404 for a stale version" do
      get "/pgbus/frontend/static/0-0-0/style.css"
      expect(response).to have_http_status(:not_found)
    end

    it "no longer serves apexcharts as a static asset" do
      get "/pgbus/frontend/static/#{version}/apexcharts.js"
      expect(response).to have_http_status(:not_found)
    end
  end

  describe "GET /pgbus/frontend/modules/:version/:id" do
    let(:frontend_root) { Pgbus::Engine.root.join("app", "frontend", "pgbus") }

    it "serves a known JS module for the current version, minified" do
      get "/pgbus/frontend/modules/#{version}/application.js"
      expect(response).to have_http_status(:ok)
      expect(response.body).to eq(frontend_root.join("application.min.js").read)
      expect(response.body).to end_with("//# sourceMappingURL=application.map\n")
    end

    it "serves apexcharts as a module, as vendored" do
      get "/pgbus/frontend/modules/#{version}/apexcharts.js"
      expect(response).to have_http_status(:ok)
      expect(response.media_type).to eq("text/javascript")
    end

    it "serves a module's sourcemap as JSON beside it" do
      get "/pgbus/frontend/modules/#{version}/application.map"
      expect(response).to have_http_status(:ok)
      expect(response.media_type).to eq("application/json")
      expect(JSON.parse(response.body)).to include("sourcesContent" => [frontend_root.join("application.js").read])
      expect(response.headers["Cache-Control"]).to include("public")
    end

    it "serves a nested module's sourcemap" do
      get "/pgbus/frontend/modules/#{version}/charts.map"
      expect(response).to have_http_status(:ok)
      expect(JSON.parse(response.body)["sources"]).to eq(["charts.js"])
    end

    it "returns 404 for the sourcemap of a module that has none" do
      get "/pgbus/frontend/modules/#{version}/apexcharts.map"
      expect(response).to have_http_status(:not_found)
    end

    it "returns 404 for an unknown sourcemap" do
      get "/pgbus/frontend/modules/#{version}/nope.map"
      expect(response).to have_http_status(:not_found)
    end

    it "returns 404 for a stale version, module and sourcemap alike" do
      get "/pgbus/frontend/modules/0-0-0/application.js"
      expect(response).to have_http_status(:not_found)
      get "/pgbus/frontend/modules/0-0-0/application.map"
      expect(response).to have_http_status(:not_found)
    end

    it "returns 404 for an unknown module id" do
      get "/pgbus/frontend/modules/#{version}/nope.js"
      expect(response).to have_http_status(:not_found)
    end
  end
end
