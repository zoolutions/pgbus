# frozen_string_literal: true

require "rails_helper"

# The workflow insights pages (issue #549): the class list and one class's
# page, the window parameter and the class-name route constraint.
RSpec.describe "Pgbus::WorkflowDefinitionsController", type: :request do
  before { @stub_data_source.fill_sample_data! }

  describe "GET /pgbus/workflows/definitions" do
    it "lists every class over the last 24 hours by default" do
      get "/pgbus/workflows/definitions"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("OrderWorkflow", "Billing::InvoiceWorkflow")
      expect(@stub_data_source.calls[:workflow_definitions].last).to eq([{ window: "24h" }])
    end

    it "passes the window asked for" do
      get "/pgbus/workflows/definitions", params: { window: "7d" }

      expect(@stub_data_source.calls[:workflow_definitions].last).to eq([{ window: "7d" }])
    end

    it "answers 400 for an unknown window" do
      get "/pgbus/workflows/definitions", params: { window: "90d" }

      expect(response).to have_http_status(:bad_request)
      expect(response.body).to include("unknown window")
    end

    it "is not read as a workflow_id" do
      expect(Pgbus::Engine.routes.recognize_path("/workflows/definitions", method: :get))
        .to include(controller: "pgbus/workflow_definitions", action: "index")
    end
  end

  describe "GET /pgbus/workflows/definitions/:workflow_class" do
    it "shows one class with the window asked for" do
      get "/pgbus/workflows/definitions/OrderWorkflow", params: { window: "1h" }

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("OrderWorkflow", "pgbus-workflow-insights-data")
      expect(@stub_data_source.calls[:workflow_definition].last).to eq(["OrderWorkflow", { window: "1h" }])
    end

    it "takes a namespaced class name" do
      get "/pgbus/workflows/definitions/Billing::InvoiceWorkflow"

      expect(response).to have_http_status(:ok)
      expect(@stub_data_source.calls[:workflow_definition].last.first).to eq("Billing::InvoiceWorkflow")
    end

    it "answers 404 for a class nothing knows" do
      get "/pgbus/workflows/definitions/NopeWorkflow"

      expect(response).to have_http_status(:not_found)
      expect(response.body).to include("Unknown workflow class")
    end

    %w[orderWorkflow Order-Workflow Billing:::Order ::OrderWorkflow].each do |name|
      it "answers 404 for the malformed class name #{name}, without asking the data source" do
        get "/pgbus/workflows/definitions/#{ERB::Util.url_encode(name)}"

        expect(response).to have_http_status(:not_found)
        expect(@stub_data_source.calls[:workflow_definition]).to be_empty
      end
    end

    it "answers 400 for an unknown window" do
      get "/pgbus/workflows/definitions/OrderWorkflow", params: { window: ["24h"] }

      expect(response).to have_http_status(:bad_request)
    end
  end
end
