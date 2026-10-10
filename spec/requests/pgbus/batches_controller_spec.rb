# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::BatchesController", type: :request do
  describe "GET /pgbus/batches" do
    it "renders the batches index" do
      get "/pgbus/batches"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/batches", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end

    context "with more batches than one page" do
      before do
        @stub_data_source.batches_list = Array.new(30) do |i|
          { batch_id: format("%08d-batch", i), description: "Batch row#{i}", status: "processing", total_jobs: 1,
            completed_jobs: 0, failed_jobs: 0, progress_pct: 0, created_at: Time.now }
        end
      end

      it "pages the batches and renders the shared pager" do
        get "/pgbus/batches", params: { page: 2 }

        expect(@stub_data_source.calls[:batches]).to eq([[{ page: 2, per_page: 25 }]])
        expect(response.body).to include("Showing 26–30 of 30", "Batch row29")
        expect(response.body).not_to include("Batch row24<")
      end

      it "pages the auto-refreshed list frame and keeps the page in its src" do
        get "/pgbus/batches", params: { frame: "list", page: 2 }

        expect(@stub_data_source.calls[:batches]).to eq([[{ page: 2, per_page: 25 }]])
        expect(response.body).to include('data-turbo-action="advance"', "page=2")
      end

      it "shows a lower bound when the count is capped" do
        @stub_data_source.capped_lists = [:batches]

        get "/pgbus/batches"

        expect(response.body).to include("Showing 1–25 of 10,000+")
      end
    end
  end

  describe "GET /pgbus/batches/:id" do
    context "when the batch is unknown" do
      it "redirects to the index with a not-found alert" do
        get "/pgbus/batches/does-not-exist"
        expect(response).to redirect_to("/pgbus/batches")
        expect(flash[:alert]).to be_present
      end
    end
  end
end
