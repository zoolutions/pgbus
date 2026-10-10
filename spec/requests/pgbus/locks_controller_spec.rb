# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::LocksController", type: :request do
  describe "GET /pgbus/locks" do
    it "renders the locks index" do
      get "/pgbus/locks"
      expect(response).to have_http_status(:ok)
    end

    context "with more rows than one page in both tables" do
      before do
        @stub_data_source.locks_list = Array.new(30) do |i|
          { lock_key: format("lock_row%02d", i), queue_name: "pgbus_default", msg_id: i, age_seconds: 5 }
        end
        @stub_data_source.concurrency_stats_hash = {
          parked_total: 0, oldest_parked_age_sec: nil, slots_held: 30, keys_at_limit: 0,
          keys: Array.new(30) do |i|
            { key: format("key_row%02d", i), value: 1, max_value: 2, expires_at: Time.now + 60, lease_fresh: true,
              parked_count: 0, oldest_parked_age_sec: nil }
          end
        }
      end

      it "pages the uniqueness locks on page and leaves the concurrency keys on their own page" do
        get "/pgbus/locks", params: { page: 2 }

        expect(@stub_data_source.calls[:job_locks]).to eq([[{ page: 2, per_page: 25 }]])
        expect(@stub_data_source.calls[:concurrency_stats]).to eq([[{ page: 1, per_page: 25 }]])
        expect(response.body).to include("lock_row29", "key_row00")
      end

      it "pages the concurrency keys on keys_page without moving the uniqueness locks" do
        get "/pgbus/locks", params: { keys_page: 2 }

        expect(@stub_data_source.calls[:job_locks]).to eq([[{ page: 1, per_page: 25 }]])
        expect(@stub_data_source.calls[:concurrency_stats]).to eq([[{ page: 2, per_page: 25 }]])
        expect(response.body).to include("lock_row00", "key_row29")
      end

      it "keeps the other table's page in each pager's links" do
        get "/pgbus/locks", params: { page: 2, keys_page: 1 }

        expect(response.body).to include("/pgbus/locks?keys_page=2&amp;page=2", "/pgbus/locks?keys_page=1&amp;page=1")
      end

      it "pages the concurrency frame" do
        get "/pgbus/locks", params: { frame: "concurrency", keys_page: 2 }

        expect(@stub_data_source.calls[:concurrency_stats]).to eq([[{ page: 2, per_page: 25 }]])
        expect(response.body).to include('data-turbo-action="advance"', "key_row29")
      end
    end

    context "with concurrency data" do
      before do
        @stub_data_source.concurrency_stats_hash = {
          parked_total: 7, oldest_parked_age_sec: 812, slots_held: 3, keys_at_limit: 1,
          keys: [{ key: "ProcessOrder-42", value: 1, max_value: 1, expires_at: Time.now + 300,
                   lease_fresh: true, parked_count: 7, oldest_parked_age_sec: 812 }]
        }
      end

      it "renders the concurrency section alongside the uniqueness table" do
        get "/pgbus/locks"

        expect(response).to have_http_status(:ok)
        expect(response.body).to include("ProcessOrder-42")
        expect(response.body).to include("locks-concurrency")
      end

      it "renders only the concurrency frame for a frame request" do
        get "/pgbus/locks", params: { frame: "concurrency" }

        expect(response).to have_http_status(:ok)
        expect(response.body).to include("locks-concurrency")
        expect(response.body).not_to include("bulk-discard-locks-form")
      end
    end
  end

  describe "POST /pgbus/locks/release_key" do
    it "releases the key and redirects with a notice" do
      post "/pgbus/locks/release_key", params: { key: "ProcessOrder-42" }

      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:notice]).to be_present
      expect(@stub_data_source.calls[:release_concurrency_key]).to eq([["ProcessOrder-42"]])
    end

    it "redirects with an alert when the key is blank" do
      post "/pgbus/locks/release_key", params: { key: "" }

      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:alert]).to be_present
      expect(@stub_data_source.calls).not_to have_key(:release_concurrency_key)
    end
  end

  describe "POST /pgbus/locks/discard_parked" do
    it "discards the parked jobs and redirects with a notice" do
      post "/pgbus/locks/discard_parked", params: { key: "ProcessOrder-42" }

      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:notice]).to be_present
      expect(@stub_data_source.calls[:discard_parked_jobs]).to eq([["ProcessOrder-42"]])
    end

    it "redirects with an alert when the key is blank" do
      post "/pgbus/locks/discard_parked", params: { key: "  " }

      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:alert]).to be_present
      expect(@stub_data_source.calls).not_to have_key(:discard_parked_jobs)
    end
  end

  describe "POST /pgbus/locks/:id/discard" do
    it "discards the lock and redirects with a notice" do
      post "/pgbus/locks/some-key/discard"
      expect(response).to redirect_to("/pgbus/locks")
      expect(flash[:notice]).to be_present
      expect(@stub_data_source.calls[:discard_lock]).to eq([["some-key"]])
    end
  end

  describe "POST /pgbus/locks/discard_selected" do
    context "when no keys are selected" do
      it "redirects with an alert" do
        post "/pgbus/locks/discard_selected", params: { lock_keys: [""] }
        expect(response).to redirect_to("/pgbus/locks")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:discard_locks)
      end
    end

    context "when keys are selected" do
      it "discards the selected locks" do
        post "/pgbus/locks/discard_selected", params: { lock_keys: %w[a b] }
        expect(response).to redirect_to("/pgbus/locks")
        expect(@stub_data_source.calls[:discard_locks]).to eq([[%w[a b]]])
      end
    end
  end

  describe "POST /pgbus/locks/discard_all" do
    it "discards all locks and redirects" do
      post "/pgbus/locks/discard_all"
      expect(response).to redirect_to("/pgbus/locks")
      expect(@stub_data_source.calls).to have_key(:discard_all_locks)
    end
  end
end
