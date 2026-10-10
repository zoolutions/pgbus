# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::OutboxController", type: :request do
  describe "GET /pgbus/outbox" do
    it "renders the outbox index" do
      get "/pgbus/outbox"
      expect(response).to have_http_status(:ok)
    end

    context "with more entries than one page" do
      before do
        @stub_data_source.outbox_entries_list = Array.new(30) do |i|
          Pgbus::Test::StubDataSource::SampleData::OutboxRow.new(
            id: 30 - i, routing_key: "orders.row#{i}", queue_name: nil, payload: "{}", priority: 0,
            published_at: nil, created_at: Time.now
          )
        end
        @stub_data_source.outbox_stats_hash = { unpublished: 30, total: 30, oldest_unpublished_age: 5 }
      end

      it "pages the entries and renders the shared pager against the outbox total" do
        get "/pgbus/outbox", params: { page: 2 }

        expect(@stub_data_source.calls[:outbox_entries]).to eq([[{ page: 2, per_page: 25 }]])
        expect(response.body).to include("Showing 26–30 of 30", 'aria-label="Pagination"', "orders.row25")
        expect(response.body).not_to include("orders.row24<")
      end
    end
  end
end
