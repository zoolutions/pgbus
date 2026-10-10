# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::EventsController", type: :request do
  describe "GET /pgbus/events" do
    let(:now) { Time.now.utc }

    def event_rows_call = @stub_data_source.calls[:event_rows].last.first

    it "renders the events index with the pending list and the processed audit" do
      get "/pgbus/events"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include('<turbo-frame id="events-list"', '<turbo-frame id="processed-events"')
      expect(event_rows_call).to include(state: nil, queue_name: nil, page: 1)
    end

    %w[ready scheduled running retrying].each do |state|
      it "filters the pending list by state=#{state}" do
        get "/pgbus/events", params: { state: state }

        expect(event_rows_call).to include(state: state)
      end
    end

    it "falls back to All for an unknown state" do
      get "/pgbus/events", params: { state: "blocked" }

      expect(event_rows_call).to include(state: nil)
    end

    it "passes a queue filter through" do
      get "/pgbus/events", params: { queue: "pgbus_orders_handler" }

      expect(event_rows_call).to include(queue_name: "pgbus_orders_handler")
    end

    it "renders only the pending list for frame=list" do
      get "/pgbus/events", params: { frame: "list", state: "ready" }

      expect(response.body).to include('<turbo-frame id="events-list"')
      expect(response.body).not_to include("<h1", 'id="processed-events"')
    end

    it "renders only the processed audit for frame=processed" do
      get "/pgbus/events", params: { frame: "processed", processed_page: "2" }

      expect(response.body).to include('<turbo-frame id="processed-events"')
      expect(response.body).not_to include("<h1", 'id="events-list"')
      expect(@stub_data_source.calls[:processed_events].last.first).to include(page: 2)
      expect(@stub_data_source.calls).not_to have_key(:event_rows)
    end
  end

  describe "GET /pgbus/events (issue #431 context card on pending events)" do
    let(:now) { Time.now.utc }

    it "renders a Context section from the envelope's persisted Current attributes" do
      message = { "event_id" => "evt-ctx-1", "payload" => { "order_id" => 1 },
                  "published_at" => "2026-08-23T00:00:00Z",
                  "pgbus_current" => { "Current" => { "tenant" => { "_aj_globalid" => "gid://app/Tenant/42" },
                                                      "request_id" => "req-9", "_aj_symbol_keys" => %w[tenant request_id] } } }
      @stub_data_source.event_rows_list = [event_row("ready", msg_id: 42, payload: JSON.generate(message))]

      get "/pgbus/events"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("data-testid=\"job-context\"")
      expect(response.body).to include("gid://app/Tenant/42")
      expect(response.body).to include("req-9")
    end

    it "renders no Context section for an untagged event" do
      @stub_data_source.event_rows_list = [event_row("ready", msg_id: 43)]

      get "/pgbus/events"

      expect(response).to have_http_status(:ok)
      expect(response.body).not_to include("data-testid=\"job-context\"")
    end
  end

  describe "GET /pgbus/events/:id" do
    it "shows the processed event's state and replay availability" do
      @stub_data_source.events_list = [{ "id" => 5, "event_id" => "evt-5", "handler_class" => "OrderHandler",
                                         "processed_at" => Time.now.utc - 60, "completed_at" => Time.now.utc - 59 }]

      get "/pgbus/events/5"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("evt-5", "Completed", "Replay")
    end
  end

  describe "POST /pgbus/events/:id/replay" do
    context "when the processed event exists" do
      before { @stub_data_source.events_list = [{ "id" => "5", "event_type" => "orders.created" }] }

      it "replays the event" do
        post "/pgbus/events/5/replay"
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:notice]).to be_present
        expect(@stub_data_source.calls).to have_key(:replay_event)
      end
    end

    context "when the processed event is unknown" do
      it "redirects with a failure alert" do
        post "/pgbus/events/999/replay"
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:replay_event)
      end
    end
  end

  describe "POST /pgbus/events/:id/discard" do
    context "when the queue is not a registered handler queue" do
      it "rejects with an alert and does not touch the data source" do
        post "/pgbus/events/5/discard", params: { queue_name: "pgbus_arbitrary" }
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
        expect(@stub_data_source.calls).not_to have_key(:discard_event)
      end
    end

    context "when the queue is a registered handler queue" do
      before do
        @stub_data_source.subscribers_list = [
          { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
        ]
      end

      it "discards the event" do
        post "/pgbus/events/5/discard", params: { queue_name: "pgbus_orders" }
        expect(response).to redirect_to("/pgbus/events")
        expect(@stub_data_source.calls[:discard_event]).to eq([%w[pgbus_orders 5]])
      end
    end
  end

  describe "POST /pgbus/events/:id/mark_handled" do
    before do
      @stub_data_source.subscribers_list = [
        { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
      ]
    end

    it "resolves the handler class server-side and marks the event handled" do
      post "/pgbus/events/5/mark_handled", params: { queue_name: "pgbus_orders", handler_class: "Evil" }
      expect(response).to redirect_to("/pgbus/events")
      expect(@stub_data_source.calls[:mark_event_handled]).to eq([%w[pgbus_orders 5 OrdersHandler]])
    end
  end

  describe "POST /pgbus/events/:id/reroute" do
    before do
      @stub_data_source.subscribers_list = [
        { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" },
        { physical_queue_name: "pgbus_audit", handler_class: "AuditHandler" }
      ]
    end

    it "reroutes between two registered queues" do
      post "/pgbus/events/5/reroute", params: { queue_name: "pgbus_orders", target_queue: "pgbus_audit" }
      expect(response).to redirect_to("/pgbus/events")
      expect(@stub_data_source.calls[:reroute_event]).to eq([%w[pgbus_orders 5 pgbus_audit]])
    end

    it "rejects rerouting to an unregistered target queue" do
      post "/pgbus/events/5/reroute", params: { queue_name: "pgbus_orders", target_queue: "pgbus_evil" }
      expect(response).to redirect_to("/pgbus/events")
      expect(flash[:alert]).to be_present
      expect(@stub_data_source.calls).not_to have_key(:reroute_event)
    end
  end

  describe "POST /pgbus/events/discard_selected" do
    context "when no valid selections" do
      it "redirects with an alert" do
        post "/pgbus/events/discard_selected", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/events")
        expect(flash[:alert]).to be_present
      end
    end

    context "when selections reference registered queues" do
      before do
        @stub_data_source.subscribers_list = [
          { physical_queue_name: "pgbus_orders", handler_class: "OrdersHandler" }
        ]
      end

      it "discards the selected events" do
        post "/pgbus/events/discard_selected",
             params: { messages: [{ queue_name: "pgbus_orders", msg_id: "9" }] }
        expect(response).to redirect_to("/pgbus/events")
        expect(@stub_data_source.calls[:discard_selected_events]).to eq([[[{ queue_name: "pgbus_orders", msg_id: "9" }]]])
      end
    end
  end
end
