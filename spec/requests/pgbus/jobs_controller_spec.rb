# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::JobsController", type: :request do
  describe "GET /pgbus/jobs" do
    def job_rows_call = @stub_data_source.calls[:job_rows].last.first

    it "renders the unified job list on the All tab" do
      get "/pgbus/jobs"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include('<turbo-frame id="jobs-list"')
      expect(job_rows_call).to include(state: nil, page: 1)
    end

    %w[ready scheduled running retrying blocked].each do |state|
      it "filters the list by state=#{state}" do
        get "/pgbus/jobs", params: { state: state }

        expect(response).to have_http_status(:ok)
        expect(job_rows_call).to include(state: state)
      end
    end

    it "leaves the EventBus handler queues to the Events page" do
      @stub_data_source.subscribers_list = [{ pattern: "orders.#", handler_class: "OrderHandler",
                                              queue_name: "orders_handler", physical_queue_name: "pgbus_orders_handler" }]

      get "/pgbus/jobs"

      expect(job_rows_call).to include(exclude: ["pgbus_orders_handler"])
      expect(@stub_data_source.calls[:job_state_counts].last.first).to include(exclude: ["pgbus_orders_handler"])
    end

    it "keeps a handler queue's messages on its own queue filter" do
      get "/pgbus/jobs", params: { queue: "pgbus_orders_handler" }

      expect(job_rows_call).to include(queue_name: "pgbus_orders_handler", exclude: nil)
    end

    it "treats the old status=failed link as the Retrying tab" do
      get "/pgbus/jobs", params: { status: "failed" }

      expect(job_rows_call).to include(state: "retrying")
    end

    it "falls back to All for an unknown state" do
      get "/pgbus/jobs", params: { state: "exploded" }

      expect(job_rows_call).to include(state: nil)
    end

    it "renders only the list frame for frame=list" do
      get "/pgbus/jobs", params: { frame: "list", state: "ready" }

      expect(response).to have_http_status(:ok)
      expect(response.body).to include('<turbo-frame id="jobs-list"')
      expect(response.body).not_to include("<h1")
    end

    it "keeps the page, state and queue in the auto-refresh source" do
      get "/pgbus/jobs", params: { state: "ready", page: "2", queue: "pgbus_default" }

      expect(job_rows_call).to include(state: "ready", page: 2, queue_name: "pgbus_default")
      src = response.body[/data-src="([^"]+)"/, 1]
      expect(CGI.unescapeHTML(src)).to include("frame=list", "page=2", "queue=pgbus_default", "state=ready")
    end
  end

  describe "redirects after acting from the queue page" do
    let(:referer) { { "HTTP_REFERER" => "http://www.example.com/pgbus/queues/pgbus_default" } }

    {
      "retry" => ["/pgbus/jobs/7/retry", {}],
      "discard" => ["/pgbus/jobs/7/discard", {}],
      "discard_all_enqueued" => ["/pgbus/jobs/discard_all_enqueued", {}],
      "discard_selected_enqueued" => ["/pgbus/jobs/discard_selected_enqueued",
                                      { messages: [{ queue_name: "pgbus_default", msg_id: "9" }] }]
    }.each do |action, (path, params)|
      it "#{action} lands back on the page it came from" do
        post path, params: params, headers: referer

        expect(response).to redirect_to("http://www.example.com/pgbus/queues/pgbus_default")
      end

      it "#{action} falls back to the Jobs page without a referer" do
        post path, params: params

        expect(response).to redirect_to("/pgbus/jobs")
      end
    end
  end

  describe "acting from a job's own detail page" do
    %w[retry discard].each do |action|
      it "#{action} returns to the Jobs list, not the detail page it just removed" do
        post "/pgbus/jobs/7/#{action}", headers: { "HTTP_REFERER" => "http://www.example.com/pgbus/jobs/7" }

        expect(response).to redirect_to("/pgbus/jobs")
      end
    end
  end

  describe "GET /pgbus/jobs/:id (issue #430 context card)" do
    let(:base_event) do
      { "id" => 7, "queue_name" => "default", "failed_at" => "2026-08-23 10:00", "error_class" => "RuntimeError",
        "error_message" => "boom", "retry_count" => 1, "backtrace" => nil, "msg_id" => 99 }
    end

    it "renders a Context card from persisted Current attributes" do
      payload = { "job_class" => "ReportJob", "arguments" => [],
                  "pgbus_current" => { "Current" => { "tenant" => { "_aj_globalid" => "gid://app/Tenant/42" },
                                                      "request_id" => "req-1", "_aj_symbol_keys" => %w[tenant request_id] } } }
      @stub_data_source.failed_events_list = [base_event.merge("payload" => JSON.generate(payload))]

      get "/pgbus/jobs/7"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("Context")
      expect(response.body).to include("Current")
      expect(response.body).to include("gid://app/Tenant/42")
      expect(response.body).to include("req-1")
    end

    it "renders no Context card when the payload has none" do
      @stub_data_source.failed_events_list = [base_event.merge("payload" => JSON.generate("job_class" => "ReportJob"))]

      get "/pgbus/jobs/7"

      expect(response).to have_http_status(:ok)
      expect(response.body).not_to include("data-testid=\"job-context\"")
    end
  end

  describe "POST /pgbus/jobs/:id/retry" do
    it "re-enqueues the failed job" do
      post "/pgbus/jobs/7/retry"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(flash[:notice]).to eq("Job re-enqueued.")
      expect(@stub_data_source.calls[:retry_failed_event]).to eq([["7"]])
    end
  end

  describe "POST /pgbus/jobs/:id/discard" do
    it "discards the failed job" do
      post "/pgbus/jobs/7/discard"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(flash[:notice]).to eq("Job discarded.")
      expect(@stub_data_source.calls[:discard_failed_event]).to eq([["7"]])
    end
  end

  describe "POST /pgbus/jobs/retry_all" do
    it "re-enqueues all failed jobs" do
      post "/pgbus/jobs/retry_all"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:retry_all_failed)
    end
  end

  describe "POST /pgbus/jobs/discard_all" do
    it "discards all failed jobs" do
      post "/pgbus/jobs/discard_all"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:discard_all_failed)
    end
  end

  describe "POST /pgbus/jobs/discard_all_enqueued" do
    it "discards all enqueued jobs" do
      post "/pgbus/jobs/discard_all_enqueued"
      expect(response).to redirect_to("/pgbus/jobs")
      expect(@stub_data_source.calls).to have_key(:discard_all_enqueued)
    end
  end

  describe "POST /pgbus/jobs/discard_selected_failed" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/jobs/discard_selected_failed", params: { ids: ["0"] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(flash[:alert]).to be_present
      end
    end

    context "when ids are selected" do
      it "discards each selected failed event" do
        post "/pgbus/jobs/discard_selected_failed", params: { ids: %w[3 4] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(@stub_data_source.calls[:discard_failed_event]).to eq([[3], [4]])
      end
    end
  end

  describe "POST /pgbus/jobs/discard_selected_enqueued" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/jobs/discard_selected_enqueued", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(flash[:alert]).to be_present
      end
    end

    context "when selections are present" do
      it "discards each selected enqueued message" do
        post "/pgbus/jobs/discard_selected_enqueued",
             params: { messages: [{ queue_name: "pgbus_default", msg_id: "9" }] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(@stub_data_source.calls[:discard_job]).to eq([%w[pgbus_default 9]])
      end
    end

    context "when failed-event rows are selected from the unified list" do
      it "discards them through their failed event" do
        post "/pgbus/jobs/discard_selected_enqueued",
             params: { ids: %w[3], messages: [{ queue_name: "pgbus_default", msg_id: "9" }] }
        expect(response).to redirect_to("/pgbus/jobs")
        expect(@stub_data_source.calls[:discard_failed_event]).to eq([[3]])
        expect(@stub_data_source.calls[:discard_job]).to eq([%w[pgbus_default 9]])
        expect(flash[:notice]).to eq("Discarded 2 selected items.")
      end
    end
  end
end
