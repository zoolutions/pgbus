# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::QueuesController", type: :request do
  describe "GET /pgbus/queues" do
    it "renders the queues index" do
      get "/pgbus/queues"
      expect(response).to have_http_status(:ok)
    end
  end

  describe "GET /pgbus/queues/:name" do
    context "when the queue exists" do
      it "renders the queue detail" do
        get "/pgbus/queues/pgbus_default"
        expect(response).to have_http_status(:ok)
      end
    end

    context "with the unified job list" do
      def job_rows_call = @stub_data_source.calls[:job_rows].last.first

      it "lists this queue's jobs on the requested tab" do
        get "/pgbus/queues/pgbus_default", params: { state: "ready" }

        expect(job_rows_call).to eq(state: "ready", queue_name: "pgbus_default", queues: nil, exclude: nil, page: 1,
                                    per_page: Pgbus.configuration.web_per_page)
      end

      it "renders only the list frame for frame=list" do
        get "/pgbus/queues/pgbus_default", params: { frame: "list" }

        expect(response).to have_http_status(:ok)
        expect(response.body).to include('<turbo-frame id="jobs-list"')
        expect(response.body).not_to include("<h1")
      end

      it "skips the queue metrics on the list-frame poll" do
        allow(@stub_data_source).to receive(:queue_detail).and_call_original

        get "/pgbus/queues/pgbus_default", params: { frame: "list" }

        expect(@stub_data_source).not_to have_received(:queue_detail)
      end

      it "refreshes the frame from the queue page, never from /pgbus/jobs" do
        get "/pgbus/queues/pgbus_default", params: { state: "ready", page: "2" }

        src = CGI.unescapeHTML(response.body[/data-src="([^"]+)"/, 1])
        expect(src).to include("/pgbus/queues/pgbus_default?", "frame=list", "page=2", "state=ready")
        expect(src).not_to include("/pgbus/jobs")
        expect(src.scan("state=").size).to eq(1)
      end

      it "keeps the state tabs on the queue page" do
        get "/pgbus/queues/pgbus_default"

        tabs = response.body.scan(/<a href="([^"]+)"[^>]*data-state=/).flatten.map { |h| CGI.unescapeHTML(h) }
        expect(tabs).not_to be_empty
        expect(tabs).to all(start_with("/pgbus/queues/pgbus_default"))
      end

      it "renders no job list for a dead-letter queue" do
        get "/pgbus/queues/pgbus_default_dlq"

        expect(response.body).not_to include('<turbo-frame id="jobs-list"')
        expect(response.body).to include("/pgbus/dlq?dlq=pgbus_default_dlq")
        expect(response.body).to include("Dead-letter queue for default — 2 messages.")
        expect(@stub_data_source.calls[:job_rows]).to be_empty
      end
    end

    context "when the queue is unknown" do
      it "redirects to the index with an alert" do
        get "/pgbus/queues/missing"
        expect(response).to redirect_to("/pgbus/queues")
        expect(flash[:alert]).to eq("Queue not found.")
      end
    end
  end

  describe "POST /pgbus/queues/:name/purge" do
    it "purges the queue and redirects to the detail" do
      post "/pgbus/queues/pgbus_default/purge"
      expect(response).to redirect_to("/pgbus/queues/pgbus_default")
      expect(flash[:notice]).to eq("Queue purged.")
      expect(@stub_data_source.calls[:purge_queue]).to eq([["pgbus_default"]])
    end
  end

  describe "POST /pgbus/queues/:name/pause" do
    it "pauses the queue with the given reason" do
      post "/pgbus/queues/pgbus_default/pause", params: { reason: "maintenance" }
      expect(response).to redirect_to("/pgbus/queues/pgbus_default")
      expect(flash[:notice]).to eq("Queue paused.")
      expect(@stub_data_source.calls[:pause_queue]).to eq([%w[pgbus_default maintenance]])
    end
  end

  describe "POST /pgbus/queues/:name/resume" do
    it "resumes the queue" do
      post "/pgbus/queues/pgbus_default/resume"
      expect(response).to redirect_to("/pgbus/queues/pgbus_default")
      expect(flash[:notice]).to eq("Queue resumed.")
      expect(@stub_data_source.calls[:resume_queue]).to eq([["pgbus_default"]])
    end
  end

  describe "DELETE /pgbus/queues/:name" do
    it "drops the queue and redirects to the index" do
      delete "/pgbus/queues/pgbus_default"
      expect(response).to redirect_to("/pgbus/queues")
      expect(@stub_data_source.calls[:drop_queue]).to eq([["pgbus_default"]])
    end
  end

  describe "POST /pgbus/queues/:name/retry_message" do
    it "retries the message and redirects back with a notice" do
      post "/pgbus/queues/pgbus_default/retry_message", params: { msg_id: "42" }
      expect(response).to have_http_status(:redirect)
      expect(@stub_data_source.calls[:retry_job]).to eq([%w[pgbus_default 42]])
    end
  end

  describe "POST /pgbus/queues/:name/discard_message" do
    it "discards the message and redirects back with a notice" do
      post "/pgbus/queues/pgbus_default/discard_message", params: { msg_id: "42" }
      expect(response).to have_http_status(:redirect)
      expect(@stub_data_source.calls[:discard_job]).to eq([%w[pgbus_default 42]])
    end
  end
end
