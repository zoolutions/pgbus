# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::DeadLetterController", type: :request do
  describe "GET /pgbus/dlq" do
    it "renders the dead letter index" do
      get "/pgbus/dlq"
      expect(response).to have_http_status(:ok)
    end

    it "renders the list turbo frame" do
      get "/pgbus/dlq", params: { frame: "list" }
      expect(response).to have_http_status(:ok)
    end

    context "with filters (issue #495)" do
      let(:card_error) { { error_class: "Stripe::CardError", error_message: "declined", retry_count: 4 } }

      before do
        dead = lambda do |error|
          Pgbus::DeadLetterHeader.build(existing: nil, reason: "max_retries_exceeded", source: "worker",
                                        source_queue: "pgbus_default", attempts: 6, max_retries: 5, error: error)
        end
        @stub_data_source.dlq_messages_list = [
          { msg_id: 1, queue_name: "pgbus_default_dlq", message: '{"job_class":"PayJob"}', headers: dead.call(card_error) },
          { msg_id: 2, queue_name: "pgbus_orders_dlq", message: '{"job_class":"ShipJob"}', headers: dead.call(nil) }
        ]
      end

      it "filters by DLQ" do
        get "/pgbus/dlq", params: { dlq: "pgbus_orders_dlq" }

        expect(response.body).to include('data-dlq-row="2"')
        expect(response.body).not_to include('data-dlq-row="1"')
      end

      it "filters by error class" do
        get "/pgbus/dlq", params: { error_class: "Stripe::CardError" }

        expect(response.body).to include('data-dlq-row="1"')
        expect(response.body).not_to include('data-dlq-row="2"')
      end

      it "ignores a dlq param that is not a DLQ" do
        get "/pgbus/dlq", params: { dlq: "pgbus_default" }

        expect(response).to have_http_status(:ok)
        expect(response.body).to include('data-dlq-row="1"', 'data-dlq-row="2"')
      end

      it "ignores an error_class longer than 200 characters" do
        get "/pgbus/dlq", params: { error_class: "X" * 201 }

        expect(response).to have_http_status(:ok)
        expect(response.body).to include('data-dlq-row="1"', 'data-dlq-row="2"')
      end

      it "keeps the filters in the list frame and its source" do
        get "/pgbus/dlq", params: { frame: "list", dlq: "pgbus_default_dlq", error_class: "Stripe::CardError" }

        expect(response).to have_http_status(:ok)
        expect(response.body).to include('data-dlq-row="1"')
        expect(response.body).to match(/data-src="[^"]*dlq=pgbus_default_dlq[^"]*"/)
        expect(response.body).to match(/data-src="[^"]*error_class=Stripe%3A%3ACardError[^"]*"/)
      end
    end
  end

  describe "GET /pgbus/dlq/:id" do
    context "when the message exists" do
      before { @stub_data_source.dlq_messages_list = [{ msg_id: 12, message: {} }] }

      it "renders the message detail" do
        get "/pgbus/dlq/12"
        expect(response).to have_http_status(:ok)
      end

      it "shows the message of the DLQ named by queue_name, not the first id match" do
        @stub_data_source.dlq_messages_list = [
          { msg_id: 12, queue_name: "pgbus_default_dlq", message: '{"job_class":"DefaultJob"}' },
          { msg_id: 12, queue_name: "pgbus_orders_dlq", message: '{"job_class":"OrdersJob"}' }
        ]

        get "/pgbus/dlq/12", params: { queue_name: "pgbus_orders_dlq" }

        expect(response.body).to include("OrdersJob")
        expect(response.body).not_to include("DefaultJob")
      end

      it "explains a legacy message without a recorded reason" do
        get "/pgbus/dlq/12"
        expect(response.body).to include('data-testid="dead-letter-reason"', "Reason not recorded")
      end
    end

    context "when the message carries persisted Current attributes (issue #430)" do
      before do
        message = JSON.generate("job_class" => "ReportJob",
                                "pgbus_current" => { "Current" => { "tenant" => "acme", "_aj_symbol_keys" => ["tenant"] } })
        @stub_data_source.dlq_messages_list = [{ msg_id: 12, message: message, queue_name: "pgbus_default_dlq" }]
      end

      it "renders a Context card" do
        get "/pgbus/dlq/12"
        expect(response).to have_http_status(:ok)
        expect(response.body).to include("data-testid=\"job-context\"")
        expect(response.body).to include("acme")
      end
    end
  end

  describe "POST /pgbus/dlq/:id/retry" do
    context "with a valid _dlq queue" do
      it "re-enqueues the message" do
        post "/pgbus/dlq/12/retry", params: { queue_name: "pgbus_default_dlq" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:retry_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end

    context "with a non-DLQ queue" do
      it "rejects with an alert and does not touch the data source" do
        post "/pgbus/dlq/12/retry", params: { queue_name: "pgbus_default" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to eq("Invalid DLQ queue.")
        expect(@stub_data_source.calls).not_to have_key(:retry_dlq_message)
      end
    end
  end

  describe "POST /pgbus/dlq/:id/discard" do
    context "with a valid _dlq queue" do
      it "discards the message" do
        post "/pgbus/dlq/12/discard", params: { queue_name: "pgbus_default_dlq" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:discard_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end

    context "with a non-DLQ queue" do
      it "rejects with an alert" do
        post "/pgbus/dlq/12/discard", params: { queue_name: "pgbus_default" }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to eq("Invalid DLQ queue.")
        expect(@stub_data_source.calls).not_to have_key(:discard_dlq_message)
      end
    end
  end

  describe "POST /pgbus/dlq/retry_all" do
    it "re-enqueues all DLQ messages" do
      post "/pgbus/dlq/retry_all"
      expect(response).to redirect_to("/pgbus/dlq")
      expect(@stub_data_source.calls).to have_key(:retry_all_dlq)
    end
  end

  describe "POST /pgbus/dlq/discard_all" do
    it "discards all DLQ messages" do
      post "/pgbus/dlq/discard_all"
      expect(response).to redirect_to("/pgbus/dlq")
      expect(@stub_data_source.calls).to have_key(:discard_all_dlq)
    end
  end

  describe "POST /pgbus/dlq/discard_selected" do
    context "when none selected" do
      it "redirects with an alert" do
        post "/pgbus/dlq/discard_selected", params: { messages: [{ queue_name: "", msg_id: "" }] }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(flash[:alert]).to be_present
      end
    end

    context "when a valid _dlq selection is present" do
      it "discards the selected DLQ message" do
        post "/pgbus/dlq/discard_selected",
             params: { messages: [{ queue_name: "pgbus_default_dlq", msg_id: "12" }] }
        expect(response).to redirect_to("/pgbus/dlq")
        expect(@stub_data_source.calls[:discard_dlq_message]).to eq([%w[pgbus_default_dlq 12]])
      end
    end
  end
end
