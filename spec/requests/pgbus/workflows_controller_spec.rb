# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::WorkflowsController", type: :request do
  let(:samples) { Pgbus::Test::StubDataSource::SampleWorkflows }
  let(:paused_id) { samples::PAUSED_ID }
  let(:group) do
    { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error", error_class: "Timeout::Error" }
  end

  before { @stub_data_source.fill_sample_data! }

  after { Pgbus.configuration.web_actor = nil }

  describe "GET /pgbus/workflows" do
    it "lists the ongoing runs and pages them" do
      get "/pgbus/workflows"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("CheckoutWorkflow", "Needs attention (7)")
      expect(@stub_data_source.calls[:workflows].last).to eq([{ status: "ongoing", page: 1, per_page: 25 }])
    end

    it "passes the status and every filter through, parsed" do
      get "/pgbus/workflows", params: { status: "paused", workflow_class: " CheckoutWorkflow ", step: "capture",
                                        error_class: "Timeout::Error", subject_type: "Order", subject_id: "1050",
                                        workflow_id: "", created_from: "2026-10-01", created_to: "2026-10-17", page: "2" }

      expect(response).to have_http_status(:ok)
      args = @stub_data_source.calls[:workflows].last.first
      expect(args).to include(status: "paused", page: 2, workflow_class: "CheckoutWorkflow", step: "capture",
                              error_class: "Timeout::Error", subject_type: "Order", subject_id: "1050")
      expect(args).not_to have_key(:workflow_id)
      expect(args[:created_from]).to eq(Time.zone.parse("2026-10-01"))
      expect(args[:created_to]).to eq(Time.zone.parse("2026-10-17").end_of_day)
    end

    it "renders only the list frame when asked" do
      get "/pgbus/workflows", params: { frame: "list" }

      expect(response.body).to include('<turbo-frame id="workflows-list"')
      expect(response.body).not_to include("<html")
    end

    it "answers 400 for an unknown status, a bad date or a repeated filter" do
      get "/pgbus/workflows", params: { status: "stuck" }
      expect(response).to have_http_status(:bad_request)
      expect(response.body).to include("unknown status")

      get "/pgbus/workflows", params: { created_from: "last tuesday-ish" }
      expect(response).to have_http_status(:bad_request)

      get "/pgbus/workflows", params: { workflow_class: %w[A B] }
      expect(response).to have_http_status(:bad_request)

      get "/pgbus/workflows", params: { error_class: "x" * 256 }
      expect(response).to have_http_status(:bad_request)
    end
  end

  describe "the list frame" do
    it "does not count the stuck runs it does not show" do
      allow(@stub_data_source).to receive(:workflow_attention_count).and_call_original

      get "/pgbus/workflows", params: { frame: "list" }

      expect(@stub_data_source).not_to have_received(:workflow_attention_count)
    end
  end

  describe "GET /pgbus/workflows/attention" do
    it "renders the needs-attention groups, not a workflow called attention" do
      get "/pgbus/workflows/attention"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("Needs attention", "Paused on an error", "Timeout::Error")
    end
  end

  describe "GET /pgbus/workflows/:workflow_id" do
    it "renders the run" do
      get "/pgbus/workflows/#{paused_id}"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("CheckoutWorkflow", "pgbus-workflow-timeline-data")
    end

    it "answers 404 for an unknown run" do
      get "/pgbus/workflows/nope"

      expect(response).to have_http_status(:not_found)
      expect(response.body).to include("Workflow not found")
    end
  end

  describe "the member actions" do
    {
      "resume" => [:resume_workflow, "Workflow resumed."],
      "cancel" => [:cancel_workflow, "Workflow cancelled."],
      "skip_step" => [:skip_workflow_step, "Step skipped."],
      "run_now" => [:run_workflow_step_now, "The scheduled step runs now."]
    }.each do |action, (method, notice)|
      it "POST #{action} calls #{method} with the dashboard actor and the reason" do
        post "/pgbus/workflows/#{paused_id}/#{action}", params: { reason: "  SMTP fixed  " }

        expect(response).to redirect_to("/pgbus/workflows/#{paused_id}")
        expect(flash[:notice]).to eq(notice)
        expect(@stub_data_source.calls[method].last).to eq([paused_id, { actor: "dashboard", reason: "SMTP fixed" }])
      end
    end

    it "names the operator through web_actor" do
      Pgbus.configuration.web_actor = ->(request) { request.headers["X-User"] }

      post "/pgbus/workflows/#{paused_id}/resume", headers: { "X-User" => "alice" }

      expect(@stub_data_source.calls[:resume_workflow].last).to eq([paused_id, { actor: "dashboard:alice", reason: nil }])
    end

    it "records plain dashboard when web_actor returns nothing or raises" do
      Pgbus.configuration.web_actor = ->(_request) { "  " }
      post "/pgbus/workflows/#{paused_id}/cancel"
      expect(@stub_data_source.calls[:cancel_workflow].last.last[:actor]).to eq("dashboard")

      allow(Pgbus.logger).to receive(:error)
      Pgbus.configuration.web_actor = ->(_request) { raise "session store down" }
      post "/pgbus/workflows/#{paused_id}/cancel"
      expect(@stub_data_source.calls[:cancel_workflow].last.last[:actor]).to eq("dashboard")
      expect(Pgbus.logger).to have_received(:error)
    end

    it "records plain dashboard when web_actor answers something that is not a name" do
      allow(Pgbus.logger).to receive(:warn)
      Pgbus.configuration.web_actor = ->(_request) { Object.new }

      post "/pgbus/workflows/#{paused_id}/cancel"

      expect(@stub_data_source.calls[:cancel_workflow].last.last[:actor]).to eq("dashboard")
      expect(Pgbus.logger).to have_received(:warn).at_least(:once)
    end

    it "flashes the lost race with the run's current status" do
      @stub_data_source.workflow_action_result = { ok: false, code: :not_allowed, status: "ready", reason: "cannot resume" }

      post "/pgbus/workflows/#{paused_id}/resume"

      expect(flash[:alert]).to eq("Not done: the run is ready now.")
    end

    it "flashes a run that is gone, and anything else with its reason" do
      @stub_data_source.workflow_action_result = { ok: false, code: :not_found, reason: "gone" }
      post "/pgbus/workflows/#{paused_id}/cancel"
      expect(flash[:alert]).to eq("That workflow no longer exists.")

      @stub_data_source.workflow_action_result = { ok: false, code: :invalid, reason: "MyWorkflow has no step nope" }
      post "/pgbus/workflows/#{paused_id}/resume_at", params: { step: "nope" }
      expect(flash[:alert]).to eq("Resume at step failed: MyWorkflow has no step nope")
    end

    it "resumes at the chosen step, and asks for one" do
      post "/pgbus/workflows/#{paused_id}/resume_at", params: { step: "receipt" }
      expect(flash[:notice]).to eq("Workflow resumed at receipt.")
      expect(@stub_data_source.calls[:resume_workflow_at].last).to eq([paused_id, { step: "receipt", actor: "dashboard", reason: nil }])

      post "/pgbus/workflows/#{paused_id}/resume_at"
      expect(flash[:alert]).to eq("Choose a step to resume at.")
    end

    it "reruns and redirects to the new run" do
      post "/pgbus/workflows/#{samples::CANCELLED_ID}/rerun"

      expect(response).to redirect_to("/pgbus/workflows/#{samples::RERUN_ID}")
      expect(flash[:notice]).to eq("Started a new run.")
    end

    it "keeps the operator on the run when a rerun is refused" do
      @stub_data_source.workflow_action_result = { ok: false, code: :not_allowed, status: "paused", reason: "x" }

      post "/pgbus/workflows/#{paused_id}/rerun"

      expect(response).to redirect_to("/pgbus/workflows/#{paused_id}")
      expect(flash[:alert]).to eq("Not done: the run is paused now.")
    end
  end

  describe "POST /pgbus/workflows/:workflow_id/signal" do
    let(:path) { "/pgbus/workflows/#{samples::SIGNAL_ID}/signal" }

    it "sends the parsed payload with the form's idempotency key" do
      post path, params: { name: " shipped ", payload: '{"carrier":"DHL"}', idempotency_key: "k-1" }

      expect(flash[:notice]).to eq("Signal shipped sent.")
      expect(@stub_data_source.calls[:signal_workflow].last)
        .to eq([samples::SIGNAL_ID, { name: "shipped", payload: { "carrier" => "DHL" }, idempotency_key: "k-1",
                                      actor: "dashboard", reason: nil }])
    end

    it "sends an empty payload as {}" do
      post path, params: { name: "shipped", payload: "" }

      expect(@stub_data_source.calls[:signal_workflow].last.last[:payload]).to eq({})
    end

    it "sends nothing for invalid JSON, a non-object, an oversized payload or a missing name" do
      post path, params: { name: "shipped", payload: "{nope" }
      expect(flash[:alert]).to eq("Not sent: the payload is not a JSON object.")

      post path, params: { name: "shipped", payload: "[1, 2]" }
      expect(flash[:alert]).to eq("Not sent: the payload is not a JSON object.")

      post path, params: { name: "shipped", payload: %({"x":"#{"a" * Pgbus.configuration.workflow_max_payload_size}"}) }
      expect(flash[:alert]).to start_with("Not sent: the payload is over")

      post path, params: { name: " ", payload: "{}" }
      expect(flash[:alert]).to eq("Not sent: the signal needs a name.")

      expect(@stub_data_source.called?(:signal_workflow)).to be(false)
    end

    it "says when the signal was already delivered" do
      @stub_data_source.workflow_action_result = { ok: true }
      allow(@stub_data_source).to receive(:signal_workflow).and_return(ok: true, duplicate: true)

      post path, params: { name: "shipped", payload: "{}" }

      expect(flash[:notice]).to eq("Signal shipped was already delivered with this key; nothing new was sent.")
    end
  end

  describe "the bulk actions" do
    it "bulk-resumes one group with the actor and reason" do
      @stub_data_source.workflow_bulk_result = { ok: true, done: 212, skipped: 2 }

      post "/pgbus/workflows/bulk_resume", params: { group: group, reason: "fix deployed" }

      expect(response).to redirect_to("/pgbus/workflows/attention")
      expect(flash[:notice]).to eq("Resumed 212; 2 had already moved on.")
      expect(@stub_data_source.calls[:bulk_resume_workflows].last)
        .to eq([{ group: group, actor: "dashboard", reason: "fix deployed" }])
    end

    it "bulk-cancels, a group without an error class too" do
      post "/pgbus/workflows/bulk_cancel", params: { group: group.merge(error_class: "") }

      expect(flash[:notice]).to eq("Cancelled 3.")
      expect(@stub_data_source.calls[:bulk_cancel_workflows].last.first[:group]).to include(error_class: nil)
    end

    it "says how many failed, and why, apart from the ones that moved on" do
      @stub_data_source.workflow_bulk_result = { ok: true, done: 1, skipped: 2, failed: 3,
                                                 failure: "GoneWorkflow has no step capture" }

      post "/pgbus/workflows/bulk_resume", params: { group: group }

      expect(flash[:notice]).to eq("Resumed 1; 2 had already moved on. 3 could not be changed: GoneWorkflow has no step capture")
    end

    it "says when the cap left more runs behind" do
      cap = Pgbus::Web::DataSource::WorkflowActions::BULK_CAP
      @stub_data_source.workflow_bulk_result = { ok: true, done: cap, skipped: 0 }

      post "/pgbus/workflows/bulk_resume", params: { group: group }

      expect(flash[:notice]).to eq("Resumed #{cap}. More runs may be waiting: press again for the next #{cap}.")
    end

    it "refuses a group with an unknown reason or no class" do
      post "/pgbus/workflows/bulk_resume", params: { group: group.merge(reason: "bored") }
      expect(flash[:alert]).to eq("That group is not valid.")

      post "/pgbus/workflows/bulk_cancel", params: { group: group.except(:workflow_class) }
      expect(flash[:alert]).to eq("That group is not valid.")

      expect(@stub_data_source.called?(:bulk_resume_workflows) || @stub_data_source.called?(:bulk_cancel_workflows)).to be(false)
    end

    it "refuses a malformed group instead of failing" do
      post "/pgbus/workflows/bulk_resume", params: { group: "CheckoutWorkflow" }
      expect(response).to redirect_to("/pgbus/workflows/attention")
      expect(flash[:alert]).to eq("That group is not valid.")

      post "/pgbus/workflows/bulk_cancel", params: { group: group.merge(workflow_class: %w[A B]) }
      expect(flash[:alert]).to eq("That group is not valid.")

      expect(@stub_data_source.called?(:bulk_resume_workflows) || @stub_data_source.called?(:bulk_cancel_workflows)).to be(false)
    end

    it "flashes a bulk call that failed" do
      @stub_data_source.workflow_bulk_result = { ok: false, code: :failed, reason: "db down", done: 0, skipped: 0 }

      post "/pgbus/workflows/bulk_cancel", params: { group: group }

      expect(flash[:alert]).to eq("Cancel all failed: db down")
    end
  end
end
