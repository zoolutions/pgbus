# frozen_string_literal: true

require "system_helper"

# The Workflows dashboard (issue #542): the list with its filters and the
# "what happens next" sentence per state, the show page (timeline, attempts,
# signals, audit trail, actions by state) and the needs-attention view.
RSpec.describe "Workflows", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.zone.now.change(usec: 0) }
  let(:samples) { Pgbus::Test::StubDataSource::SampleWorkflows }

  before do
    travel_to(now)
    @stub_data_source.fill_sample_data!
  end

  after do
    travel_back
    Pgbus.configuration.web_actor = nil
    Pgbus.configuration.workflow_subject_url = nil
  end

  def row_for(id) = find("tr[data-workflow-id='#{id}']")

  describe "the list" do
    it "lists ongoing runs by default, each saying what it does and what happens next" do
      visit "/pgbus/workflows"

      expect(page).to have_css("h1", text: "Workflows")
      expect(page).to have_css("a[aria-current=page]", text: "Ongoing")
      within(row_for(samples::PERFORMING_ID)) do
        expect(page).to have_text("Performing")
        expect(page).to have_text("Performing capture (attempt 2).")
      end
      within(row_for(samples::SIGNAL_ID)) { expect(page).to have_text("Waiting for signal shipped since 3h ago.") }
      within(row_for(samples::FAN_OUT_ID)) do
        expect(page).to have_text("Waiting for batch OrderWorkflow#notify_vendors (7 of 12 jobs done).")
        expect(page).to have_link("OrderWorkflow#notify_vendors", href: "/pgbus/batches/#{samples::BATCH_ID}")
      end
      expect(page).to have_no_css("tr[data-workflow-id='#{samples::FINISHED_ID}']")
    end

    it "says why a run is paused, when a scheduled one runs and that an overdue one is sent again" do
      visit "/pgbus/workflows"

      within(row_for(samples::PAUSED_ID)) do
        expect(page).to have_text("Paused on capture: PaymentGateway::FraudSuspected — resume or cancel.")
        expect(page).to have_text("Order 1042")
      end
      within(row_for(samples::SCHEDULED_ID)) do
        expect(page).to have_text("Scheduled")
        expect(page).to have_text("Scheduled: reminder runs in 2d")
      end
      within(row_for(samples::OVERDUE_ID)) { expect(page).to have_text("Overdue: welcome was due 2h ago; the sweep sends it again.") }
    end

    it "filters by status, and says how finished and cancelled runs ended" do
      visit "/pgbus/workflows"
      click_link "Finished"

      expect(page).to have_css("a[aria-current=page]", text: "Finished")
      within(row_for(samples::FINISHED_ID)) { expect(page).to have_text("Finished 23h ago.") }
      expect(page).to have_no_css("tr[data-workflow-id='#{samples::PAUSED_ID}']")

      click_link "Cancelled"
      within(row_for(samples::CANCELLED_ID)) { expect(page).to have_text("Cancelled 22h ago.") }
    end

    it "combines filters, and keeps them while paging" do
      visit "/pgbus/workflows?status=all"
      fill_in "Workflow class", with: "NightlyExportWorkflow"
      click_button "Apply filters"

      expect(page).to have_css("tr[data-workflow-id]", count: 16)
      expect(page).to have_no_text("CheckoutWorkflow")

      visit "/pgbus/workflows?status=all"
      click_link "Next"
      expect(page).to have_current_path(/page=2/)
      expect(page).to have_current_path(/status=all/)

      visit "/pgbus/workflows?status=paused&workflow_class=CheckoutWorkflow&error_class=Timeout%3A%3AError"
      expect(page).to have_css("tr[data-workflow-id]", count: 3)
      expect(page).to have_field("Error class", with: "Timeout::Error")
      click_link "Clear"
      expect(page).to have_field("Error class", with: "")
    end

    it "shows the empty state" do
      @stub_data_source.workflows_list = []

      visit "/pgbus/workflows"

      expect(page).to have_text("No workflows match.")
    end

    it "links the subject through workflow_subject_url, and only to http(s) or a path" do
      Pgbus.configuration.workflow_subject_url = ->(type, id) { type == "Order" ? "https://shop.example/orders/#{id}" : "javascript:alert(1)" }

      visit "/pgbus/workflows"

      within(row_for(samples::PAUSED_ID)) { expect(page).to have_link("Order 1042", href: "https://shop.example/orders/1042") }
      within(row_for(samples::SCHEDULED_ID)) do
        expect(page).to have_text("User 42")
        expect(page).to have_no_link("User 42")
      end
    end

    it "renders in dark mode" do
      visit_dark "/pgbus/workflows"

      expect(page).to have_css("html.dark")
      expect(page).to have_text("Performing capture (attempt 2).")
    end
  end

  describe "the show page" do
    it "shows the header, the arguments and the audit trail" do
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      expect(page).to have_css("h1", text: "CheckoutWorkflow")
      expect(page).to have_text("Paused on capture: PaymentGateway::FraudSuspected — resume or cancel.")
      within("[data-testid=workflow-arguments]") do
        expect(page).to have_css("dt", text: "currency")
        expect(page).to have_css("dd", text: "EUR")
      end
      within("[data-testid=workflow-events]") do
        expect(page).to have_text("Resumed by dashboard:alice: Gateway is back")
        expect(page).to have_text("Paused by mcp: Holding while the gateway recovers")
        expect(page).to have_text("Started by app")
      end
    end

    it "lists every attempt with its durations, worker, error and backtrace" do
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      within("[data-testid=workflow-steps]") do
        expect(page).to have_css("tr[data-step-attempt]", count: 3)
        within("tr[data-step-attempt='capture-2']") do
          expect(page).to have_text("Failed")
          expect(page).to have_text("PaymentGateway::FraudSuspected")
          expect(page).to have_text(samples::FRAUD)
          expect(page).to have_text("1m 0s") # the reattempt's wait
          expect(page).to have_text("app-1:4101")
          find("summary", text: "Show backtrace").click
          expect(page).to have_text("PaymentGateway::Client#capture")
          expect(page).to have_text("Net::HTTPClientException: 402 Payment Required")
        end
      end
    end

    it "draws the timeline from one series row per attempt and the pauses" do
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      payload = JSON.parse(find("#pgbus-workflow-timeline-data", visible: :all).text(:all))
      expect(payload["rows"].map { |r| r["label"] }).to eq(["reserve #1", "capture #1", "capture #2"])
      expect(payload["paused"].size).to eq(2)
      expect(page).to have_css("#workflow-timeline-chart[role=img]")
      expect(page).to have_css("#workflow-timeline-chart .apexcharts-rangebar-area", wait: 10)
    end

    it "honours dark mode in the timeline" do
      visit_dark "/pgbus/workflows/#{samples::PAUSED_ID}"

      expect(page).to have_css("#workflow-timeline-chart .apexcharts-rangebar-area", wait: 10)
      expect(page.evaluate_script("document.querySelector('#workflow-timeline-chart .apexcharts-canvas') !== null")).to be(true)
      expect(page).to have_css("#workflow-timeline-chart text", text: "capture #2")
    end

    it "offers only the actions the run's state allows, Resume naming what it will do" do
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      expect(page).to have_button("Resume: retry capture")
      expect(page).to have_button("Resume at step")
      expect(page).to have_button("Cancel")
      expect(page).to have_no_button("Run now")
      expect(page).to have_no_button("Skip step")
      expect(page).to have_no_button("Rerun")

      visit "/pgbus/workflows/#{samples::SCHEDULED_ID}"
      expect(page).to have_button("Run now")
      expect(page).to have_no_button(/\AResume/)

      visit "/pgbus/workflows/#{samples::SIGNAL_ID}"
      expect(page).to have_button("Skip step")
      expect(page).to have_select("Signal name", options: ["shipped"])

      visit "/pgbus/workflows/#{samples::FINISHED_ID}"
      expect(page).to have_button("Rerun")
      expect(page).to have_no_button("Cancel")
      expect(page).to have_no_button("Send signal")
    end

    it "resumes with the dashboard actor and the reason, and flashes the outcome" do
      Pgbus.configuration.web_actor = ->(_request) { "alice" }
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      fill_in "Reason (optional)", with: "Fraud check cleared"
      click_button "Resume: retry capture"
      accept_confirm_dialog

      expect(page).to have_text("Workflow resumed.")
      expect(@stub_data_source.calls[:resume_workflow].last)
        .to eq([samples::PAUSED_ID, { actor: "dashboard:alice", reason: "Fraud check cleared" }])
    end

    it "reports a lost race instead of claiming success" do
      @stub_data_source.workflow_action_result = { ok: false, code: :not_allowed, status: "cancelled",
                                                   reason: "cannot cancel a cancelled workflow" }
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      click_button "Cancel"
      accept_confirm_dialog

      expect(page).to have_text("Not done: the run is cancelled now.")
      expect(@stub_data_source.calls[:cancel_workflow].last).to eq([samples::PAUSED_ID, { actor: "dashboard", reason: nil }])
    end

    it "resumes at a chosen step" do
      visit "/pgbus/workflows/#{samples::PAUSED_ID}"

      select "receipt", from: "Step to resume at"
      click_button "Resume at step"
      accept_confirm_dialog

      expect(page).to have_text("Workflow resumed at receipt.")
      expect(@stub_data_source.calls[:resume_workflow_at].last)
        .to eq([samples::PAUSED_ID, { step: "receipt", actor: "dashboard", reason: nil }])
    end

    it "runs a scheduled step now and skips a waiting one" do
      visit "/pgbus/workflows/#{samples::SCHEDULED_ID}"
      click_button "Run now"
      accept_confirm_dialog
      expect(page).to have_text("The scheduled step runs now.")
      expect(@stub_data_source.calls[:run_workflow_step_now].last.first).to eq(samples::SCHEDULED_ID)

      visit "/pgbus/workflows/#{samples::SIGNAL_ID}"
      click_button "Skip step"
      accept_confirm_dialog
      expect(page).to have_text("Step skipped.")
      expect(@stub_data_source.calls[:skip_workflow_step].last.first).to eq(samples::SIGNAL_ID)
    end

    it "sends a signal with a JSON payload, and sends nothing for invalid JSON" do
      visit "/pgbus/workflows/#{samples::SIGNAL_ID}"

      fill_in "Payload (JSON)", with: "{not json"
      click_button "Send signal"
      accept_confirm_dialog
      expect(page).to have_text("Not sent: the payload is not a JSON object.")
      expect(@stub_data_source.called?(:signal_workflow)).to be(false)

      fill_in "Payload (JSON)", with: '{"carrier": "DHL"}'
      click_button "Send signal"
      accept_confirm_dialog
      expect(page).to have_text("Signal shipped sent.")
      id, args = @stub_data_source.calls[:signal_workflow].last
      expect(id).to eq(samples::SIGNAL_ID)
      expect(args).to include(name: "shipped", payload: { "carrier" => "DHL" }, actor: "dashboard")
      expect(args[:idempotency_key]).to match(/\A\h{8}-\h{4}-\h{4}-\h{4}-\h{12}\z/)
    end

    it "reruns a finished run and opens the new one" do
      visit "/pgbus/workflows/#{samples::CANCELLED_ID}"

      expect(page).to have_link(samples::RERUN_ID[0, 8], href: "/pgbus/workflows/#{samples::RERUN_ID}")
      click_button "Rerun"
      accept_confirm_dialog

      expect(page).to have_text("Started a new run.")
      expect(page).to have_current_path("/pgbus/workflows/#{samples::RERUN_ID}")
    end

    it "links a rerun back to the run it repeats" do
      visit "/pgbus/workflows/#{samples::RERUN_ID}"

      expect(page).to have_text("Rerun of")
      expect(page).to have_link(samples::CANCELLED_ID[0, 8], href: "/pgbus/workflows/#{samples::CANCELLED_ID}")
    end

    it "says when a run started on an older definition" do
      visit "/pgbus/workflows/#{samples::STALLED_ID}"

      expect(page).to have_text("This run started on an older definition of ImportWorkflow")
    end

    it "lists the run's signals" do
      visit "/pgbus/workflows/#{samples::SIGNAL_ID}"

      within("[data-testid=workflow-signals]") do
        expect(page).to have_text("label_printed")
        expect(page).to have_text("Consumed")
        expect(page).to have_text("address_changed")
        expect(page).to have_text("Pending")
        expect(page).to have_text("evt_9f2c41")
      end
    end

    it "answers 404 for an unknown run" do
      visit "/pgbus/workflows/nope"

      expect(page).to have_text("Workflow not found")
    end

    it "renders in dark mode" do
      visit_dark "/pgbus/workflows/#{samples::FAN_OUT_ID}"

      expect(page).to have_css("html.dark")
      expect(page).to have_text("Waiting for batch OrderWorkflow#notify_vendors (7 of 12 jobs done).")
    end
  end

  describe "the needs-attention view" do
    it "groups stuck runs with counts, first/last seen and the newest error" do
      visit "/pgbus/workflows/attention"

      expect(page).to have_css("h1", text: "Needs attention")
      group = find("tr", text: "Timeout::Error")
      within(group) do
        expect(page).to have_text("CheckoutWorkflow")
        expect(page).to have_text("capture")
        expect(page).to have_text("Paused on an error")
        expect(page).to have_text("3")
        expect(page).to have_text("execution expired")
        expect(page).to have_text("20m ago")
        expect(page).to have_link("Show runs")
      end
      within("tr", text: "waiting_overdue".then { "Waiting too long" }) { expect(page).to have_no_button(/Resume all/) }
    end

    it "bulk-resumes a group and reports the real outcome" do
      @stub_data_source.workflow_bulk_result = { ok: true, done: 2, skipped: 1 }
      visit "/pgbus/workflows/attention"

      within(find("tr", text: "Timeout::Error")) do
        fill_in "Reason (optional)", with: "Gateway timeout raised"
        click_button "Resume all 3"
      end
      accept_confirm_dialog

      expect(page).to have_text("Resumed 2; 1 had already moved on.")
      expect(@stub_data_source.calls[:bulk_resume_workflows].last.first)
        .to eq(group: { workflow_class: "CheckoutWorkflow", step_name: "capture", reason: "paused_error",
                        error_class: "Timeout::Error" },
               actor: "dashboard", reason: "Gateway timeout raised")
    end

    it "bulk-cancels a group" do
      visit "/pgbus/workflows/attention"

      within(find("tr", text: "Waiting too long")) { click_button "Cancel all 1" }
      accept_confirm_dialog

      expect(page).to have_text("Cancelled 1.")
    end

    it "links each group to the filtered list" do
      visit "/pgbus/workflows/attention"

      within(find("tr", text: "Timeout::Error")) { click_link "Show runs" }

      expect(page).to have_css("tr[data-workflow-id]", count: 3)
      expect(page).to have_css("a[aria-current=page]", text: "Paused")
    end

    it "shows the empty state" do
      @stub_data_source.workflow_attention_groups_list = []

      visit "/pgbus/workflows/attention"

      expect(page).to have_text("Nothing needs attention.")
    end

    it "renders in dark mode" do
      visit_dark "/pgbus/workflows/attention"

      expect(page).to have_css("html.dark")
      expect(page).to have_text("Paused on an error")
    end
  end

  describe "the home page" do
    it "counts ongoing, paused and stuck runs, linking to the list and the attention view" do
      visit "/pgbus"

      card = find("[data-testid=workflows-card]")
      within(card) do
        expect(page).to have_text("Workflows")
        expect(page).to have_text("11")
        expect(page).to have_text("5 paused")
        expect(page).to have_link("7 need attention", href: "/pgbus/workflows/attention")
      end
      expect(page).to have_link("Workflows", href: "/pgbus/workflows")
    end
  end
end
