# frozen_string_literal: true

require "system_helper"

RSpec.describe "Dead Letter Queue", type: :system do
  it "shows empty state" do
    visit "/pgbus/dlq"

    expect(page).to have_css("h1", text: "Dead Letter Queue")
    expect(page).to have_text("Dead letter queue is empty")
  end

  context "with DLQ messages" do
    before do
      @stub_data_source.dlq_messages_list = [
        { msg_id: 99, queue_name: "pgbus_default_dlq", read_ct: 6,
          enqueued_at: Time.now.utc.iso8601, message: '{"job_class":"FailedJob"}' }
      ]
    end

    it "displays DLQ messages with total count" do
      visit "/pgbus/dlq"

      expect(page).to have_text("99")
      expect(page).to have_text("pgbus_default")
      expect(page).to have_text("FailedJob")
      expect(page).to have_text("Showing 1–1 of 1")
    end

    it "shows bulk action buttons" do
      visit "/pgbus/dlq"

      expect(page).to have_button("Retry All")
      expect(page).to have_button("Discard All")
    end

    it "shows per-message actions" do
      visit "/pgbus/dlq"

      expect(page).to have_button("Retry")
      expect(page).to have_button("Discard")
    end

    it "shows checkboxes for DLQ messages" do
      visit "/pgbus/dlq"

      expect(page).to have_css("input[data-bulk-item]", count: 1)
      expect(page).to have_css("input[data-bulk-select-all]")
    end

    it "retry DLQ message: no confirm, shows toast" do
      visit "/pgbus/dlq"

      find("details[data-job-toggle] summary").click
      click_button "Retry"

      expect(page).to have_toast("re-enqueued")
      expect(@stub_data_source).to be_called(:retry_dlq_message)
    end

    it "discard DLQ message: confirm and shows toast" do
      visit "/pgbus/dlq"

      find("details[data-job-toggle] summary").click
      click_button "Discard"
      accept_confirm_dialog

      expect(page).to have_toast("discarded")
      expect(@stub_data_source).to be_called(:discard_dlq_message)
    end

    it "retry all: confirm and shows toast" do
      visit "/pgbus/dlq"

      click_button "Retry All"
      accept_confirm_dialog

      expect(page).to have_toast("Re-enqueued")
      expect(@stub_data_source).to be_called(:retry_all_dlq)
    end

    it "discard all: confirm and shows toast" do
      visit "/pgbus/dlq"

      click_button "Discard All"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_all_dlq)
    end
  end

  context "when every message says why it died (issue #495)" do
    let(:now) { Time.now.utc }
    let(:card_error) do
      { error_class: "Stripe::CardError", error_message: "Your card was declined", retry_count: 2,
        backtrace: "app/jobs/process_payment_job.rb:14:in 'charge'\napp/jobs/process_payment_job.rb:6:in 'perform'",
        failed_at: (now - 60).iso8601 }
    end

    def dead(source:, error: nil, existing: nil, queue: "pgbus_default")
      Pgbus::DeadLetterHeader.build(existing: existing, reason: "max_retries_exceeded", source: source,
                                    source_queue: queue, attempts: 6, max_retries: 5, error: error, now: now)
    end

    before do
      @stub_data_source.dlq_messages_list = [
        { msg_id: 301, queue_name: "pgbus_default_dlq", read_ct: 0, enqueued_at: now - 120,
          headers: dead(source: "worker", error: card_error),
          message: { job_class: "ProcessPaymentJob", job_id: "j-301", arguments: [] }.to_json },
        { msg_id: 302, queue_name: "pgbus_orders_dlq", read_ct: 0, enqueued_at: (now - 300).iso8601,
          headers: dead(source: "consumer", queue: "pgbus_orders"),
          message: { event_id: "e-1", payload: {}, headers: { routing_key: "orders.created" } }.to_json },
        { msg_id: 303, queue_name: "pgbus_default_dlq", read_ct: 0, enqueued_at: now - 900, headers: nil,
          message: { job_class: "SyncInventoryJob", job_id: "j-303", arguments: [] }.to_json },
        { msg_id: 304, queue_name: "pgbus_default_dlq", read_ct: 0, enqueued_at: now - 1200,
          headers: dead(source: "worker", existing: '{"pgbus_dlq_retries":1}'),
          message: { job_class: "GenerateReportJob", job_id: "j-304", arguments: [] }.to_json }
      ]
    end

    def row_for(id) = find("tr[data-dlq-row='#{id}']")

    it "lays the list out as a real table with one cell per column" do
      visit "/pgbus/dlq"

      ["ID", "Job", "Source queue", "Died", "Attempts", "Reason", "Actions"].each do |header|
        expect(page).to have_css("thead th", text: /\A#{header}\z/i)
      end
      within(row_for(301)) do
        expect(page).to have_css("td[data-label]", minimum: 7)
      end
    end

    it "says what killed a job, with its attempts" do
      visit "/pgbus/dlq"

      within(row_for(301)) do
        expect(page).to have_css("[data-testid='dlq-reason']", text: "Stripe::CardError: Your card was declined")
        expect(page).to have_text("6/5")
        expect(page).to have_text("ProcessPaymentJob")
        expect(page).to have_text("pgbus_default")
      end
    end

    it "shows the full error, the backtrace head and the silent later attempts in the expansion" do
      visit "/pgbus/dlq"

      within(row_for(301)) { find("details[data-job-toggle] summary").click }

      detail = find("tr[data-dlq-detail='301']")
      expect(detail).to have_text("Stripe::CardError: Your card was declined")
      expect(detail).to have_css("pre", text: "process_payment_job.rb:14")
      expect(detail).to have_text("Error from attempt 3; attempts 4–5 recorded no error")
    end

    it "says an event's handler error was not recorded and names its routing key" do
      visit "/pgbus/dlq"

      within(row_for(302)) do
        expect(page).to have_text("Handler failed on 6 deliveries (max 5) — no handler error was recorded")
        expect(page).to have_text("orders.created")
      end
    end

    it "explains a message dead-lettered before the reason was recorded" do
      visit "/pgbus/dlq"

      within(row_for(303)) do
        expect(page).to have_text("Reason not recorded (dead-lettered before pgbus #{Pgbus::DeadLetterHeader::SINCE})")
        expect(find("td[data-label='Attempts']")).to have_text("—")
      end
    end

    it "says when a message was retried from the DLQ before" do
      visit "/pgbus/dlq"

      within(row_for(304)) { expect(page).to have_text(/retried from the DLQ once before/i) }
    end

    it "filters by error class from the reason cell, keeps the filter in the frame and offers to clear it" do
      visit "/pgbus/dlq"

      within(row_for(301)) { click_link "Stripe::CardError" }

      expect(page).to have_current_path(/error_class=Stripe%3A%3ACardError/)
      expect(page).to have_css("tr[data-dlq-row='301']")
      expect(page).to have_no_css("tr[data-dlq-row='302']")
      expect(page).to have_css("turbo-frame#dlq-messages[data-src*='error_class=Stripe']", visible: :all)

      click_link "Clear filter"
      expect(page).to have_css("tr[data-dlq-row='302']")
    end

    it "filters by DLQ from the chips, with counts" do
      visit "/pgbus/dlq"

      within("nav[data-testid='dlq-filters']") do
        expect(page).to have_link(text: /pgbus_orders_dlq\s*1/)
        click_link(text: /pgbus_orders_dlq/)
      end

      expect(page).to have_current_path(/dlq=pgbus_orders_dlq/)
      expect(page).to have_css("tr[data-dlq-row='302']")
      expect(page).to have_no_css("tr[data-dlq-row='301']")
      expect(page).to have_css("nav[data-testid='dlq-filters'] a[aria-current='page']", text: "pgbus_orders_dlq")
    end

    it "opens the show page with a Why it died card" do
      visit "/pgbus/dlq/301"

      card = find("[data-testid='dead-letter-reason']")
      expect(card).to have_css("h2", text: "Why it died")
      expect(card).to have_text("Stripe::CardError: Your card was declined")
      expect(card).to have_css("pre", text: "process_payment_job.rb:14")
      expect(card).to have_text("pgbus_default")
    end

    it "keeps the reason readable in dark mode" do
      visit_dark("/pgbus/dlq")

      within(row_for(301)) { find("details[data-job-toggle] summary").click }
      expect(page).to have_css("[data-testid='dlq-reason']", text: "Stripe::CardError")
      expect(page).to be_accessible
    end

    it "keeps the Why it died card readable in dark mode" do
      visit_dark("/pgbus/dlq/301")

      expect(page).to have_css("[data-testid='dead-letter-reason']")
      expect(page).to be_accessible
    end
  end
end
