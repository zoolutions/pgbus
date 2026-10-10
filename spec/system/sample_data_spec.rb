# frozen_string_literal: true

require "system_helper"

# The sample data behind rake dummy:server, Lighthouse, the PR screenshots and
# the accessibility gate. Before #498 the dummy server had its own stub that
# 500'd on Events, DLQ and Locks and left Outbox and Insights empty; these
# examples keep every list on the page so it cannot quietly go empty again.
RSpec.describe "Dashboard sample data", type: :system do
  before { @stub_data_source.fill_sample_data! }

  it "fills Events with subscribers, pending and processed events" do
    visit "/pgbus/events"
    expect(page).to have_text("Billing::InvoiceHandler")
    expect(page).to have_text("evt-pending-1")
    expect(page).to have_text("evt-processed-1")
  end

  it "fills the Outbox with entries" do
    visit "/pgbus/outbox"
    expect(page).to have_text("orders.created")
  end

  it "fills every Insights table" do
    visit "/pgbus/insights"
    expect(page).to have_text("GenerateReportJob")
    expect(page).to have_text("chat:lobby")
  end

  it "fills both lock tables" do
    visit "/pgbus/locks"
    expect(page).to have_text("uniqueness:ProcessPaymentJob:abc123")
    expect(page).to have_text("ImportCsvJob/account:42")
  end

  it "serves the DLQ list" do
    visit "/pgbus/dlq"
    expect(page).to have_text("ProcessPaymentJob")
  end

  it "shows recurring task executions" do
    visit "/pgbus/recurring_tasks/1"
    expect(page).to have_no_text(I18n.t("pgbus.recurring_tasks.show.no_executions"))
  end
end
