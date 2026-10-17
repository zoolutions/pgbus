# frozen_string_literal: true

require "system_helper"

# The sample data behind rake dummy:server, Lighthouse, the PR screenshots and
# the accessibility gate. Before #498 the dummy server had its own stub that
# 500'd on Events, DLQ and Locks and left Outbox and Insights empty; these
# examples keep every list on the page so it cannot quietly go empty again.
RSpec.describe "Dashboard sample data", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  after { travel_back }

  before { @stub_data_source.fill_sample_data! }

  it "fills Events with subscribers, pending and processed events" do
    visit "/pgbus/events"
    expect(page).to have_text("Billing::InvoiceHandler")
    expect(page).to have_text("evt-pending-801")
    expect(page).to have_text("evt-processed-1")
    expect(page).to have_text("No running consumer subscribes to webhook.#")
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

  it "explains each sample queue in words" do
    travel_to Time.current.change(usec: 0)
    @stub_data_source.fill_sample_data!

    visit "/pgbus/queues/pgbus_mailers"
    expect(page).to have_text("Paused 25m ago — Mail provider maintenance")

    visit "/pgbus/queues/pgbus_events"
    expect(page).to have_text("Paused automatically 1m ago after 5 consecutive failures")

    visit "/pgbus/queues/pgbus_imports"
    expect(page).to have_text("No worker capsule drains this queue")
    expect(page).to have_text("ImportCsvJob")

    visit "/pgbus/queues/pgbus_default_p1"
    expect(page).to have_text("Priority level 1 of default")
    expect(page).to have_link("pgbus_default_p0")
  end

  it "serves the DLQ list" do
    visit "/pgbus/dlq"
    expect(page).to have_text("ProcessPaymentJob")
  end

  it "shows deploy task runs and a deploy mid-rollover" do
    visit "/pgbus/deploy_tasks/1"
    expect(page).to have_no_text(I18n.t("pgbus.deploy_tasks.show.no_runs"))

    visit "/pgbus/deploy_tasks"
    expect(page).to have_text("worker@app-2 pid 4102 on 3f2a9c1")
    expect(page).to have_css("tr[data-state=waiting]")
    expect(page).to have_css("tr[data-state=ran]")
    expect(page).to have_css("tr[data-state=never]")
    expect(page).to have_css("tr[data-state=disabled]")
  end

  it "shows a workflow in every state, the attention groups and a full timeline" do
    samples = Pgbus::Test::StubDataSource::SampleWorkflows

    visit "/pgbus/workflows?status=all"
    %w[ready performing waiting paused finished cancelled].each { |status| expect(page).to have_css("tr[data-status=#{status}]") }

    visit "/pgbus/workflows/attention"
    %w[paused_error stalled overdue waiting_overdue].each { |reason| expect(page).to have_css("tr[data-reason=#{reason}]") }

    visit "/pgbus/workflows/#{samples::PAUSED_ID}"
    expect(page).to have_no_text(I18n.t("pgbus.workflows.steps.empty"))
    expect(page).to have_no_text(I18n.t("pgbus.workflows.audit.empty"))
    visit "/pgbus/workflows/#{samples::SIGNAL_ID}"
    expect(page).to have_no_text(I18n.t("pgbus.workflows.signals.empty"))
  end

  it "fills the workflow insights: every class, and a class page without an empty block" do
    visit "/pgbus/workflows/definitions"
    expect(page).to have_css("tr[data-workflow-class]", count: 4)

    visit "/pgbus/workflows/definitions/OrderWorkflow"
    expect(page).to have_css("[data-testid=workflow-pipeline] > li", count: 4)
    expect(page).to have_no_css("[data-testid=chart-empty]")
    expect(page).to have_no_text(I18n.t("pgbus.workflow_insights.show.funnel.empty"))
    expect(page).to have_no_text(I18n.t("pgbus.workflow_insights.show.by_revision.empty"))
  end

  it "shows recurring task executions" do
    visit "/pgbus/recurring_tasks/1"
    expect(page).to have_no_text(I18n.t("pgbus.recurring_tasks.show.no_executions"))
  end
end
