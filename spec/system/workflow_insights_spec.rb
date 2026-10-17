# frozen_string_literal: true

require "system_helper"

# The workflow insights (issue #549): the class list, one class's pipeline
# with live counts and step performance, its charts, funnel and per-revision
# rows, the window selector, the "few samples" and "definition not loaded"
# notes, the links into the filtered run list, and the empty states.
RSpec.describe "Workflow insights", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.zone.now.change(usec: 0) }
  let(:since) { (now - 86_400).utc.iso8601 }
  let(:until_now) { now.utc.iso8601 }

  before do
    travel_to(now)
    @stub_data_source.fill_sample_data!
  end

  after { travel_back }

  def runs_path(**query) = "/pgbus/workflows?#{query.to_query}"

  def class_row(name) = find("tr[data-workflow-class='#{name}']")

  def step_card(name) = find("[data-testid=workflow-pipeline] > li[data-step='#{name}']")

  describe "the class list" do
    it "lists every sample class with its counts, each linking to the runs it counts" do
      visit "/pgbus/workflows/definitions"

      expect(page).to have_css("h1", text: "Workflow insights")
      expect(page).to have_text("over the last 24 hours")
      within(class_row("OrderWorkflow")) do
        expect(page).to have_link("11", href: runs_path(workflow_class: "OrderWorkflow"))
        expect(page).to have_link("1", href: runs_path(status: "paused", workflow_class: "OrderWorkflow"), match: :first)
        expect(page).to have_link("412", href: runs_path(ended_from: since, ended_to: until_now, status: "finished", workflow_class: "OrderWorkflow"))
        expect(page).to have_link("9", href: runs_path(ended_from: since, ended_to: until_now, status: "cancelled", workflow_class: "OrderWorkflow"))
        expect(page).to have_link("1", href: "/pgbus/workflows/attention?workflow_class=OrderWorkflow")
        expect(page).to have_text("48.2s")
        expect(page).to have_text("5m 12s")
      end
      within(class_row("SignupWorkflow")) do
        expect(page).to have_link("2 runs on removed step nudge",
                                  href: runs_path(step: "nudge", workflow_class: "SignupWorkflow"))
      end
      within(class_row("Billing::InvoiceWorkflow")) { expect(page).to have_text("Definition not loaded") }
      expect(page).to have_link("OrderWorkflow", href: "/pgbus/workflows/definitions/OrderWorkflow?window=24h")
    end

    it "switches the window, and keeps it on the way into a class" do
      visit "/pgbus/workflows/definitions"
      click_link "7d"

      expect(page).to have_css("a[aria-current=page]", text: "7d")
      expect(page).to have_text("over the last 7 days")
      expect(@stub_data_source.calls[:workflow_definitions].last).to eq([{ window: "7d" }])

      click_link "OrderWorkflow"
      expect(page).to have_css("h1", text: "OrderWorkflow")
      expect(page).to have_css("a[aria-current=page]", text: "7d")
      expect(@stub_data_source.calls[:workflow_definition].last).to eq(["OrderWorkflow", { window: "7d" }])
    end

    it "says when the window is capped at workflow_retention" do
      Pgbus.configuration.workflow_retention = (2 * 86_400) + 5_400
      visit "/pgbus/workflows/definitions?window=30d"

      expect(page).to have_text("Capped at workflow_retention (2d 1h)")
    ensure
      Pgbus.configuration.workflow_retention = 30 * 86_400
    end

    it "is reached from the Workflows page" do
      visit "/pgbus/workflows"
      click_link "Insights by class"

      expect(page).to have_current_path("/pgbus/workflows/definitions")
    end

    it "shows the empty state" do
      @stub_data_source.workflow_insights_samples = {}

      visit "/pgbus/workflows/definitions"

      expect(page).to have_text("No workflow class has run yet.")
    end

    it "renders in dark mode" do
      visit_dark "/pgbus/workflows/definitions"

      expect(page).to have_css("html.dark")
      expect(page).to have_css("tr[data-workflow-class='OrderWorkflow']")
    end
  end

  describe "a class's page" do
    it "shows the pipeline in step order, each step with the runs at it now and its numbers" do
      visit "/pgbus/workflows/definitions/OrderWorkflow"

      expect(page).to have_css("h1", text: "OrderWorkflow")
      expect(all("[data-testid=workflow-pipeline] > li").map { |li| li["data-step"] })
        .to eq(%w[reserve capture notify_vendors confirm])
      within(step_card("capture")) do
        expect(page).to have_link("2", href: runs_path(status: "performing", step: "capture", workflow_class: "OrderWorkflow"))
        expect(page).to have_link("1", href: runs_path(status: "paused", step: "capture", workflow_class: "OrderWorkflow"))
        expect(page).to have_text("p50 820ms · p95 4.1s · p99 9.8s")
        expect(page).to have_text("Failed 8.3%")
        expect(page).to have_text("Reattempted 6.6%")
        expect(page).to have_text("468 attempts")
      end
      within(step_card("notify_vendors")) do
        expect(page).to have_text("Fan-out")
        expect(page).to have_link("4", href: runs_path(status: "waiting", step: "notify_vendors", workflow_class: "OrderWorkflow"))
      end
    end

    it "sums the window up, each count linking to its runs" do
      visit "/pgbus/workflows/definitions/OrderWorkflow"

      within("[data-testid=workflow-insights-summary]") do
        expect(page).to have_link("432", href: runs_path(created_from: since, created_to: until_now, status: "all", workflow_class: "OrderWorkflow"))
        expect(page).to have_link("412", href: runs_path(ended_from: since, ended_to: until_now, status: "finished", workflow_class: "OrderWorkflow"))
        expect(page).to have_text("48.2s")
      end
    end

    it "notes percentiles over too few samples, and a removed step runs still sit on" do
      visit "/pgbus/workflows/definitions/SignupWorkflow"

      within(step_card("reminder")) { expect(page).to have_text("n=7, few samples") }
      within(step_card("nudge")) do
        expect(page).to have_text("Removed")
        expect(page).to have_link("2", href: runs_path(status: "performing", step: "nudge", workflow_class: "SignupWorkflow"))
      end
      expect(page).to have_link("2 runs on removed step nudge", href: runs_path(step: "nudge", workflow_class: "SignupWorkflow"))
    end

    it "links its need-attention count to that class's attention groups" do
      visit "/pgbus/workflows/definitions/Billing::InvoiceWorkflow"

      within("[data-testid=workflow-insights-summary]") do
        find_link("2", href: "/pgbus/workflows/attention?workflow_class=Billing%3A%3AInvoiceWorkflow").click
      end

      expect(page).to have_css("h1", text: "Needs attention")
      expect(page).to have_text("Billing::InvoiceWorkflow only")
    end

    it "builds the funnel: how many reached each step, and where the rest are now" do
      visit "/pgbus/workflows/definitions/OrderWorkflow"

      within("[data-testid=workflow-funnel]") do
        within("tr[data-step=reserve]") do
          expect(page).to have_text("432")
          expect(page).to have_text("100%")
        end
        within("tr[data-step=capture]") do
          expect(page).to have_link("6 cancelled",
                                    href: runs_path(created_from: since, created_to: until_now, status: "cancelled", step: "capture",
                                                    workflow_class: "OrderWorkflow"))
        end
        within("tr[data-step=confirm]") { expect(page).to have_text("412 finished") }
      end
    end

    it "lists the numbers per app revision, newest first, and marks a step that got worse" do
      visit "/pgbus/workflows/definitions/OrderWorkflow"

      within("[data-testid=workflow-by-revision]") do
        revisions = all("tr[data-revision]").map { |tr| tr["data-revision"] }.uniq
        expect(revisions).to eq(%w[8d41b07 3f2a9c1])
        within("tr[data-revision='8d41b07'][data-step=capture]") do
          expect(page).to have_text("18%")
          expect(page).to have_text("up from 1% on 3f2a9c1")
        end
      end
    end

    it "draws the throughput and duration charts, each with its caption and data table" do
      visit "/pgbus/workflows/definitions/OrderWorkflow"

      expect(page).to have_css("#workflow-throughput-chart[role=img]")
      expect(page).to have_css("#workflow-duration-chart[role=img]")
      expect(page).to have_text("Runs started, finished and cancelled per 1h over the last 24 hours")
      expect(page).to have_css("#workflow-throughput-chart svg", wait: 5)
      all("summary", text: "Data table").each(&:click)
      expect(page).to have_css("td", text: "10s–1m")
      expect(page).to have_link("11", href: %r{/pgbus/workflows\?created_from=.*&created_to=.*&status=all&workflow_class=OrderWorkflow\z},
                                      match: :first)
    end

    it "says a class's definition is not loaded, and orders its steps as its runs reached them" do
      visit "/pgbus/workflows/definitions/Billing::InvoiceWorkflow"

      expect(page).to have_css("h1", text: "Billing::InvoiceWorkflow")
      expect(page).to have_text("Definition not loaded")
      expect(all("[data-testid=workflow-pipeline] > li").map { |li| li["data-step"] }).to eq(%w[draft send collect])
      expect(page).to have_text("No attempt in this window recorded an app revision")
    end

    it "says when no class has that name" do
      visit "/pgbus/workflows/definitions/NopeWorkflow"

      expect(page).to have_css("h1", text: "Unknown workflow class")
      expect(page).to have_link("All workflow classes")
    end

    it "renders in dark mode, charts included" do
      visit_dark "/pgbus/workflows/definitions/OrderWorkflow"

      expect(page).to have_css("html.dark")
      expect(page).to have_css("#workflow-throughput-chart svg", wait: 5)
    end
  end
end
