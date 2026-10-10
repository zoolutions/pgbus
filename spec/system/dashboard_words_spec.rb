# frozen_string_literal: true

require "system_helper"

# Issue #492: every home card and panel says what its number means, why, and
# what happens next, and links to the page that explains it.
RSpec.describe "Dashboard words", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.utc(2026, 10, 10, 12, 0, 0) }

  around { |example| travel_to(now) { example.run } }

  def card(path) = find("turbo-frame#dashboard-stats div.grid > a[href='#{path}']")

  context "with sample data" do
    before { @stub_data_source.fill_sample_data! }

    %i[light dark].each do |mode|
      context "with #{mode} mode" do
        before { mode == :dark ? visit_dark("/pgbus") : visit_light("/pgbus") }

        it "states the health verdict with its reasons and where to look", :aggregate_failures do
          within("[data-testid=health-verdict]") do
            expect(page).to have_text("Health: STALLED")
            expect(page).to have_css("li", text: "1 worker(s) stalled (heart-beating but claim loop not advancing)")
            expect(page).to have_css("li", text: "1 process(es) stale (no recent heartbeat)")
            expect(page).to have_link("Processes", href: "/pgbus/processes")
            expect(page).to have_link("Dead letters", href: "/pgbus/dlq")
          end
        end

        it "puts words under every card", :aggregate_failures do
          expect(card("/pgbus/queues")).to have_text("8").and have_text("2 paused · 2 dead-letter")
          expect(card("/pgbus/queues")).to have_text("1 holds jobs that nothing drains")
          expect(card("/pgbus/jobs?state=ready")).to have_text("141")
          expect(card("/pgbus/jobs?state=ready"))
            .to have_text("80 claimable now · 24 scheduled, running or retrying · 37 in paused queues")
          expect(card("/pgbus/processes")).to have_text("7 healthy · 2 stalled · 1 stale")
          expect(card("/pgbus/processes")).to have_text("stale processes stopped heartbeating")
          expect(card("/pgbus/recurring_tasks")).to have_text("30")
          expect(card("/pgbus/jobs?state=retrying")).to have_text("2")
          expect(card("/pgbus/jobs?state=retrying")).to have_text("next attempt in 40s (12:00)")
          expect(card("/pgbus/jobs?state=retrying")).to have_text("newest error Net::ReadTimeout")
          expect(card("/pgbus/dlq")).to have_text("4")
          expect(card("/pgbus/dlq")).to have_text("newest: orders.created 15m ago — no error recorded")
          expect(card("/pgbus/insights")).to have_text("38.2").and have_text("jobs/min over the last 5 min")
          expect(card("/pgbus/locks")).to have_text("Parked jobs")
        end

        it "says what each queue is doing in its row", :aggregate_failures do
          within("turbo-frame#dashboard-queues") do
            expect(find("tr", text: "pgbus_mailers")).to have_text("Paused 25m ago — Mail provider maintenance")
            expect(find("tr", text: "pgbus_imports"))
              .to have_text("No worker capsule drains this queue — jobs wait until one is configured")
            expect(find("tr", text: "pgbus_default_p1")).to have_text("9 claimable now, oldest waiting 7m 0s")
            expect(find("tr", text: "pgbus_default_dlq")).to have_text("Dead-letter queue for default")
            expect(page).to have_no_text("Oldest claimable (s)")
          end
        end

        it "states each Queue Health number's level and its thresholds", :aggregate_failures do
          within("turbo-frame#dashboard-health") do
            expect(page).to have_text("12% on pgmq.q_pgbus_events — vacuum due (warns above 10%, critical above 20%)")
            expect(page).to have_text("Fine — warns above 10,000 dead rows")
            expect(page).to have_text("Fine — warns after 1h 0m, critical after 1d 0h")
            expect(page).to have_text("Fine — warns at 30s, alerts at 1m 0s; the health verdict degrades after 5m 0s")
          end
        end

        it "lists the recent failures with job, attempt and a link to each", :aggregate_failures do
          within("turbo-frame#dashboard-failures") do
            row = find("tr", text: "Stripe::InvalidRequestError")
            expect(row).to have_text("ProcessPaymentJob").and have_text("4 / 5")
            expect(row).to have_link(href: "/pgbus/jobs/1")
            expect(page).to have_link("View all", href: "/pgbus/jobs?state=retrying")
          end
        end
      end
    end

    it "opens the page each card explains" do
      visit "/pgbus"

      card("/pgbus/jobs?state=retrying").click

      expect(page).to have_current_path("/pgbus/jobs?state=retrying")
      expect(page).to have_css("h1", text: "Jobs")
    end
  end

  context "with the sparse stub" do
    before { visit "/pgbus" }

    it "says what an empty or unconfigured signal means", :aggregate_failures do
      expect(card("/pgbus/jobs?state=retrying")).to have_text("Nothing is retrying")
      expect(card("/pgbus/insights")).to have_text("needs job stats (rails g pgbus:add_job_stats)")
      expect(card("/pgbus/dlq")).to have_text("open the dead-letter page to see why each died")
    end

    it "says when the verdict could not be computed" do
      @stub_data_source.dashboard_signals_overrides = { verdict: nil }
      visit "/pgbus"

      expect(find("[data-testid=health-verdict]")).to have_text("Health verdict unavailable")
    end

    it "shows the Queue Health table's empty state" do
      within("turbo-frame#dashboard-health") do
        expect(page).to have_css("td[colspan='6']", text: "No queue tables to measure yet")
      end
    end
  end
end
