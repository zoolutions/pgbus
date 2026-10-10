# frozen_string_literal: true

require "system_helper"

RSpec.describe "Queues", type: :system do
  describe "index page" do
    it "lists all queues with metrics" do
      visit "/pgbus/queues"

      expect(page).to have_css("h1", text: "Queues")
      expect(page).to have_text("pgbus_default")
      expect(page).to have_text("pgbus_default_dlq")
      expect(page).to have_text("500") # total_messages
    end

    it "shows purge and delete buttons for each queue" do
      visit "/pgbus/queues"

      expect(page).to have_button("Purge", count: 2)
      expect(page).to have_button("Delete", count: 2)
    end

    it "shows pause button for unpaused queues" do
      visit "/pgbus/queues"

      expect(page).to have_button("Pause", count: 2)
      expect(page).not_to have_button("Resume")
    end

    it "shows resume button for paused queues" do
      @stub_data_source.queues = [
        { name: "pgbus_default", queue_length: 10, queue_visible_length: 8,
          oldest_msg_age_sec: 120, newest_msg_age_sec: 5, total_messages: 500, paused: true }
      ]

      visit "/pgbus/queues"

      expect(page).to have_button("Resume", count: 1)
      expect(page).to have_text("Paused")
    end

    it "shows empty state when no queues" do
      @stub_data_source.queues = []

      visit "/pgbus/queues"

      expect(page).to have_text("No queues found")
    end
  end

  describe "show page" do
    let(:now) { Time.now.utc }

    it "displays queue name and metrics in words" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_css("h1", text: "pgbus_default")
      within("[data-testid=queue-metrics]") do
        expect(page).to have_text("Depth")
        expect(page).to have_text("10")
        expect(page).to have_text("Oldest claimable")
        expect(page).to have_text("1m 30s")
      end
    end

    it "shows purge and delete buttons" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Purge Queue")
      expect(page).to have_button("Delete Queue")
    end

    it "shows pause button when queue is not paused" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Pause")
      expect(page).not_to have_button("Resume")
    end

    it "shows resume button and paused badge when queue is paused" do
      @stub_data_source.paused_queues = ["pgbus_default"]

      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_button("Resume")
      expect(page).not_to have_button("Pause")
      expect(page).to have_text("Paused")
    end

    it "shows the empty state of the job list" do
      visit "/pgbus/queues/pgbus_default"

      expect(page).to have_text("No jobs")
      expect(page).to have_no_button("Discard All Enqueued")
    end

    describe "summary in words" do
      def summary = find("[data-testid=queue-summary]")

      it "says who drains the queue and what is waiting" do
        @stub_data_source.queue_drainers_hash["pgbus_default"] = { live_workers: 2 }

        visit "/pgbus/queues/pgbus_default"

        expect(summary).to have_text("Drained by default (2 healthy workers)")
        expect(summary).to have_text("8 claimable now, oldest waiting 1m 30s · 2 parked (scheduled or retrying)")
      end

      it "explains an operator pause" do
        @stub_data_source.paused_queues = ["pgbus_default"]
        @stub_data_source.queue_pause_states["pgbus_default"] = { reason: "maintenance", paused_at: now - 300 }

        visit "/pgbus/queues/pgbus_default"

        expect(summary).to have_text("Paused 5m ago — maintenance")
      end

      it "explains a circuit-breaker pause and when it lifts" do
        @stub_data_source.paused_queues = ["pgbus_default"]
        @stub_data_source.queue_pause_states["pgbus_default"] = {
          reason: "circuit_breaker: 5 consecutive failures", paused_at: now - 35, resumes_at: now + 25, trip_count: 2
        }

        visit "/pgbus/queues/pgbus_default"

        expect(summary).to have_text("Paused automatically 35s ago after 5 consecutive failures")
        expect(summary).to have_text(/resumes in \d+s/)
        expect(summary).to have_text("trip #2")
      end

      it "warns when no worker capsule drains the queue" do
        @stub_data_source.queue_drainers_hash["pgbus_default"] = { capsules: [] }

        visit "/pgbus/queues/pgbus_default"

        expect(summary).to have_text("No worker capsule drains this queue")
      end

      it "warns when no healthy worker is running" do
        @stub_data_source.queue_drainers_hash["pgbus_default"] = { live_workers: 0 }

        visit "/pgbus/queues/pgbus_default"

        expect(summary).to have_text("No healthy worker running — 8 jobs are claimable but nothing claims them")
      end

      it "names the priority level and links its sibling levels" do
        @stub_data_source.queues = %w[pgbus_default_p0 pgbus_default_p1].map do |name|
          { name: name, queue_length: 3, queue_visible_length: 3, parked_length: 0, oldest_msg_age_sec: 60,
            oldest_claimable_age_sec: 60, newest_msg_age_sec: 1, total_messages: 30 }
        end

        visit_dark "/pgbus/queues/pgbus_default_p1"

        expect(summary).to have_text("Priority level 1 of default")
        within(summary) { click_link "pgbus_default_p0" }
        expect(page).to have_current_path("/pgbus/queues/pgbus_default_p0")
        expect(page).to be_accessible
      end
    end

    it "points a dead-letter queue at the Dead Letter page instead of listing jobs" do
      visit "/pgbus/queues/pgbus_default_dlq"

      expect(page).to have_text("Dead-letter queue for default")
      expect(page).to have_no_css("turbo-frame#jobs-list")
      expect(page).to have_link("Dead Letter page", href: "/pgbus/dlq?dlq=pgbus_default_dlq")
    end

    context "with jobs in this queue and another" do
      before do
        @stub_data_source.job_rows_list = [
          job_row("ready", msg_id: 11),
          job_row("scheduled", msg_id: 12, vt: now + 5400),
          job_row("retrying", msg_id: 14, read_ct: 2, last_read_at: now - 30, vt: now + 40,
                              error_class: "Net::ReadTimeout", failed_event_id: 7),
          job_row("ready", msg_id: 50, queue_name: "pgbus_mailers", logical_queue: "mailers")
        ]
        @stub_data_source.jobs_ahead_hash = { ["pgbus_default", 11] => 3 }
      end

      it "counts only this queue's jobs on the state tabs" do
        visit "/pgbus/queues/pgbus_default"

        within("[data-testid=job-state-tabs]") do
          expect(page).to have_css("a[data-state=all]", text: "3")
          expect(page).to have_css("a[data-state=ready]", text: "1")
          expect(page).to have_css("a[data-state=retrying]", text: "1")
        end
      end

      it "stays on the queue page when switching tabs" do
        visit "/pgbus/queues/pgbus_default"

        click_link "Retrying"

        expect(page).to have_current_path("/pgbus/queues/pgbus_default?state=retrying")
        expect(page).to have_css("tr[data-job-row]", count: 1)
        expect(page).to have_css("a[aria-current=page][data-state=retrying]")
      end

      it "explains each row and expands it in place" do
        visit "/pgbus/queues/pgbus_default"

        within("tr[data-state=ready]") do
          expect(page).to have_text("Ready")
          expect(page).to have_text("Waiting — 3 ahead")
          expect(page).to have_text("0/5")
          expect(page).to have_button("Retry")
          expect(page).to have_button("Discard")
          find("details[data-job-toggle] summary").click
        end

        expect(page).to have_css("tr[data-job-detail]", visible: :visible, count: 1)
        expect(page).to have_text("job-ready")
      end

      it "offers no queue-wide Discard All Enqueued, only Purge Queue" do
        visit "/pgbus/queues/pgbus_default"

        expect(page).to have_no_button("Discard All Enqueued")
        expect(page).to have_button("Purge Queue")
      end

      it "bulk-discards and lands back on the queue page" do
        visit "/pgbus/queues/pgbus_default"

        within("turbo-frame#jobs-list") { find("input[data-bulk-select-all]").click }
        click_button "Discard Selected"
        accept_confirm_dialog

        expect(page).to have_toast("Discarded 3 selected")
        expect(page).to have_current_path("/pgbus/queues/pgbus_default")
      end

      it "passes the accessibility gate in dark mode" do
        visit_dark "/pgbus/queues/pgbus_default"

        expect(page).to be_accessible
      end
    end

    context "with more jobs than one page" do
      before do
        allow(Pgbus.configuration).to receive(:web_per_page).and_return(2)
        @stub_data_source.job_rows_list = Array.new(3) { |i| job_row("ready", msg_id: 20 + i, job_class: "PagedJob#{i}") }
      end

      it "pages without leaving the queue page" do
        visit "/pgbus/queues/pgbus_default"

        expect(page).to have_text("Showing 1–2 of 3")
        click_link "Next"

        expect(page).to have_text("PagedJob2")
        expect(page).to have_current_path("/pgbus/queues/pgbus_default?page=2")
      end
    end
  end

  describe "queue actions" do
    it "purge: confirm dialog accepts and shows toast" do
      visit "/pgbus/queues/pgbus_default"

      click_button "Purge Queue"
      accept_confirm_dialog

      expect(page).to have_toast("Queue purged")
      expect(@stub_data_source).to be_called(:purge_queue)
    end

    it "purge: cancel dialog does not purge" do
      visit "/pgbus/queues/pgbus_default"

      click_button "Purge Queue"
      dismiss_confirm_dialog

      # Still on show page, no toast, no call
      expect(page).to have_css("h1", text: "pgbus_default")
      expect(@stub_data_source).not_to be_called(:purge_queue)
    end

    it "delete: confirm dialog deletes and redirects to index" do
      visit "/pgbus/queues/pgbus_default"

      click_button "Delete Queue"
      accept_confirm_dialog

      expect(page).to have_toast("deleted")
      expect(page).to have_css("h1", text: "Queues")
      expect(@stub_data_source).to be_called(:drop_queue)
    end

    it "pause: confirm dialog pauses and shows toast" do
      visit "/pgbus/queues/pgbus_default"

      click_button "Pause"
      accept_confirm_dialog

      expect(page).to have_toast("Queue paused")
      expect(@stub_data_source).to be_called(:pause_queue)
    end

    it "resume: no confirm needed, shows toast" do
      @stub_data_source.paused_queues = ["pgbus_default"]

      visit "/pgbus/queues/pgbus_default"

      click_button "Resume"

      expect(page).to have_toast("Queue resumed")
      expect(@stub_data_source).to be_called(:resume_queue)
    end

    context "with messages" do
      let(:now) { Time.now.utc }

      before { @stub_data_source.job_rows_list = [job_row("ready", msg_id: 42)] }

      it "discard message: confirm from the actions column and shows toast" do
        visit "/pgbus/queues/pgbus_default"

        within("tr[data-state=ready]") { click_button "Discard" }
        accept_confirm_dialog

        expect(page).to have_toast("Message discarded")
        expect(@stub_data_source.calls[:discard_job]).to eq([%w[pgbus_default 42]])
        expect(page).to have_current_path("/pgbus/queues/pgbus_default")
      end

      it "retry message: confirm from the actions column and shows toast" do
        visit "/pgbus/queues/pgbus_default"

        within("tr[data-state=ready]") { click_button "Retry" }
        accept_confirm_dialog

        expect(page).to have_toast("Message visibility reset")
        expect(@stub_data_source.calls[:retry_job]).to eq([%w[pgbus_default 42]])
      end
    end
  end
end
