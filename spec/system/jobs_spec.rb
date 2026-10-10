# frozen_string_literal: true

require "system_helper"

RSpec.describe "Jobs", type: :system do
  let(:now) { Time.now.utc }
  let(:rows) do
    [
      job_row("ready", msg_id: 11),
      job_row("scheduled", msg_id: 12, vt: now + 5400),
      job_row("running", msg_id: 13, read_ct: 1, last_read_at: now - 12, vt: now + 48),
      job_row("retrying", msg_id: 14, read_ct: 2, last_read_at: now - 30, vt: now + 40,
                          error_class: "Net::ReadTimeout", error_message: "execution expired", failed_event_id: 7),
      job_row("retrying", source: "failed", id: 8, msg_id: 99, job_class: "OrphanJob", read_ct: 3, vt: nil,
                          error_class: "RuntimeError", failed_event_id: 8),
      job_row("blocked", source: "blocked", id: 5, msg_id: nil, read_ct: nil, vt: nil, queue_name: "default",
                         concurrency_key: "Import:42", slots_held: 2, slots_max: 2)
    ]
  end

  it "shows an empty state for every tab" do
    visit "/pgbus/jobs"

    expect(page).to have_css("h1", text: "Jobs")
    expect(page).to have_text("No jobs")

    click_link "Blocked"
    expect(page).to have_text("No jobs blocked by a concurrency limit")
  end

  context "with a job in every state" do
    before do
      @stub_data_source.job_rows_list = rows
      @stub_data_source.jobs_ahead_hash = { ["pgbus_default", 11] => 3 }
    end

    it "renders the state tabs with their counts" do
      visit "/pgbus/jobs"

      within("[data-testid=job-state-tabs]") do
        expect(page).to have_css("a[data-state=all]", text: "6")
        expect(page).to have_css("a[data-state=ready]", text: "1")
        expect(page).to have_css("a[data-state=retrying]", text: "2")
        expect(page).to have_css("a[data-state=blocked]", text: "1")
        expect(page).to have_css("a[aria-current=page][data-state=all]")
      end
    end

    it "explains every row in words, with its state, attempts and next run" do
      visit "/pgbus/jobs"

      within("tr[data-state=ready]") do
        expect(page).to have_text("Ready")
        expect(page).to have_text("Waiting — 3 ahead")
        expect(page).to have_text("0/5")
      end
      within("tr[data-state=scheduled]") { expect(page).to have_text("Scheduled — runs in 1h") }
      within("tr[data-state=running]") { expect(page).to have_text(/Claimed \d+s ago · lease expires in \d+s/) }
      expect(page).to have_text(%r{Attempt 2/5 failed: Net::ReadTimeout — next attempt in \d+s})
      expect(page).to have_text("Failed with RuntimeError — message no longer in queue")
      within("tr[data-state=blocked]") do
        expect(page).to have_text("Waiting for a concurrency slot on Import:42 (held 2/2)")
        expect(page).to have_text("—")
      end
    end

    it "shows only the selected state on a tab" do
      visit "/pgbus/jobs"

      click_link "Retrying"

      expect(page).to have_css("tr[data-job-row]", count: 2)
      expect(page).to have_css("tr[data-state=retrying]", count: 2)
      expect(page).to have_css("a[aria-current=page][data-state=retrying]")
    end

    it "lands the dashboard's failed-jobs link on the Retrying tab" do
      visit "/pgbus/jobs?status=failed"

      expect(page).to have_css("a[aria-current=page][data-state=retrying]")
      expect(page).to have_css("tr[data-job-row]", count: 2)
    end

    it "links a blocked job to the Locks page" do
      visit "/pgbus/jobs"

      within("tr[data-state=blocked]") { click_link "View locks" }

      expect(page).to have_current_path("/pgbus/locks")
    end

    it "expands a row to show its payload and keeps the cells aligned" do
      visit "/pgbus/jobs"

      expect(page).to have_no_css("tr[data-job-detail]", visible: :visible)
      within("tr[data-state=ready]") { find("details[data-job-toggle] summary").click }

      expect(page).to have_css("tr[data-job-detail]", visible: :visible, count: 1)
      expect(page).to have_text("job-ready")
      expect(page).to have_css("tr[data-job-row] > td", minimum: 8)
    end

    it "bulk-discards queue messages and failed rows together" do
      visit "/pgbus/jobs"

      within("turbo-frame#jobs-list") do
        expect(page).to have_css("input[data-bulk-item]", count: 5)
        find("input[data-bulk-select-all]").click
        expect(page).to have_css("input[data-bulk-item]:checked", count: 5)
      end

      click_button "Discard Selected"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded 5 selected")
      expect(@stub_data_source.calls[:discard_failed_event]).to contain_exactly([7], [8])
      expect(@stub_data_source.calls[:discard_job].size).to eq(3)
    end

    it "retries a retrying job through its failed event" do
      visit "/pgbus/jobs"

      within("tr[data-state=retrying]", match: :first) { click_button "Retry" }

      expect(page).to have_toast("re-enqueued")
      expect(@stub_data_source).to be_called(:retry_failed_event)
    end

    it "discards a ready message after confirming" do
      visit "/pgbus/jobs"

      within("tr[data-state=ready]") { click_button "Discard" }
      accept_confirm_dialog

      expect(page).to have_toast("Message discarded")
      expect(@stub_data_source).to be_called(:discard_job)
    end

    it "offers no Retry for a running job" do
      visit "/pgbus/jobs"

      within("tr[data-state=running]") do
        expect(page).to have_no_button("Retry")
        expect(page).to have_button("Discard")
      end
    end

    it "offers no Retry for a retry attempt that is running now" do
      @stub_data_source.job_rows_list = [job_row("running", msg_id: 30, read_ct: 2, last_read_at: now - 5, vt: now + 55,
                                                            error_class: "Net::ReadTimeout", failed_event_id: 9)]
      visit "/pgbus/jobs"

      within("tr[data-state=running]") do
        expect(page).to have_no_button("Retry")
        expect(page).to have_button("Discard")
      end
    end

    it "keeps Retry All while a queue filter hides the failures of other queues" do
      @stub_data_source.failed_events_list = [{ "id" => 7, "queue_name" => "default" }]

      visit "/pgbus/jobs?queue=pgbus_mailers"

      expect(page).to have_text("No jobs")
      expect(page).to have_button("Retry All")
    end

    it "offers Discard All Enqueued only while no queue filter is set" do
      visit "/pgbus/jobs"
      expect(page).to have_button("Discard All Enqueued")

      visit "/pgbus/jobs?queue=pgbus_default"
      expect(page).to have_css("tr[data-job-row]", minimum: 1)
      expect(page).to have_no_button("Discard All Enqueued")
    end

    it "shows Retry All and Discard All while jobs are retrying" do
      visit "/pgbus/jobs"

      expect(page).to have_button("Retry All")
      expect(page).to have_button("Discard All", minimum: 1)
    end
  end

  context "with more jobs than one page" do
    before do
      allow(Pgbus.configuration).to receive(:web_per_page).and_return(2)
      @stub_data_source.job_rows_list = Array.new(3) { |i| job_row("ready", msg_id: 20 + i, job_class: "PagedJob#{i}") }
    end

    it "pages through the list" do
      visit "/pgbus/jobs"

      expect(page).to have_text("Showing 1–2 of 3")
      click_link "Next"

      expect(page).to have_text("PagedJob2")
      expect(page).to have_text("Showing 3–3 of 3")
    end

    it "keeps a Next link when a capped count undercounts the tab" do
      capped = Pgbus::Web::DataSource::JobList::StateCounts.new(counts: { "all" => 0 }, capped: Set["all"])
      allow(@stub_data_source).to receive(:job_state_counts).and_return(capped)

      visit "/pgbus/jobs"

      expect(page).to have_text("Showing 1–2 of 2+")
      expect(page).to have_link("Next")
    end
  end
end
