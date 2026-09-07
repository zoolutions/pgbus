# frozen_string_literal: true

require "system_helper"

RSpec.describe "Turbo Frames", type: :system do
  it "dashboard has turbo-frame elements with auto-refresh" do
    visit "/pgbus"

    expect(page).to have_css("turbo-frame#dashboard-stats")
    expect(page).to have_css("turbo-frame#dashboard-queues")
    expect(page).to have_css("turbo-frame#dashboard-processes")
    expect(page).to have_css("turbo-frame#dashboard-failures")
  end

  it "queues index has turbo-frame" do
    visit "/pgbus/queues"

    expect(page).to have_css("turbo-frame#queues-list")
  end

  it "jobs index has turbo-frames for failed and enqueued" do
    visit "/pgbus/jobs"

    expect(page).to have_css("turbo-frame#jobs-failed")
    expect(page).to have_css("turbo-frame#jobs-enqueued")
  end

  it "processes index has turbo-frame" do
    visit "/pgbus/processes"

    expect(page).to have_css("turbo-frame#processes-list")
  end

  it "DLQ index has turbo-frame" do
    visit "/pgbus/dlq"

    expect(page).to have_css("turbo-frame#dlq-messages")
  end

  it "batches index has an auto-refreshing turbo-frame" do
    visit "/pgbus/batches"

    expect(page).to have_css("turbo-frame#batches-list[data-auto-refresh]")
  end

  # A batch detail page is watched while the batch drains, so the counters and
  # the progress bar have to move without a manual reload.
  context "with a batch in flight" do
    before do
      @stub_data_source.batch_detail_hash = {
        batch_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890", description: "Backfill",
        status: "processing", total_jobs: 807, completed_jobs: 483, failed_jobs: 0,
        pending_jobs: 324, progress_pct: 59, properties: nil,
        created_at: Time.current, finished_at: nil
      }
    end

    it "batch show has an auto-refreshing progress turbo-frame" do
      visit "/pgbus/batches/a1b2c3d4-e5f6-7890-abcd-ef1234567890"

      expect(page).to have_css("turbo-frame#batch-progress[data-auto-refresh]")
    end

    it "batch progress frame endpoint returns only the partial" do
      visit "/pgbus/batches/a1b2c3d4-e5f6-7890-abcd-ef1234567890?frame=progress"

      expect(page).to have_css("turbo-frame#batch-progress")
      expect(page).to have_no_css("nav")
    end
  end

  it "dashboard frame endpoint returns only the partial" do
    visit "/pgbus?frame=stats"

    expect(page).to have_css("turbo-frame#dashboard-stats")
    expect(page).not_to have_css("nav")
  end

  it "has custom confirm dialog element" do
    visit "/pgbus"

    expect(page).to have_css("dialog#pgbus-confirm-dialog", visible: :hidden)
    expect(page).to have_css("dialog#pgbus-alert-dialog", visible: :hidden)
  end

  it "has toast container" do
    visit "/pgbus"

    expect(page).to have_css("#pgbus-toast-container")
  end
end
