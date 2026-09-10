# frozen_string_literal: true

require "system_helper"

RSpec.describe "Dashboard", type: :system do
  it "renders all dashboard sections" do
    visit "/pgbus"

    expect(page).to have_css("h1", text: "Dashboard")

    # Stats cards
    expect(page).to have_text("Queues")
    expect(page).to have_text("Enqueued")
    expect(page).to have_text("Processes")
    expect(page).to have_text("Failed / DLQ")
    expect(page).to have_text("15")
    expect(page).to have_text("3 / 2") # failed / dlq
  end

  context "with parked jobs" do
    before do
      @stub_data_source.stats[:parked_total] = 7
      @stub_data_source.stats[:oldest_parked_age_sec] = 812
    end

    it "shows the parked jobs card with the oldest wait, linking to the locks page" do
      visit "/pgbus"

      card = find("a[href$='/pgbus/locks']", text: "Parked jobs")
      expect(card).to have_text("7")
      expect(card).to have_text("oldest 13m 32s")
    end
  end

  it "shows queues table with metrics" do
    visit "/pgbus"

    expect(page).to have_text("pgbus_default")
    expect(page).to have_text("500") # total_messages
  end

  it "shows active processes" do
    visit "/pgbus"

    expect(page).to have_text("worker")
    expect(page).to have_text("test-host")
    expect(page).to have_text("12345")
  end

  it "shows empty state for failures" do
    visit "/pgbus"

    expect(page).to have_text("No failures")
  end

  context "with failed events" do
    before do
      @stub_data_source.failed_events_list = [
        { "id" => 1, "queue_name" => "pgbus_default", "error_class" => "RuntimeError",
          "error_message" => "Something went wrong", "failed_at" => Time.now.utc.iso8601 }
      ]
      @stub_data_source.stats[:failed_count] = 1
    end

    it "shows recent failures" do
      visit "/pgbus"

      expect(page).to have_text("RuntimeError")
      expect(page).to have_text("Something went wrong")
    end
  end

  it "navigates to queues page" do
    visit "/pgbus"

    click_link "Queues", match: :first
    expect(page).to have_css("h1", text: "Queues")
  end

  it "navigates to jobs page" do
    visit "/pgbus"

    click_link "Jobs"
    expect(page).to have_css("h1", text: "Jobs")
  end

  it "shows Pgbus branding and version" do
    visit "/pgbus"

    within("nav") do
      expect(page).to have_text("Pgbus")
      expect(page).to have_text(Pgbus::VERSION)
    end
  end
end
