# frozen_string_literal: true

require "system_helper"

RSpec.describe "Locks", type: :system do
  it "shows empty state" do
    visit "/pgbus/locks"

    expect(page).to have_css("h1", text: "Locks")
    expect(page).to have_css("h2", text: "Uniqueness Keys")
    expect(page).to have_text("No active locks")
    expect(page).to have_text("No concurrency keys in use")
  end

  context "with locks" do
    before do
      @stub_data_source.locks_list = [
        { lock_key: "import-42", queue_name: "default", msg_id: 123,
          created_at: Time.now.utc, age_seconds: 120 },
        { lock_key: "export-99", queue_name: "urgent", msg_id: 456,
          created_at: Time.now.utc, age_seconds: 60 }
      ]
    end

    it "displays locks with details" do
      visit "/pgbus/locks"

      expect(page).to have_text("import-42")
      expect(page).to have_text("export-99")
      expect(page).to have_text("default")
      expect(page).to have_text("urgent")
    end

    it "shows per-lock discard buttons" do
      visit "/pgbus/locks"

      expect(page).to have_button("Discard", minimum: 2)
    end

    it "shows Discard All button" do
      visit "/pgbus/locks"

      expect(page).to have_button("Discard All")
    end

    it "shows checkboxes for each lock" do
      visit "/pgbus/locks"

      expect(page).to have_css("input[data-bulk-item]", count: 2)
      expect(page).to have_css("input[data-bulk-select-all]")
    end

    it "discard single lock: confirm and shows toast" do
      visit "/pgbus/locks"

      # Click the first per-row Discard button (inside button_to forms)
      within("[data-bulk-scope=\"locks\"] tbody") do
        all("tr").first.click_button("Discard")
      end
      accept_confirm_dialog

      expect(page).to have_toast("Lock discarded")
      expect(@stub_data_source).to be_called(:discard_lock)
    end

    it "discard all locks: confirm and shows toast" do
      visit "/pgbus/locks"

      click_button "Discard All"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_all_locks)
    end

    it "bulk select and discard selected locks" do
      visit "/pgbus/locks"

      # Initially the "Discard Selected" button is hidden
      expect(page).not_to have_button("Discard Selected", visible: :visible)

      # Check individual checkboxes
      all("input[data-bulk-item]").each(&:check)

      # Now the "Discard Selected" button should appear
      expect(page).to have_button("Discard Selected", visible: :visible)

      click_button "Discard Selected"
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_locks)
    end

    it "select all checkbox toggles all items" do
      visit "/pgbus/locks"

      find("input[data-bulk-select-all]").check

      expect(all("input[data-bulk-item]")).to all(be_checked)

      find("input[data-bulk-select-all]").uncheck

      all("input[data-bulk-item]").each do |cb|
        expect(cb).not_to be_checked
      end
    end
  end

  context "without locks" do
    it "does not show action buttons" do
      visit "/pgbus/locks"

      expect(page).not_to have_button("Discard All")
      expect(page).not_to have_button("Discard Selected")
      expect(page).not_to have_css("input[data-bulk-select-all]")
    end
  end

  context "with concurrency keys" do
    before do
      @stub_data_source.concurrency_stats_hash = {
        parked_total: 7, oldest_parked_age_sec: 812, slots_held: 3, keys_at_limit: 1,
        keys: [
          { key: "ProcessOrder-42", value: 1, max_value: 1, expires_at: Time.now.utc + 300,
            lease_fresh: true, parked_count: 7, oldest_parked_age_sec: 812 },
          { key: "SyncUser-7", value: 2, max_value: 3, expires_at: Time.now.utc - 300,
            lease_fresh: false, parked_count: 0, oldest_parked_age_sec: nil }
        ]
      }
    end

    it "shows the summary cards" do
      visit "/pgbus/locks"

      within("#locks-concurrency") do
        expect(page).to have_css("h2", text: "Concurrency")
        expect(page).to have_text("7")
        expect(page).to have_text("13m 32s")
        expect(page).to have_text("Slots held")
        expect(page).to have_text("Keys at limit")
      end
    end

    it "lists each key with its value, limit, lease and parked count" do
      visit "/pgbus/locks"

      within("#locks-concurrency") do
        expect(page).to have_text("ProcessOrder-42")
        expect(page).to have_text("1 / 1")
        expect(page).to have_text("Live")
        expect(page).to have_text("SyncUser-7")
        expect(page).to have_text("2 / 3")
        expect(page).to have_text("Expired")
      end
    end

    it "warns that a job is probably still running when the lease is fresh" do
      visit "/pgbus/locks"

      confirms = all("#locks-concurrency form[action$='release_key'] button").map do |button|
        button["data-turbo-confirm"]
      end

      expect(confirms.first).to include("lease on ProcessOrder-42 is still fresh")
      expect(confirms.last).to include("Release SyncUser-7 and promote its parked jobs?")
    end

    it "only offers Discard parked for a key that has parked jobs, naming the count" do
      visit "/pgbus/locks"

      buttons = all("#locks-concurrency form[action$='discard_parked'] button")

      expect(buttons.size).to eq(1)
      expect(buttons.first["data-turbo-confirm"]).to include("Discard the 7 jobs parked behind this key?")
    end

    it "releases a key and shows a toast" do
      visit "/pgbus/locks"

      within("#locks-concurrency") { first("form[action$='release_key']").click_button("Release") }
      accept_confirm_dialog

      expect(page).to have_toast("Released")
      expect(@stub_data_source).to be_called(:release_concurrency_key)
    end

    it "discards the parked jobs and shows a toast" do
      visit "/pgbus/locks"

      within("#locks-concurrency") { first("form[action$='discard_parked']").click_button("Discard parked") }
      accept_confirm_dialog

      expect(page).to have_toast("Discarded")
      expect(@stub_data_source).to be_called(:discard_parked_jobs)
    end
  end
end
