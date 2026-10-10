# frozen_string_literal: true

require "system_helper"

# Every list that can grow pages through shared/_pager (issue #496). The
# sample data holds 30 rows per list; web_per_page is 25.
RSpec.describe "List pagers", type: :system do
  before { @stub_data_source.fill_sample_data! }

  def showing(first, last, total) = I18n.t("pgbus.helpers.pagination.showing", from: first, to: last, total: total)

  it "pages the Outbox" do
    visit "/pgbus/outbox"
    expect(page).to have_text(showing(1, 25, 30))

    click_link "Next"

    expect(page).to have_text(showing(26, 30, 30))
    expect(page).to have_current_path(/page=2/)
  end

  it "pages Batches inside the auto-refresh frame and advances the URL" do
    visit "/pgbus/batches"
    expect(page).to have_text(showing(1, 25, 30))

    within("turbo-frame#batches-list") { click_link "Next" }

    expect(page).to have_text(showing(26, 30, 30))
    expect(page).to have_current_path(/page=2/)
    expect(page).to have_text("Nightly export 27")
  end

  it "pages Recurring tasks and still counts all of them" do
    visit "/pgbus/recurring_tasks"
    expect(page).to have_text("30 tasks configured")

    within("turbo-frame#recurring-tasks") { click_link "Next" }

    expect(page).to have_text(showing(26, 30, 30))
    expect(page).to have_current_path(/page=2/)
    expect(page).to have_text("30 tasks configured")
  end

  it "says when the count is only a lower bound" do
    @stub_data_source.capped_lists = [:batches]

    visit "/pgbus/batches"

    expect(page).to have_text(showing(1, 25, "10,000+"))
  end

  describe "Locks, two pagers on one page" do
    # Found fresh each time: a page link reloads the whole page.
    def uniqueness = find("h2", text: I18n.t("pgbus.locks.index.uniqueness_title")).ancestor("div.mb-8")

    it "pages the concurrency keys without moving the uniqueness locks" do
      visit "/pgbus/locks"

      within("turbo-frame#locks-concurrency") { click_link "Next" }

      expect(page).to have_current_path(/keys_page=2/)
      within("turbo-frame#locks-concurrency") { expect(page).to have_text(showing(26, 30, 30)) }
      within(uniqueness) { expect(page).to have_text(showing(1, 25, 30)) }
    end

    it "pages the uniqueness locks and keeps the concurrency page" do
      visit "/pgbus/locks?keys_page=2"

      within(uniqueness) { click_link "Next" }

      expect(page).to have_current_path(/keys_page=2/)
      expect(page).to have_current_path(/(?<!keys_)page=2/)
      within(uniqueness) { expect(page).to have_text(showing(26, 30, 30)) }
      within("turbo-frame#locks-concurrency") { expect(page).to have_text(showing(26, 30, 30)) }
    end
  end

  %w[/pgbus/outbox /pgbus/batches /pgbus/recurring_tasks /pgbus/locks].each do |path|
    it "renders the pager on #{path} in dark mode, with the disabled Previous visible" do
      visit_dark(path)

      expect(page).to have_text(showing(1, 25, 30))
      expect(page).to have_css("nav[aria-label='#{I18n.t("pgbus.helpers.pagination.label")}'] span.cursor-not-allowed",
                               text: I18n.t("pgbus.helpers.pagination.previous"))
    end
  end
end
