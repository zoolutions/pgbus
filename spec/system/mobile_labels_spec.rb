# frozen_string_literal: true

require "system_helper"

# Below 1024 px every pgbus-table row is a card and each cell shows its
# data-label as a ::before label (tailwind.css). The labels reuse the column
# header's translation, so they follow the locale (issue #496).
RSpec.describe "Mobile card labels", type: :system do
  before { @stub_data_source.fill_sample_data! }

  def de(key) = I18n.t(key, locale: :de)

  {
    "/pgbus/outbox" => "pgbus.outbox.index.headers.queue_topic",
    "/pgbus/batches" => "pgbus.batches.index.headers.progress",
    "/pgbus/recurring_tasks" => "pgbus.recurring_tasks.tasks_table.headers.next_run",
    "/pgbus/recurring_tasks/1" => "pgbus.recurring_tasks.show.execution_headers.scheduled_for",
    "/pgbus/queues" => "pgbus.queues.queues_list.headers.total_ever",
    "/pgbus/processes" => "pgbus.processes.processes_table.headers.hostname",
    "/pgbus/events" => "pgbus.events.processed.headers.reason",
    "/pgbus/insights" => "pgbus.insights.show.slowest.headers.job_class",
    "/pgbus" => "pgbus.dashboard.recent_failures.headers.when"
  }.each do |path, key|
    it "labels the cells on #{path} in German" do
      visit "#{path}?locale=de"

      expect(page).to have_css("td[data-label='#{de(key)}']", visible: :all)
    end
  end

  it "labels both lock tables, including their action cells" do
    visit "/pgbus/locks?locale=de"

    %w[pgbus.locks.index.headers.lock_key pgbus.locks.index.headers.actions
       pgbus.locks.concurrency.headers.lease pgbus.locks.concurrency.headers.actions].each do |key|
      expect(page).to have_css("td[data-label='#{de(key)}']", visible: :all)
    end
  end

  it "shows the German label on a phone-width card" do
    page.current_window.resize_to(390, 844)
    visit "/pgbus/outbox?locale=de"

    label = page.evaluate_script(<<~JS)
      getComputedStyle(document.querySelector("table.pgbus-table tbody td[data-label]"), "::before").content
    JS

    expect(label).to eq(de("pgbus.outbox.index.headers.id").to_json)
  end
end
