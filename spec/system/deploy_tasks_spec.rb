# frozen_string_literal: true

require "system_helper"

RSpec.describe "Deploy Tasks", type: :system do
  include ActiveSupport::Testing::TimeHelpers

  let(:now) { Time.zone.now.change(usec: 0) }
  let(:ready) do
    { revision: "rev-b", reason: :ready, lagging: [], workers_live: 2,
      live_by_revision: { "rev-b" => 4, "rev-a" => 2 }, scheduler: true }
  end
  let(:lagging) do
    ready.merge(reason: :lagging, lagging: [{ kind: "worker", hostname: "host-a", pid: 41, revision: "rev-a" },
                                            { kind: "dispatcher", hostname: "host-a", pid: 42, revision: nil }])
  end

  def task(id, key, **attrs)
    { id: id, key: key, class_name: "#{key.split("_").map(&:capitalize).join}Job", command: nil, version: "1",
      every_deploy: false, queue_name: "maintenance", priority: 0, description: nil, enabled: true, static: true,
      armed_at: nil, armed_for: nil, last_rollover_run: nil, last_manual_run: nil, created_at: now - 86_400,
      updated_at: now - 60 }.merge(attrs)
  end

  def rollover_run(ago, revision: "rev-a") = { run_at: now - ago, revision: revision, version: "1", triggered_by: "rollover" }

  before do
    travel_to(now)
    @stub_data_source.deploy_rollover_hash = ready
    @stub_data_source.deploy_tasks_list = [
      task(1, "backfill_totals", version: "2", armed_at: now - 120, armed_for: "2",
                                 description: "Recompute totals after the currency migration"),
      task(2, "warm_caches", class_name: nil, command: "FragmentCacheWarmer.run", every_deploy: true,
                             last_rollover_run: rollover_run(300), last_manual_run: { run_at: now - 60, revision: "rev-b",
                                                                                      version: "1", triggered_by: "manual" }),
      task(3, "fix_currencies"),
      task(4, "purge_tokens", enabled: false)
    ]
  end

  after { travel_back }

  describe "the rollover banner" do
    it "says the fleet rolled over and counts live processes per revision" do
      visit "/pgbus/deploy_tasks"

      expect(page).to have_css("h1", text: "Deploy Tasks")
      expect(page).to have_text("Rolled over: every live process runs rev-b.")
      expect(page).to have_text("4 on rev-b")
      expect(page).to have_text("2 on rev-a")
    end

    it "names the processes still on another revision" do
      @stub_data_source.deploy_rollover_hash = lagging

      visit "/pgbus/deploy_tasks"

      expect(page).to have_text("Waiting for rollover: 2 processes are not on rev-b yet.")
      expect(page).to have_text("worker@host-a pid 41 on rev-a")
      expect(page).to have_text("dispatcher@host-a pid 42 on no revision")
    end

    it "waits for a worker on the new revision" do
      @stub_data_source.deploy_rollover_hash = ready.merge(reason: :no_workers, workers_live: 0)

      visit "/pgbus/deploy_tasks"

      expect(page).to have_text("Waiting for rollover: no worker on rev-b yet.")
    end

    it "says when no app revision is configured, and how to set one" do
      @stub_data_source.deploy_rollover_hash = ready.merge(reason: :no_revision, revision: nil)

      visit "/pgbus/deploy_tasks"

      expect(page).to have_text("App revision not configured: deploy tasks wait until it is.")
      expect(page).to have_text("config.app_revision")
    end

    it "says when no scheduler is running" do
      @stub_data_source.deploy_rollover_hash = ready.merge(scheduler: false)

      visit "/pgbus/deploy_tasks"

      expect(page).to have_text("No scheduler is running")
    end

    it "renders in dark mode" do
      @stub_data_source.deploy_rollover_hash = lagging

      visit_dark "/pgbus/deploy_tasks"

      expect(page).to have_css("html.dark")
      expect(page).to have_text("Waiting for rollover: 2 processes are not on rev-b yet.")
    end
  end

  describe "task states" do
    it "says each state in words" do
      visit "/pgbus/deploy_tasks"

      within("tr", text: "backfill_totals") do
        expect(page).to have_text("Due")
        expect(page).to have_text("Due: runs on the scheduler's next tick.")
        expect(page).to have_text("v2")
      end
      within("tr", text: "warm_caches") do
        expect(page).to have_text("Ran 5m ago on rev-a (rollover).")
        expect(page).to have_text("ran 1m ago (manual)")
        expect(page).to have_text("Every deploy")
      end
      within("tr", text: "fix_currencies") { expect(page).to have_text("Never ran.") }
      within("tr", text: "purge_tokens") { expect(page).to have_text("Disabled: the scheduler skips it.") }
    end

    it "gives the gate's reason for an armed task while the gate is closed" do
      @stub_data_source.deploy_rollover_hash = lagging

      visit "/pgbus/deploy_tasks"

      within("tr", text: "backfill_totals") do
        expect(page).to have_text("Waiting")
        expect(page).to have_text("Waiting for rollover: 2 processes are not on rev-b yet.")
      end
    end

    it "hides Run now for a disabled task" do
      visit "/pgbus/deploy_tasks"

      within("tr", text: "purge_tokens") do
        expect(page).to have_button("Enable")
        expect(page).to have_no_button("Run Now")
      end
      within("tr", text: "fix_currencies") { expect(page).to have_button("Run Now") }
    end

    it "states the words in dark mode too" do
      visit_dark "/pgbus/deploy_tasks"

      expect(page).to have_text("Due: runs on the scheduler's next tick.")
    end
  end

  describe "Run now" do
    it "flashes success when the task was enqueued" do
      visit "/pgbus/deploy_tasks"

      within("tr", text: "fix_currencies") { click_button "Run Now" }

      expect(page).to have_text("Deploy task enqueued")
      expect(@stub_data_source.calls[:enqueue_deploy_task_now]).to eq([["3"]])
    end

    it "flashes the failure when it was not" do
      @stub_data_source.deploy_enqueue_result = :failed
      visit "/pgbus/deploy_tasks"

      within("tr", text: "fix_currencies") { click_button "Run Now" }

      expect(page).to have_text("The deploy task was not enqueued")
    end
  end

  describe "empty state" do
    it "says where deploy tasks are declared" do
      @stub_data_source.deploy_tasks_list = []

      visit "/pgbus/deploy_tasks"

      expect(page).to have_text("No deploy tasks declared")
      expect(page).to have_text("config/deploy_tasks.yml")
    end
  end

  describe "pager" do
    it "pages past 25 tasks" do
      @stub_data_source.deploy_tasks_list = Array.new(30) { |i| task(i + 1, format("task_%02d", i + 1)) }
      showing = ->(from, to) { I18n.t("pgbus.helpers.pagination.showing", from: from, to: to, total: 30) }

      visit "/pgbus/deploy_tasks"
      expect(page).to have_text(showing.call(1, 25))

      within("turbo-frame#deploy-tasks") { click_link "Next" }

      expect(page).to have_text(showing.call(26, 30))
      expect(page).to have_text("task_30")
    end
  end

  describe "show page" do
    before do
      @stub_data_source.deploy_task_runs_list = [
        { run_at: now - 60, revision: "rev-b", version: "1", triggered_by: "manual", queue_name: "low", msg_id: 9 },
        { run_at: now - 300, revision: "rev-a", version: "1", triggered_by: "rollover", queue_name: "low", msg_id: 8 }
      ]
    end

    it "shows the configuration and the runs with trigger, version, revision and time" do
      visit "/pgbus/deploy_tasks/2"

      expect(page).to have_css("h1", text: "warm_caches")
      expect(page).to have_text("FragmentCacheWarmer.run")
      expect(page).to have_text("Configuration")
      within("table", text: /triggered by/i) do
        expect(page).to have_text("Manual")
        expect(page).to have_text("Rollover")
        expect(page).to have_text("rev-a")
        expect(page).to have_css("time", text: "5m ago")
      end
    end

    it "redirects an unknown task to the list" do
      visit "/pgbus/deploy_tasks/999"

      expect(page).to have_text("Deploy task not found")
      expect(page).to have_css("h1", text: "Deploy Tasks")
    end

    it "renders in dark mode" do
      visit_dark "/pgbus/deploy_tasks/1"

      expect(page).to have_css("html.dark")
      expect(page).to have_text("Recompute totals after the currency migration")
    end
  end
end
