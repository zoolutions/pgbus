# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Dashboard interaction UX" do # rubocop:disable RSpec/DescribeClass
  let(:root) { Pathname.new(File.expand_path("../../..", __dir__)) }
  let(:views_dir) { root.join("app", "views") }
  let(:frontend_dir) { root.join("app", "frontend", "pgbus") }

  describe "auto-refresh pauses on user interaction" do
    let(:application_js) { frontend_dir.join("application.js").read }

    it "checks for open details elements before refreshing" do
      expect(application_js).to include("details[open]")
    end

    it "checks for checked checkboxes before refreshing" do
      expect(application_js).to include("data-bulk-item]:checked")
    end

    it "skips refresh when user interaction is detected" do
      expect(application_js).to include("hasUserInteraction")
    end
  end

  describe "recurring tasks table links" do
    let(:tasks_table) { views_dir.join("pgbus", "recurring_tasks", "_tasks_table.html.erb").read }

    it "uses turbo_frame _top for task links to avoid Content missing" do
      expect(tasks_table).to include("turbo_frame: \"_top\"")
    end
  end

  describe "batches table links" do
    let(:batches_table) { views_dir.join("pgbus", "batches", "_batches_table.html.erb").read }

    it "uses turbo_frame _top for batch links to break out of the turbo frame" do
      expect(batches_table).to include("turbo_frame: \"_top\"")
    end
  end

  describe "queue show page" do
    let(:queue_show) { views_dir.join("pgbus", "queues", "show.html.erb").read }

    it "renders the shared Jobs tabs and list instead of its own message table" do
      expect(queue_show).to include('render "pgbus/jobs/tabs"')
      expect(queue_show).to include('render "pgbus/jobs/list"')
      expect(queue_show).not_to include('colspan="5"')
    end

    it "builds no Jobs-page URL in the shared list partials" do
      %w[_list _tabs].each do |partial|
        expect(views_dir.join("pgbus", "jobs", "#{partial}.html.erb").read).not_to include("jobs_path(")
      end
    end
  end
end
