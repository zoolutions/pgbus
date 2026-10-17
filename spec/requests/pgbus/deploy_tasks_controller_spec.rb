# frozen_string_literal: true

require "rails_helper"

RSpec.describe "Pgbus::DeployTasksController", type: :request do
  def task(id, key, **attrs)
    { id: id, key: key, class_name: "TaskJob", version: "1", every_deploy: false, enabled: true, static: true,
      armed_at: nil, last_rollover_run: nil, last_manual_run: nil }.merge(attrs)
  end

  describe "GET /pgbus/deploy_tasks" do
    it "renders the index with the rollover banner" do
      get "/pgbus/deploy_tasks"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("No scheduler is running")
    end

    it "renders only the deploy_tasks turbo frame when asked" do
      get "/pgbus/deploy_tasks", params: { frame: "deploy_tasks" }

      expect(response).to have_http_status(:ok)
      expect(response.body).to include('<turbo-frame id="deploy-tasks"')
      expect(response.body).not_to include("<html")
    end

    it "pages the tasks and counts all of them" do
      @stub_data_source.deploy_tasks_list = Array.new(30) { |i| task(i + 1, format("task_row%02d", i)) }

      get "/pgbus/deploy_tasks", params: { page: 2 }

      expect(@stub_data_source.calls[:deploy_tasks]).to eq([[{ page: 2, per_page: 25 }]])
      expect(response.body).to include("30 tasks declared", "Showing 26–30 of 30", "task_row29")
    end
  end

  describe "GET /pgbus/deploy_tasks/:id" do
    it "renders the task" do
      @stub_data_source.deploy_tasks_list = [task(1, "backfill")]

      get "/pgbus/deploy_tasks/1"

      expect(response).to have_http_status(:ok)
      expect(response.body).to include("backfill")
    end

    it "redirects an unknown task with an alert" do
      get "/pgbus/deploy_tasks/999"

      expect(response).to redirect_to("/pgbus/deploy_tasks")
      expect(flash[:alert]).to eq(I18n.t("pgbus.deploy_tasks.not_found"))
    end
  end

  describe "POST /pgbus/deploy_tasks/:id/toggle" do
    before { @stub_data_source.deploy_tasks_list = [task(1, "backfill")] }

    it "flips the task and flashes the new state" do
      post "/pgbus/deploy_tasks/1/toggle"

      expect(response).to redirect_to("/pgbus/deploy_tasks")
      expect(flash[:notice]).to eq(I18n.t("pgbus.deploy_tasks.toggle.disabled"))

      post "/pgbus/deploy_tasks/1/toggle"
      expect(flash[:notice]).to eq(I18n.t("pgbus.deploy_tasks.toggle.enabled"))
    end

    it "flashes a failure for an unknown task" do
      post "/pgbus/deploy_tasks/999/toggle"

      expect(flash[:alert]).to eq(I18n.t("pgbus.deploy_tasks.toggle.failed"))
    end
  end

  describe "POST /pgbus/deploy_tasks/:id/enqueue" do
    it "flashes success when the task was enqueued" do
      post "/pgbus/deploy_tasks/1/enqueue"

      expect(@stub_data_source.calls[:enqueue_deploy_task_now]).to eq([["1"]])
      expect(flash[:notice]).to eq(I18n.t("pgbus.deploy_tasks.enqueue.enqueued"))
    end

    it "flashes the real failure when it was not" do
      @stub_data_source.deploy_enqueue_result = :failed

      post "/pgbus/deploy_tasks/1/enqueue"

      expect(flash[:alert]).to eq(I18n.t("pgbus.deploy_tasks.enqueue.failed"))
    end
  end
end
