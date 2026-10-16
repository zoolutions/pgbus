# frozen_string_literal: true

module Pgbus
  class DeployTasksController < ApplicationController
    def index
      @page = page_param
      @per_page = per_page
      @deploy_tasks = data_source.deploy_tasks(page: @page, per_page: @per_page)
      @deploy_tasks_count = data_source.list_count(:deploy_tasks)
      @rollover = data_source.deploy_rollover
      render_frame("pgbus/deploy_tasks/tasks_table") if params[:frame] == "deploy_tasks"
    end

    def show
      @task = data_source.deploy_task(params[:id])
      return redirect_to pgbus.deploy_tasks_path, alert: t("pgbus.deploy_tasks.not_found") unless @task

      @rollover = data_source.deploy_rollover
    end

    def toggle
      result = data_source.toggle_deploy_task(params[:id])
      if result
        redirect_to pgbus.deploy_tasks_path, notice: t("pgbus.deploy_tasks.toggle.#{result}")
      else
        redirect_to pgbus.deploy_tasks_path, alert: t("pgbus.deploy_tasks.toggle.failed")
      end
    end

    def enqueue
      if data_source.enqueue_deploy_task_now(params[:id]) == :enqueued
        redirect_to pgbus.deploy_tasks_path, notice: t("pgbus.deploy_tasks.enqueue.enqueued")
      else
        redirect_to pgbus.deploy_tasks_path, alert: t("pgbus.deploy_tasks.enqueue.failed")
      end
    end
  end
end
