# frozen_string_literal: true

module Pgbus
  class RecurringTasksController < ApplicationController
    def index
      @page = page_param
      @per_page = per_page
      @recurring_tasks = data_source.recurring_tasks(page: @page, per_page: @per_page)
      @recurring_tasks_count = data_source.list_count(:recurring_tasks)
      render_frame("pgbus/recurring_tasks/tasks_table") if params[:frame] == "recurring_tasks"
    end

    def show
      @task = data_source.recurring_task(params[:id])
      redirect_to pgbus.recurring_tasks_path, alert: "Task not found" unless @task
    end

    def toggle
      result = data_source.toggle_recurring_task(params[:id])
      if result
        message = result == :enabled ? t("pgbus.recurring_tasks.toggle.enabled") : t("pgbus.recurring_tasks.toggle.disabled")
        redirect_to pgbus.recurring_tasks_path, notice: message
      else
        redirect_to pgbus.recurring_tasks_path, alert: t("pgbus.recurring_tasks.toggle.failed")
      end
    end

    def enqueue
      if data_source.enqueue_recurring_task_now(params[:id])
        redirect_to pgbus.recurring_tasks_path, notice: "Task enqueued"
      else
        redirect_to pgbus.recurring_tasks_path, alert: "Failed to enqueue task"
      end
    end
  end
end
