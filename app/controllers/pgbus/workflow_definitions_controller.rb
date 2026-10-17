# frozen_string_literal: true

module Pgbus
  # The workflow insights (issue #549): one row per workflow class, and one
  # class's pipeline, step performance, throughput, durations, funnel and
  # per-revision numbers, over a Web::WorkflowWindow (?window=1h|24h|7d|30d).
  # The route constrains the class name to a constant path; one no class or
  # run has is a 404 page.
  class WorkflowDefinitionsController < ApplicationController
    rescue_from Web::WorkflowWindow::Invalid do |error|
      render plain: t("pgbus.workflow_insights.bad_request", reason: error.message), status: :bad_request
    end

    def index
      @window = Web::WorkflowWindow.parse(params[:window])
      @definitions = data_source.workflow_definitions(window: @window)
      render "pgbus/workflows/definitions/index"
    end

    def show
      @window = Web::WorkflowWindow.parse(params[:window])
      @definition = data_source.workflow_definition(params[:workflow_class], window: @window)
      return render("pgbus/workflows/definitions/not_found", status: :not_found) unless @definition

      render "pgbus/workflows/definitions/show"
    end
  end
end
