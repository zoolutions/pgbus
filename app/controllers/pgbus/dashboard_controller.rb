# frozen_string_literal: true

module Pgbus
  class DashboardController < ApplicationController
    include ProcessListing

    def show
      case params[:frame]
      when "stats"
        load_stats
        render_frame("pgbus/dashboard/stats_cards")
      when "queues"
        load_queues
        render_frame("pgbus/dashboard/queues_table")
      when "processes"
        load_processes
        render_frame("pgbus/dashboard/processes_table")
      when "failures"
        @recent_failures = data_source.failed_events(page: 1, per_page: 5)
        render_frame("pgbus/dashboard/recent_failures")
      when "health"
        @health = data_source.queue_health_stats
        render_frame("pgbus/dashboard/queue_health")
      else
        load_stats
        load_queues
        load_processes
        @recent_failures = data_source.failed_events(page: 1, per_page: 5)
        @health = data_source.queue_health_stats
      end
    end

    private

    def load_stats
      @stats = data_source.summary_stats
      @signals = data_source.dashboard_signals
      @workflow_counts = { ongoing: data_source.ongoing_workflows_count, paused: data_source.paused_workflows_count,
                           attention: data_source.workflow_attention_count }
    end

    def load_queues
      @queues = data_source.queues_with_metrics
      @queue_lines = data_source.queue_status_lines
    end
  end
end
