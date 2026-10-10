# frozen_string_literal: true

module Pgbus
  class DashboardController < ApplicationController
    include ProcessListing

    def show
      case params[:frame]
      when "stats"
        @stats = data_source.summary_stats
        render_frame("pgbus/dashboard/stats_cards")
      when "queues"
        @queues = data_source.queues_with_metrics
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
        @stats = data_source.summary_stats
        @queues = data_source.queues_with_metrics
        load_processes
        @recent_failures = data_source.failed_events(page: 1, per_page: 5)
        @health = data_source.queue_health_stats
      end
    end
  end
end
