# frozen_string_literal: true

module Pgbus
  class InsightsController < ApplicationController
    def show
      @minutes = insights_minutes
      @summary = data_source.job_stats_summary(minutes: @minutes)
      @slowest = data_source.slowest_job_classes(minutes: @minutes)
      @latency_by_queue = data_source.latency_by_queue(minutes: @minutes)
      @latency_available = data_source.latency_available?
      load_charts

      @live_stream_metrics = data_source.live_stream_metrics

      @stream_stats_available = data_source.stream_stats_available?
      return unless @stream_stats_available

      @stream_summary = data_source.stream_stats_summary(minutes: @minutes)
      @top_streams = data_source.top_streams(minutes: @minutes)
    end

    private

    # The chart series, read once: the view renders their captions and data
    # tables, and embeds the same series for charts.js (issue #492), so the
    # page no longer fetches /pgbus/api/insights to draw them.
    def load_charts
      throughput = data_source.job_throughput(minutes: @minutes)
      status_counts = data_source.job_status_counts(minutes: @minutes)
      latency_trend = @latency_available ? data_source.latency_trend(minutes: @minutes) : []
      @charts = {
        throughput: Web::InsightsCharts.throughput(throughput, minutes: @minutes),
        status: Web::InsightsCharts.status(status_counts),
        latency: Web::InsightsCharts.latency(latency_trend, minutes: @minutes)
      }
      @chart_payload = { throughput: throughput, status_counts: status_counts, latency_trend: latency_trend }
    end
  end
end
