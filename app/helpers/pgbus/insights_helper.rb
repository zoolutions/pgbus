# frozen_string_literal: true

module Pgbus
  # The Insights charts' words (issue #492): one sentence per chart, used as
  # its figcaption and as the chart's accessible name.
  module InsightsHelper
    def pgbus_job_stat_status(status)
      t("pgbus.insights.show.statuses.#{status}", default: status.to_s.humanize)
    end

    def pgbus_throughput_caption(chart, minutes)
      t("pgbus.insights.show.charts.captions.throughput_html",
        total: number_with_delimiter(chart.total), range: pgbus_time_range_label(minutes),
        peak: number_with_delimiter(chart.peak), at: pgbus_time(chart.peak_at, clock: true))
    end

    def pgbus_status_caption(chart)
      t("pgbus.insights.show.charts.captions.status", success: number_with_delimiter(chart[:success]),
                                                      failed: number_with_delimiter(chart[:failed]),
                                                      dead_lettered: number_with_delimiter(chart[:dead_lettered]))
    end

    def pgbus_latency_caption(summary)
      t("pgbus.insights.show.charts.captions.latency", avg: pgbus_ms_duration(summary[:avg_latency_ms]),
                                                       p95: pgbus_ms_duration(summary[:p95_latency_ms]))
    end

    # The labels charts.js puts on the series and the donut legend.
    def pgbus_chart_i18n
      {
        seriesName: t("pgbus.insights.show.charts.series_name"),
        latencyAvg: t("pgbus.insights.show.charts.latency_avg"),
        latencyP95: t("pgbus.insights.show.charts.latency_p95"),
        milliseconds: t("pgbus.helpers.time.units.milliseconds", count: "%n"),
        statuses: Web::InsightsCharts::STATUS_ORDER.index_with { |status| pgbus_job_stat_status(status) }
      }
    end
  end
end
