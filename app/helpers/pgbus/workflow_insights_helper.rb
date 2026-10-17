# frozen_string_literal: true

require "json"

module Pgbus
  # The workflow insights' words and links (issue #549). Every count that
  # counts runs links to the Workflows list filtered to exactly those runs;
  # attempt-level numbers (percentiles, rates) are text. Durations go through
  # pgbus_ms_duration under a minute and pgbus_duration past it, so a two-day
  # run reads "2d 0h", not "2880.0m".
  module WorkflowInsightsHelper
    LIVE_STATUSES = Workflow::States::ONGOING
    STOPPED_STATUSES = Workflow::States::WORKFLOW

    KIND_TONES = { "fan_out" => :blue, "signal" => :yellow, "removed" => :gray, "undeclared" => :red }.freeze

    def pgbus_insight_ms(millis)
      return Web::TimeFormat::NONE if millis.nil?

      millis < 60_000 ? pgbus_ms_duration(millis) : pgbus_duration(millis / 1000.0)
    end

    # "p50 820ms · p95 4.1s · p99 9.8s", then "(n=7, few samples)" under
    # FEW_SAMPLES; "—" without samples.
    def pgbus_insight_percentiles(percentiles, keys = %i[p50 p95 p99])
      samples = percentiles[:samples].to_i
      return Web::TimeFormat::NONE if samples.zero?

      values = keys.map { |key| t("pgbus.workflow_insights.percentile", name: key.to_s, value: pgbus_insight_ms(percentiles[key])) }
      text = values.join(" · ")
      return text if samples >= Web::WorkflowInsightsReport::FEW_SAMPLES

      "#{text} (#{t("pgbus.workflow_insights.few_samples", count: samples)})"
    end

    def pgbus_insight_rate(ratio) = ratio.nil? ? Web::TimeFormat::NONE : pgbus_percent(ratio)

    # "1h", "6h", "2d": a bucket, a bin bound or the retention, in its largest unit.
    def pgbus_insight_span(seconds) = Web::TimeFormat.largest_unit(seconds.to_i)

    def pgbus_insight_bin_label(bin)
      from = bin[:from_ms] / 1000
      if bin[:from_ms].zero? then t("pgbus.workflow_insights.show.charts.durations.under", to: pgbus_insight_span(bin[:to_ms] / 1000))
      elsif bin[:to_ms].nil? then t("pgbus.workflow_insights.show.charts.durations.over", from: pgbus_insight_span(from))
      else t("pgbus.workflow_insights.show.charts.durations.between", from: pgbus_insight_span(from),
                                                                      to: pgbus_insight_span(bin[:to_ms] / 1000))
      end
    end

    def pgbus_insight_window_name(window) = t("pgbus.workflow_insights.windows.#{window.name}")

    # The filtered Workflows list; Time values as ISO 8601 (microseconds
    # only when there are some).
    def pgbus_insight_runs_path(**query)
      pgbus.workflows_path(query.compact.transform_values { |value| value.is_a?(Time) ? pgbus_insight_param_time(value) : value })
    end

    def pgbus_insight_param_time(time)
      time = time.utc
      time.usec.zero? ? time.iso8601 : time.iso8601(6)
    end

    # A count of runs: the number, linked to +path+ (named +label+ for
    # assistive tech, which keeps the number), or plain "0". size: :lg for
    # a summary card's number.
    def pgbus_insight_count(count, path, label, size: :md)
      return pgbus_number(0) if count.to_i.zero?

      number = pgbus_number(count)
      pgbus_link_to(number, path, size: size, aria: { label: "#{number} #{label}" }, data: { turbo_frame: "_top" })
    end

    def pgbus_insight_definition_path(name, window) = pgbus.workflow_definition_path(name, window: window.name)

    def pgbus_insight_kind(step)
      key = step[:declared] ? step[:kind] : "undeclared"
      return unless KIND_TONES.key?(key)

      pgbus_workflow_tone_badge(t("pgbus.workflow_insights.show.pipeline.kinds.#{key}"), KIND_TONES.fetch(key))
    end

    # "2 runs on removed step nudge", per gravestone some ongoing run still
    # sits on, each linked to those runs.
    def pgbus_insight_removed_links(name, removed_in_flight)
      links = removed_in_flight.filter_map do |step, runs|
        next if runs.zero?

        label = t("pgbus.workflow_insights.removed_in_flight", count: runs, step: step)
        path = pgbus_insight_runs_path(workflow_class: name, step: step)
        pgbus_link_to(label, path, size: :sm, tone: :yellow, data: { turbo_frame: "_top" })
      end
      safe_join(links, " ")
    end

    def pgbus_insight_not_loaded_badge = pgbus_workflow_tone_badge(t("pgbus.workflow_insights.not_loaded"), :gray)

    # The same step on the next older revision, when its failure rate was
    # lower by more than five points: what "up from 1% on 3f2a9c1" says.
    def pgbus_insight_worse(revisions, index, step)
      older = revisions[index + 1]
      before = older && (step ? older[:steps].find { |s| s[:name] == step[:name] } : older)
      now = (step || revisions[index])[:rates][:failed]
      return unless before && now && before[:rates][:failed] && now - before[:rates][:failed] > 0.05

      t("pgbus.workflow_insights.show.by_revision.worse", rate: pgbus_percent(before[:rates][:failed]),
                                                          revision: older[:revision])
    end

    # What charts.js renderWorkflowInsights draws.
    def pgbus_insight_chart_payload(definition)
      { throughput: definition[:throughput].map do |bucket|
        { time: bucket[:from].utc.iso8601, started: bucket[:started], finished: bucket[:finished], cancelled: bucket[:cancelled] }
      end,
        durations: definition[:duration_histogram].map { |bin| { label: pgbus_insight_bin_label(bin), count: bin[:count] } } }
    end

    def pgbus_insight_chart_i18n
      { started: t("pgbus.workflow_insights.show.charts.series.started"),
        finished: t("pgbus.workflow_insights.show.charts.series.finished"),
        cancelled: t("pgbus.workflow_insights.show.charts.series.cancelled"),
        runs: t("pgbus.workflow_insights.show.charts.series.runs") }
    end
  end
end
