# frozen_string_literal: true

module Pgbus
  module Web
    # What each Insights chart says in words, and the rows of its data table
    # (issue #492): the accessible alternative to the SVG, rendered on the
    # server. Pure: it takes the DataSource's chart series and does no I/O.
    #
    # Series are minute buckets; a 30-day window has up to 43,200 of them, so
    # the tables roll consecutive minutes up into at most MAX_TABLE_ROWS rows
    # (the chart itself still plots every minute).
    module InsightsCharts
      MAX_TABLE_ROWS = 60
      STATUS_ORDER = %w[success failed dead_lettered].freeze

      Throughput = Data.define(:total, :peak, :peak_at, :step_minutes, :rows) do
        def empty? = rows.empty?
      end

      Status = Data.define(:total, :rows) do
        def empty? = total.zero?
        def [](status) = rows.find { |r| r[:status] == status.to_s }&.dig(:count).to_i
      end

      Latency = Data.define(:step_minutes, :rows) do
        def empty? = rows.empty?
      end

      module_function

      def throughput(points, minutes:)
        series = points.filter_map { |p| (time = TimeFormat.coerce(p[:time])) && [time, p[:count].to_i] }
        busiest = series.max_by(&:last)
        step = step_minutes(minutes)
        rows = bucket(series, step).map { |from, group| { from: from, count: group.sum(&:last) } }
        Throughput.new(total: series.sum(&:last), peak: busiest&.last, peak_at: busiest&.first, step_minutes: step,
                       rows: rows)
      end

      def status(counts)
        rows = counts.to_h { |status, count| [status.to_s, count.to_i] }
                     .sort_by { |status, _| [STATUS_ORDER.index(status) || STATUS_ORDER.size, status] }
        total = rows.sum(&:last)
        Status.new(total: total, rows: rows.map do |status, count|
          { status: status, count: count, share: total.zero? ? 0.0 : count.to_f / total }
        end)
      end

      # Each row: the mean of its minutes' averages and the highest minute p95
      # (percentiles do not average).
      def latency(points, minutes:)
        series = points.filter_map { |p| (time = TimeFormat.coerce(p[:time])) && [time, p[:avg_ms].to_f, p[:p95_ms].to_f] }
        step = step_minutes(minutes)
        rows = bucket(series, step).map do |from, group|
          { from: from, avg_ms: (group.sum { |g| g[1] } / group.size).round, p95_ms: group.map(&:last).max.round }
        end
        Latency.new(step_minutes: step, rows: rows)
      end

      def step_minutes(minutes)
        [(minutes.to_f / MAX_TABLE_ROWS).ceil, 1].max
      end

      # [[bucket start, [entries]], ...] in time order; entries start with their Time.
      def bucket(series, step)
        seconds = step * 60
        series.group_by { |entry| (entry.first.to_i / seconds) * seconds }
              .sort_by(&:first)
              .map { |start, group| [Time.at(start).utc, group] }
      end
    end
  end
end
