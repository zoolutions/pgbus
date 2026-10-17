# frozen_string_literal: true

module Pgbus
  module Web
    # Where a workflow run's time went (issue #542), from its step rows and
    # audit trail. Each attempt splits into
    #
    #   waited   created_at → scheduled_at   the step's wait: (or a reattempt's)
    #   queued   scheduled_at → started_at   due, not yet claimed by a worker
    #   ran      started_at → finished_at    the body (up to now while in progress)
    #   waiting  waiting_since → …           parked on a signal or its fan-out batch
    #
    # and a paused run's pauses (paused → resumed, from the events) are their
    # own segments, so a long gap explains itself. The show page's step table
    # prints durations; the range-bar chart (charts.js renderTimeline) draws
    # chart. Pure: rows in, hashes out.
    module WorkflowTimeline
      OPEN = %w[scheduled waiting in_progress].freeze
      PAUSE_ENDS = %w[resumed cancelled finished].freeze
      PAUSE_EVENTS = (%w[paused] + PAUSE_ENDS).freeze
      RAN_SERIES = { "completed" => "ran_ok", "skipped" => "ran_ok", "failed" => "ran_failed",
                     "in_progress" => "ran_open", "waiting" => "ran_open" }.freeze

      module_function

      # [{ kind:, from:, to: }] in time order. +step+ is a step hash (the
      # data source's) with Time values.
      def segments(step, now:)
        created, scheduled, started, since = step.values_at(:created_at, :scheduled_at, :started_at, :waiting_since)
        ending = step[:finished_at] || (OPEN.include?(step[:status].to_s) ? now : nil)
        pieces = if since && scheduled && since < scheduled
                   # A signal step woken by its signal: the row was scheduled
                   # again at the wake and claimed a second time.
                   [[:waited, created, since], [:waiting, since, scheduled], [:queued, scheduled, started],
                    [:ran, started, ending]]
                 elsif since
                   [[:waited, created, scheduled], [:queued, scheduled, started], [:ran, started, since],
                    [:waiting, since, ending]]
                 else
                   [[:waited, created, scheduled], [:queued, scheduled, started || ending], [:ran, started, ending]]
                 end
        clip(pieces, step[:finished_at])
      end

      # { waited:, queued:, ran:, waiting: } in seconds; nil for a kind the
      # attempt never reached.
      def durations(step, now:)
        totals = segments(step, now: now).group_by { |segment| segment[:kind] }
        %i[waited queued ran waiting].to_h do |kind|
          pieces = totals[kind]
          [kind, pieces&.sum { |segment| (segment[:to] - segment[:from]).to_f }]
        end
      end

      # [{ from:, to: }]: each "paused" event up to the event that ended it,
      # oldest first; a pause still open runs to now. +events+ in any order.
      def paused_periods(events, status:, now:)
        periods = []
        opened = nil
        events.sort_by { |event| event[:created_at] }.each do |event|
          if event[:event] == "paused"
            opened ||= event[:created_at]
          elsif opened && PAUSE_ENDS.include?(event[:event])
            periods << { from: opened, to: event[:created_at] }
            opened = nil
          end
        end
        periods << { from: opened, to: now } if opened && status.to_s == "paused"
        periods
      end

      # The chart payload: one row per attempt (oldest first) and the paused
      # periods. Times are epoch milliseconds; each segment carries its
      # series (its colour: ran is coloured by how the attempt ended) and its
      # duration in words for the tooltip.
      def chart(steps, events, status:, now:)
        rows = steps.sort_by { |step| [step[:created_at], step[:attempt].to_i] }.map do |step|
          { label: "#{step[:step_name]} ##{step[:attempt]}", status: step[:status], outcome: step[:outcome],
            error_class: step[:error_class],
            segments: segments(step, now: now).map { |segment| chart_segment(segment, series_for(segment[:kind], step)) } }
        end
        paused = paused_periods(events, status: status, now: now).map do |period|
          chart_segment(period.merge(kind: :paused), "paused")
        end
        { rows: rows, paused: paused }
      end

      def series_for(kind, step)
        return kind.to_s unless kind == :ran

        RAN_SERIES.fetch(step[:status].to_s, "ran_cancelled")
      end

      def chart_segment(segment, series)
        { kind: segment[:kind].to_s, series: series, from: millis(segment[:from]), to: millis(segment[:to]),
          duration: TimeFormat.duration(segment[:to] - segment[:from]) }
      end

      def millis(time) = (time.to_f * 1000).round

      # Drops a piece without both ends or of no length, and ends every piece
      # at the attempt's finished_at (a step cancelled before its scheduled_at).
      def clip(pieces, finished)
        pieces.filter_map do |kind, from, to|
          next unless from && to

          to = [to, finished].min if finished
          { kind: kind, from: from, to: to } if to > from
        end
      end
    end
  end
end
