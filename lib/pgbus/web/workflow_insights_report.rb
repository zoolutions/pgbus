# frozen_string_literal: true

module Pgbus
  module Web
    # Shapes WorkflowInsightsQueries' rows into the hashes the insights pages
    # and MCP tools read (issue #549). Pure: no I/O.
    #
    # Steps come in the class's declaration order (gravestones included, as
    # kind "removed"), then any step name the rows hold that the class does
    # not declare (declared: false). For a class that no longer loads, the
    # names in the rows, ordered by their mean position in the runs started
    # in the window. Live counts are keyed by the run's status (ready,
    # performing, waiting, paused), so each links to the filtered list.
    module WorkflowInsightsReport
      LIVE = Workflow::States::ONGOING.map(&:to_sym).freeze
      STOPPED = Workflow::States::WORKFLOW.map(&:to_sym).freeze
      OUTCOMES = %i[completed failed reattempted skipped cancelled].freeze
      # Percentiles over fewer samples than this get a "few samples" note.
      FEW_SAMPLES = 20

      module_function

      def summary(name, klass, ongoing_rows, ended_row, attention)
        live = ongoing_rows.sum { |row| row[:runs] }
        e2e = ended_row ? ended_row[:e2e_ms].slice(:p50, :p95, :samples) : { p50: nil, p95: nil, samples: 0 }
        { workflow_class: name, loaded: !klass.nil?, ongoing: live,
          paused: ongoing_rows.select { |row| row[:status] == "paused" }.sum { |row| row[:runs] },
          attention: attention, finished: ended_row&.fetch(:finished) || 0, cancelled: ended_row&.fetch(:cancelled) || 0,
          e2e_ms: e2e, removed_in_flight: removed_in_flight(klass, ongoing_rows) }
      end

      def detail(name, klass, window:, since:, now:, ongoing:, ended:, attention:, stats:, histogram:, throughput:, reached:,
                 stopped:, revisions:)
        names = step_list(klass, stats, ongoing, reached, stopped)
        summary(name, klass, ongoing, ended, attention).merge(
          window: window.to_h.merge(since: since), bucket_seconds: window.bucket_seconds,
          started: stopped.values.sum, e2e_ms: ended ? ended[:e2e_ms] : empty_percentiles(%i[p50 p95 p99]),
          steps: names.map { |entry| step(entry, stats[entry[:name]], ongoing) },
          duration_histogram: duration_histogram(histogram),
          throughput: throughput_buckets(throughput, since: since, now: now, bucket: window.bucket_seconds),
          funnel: names.map { |entry| funnel_step(entry[:name], reached, stopped) },
          by_revision: by_revision(revisions, names.map { |entry| entry[:name] })
        )
      end

      # [{ name:, kind:, removed:, declared: }] in pipeline order.
      def step_list(klass, stats, ongoing, reached, stopped)
        seen = (stats.keys + ongoing.map { |row| row[:step] } + reached.keys + stopped.keys.map(&:first)).compact.uniq
        declared = klass ? klass.workflow_steps.entries.map { |entry| declared_step(entry) } : []
        extra = (seen - declared.map { |entry| entry[:name] }).sort_by do |name|
          [reached.dig(name, :position) || Float::INFINITY, stats.dig(name, :first_seen_at)&.to_f || Float::INFINITY, name]
        end
        declared + extra.map { |name| { name: name, kind: nil, removed: false, declared: false } }
      end

      def declared_step(entry)
        kind = if entry.removed? then "removed"
               elsif entry.kind == "fan_out" then "fan_out"
               elsif entry.waits_for_signal? then "signal"
               else "step"
               end
        { name: entry.name, kind: kind, removed: entry.removed?, declared: true }
      end

      def step(entry, stats, ongoing)
        counts = LIVE.to_h { |status| [status, 0] }
        ongoing.each { |row| counts[row[:status].to_sym] += row[:runs] if row[:step] == entry[:name] }
        stats ||= { attempts: 0, run_ms: empty_percentiles(%i[p50 p95 p99]), pickup_ms: empty_percentiles(%i[p50 p95]) }
        entry.merge(counts: counts, sample_size: stats[:attempts], outcomes: OUTCOMES.to_h { |key| [key, stats[key].to_i] },
                    rates: rates(stats), run_ms: stats[:run_ms], pickup_ms: stats[:pickup_ms])
      end

      # Shares of the attempts: failed (an error was recorded), reattempted,
      # skipped. nil without attempts.
      def rates(stats)
        attempts = stats[:attempts].to_i
        %i[failed reattempted skipped].to_h do |key|
          [key, attempts.zero? ? nil : (stats[key].to_f / attempts).round(4)]
        end
      end

      def empty_percentiles(keys) = keys.to_h { |key| [key, nil] }.merge(samples: 0)

      # Gravestones some ongoing run still sits on: { "reminder" => 3 }.
      def removed_in_flight(klass, ongoing_rows)
        return {} unless klass

        names = klass.workflow_steps.removed_names
        ongoing_rows.select { |row| names.include?(row[:step]) }
                    .each_with_object(Hash.new(0)) { |row, counts| counts[row[:step]] += row[:runs] }.to_h
      end

      def duration_histogram(histogram)
        bounds = WorkflowInsightsQueries::DURATION_BOUNDS_MS
        (0..bounds.size).map do |bin|
          { from_ms: bin.zero? ? 0 : bounds[bin - 1], to_ms: bounds[bin], count: histogram.fetch(bin, 0) }
        end
      end

      # Every bucket of the window, oldest first, empty ones as zeros.
      def throughput_buckets(counts, since:, now:, bucket:)
        first = (since.to_i / bucket) * bucket
        (first..now.to_i).step(bucket).map do |start|
          started, finished, cancelled = counts.fetch(start, [0, 0, 0])
          { from: Time.at(start).utc, started: started, finished: finished, cancelled: cancelled }
        end
      end

      def funnel_step(name, reached, stopped)
        { name: name, reached: reached.dig(name, :reached) || 0,
          stopped: STOPPED.to_h { |status| [status, stopped.fetch([name, status.to_s], 0)] } }
      end

      # One entry per revision (newest first): its totals and its steps in
      # pipeline order.
      def by_revision(rows, order)
        rows.group_by { |row| row[:revision] }.map do |revision, group|
          total = group.find { |row| row[:step].nil? }
          steps = group.select { |row| row[:step] }.sort_by { |row| order.index(row[:step]) || order.size }
          { revision: revision, first_seen_at: total[:first_seen_at], last_seen_at: total[:last_seen_at],
            **revision_numbers(total[:stats]),
            steps: steps.map { |row| { name: row[:step], **revision_numbers(row[:stats]) } } }
        end
      end

      def revision_numbers(stats)
        { attempts: stats[:attempts], rates: rates(stats), run_ms: stats[:run_ms].slice(:p50, :p95, :samples) }
      end
    end
  end
end
