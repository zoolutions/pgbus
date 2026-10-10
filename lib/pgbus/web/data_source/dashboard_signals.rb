# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The facts behind the home page's words (issue #492): what is retrying
      # and when it runs next, what died last and why, how the processes and
      # queues stand, the job rate, and the health verdict /readyz serves.
      #
      # summary_stats keeps its shape (the stats API, MetricsSerializer and the
      # AppSignal probe read it); everything here is home-page only. Each signal
      # degrades to its empty shape and logs, so one failing query never blanks
      # the page.
      module DashboardSignals
        THROUGHPUT_WINDOW_MINUTES = 5

        EMPTY_RETRYING = { count: 0, capped: false, next_attempt_at: nil, due_now_count: 0,
                           newest_error_class: nil }.freeze

        def dashboard_signals
          {
            retrying: retrying_signal, dlq: dlq_signal, processes: process_signal,
            queues: queue_signal, throughput: throughput_signal, verdict: verdict_signal
          }
        end

        # The Retrying tab of the Jobs page (handler queues left to the Events
        # page), plus when the next backoff ends and how many are due now.
        # Each queue contributes its earliest COUNT_CAP + 1 rows by vt (the
        # vt index), so the next attempt is exact and the due count is a
        # lower bound only past the cap, like the tab counts.
        def retrying_signal
          counts = job_state_counts(exclude: handler_queue_physical_names)
          return EMPTY_RETRYING if counts["retrying"].zero?

          schedule = retry_schedule
          {
            count: counts["retrying"], capped: counts.capped?("retrying"),
            next_attempt_at: JobState.time(schedule&.dig("next_at")), due_now_count: schedule&.dig("due").to_i,
            newest_error_class: newest_retry_error_class
          }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the retry signal: #{e.class}: #{e.message}" }
          EMPTY_RETRYING
        end

        # Depth from the cached metrics; the newest message is read from each
        # non-empty DLQ's tail, so the query stays one index-backed row per DLQ.
        def dlq_signal
          counts = dlq_counts_by_queue
          depth = counts.values.sum
          { depth: depth, newest: depth.zero? ? nil : newest_dead_letter(counts.select { |_, n| n.positive? }.keys) }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the dead-letter signal: #{e.class}: #{e.message}" }
          { depth: depth.to_i, newest: nil }
        end

        def process_signal
          rows = processes
          by_status = rows.group_by { |p| p[:status].to_s }.transform_values(&:size)
          by_kind = rows.group_by { |p| p[:kind].to_s }.transform_values(&:size)
          { total: rows.size, healthy: by_status.fetch("healthy", 0), stalled: by_status.fetch("stalled", 0),
            stale: by_status.fetch("stale", 0), workers: by_kind.fetch("worker", 0),
            consumers: by_kind.fetch("consumer", 0), dispatchers: by_kind.fetch("dispatcher", 0) }
        end

        # undrained follows the health verdict: an active (unpaused) backlog on
        # a job queue no capsule or handler drains. A wildcard drains everything.
        def queue_signal
          queues = queues_with_metrics
          dlq, live = queues.partition { |q| q[:name].to_s.end_with?(Pgbus::DEAD_LETTER_SUFFIX) }
          { total: queues.size, paused: live.count { |q| q[:paused] }, dlq: dlq.size, undrained: undrained_count(live) }
        end

        def throughput_signal
          window = THROUGHPUT_WINDOW_MINUTES
          return { available: false, jobs_per_minute: nil, window_minutes: window } unless JobStat.table_exists?

          total = JobStat.summary(minutes: window)[:total].to_i
          { available: true, jobs_per_minute: (total.to_f / window).round(1), window_minutes: window }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the throughput signal: #{e.class}: #{e.message}" }
          { available: false, jobs_per_minute: nil, window_minutes: THROUGHPUT_WINDOW_MINUTES }
        end

        # The verdict /readyz and the MCP serve. The analyzer lives outside
        # Zeitwerk (pgbus/mcp is ignored), as in HealthApp.
        def verdict_signal
          require "pgbus/mcp/health_analyzer" unless defined?(Pgbus::MCP::HealthAnalyzer)
          Pgbus::MCP::HealthAnalyzer.new(self).verdict
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error computing the health verdict: #{e.class}: #{e.message}" }
          nil
        end

        # { physical name => QueueSummary::Line } for the home queues table:
        # the facts QueueSummary needs for every row, loaded once per render
        # (pause states in one query, processes and stream names once).
        def queue_status_lines
          queues = queues_with_metrics
          pauses = queue_pause_states(queues.map { |q| q[:name] })
          healthy = processes.select { |p| p[:healthy] }
          streams = stream_queue_names
          max_retries = Pgbus.configuration.max_retries
          queues.to_h do |q|
            drainers = queue_drainers(q[:name], healthy: healthy, streams: streams)
            lines = Web::QueueSummary.present(q, pauses.fetch(q[:name]), drainers, max_retries: max_retries)
            [q[:name], Web::QueueSummary.headline(lines)]
          end
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the queue status lines: #{e.class}: #{e.message}" }
          {}
        end

        private

        def retry_schedule
          scope = job_list_scope(nil, exclude: handler_queue_physical_names)
          return unless scope && scope[:queues].any?

          fragments = scope[:queues].map do |(qtable, logical)|
            <<~SQL.strip
              (SELECT m.vt FROM pgmq.q_#{qtable} m #{failed_join(qtable, logical)}
               WHERE #{JobList::STATE_PREFILTER["retrying"]} AND (#{JobList::STATE_CASE}) = 'retrying'
               ORDER BY m.vt LIMIT #{JobList::COUNT_CAP + 1})
            SQL
          end
          connection.select_one(<<~SQL, "Pgbus Retry Schedule")
            SELECT MIN(vt) FILTER (WHERE vt > now()) AS next_at, COUNT(*) FILTER (WHERE vt <= now()) AS due
            FROM (#{fragments.join("\nUNION ALL\n")}) AS retrying
          SQL
        end

        def newest_retry_error_class
          connection.select_value(
            "SELECT error_class FROM pgbus_failed_events WHERE #{NOT_HANDLER_QUEUE} ORDER BY failed_at DESC LIMIT 1",
            "Pgbus Newest Retry Error", [handler_queue_names_literal]
          )
        end

        def newest_dead_letter(names)
          fragments = names.map do |name|
            "(SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers, '#{sanitize_name(name)}' " \
              "AS queue_name FROM pgmq.q_#{sanitize_name(name)} ORDER BY msg_id DESC LIMIT 1)"
          end
          row = connection.select_one(
            "SELECT * FROM (#{fragments.join(" UNION ALL ")}) AS newest ORDER BY enqueued_at DESC LIMIT 1",
            "Pgbus Newest Dead Letter"
          )
          row && format_message(row, row["queue_name"])
        end

        def undrained_count(live)
          drained = drained_queue_names
          return 0 if drained.nil?

          backlog = live.reject { |q| q[:paused] || drained.include?(q[:name].to_s) }
                        .select { |q| q[:queue_visible_length].to_i.positive? }
          return 0 if backlog.empty?

          streams = stream_queue_names
          backlog.count { |q| !streams.include?(q[:name].to_s) }
        end
      end
    end
  end
end
