# frozen_string_literal: true

require "pgbus/mcp/health_analyzer"

module Pgbus
  module Test
    class StubDataSource
      # The home page's signals (issue #492), derived from the stub's own rows
      # the way DataSource::DashboardSignals derives them from the database, so
      # the cards, the Jobs page and the DLQ page agree in every spec and
      # screenshot. The verdict is the real HealthAnalyzer over the stub.
      # dashboard_signals_overrides replaces whole signals for a spec.
      module DashboardSignals
        def dashboard_signals
          {
            retrying: retrying_signal, dlq: dlq_signal, processes: process_signal, queues: queue_signal,
            throughput: @throughput_signal, verdict: verdict_signal
          }.merge(@dashboard_signals_overrides)
        end

        def queue_status_lines
          max_retries = Pgbus.configuration.max_retries
          @queues.to_h do |q|
            lines = Pgbus::Web::QueueSummary.present(q, queue_pause_state(q[:name]), queue_drainers(q[:name]),
                                                     max_retries: max_retries)
            [q[:name], Pgbus::Web::QueueSummary.headline(lines)]
          end
        end

        # What HealthAnalyzer reads besides the queues and processes.
        def stream_queue_names = Set.new

        def drained_queue_names
          @queues.map { |q| q[:name] }.select do |name|
            drainers = queue_drainers(name)
            drainers[:wildcard] || drainers[:handler] || Array(drainers[:capsules]).any?
          end.to_set
        end

        private

        # As DashboardSignals#retrying_signal: the count is the Retrying tab;
        # next attempt, due count and newest error come from the retrying rows
        # still in their queue (orphaned failed rows have no vt).
        def retrying_signal
          rows = state_rows(job_rows_in(nil, handler_queue_physical_names), "retrying")
          return Pgbus::Web::DataSource::DashboardSignals::EMPTY_RETRYING if rows.empty?

          now = Time.now
          queued = rows.select { |r| r[:source] == "queue" }
          vts = queued.filter_map { |r| Pgbus::Web::JobState.time(r[:vt]) }
          newest = queued.max_by { |r| Pgbus::Web::JobState.time(r[:last_read_at]) || Time.at(0) }
          { count: rows.size, capped: false, next_attempt_at: vts.select { |t| t > now }.min,
            due_now_count: vts.count { |t| t <= now }, newest_error_class: newest&.dig(:error_class) }
        end

        def dlq_signal
          depth = @queues.select { |q| q[:name].end_with?(Pgbus::DEAD_LETTER_SUFFIX) }.sum { |q| q[:queue_length].to_i }
          newest = @dlq_messages_list.max_by { |m| Pgbus::Web::JobState.time(m[:enqueued_at]) } if depth.positive?
          { depth: depth, newest: newest }
        end

        def process_signal
          status = ->(p) { (p[:status] || (p[:healthy] ? :healthy : :stale)).to_s }
          by_status = @processes_list.group_by(&status).transform_values(&:size)
          by_kind = @processes_list.group_by { |p| p[:kind].to_s }.transform_values(&:size)
          { total: @processes_list.size, healthy: by_status.fetch("healthy", 0),
            stalled: by_status.fetch("stalled", 0), stale: by_status.fetch("stale", 0),
            workers: by_kind.fetch("worker", 0), consumers: by_kind.fetch("consumer", 0),
            dispatchers: by_kind.fetch("dispatcher", 0) }
        end

        def queue_signal
          dlq, live = @queues.partition { |q| q[:name].end_with?(Pgbus::DEAD_LETTER_SUFFIX) }
          drained = drained_queue_names
          undrained = live.count do |q|
            !queue_paused?(q[:name]) && !drained.include?(q[:name]) && q[:queue_visible_length].to_i.positive?
          end
          paused, active = live.partition { |q| queue_paused?(q[:name]) }
          depth = ->(list) { list.sum { |q| q[:queue_length].to_i } }
          claimable = active.sum { |q| q[:queue_visible_length].to_i }
          { total: @queues.size, paused: paused.size, dlq: dlq.size, undrained: undrained,
            depth: depth.call(live), claimable: claimable, waiting: [depth.call(active) - claimable, 0].max,
            paused_depth: depth.call(paused) }
        end

        def verdict_signal
          Pgbus::MCP::HealthAnalyzer.new(VerdictView.new(self)).verdict
        end

        # The stub as HealthAnalyzer sees DataSource: rows carry :status and
        # :paused as DataSource derives them.
        class VerdictView < SimpleDelegator
          def queues_with_metrics
            __getobj__.queues_with_metrics.map { |q| q.merge(paused: queue_paused?(q[:name])) }
          end

          def processes(**)
            __getobj__.processes.map { |p| { status: p[:healthy] ? :healthy : :stale }.merge(p) }
          end
        end
      end
    end
  end
end
