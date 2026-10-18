# frozen_string_literal: true

module Pgbus
  module Metrics
    # The pgbus.workflow.* event → metric mapping (issue #540), shared by
    # Metrics::Subscriber (Prometheus, StatsD) and the AppSignal subscriber so
    # both backends report the same names and labels:
    #
    #   pgbus_workflow_step_duration_seconds{workflow_class, step, outcome}  histogram
    #   pgbus_workflow_steps_total{workflow_class, step, outcome}            counter
    #   pgbus_workflows_finished_total{workflow_class}                       counter
    #   pgbus_workflows_paused_total{workflow_class, error_class}            counter (error_class absent for a manual pause)
    #   pgbus_workflows_cancelled_total{workflow_class}                      counter
    #   pgbus_workflow_stalls_total{workflow_class, step, detection}         counter (detection: redelivery | sweep)
    #   pgbus_workflow_wait_overdue_total{workflow_class, step}              counter (a late notice of an on_timeout: :notify wait)
    #   pgbus_workflows{workflow_class, status}                              gauge, from each sweep
    #   pgbus_workflows_stuck{workflow_class, reason}                        gauge, from each sweep
    #
    # Label values come from code (class, step and exception names), so
    # their cardinality is bounded; workflow ids never become labels.
    module WorkflowMetrics
      EVENTS = %w[
        pgbus.workflow.step pgbus.workflow_finished pgbus.workflow_paused pgbus.workflow_cancelled
        pgbus.workflow.stall_detected pgbus.workflow.wait_timed_out pgbus.workflow.gauge
      ].freeze

      module_function

      # [[kind, name, value, tags], ...] for one event; kind is :histogram,
      # :counter or :gauge.
      def samples(event_name, payload)
        klass = payload[:workflow_class]
        case event_name
        when "pgbus.workflow.step" then step_samples(klass, payload)
        when "pgbus.workflow_finished" then [counter("pgbus_workflows_finished_total", workflow_class: klass)]
        when "pgbus.workflow_paused"
          [counter("pgbus_workflows_paused_total", workflow_class: klass, error_class: payload[:error_class])]
        when "pgbus.workflow_cancelled" then [counter("pgbus_workflows_cancelled_total", workflow_class: klass)]
        when "pgbus.workflow.stall_detected"
          [counter("pgbus_workflow_stalls_total", workflow_class: klass, step: payload[:step_name], detection: payload[:detection])]
        when "pgbus.workflow.wait_timed_out" then wait_timed_out_samples(klass, payload)
        when "pgbus.workflow.gauge" then gauge_samples(payload)
        else []
        end
      end

      def step_samples(klass, payload)
        tags = { workflow_class: klass, step: payload[:step_name], outcome: payload[:outcome] }.compact
        samples = [[:counter, "pgbus_workflow_steps_total", 1, tags]]
        samples.unshift([:histogram, "pgbus_workflow_step_duration_seconds", payload[:duration], tags]) if payload[:duration]
        samples
      end

      # Only :notify keeps waiting past the deadline; every other on_timeout
      # ends the wait and is counted by the step or run it ends.
      def wait_timed_out_samples(klass, payload)
        return [] unless payload[:action] == "notify"

        [counter("pgbus_workflow_wait_overdue_total", workflow_class: klass, step: payload[:step_name])]
      end

      def gauge_samples(payload)
        statuses = Array(payload[:statuses]).map do |row|
          [:gauge, "pgbus_workflows", row[:count], { workflow_class: row[:workflow_class], status: row[:status] }]
        end
        stuck = Array(payload[:stuck]).map do |row|
          [:gauge, "pgbus_workflows_stuck", row[:count], { workflow_class: row[:workflow_class], reason: row[:reason] }]
        end
        statuses + stuck
      end

      def counter(name, **tags) = [:counter, name, 1, tags.compact]
    end
  end
end
