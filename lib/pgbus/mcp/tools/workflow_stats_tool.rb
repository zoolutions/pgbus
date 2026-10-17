# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # One workflow class's insights over a window (issue #549): its
      # pipeline with live counts and step performance, throughput, durations,
      # funnel and per-revision numbers. Maps to DataSource#workflow_definition,
      # the dashboard's /pgbus/workflows/definitions/:workflow_class.
      class WorkflowStatsTool < BaseTool
        tool_name "pgbus_workflow_stats"
        title "Pgbus Workflow Stats"
        description <<~DESC
          One Pgbus::Workflow class over a window (1h, 24h default, 7d or 30d, capped at workflow_retention). steps, in declaration order (gravestones as kind "removed"; names only the rows hold as declared: false): counts (runs sitting at the step now by run status ready/performing/waiting/paused), run_ms (p50/p95/p99 of finished_at - started_at over completed or failed attempts) and pickup_ms (p50/p95 of scheduled → started), rates (failed: an error was recorded, reattempted, skipped) and outcomes over the sample_size attempts that finished in the window; percentiles over fewer than 20 samples are noisy. Also: started/finished/cancelled, e2e_ms, duration_histogram (finished runs per created → finished range), throughput (runs started/finished/cancelled per bucket_seconds), funnel (of the runs started in the window: reached = runs with a row for the step, stopped = where each run is now by status) and by_revision (the five app revisions that finished an attempt last, newest first, totals and per step).
        DESC

        input_schema(
          properties: {
            workflow_class: { type: "string", description: "The class name, e.g. OrderWorkflow or Billing::OrderWorkflow." },
            window: { type: "string", enum: Web::WorkflowWindow::CHOICES.keys,
                      description: "1h, 24h (default), 7d or 30d; capped at workflow_retention." }
          },
          required: %w[workflow_class]
        )

        def self.call(workflow_class:, window: nil, server_context: nil)
          name = workflow_class.to_s
          unless Web::DataSource::WorkflowInsights::CLASS_NAME.match?(name)
            return error_response("#{name.inspect} is not a class name (OrderWorkflow or Billing::OrderWorkflow)")
          end

          detail = data_source_from(server_context).workflow_definition(name, window: Web::WorkflowWindow.parse(window))
          return error_response("No workflow class or run named #{name}") unless detail

          json_response(Serialization.serialize(workflow: WorkflowInsightsPayload.row(detail)))
        rescue Web::WorkflowWindow::Invalid => e
          error_response(e.message)
        end
      end
    end
  end
end
