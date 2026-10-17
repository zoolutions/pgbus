# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Every workflow class with its runs going on now, the runs that ended
      # in a window and the end-to-end percentiles (issue #549). Maps to
      # DataSource#workflow_definitions, the dashboard's
      # /pgbus/workflows/definitions.
      class WorkflowDefinitionsTool < BaseTool
        tool_name "pgbus_workflow_definitions"
        title "Pgbus Workflow Definitions"
        description <<~DESC
          One row per Pgbus::Workflow class (every registered subclass and every class name still in pgbus_workflows): loaded (false when the class no longer loads in this app), ongoing and paused runs now, attention (runs that need a person, as pgbus_workflows_attention groups them), finished and cancelled runs that ended in the window, e2e_ms (p50/p95 of created → finished over the finished ones, with samples) and removed_in_flight (ongoing runs still on a removed_step). window is 1h, 24h (default), 7d or 30d, capped at workflow_retention; the answer says what it read. Drill into one class with pgbus_workflow_stats.
        DESC

        input_schema(
          properties: {
            window: { type: "string", enum: Web::WorkflowWindow::CHOICES.keys,
                      description: "1h, 24h (default), 7d or 30d; capped at workflow_retention." }
          },
          required: []
        )

        def self.call(window: nil, server_context: nil)
          parsed = Web::WorkflowWindow.parse(window)
          definitions = data_source_from(server_context).workflow_definitions(window: parsed)
          json_response(Serialization.serialize(window: parsed.to_h, definitions: definitions))
        rescue Web::WorkflowWindow::Invalid => e
          error_response(e.message)
        end
      end
    end
  end
end
