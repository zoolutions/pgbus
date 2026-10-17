# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # One workflow run (issue #542): every attempt with its durations, the
      # signals, the audit trail and the outside verbs its status allows.
      # Maps to DataSource#workflow_detail.
      class WorkflowDetailTool < BaseTool
        tool_name "pgbus_workflow"
        title "Pgbus Workflow"
        description <<~DESC
          One Pgbus::Workflow run by workflow_id: status, current step, every step attempt (newest first, at most 100) with status, outcome, scheduled/started/finished times, the waited/queued/ran/waiting durations in seconds, worker, app revision and error class; the signals delivered to it; the audit trail (event, actor, reason; newest first); available_actions (the outside verbs its status allows) and resume_action (what resume would do). Arguments, step outputs, signal payloads and error messages/backtraces are redacted unless the server allows payloads and include_payloads is set.
        DESC

        input_schema(
          properties: {
            workflow_id: { type: "string", description: "The run's workflow_id (a UUID)." },
            include_payloads: {
              type: "boolean",
              description: "Include arguments, outputs, signal payloads and error text. Only honored when the server allows payloads."
            }
          },
          required: %w[workflow_id]
        )

        def self.call(workflow_id:, include_payloads: false, server_context: nil)
          detail = data_source_from(server_context).workflow_detail(workflow_id)
          return error_response("Workflow #{workflow_id} not found") unless detail

          allowed = payloads_allowed?(server_context, include_payloads)
          json_response({ workflow: WorkflowPayloads.detail(detail, allowed: allowed) },
                        server_context: server_context, include_payloads: include_payloads)
        end
      end
    end
  end
end
