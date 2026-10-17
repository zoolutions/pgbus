# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # The dashboard's needs-attention view (issue #542): ongoing runs that
      # wait on a person, grouped. Maps to DataSource#workflow_attention_groups.
      class WorkflowsAttentionTool < BaseTool
        tool_name "pgbus_workflows_attention"
        title "Pgbus Workflows Needing Attention"
        description <<~DESC
          Ongoing Pgbus::Workflow runs that need a person, grouped by (workflow_class, step_name, reason, error_class): reason is paused_error, stalled, paused_manual, overdue (due but not picked up past workflow_stall_threshold) or waiting_overdue (waiting past workflow_waiting_visibility_threshold without a wait_timeout). Each group has its run count, first_seen_at/last_seen_at, the status its runs are in and the actions that status allows; total_runs sums the groups (at most 200 groups, biggest first). The newest error message is redacted unless the server allows payloads and include_payloads is set.
        DESC

        input_schema(
          properties: {
            include_payloads: {
              type: "boolean", description: "Include error messages. Only honored when the server allows payloads."
            }
          },
          required: []
        )

        def self.call(include_payloads: false, server_context: nil)
          groups = data_source_from(server_context).workflow_attention_groups
          allowed = payloads_allowed?(server_context, include_payloads)

          json_response(
            { groups: groups.map { |group| WorkflowPayloads.group(group, allowed: allowed) },
              total_runs: groups.sum { |group| group[:count] } },
            server_context: server_context, include_payloads: include_payloads
          )
        end
      end
    end
  end
end
