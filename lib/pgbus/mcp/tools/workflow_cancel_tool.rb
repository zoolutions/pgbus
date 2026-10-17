# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Cancels an ongoing workflow run (issue #550). DataSource#cancel_workflow.
      class WorkflowCancelTool < WriteTool
        tool_name "pgbus_workflow_cancel"
        title "Pgbus Workflow: Cancel"
        description verb_description(:cancel, "Cancel an ongoing Pgbus::Workflow run. A cancelled run does not come back.")
        idempotent!
        input_schema(**workflow_input_schema)

        def self.call(workflow_id:, reason:, expected_status: nil, server_context: nil)
          perform(:cancel, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                           server_context: server_context) do |data_source, **who|
            data_source.cancel_workflow(workflow_id, **who)
          end
        end
      end
    end
  end
end
