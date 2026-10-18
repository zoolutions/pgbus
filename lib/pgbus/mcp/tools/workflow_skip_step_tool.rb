# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Skips the step a waiting workflow run waits on (issue #550).
      # DataSource#skip_workflow_step.
      class WorkflowSkipStepTool < WriteTool
        tool_name "pgbus_workflow_skip_step"
        title "Pgbus Workflow: Skip Step"
        description verb_description(:skip, "Skip the step a waiting Pgbus::Workflow run waits on (a signal or a fan-out batch).")
        input_schema(**workflow_input_schema)

        def self.call(workflow_id:, reason:, expected_status: nil, server_context: nil)
          perform(:skip, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                         server_context: server_context) do |data_source, **who|
            data_source.skip_workflow_step(workflow_id, **who)
          end
        end
      end
    end
  end
end
