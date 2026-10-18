# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Sends a ready run's scheduled step now (issue #550).
      # DataSource#run_workflow_step_now.
      class WorkflowRunNowTool < WriteTool
        tool_name "pgbus_workflow_run_now"
        title "Pgbus Workflow: Run Now"
        description verb_description(:run_now, "Run a ready Pgbus::Workflow run's scheduled step now instead of after its wait.")
        input_schema(**workflow_input_schema)

        def self.call(workflow_id:, reason:, expected_status: nil, server_context: nil)
          perform(:run_now, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                            server_context: server_context) do |data_source, **who|
            data_source.run_workflow_step_now(workflow_id, **who)
          end
        end
      end
    end
  end
end
