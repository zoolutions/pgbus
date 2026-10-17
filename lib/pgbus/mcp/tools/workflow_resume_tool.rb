# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Resumes a paused workflow run (issue #550). DataSource#resume_workflow.
      class WorkflowResumeTool < WriteTool
        tool_name "pgbus_workflow_resume"
        title "Pgbus Workflow: Resume"
        description verb_description(
          :resume, "Resume a paused Pgbus::Workflow run (pgbus_workflow's resume_action says what runs next)."
        )
        input_schema(**workflow_input_schema)

        def self.call(workflow_id:, reason:, expected_status: nil, server_context: nil)
          perform(:resume, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                           server_context: server_context) do |data_source, **who|
            data_source.resume_workflow(workflow_id, **who)
          end
        end
      end
    end
  end
end
