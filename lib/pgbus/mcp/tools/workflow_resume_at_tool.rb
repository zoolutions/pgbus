# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Resumes a paused workflow run at a named step (issue #550).
      # DataSource#resume_workflow_at.
      class WorkflowResumeAtTool < WriteTool
        tool_name "pgbus_workflow_resume_at"
        title "Pgbus Workflow: Resume At"
        description verb_description(
          :resume_at, "Resume a paused Pgbus::Workflow run at the named step (pgbus_workflow lists its step_names)."
        )
        input_schema(
          **workflow_input_schema(
            properties: { step: { type: "string", minLength: 1, description: "The step to run next, e.g. capture." } },
            required: %w[step]
          )
        )

        def self.call(workflow_id:, reason:, step:, expected_status: nil, server_context: nil)
          step = step.to_s.strip
          perform(:resume_at, workflow_id: workflow_id, reason: reason, expected_status: expected_status,
                              server_context: server_context,
                              invalid: step.empty? ? "step must name a step" : nil) do |data_source, **who|
            data_source.resume_workflow_at(workflow_id, step: step, **who)
          end
        end
      end
    end
  end
end
