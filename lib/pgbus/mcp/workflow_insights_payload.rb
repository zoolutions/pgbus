# frozen_string_literal: true

module Pgbus
  module MCP
    # The workflow insights' hashes as the MCP tools send them (issue #549).
    # removed_in_flight is keyed by step name, and a step may be named like a
    # payload key (removed_step :payload), which the Redactor would blank:
    # the tools send it as [{ step:, runs: }] rows instead.
    module WorkflowInsightsPayload
      module_function

      def row(row) = row.merge(removed_in_flight: row.fetch(:removed_in_flight, {}).map { |step, runs| { step: step, runs: runs } })
    end
  end
end
