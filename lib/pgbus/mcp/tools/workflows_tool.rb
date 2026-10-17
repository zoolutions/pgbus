# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Workflow runs, filtered like the dashboard's /pgbus/workflows (issue
      # #542). Maps to DataSource#workflows and #workflows_count; the filters
      # are checked by the same Web::WorkflowQuery the index uses.
      class WorkflowsTool < BaseTool
        tool_name "pgbus_workflows"
        title "Pgbus Workflows"
        description <<~DESC
          List Pgbus::Workflow runs, newest first: class, subject, status, current step and attempt, what a waiting step waits for (signal name, fan-out batch progress), when a scheduled step runs, and the error class of a paused one. status is "ongoing" (default: ready, performing, waiting or paused), one state, or "all"; the other filters combine with AND. created_from/created_to take an ISO 8601 date or time. Paginated (default 25, max 100); total is capped at 10000. The error message is redacted unless the server allows payloads and include_payloads is set.
        DESC

        MAX_PER_PAGE = 100
        MAX_PAGE = 1_000
        TEXT = %w[workflow_class step error_class subject_type subject_id workflow_id].freeze

        input_schema(
          properties: {
            status: { type: "string", enum: Web::DataSource::WorkflowList::STATUSES,
                      description: "ongoing (default), ready, performing, waiting, paused, finished, cancelled or all." },
            **TEXT.to_h { |name| [name.to_sym, { type: "string", description: "Exact #{name.tr("_", " ")}." }] },
            created_from: { type: "string", description: "Created at or after this ISO 8601 date or time." },
            created_to: { type: "string", description: "Created at or before this ISO 8601 date (the whole day) or time." },
            page: { type: "integer", description: "1-based page number (default 1, max 1000).", minimum: 1 },
            per_page: { type: "integer", description: "Rows per page (default 25, max 100).", minimum: 1 },
            include_payloads: {
              type: "boolean", description: "Include error messages. Only honored when the server allows payloads."
            }
          },
          required: []
        )

        def self.call(page: 1, per_page: 25, include_payloads: false, server_context: nil, **filters)
          query = Web::WorkflowQuery.new(filters)
          data_source = data_source_from(server_context)
          per_page = per_page.to_i.clamp(1, MAX_PER_PAGE)
          page = page.to_i.clamp(1, MAX_PAGE)
          rows = data_source.workflows(status: query.status, page: page, per_page: per_page, **query.filters)
          count = data_source.workflows_count(status: query.status, **query.filters)
          allowed = payloads_allowed?(server_context, include_payloads)

          json_response(
            { workflows: rows.map { |row| WorkflowPayloads.row(row, allowed: allowed) }, page: page, per_page: per_page,
              total: count.total, has_more: more_pages?(count, page: page, per_page: per_page, shown: rows.size) },
            server_context: server_context, include_payloads: include_payloads
          )
        rescue Web::WorkflowQuery::Invalid => e
          error_response(e.message)
        end
      end
    end
  end
end
