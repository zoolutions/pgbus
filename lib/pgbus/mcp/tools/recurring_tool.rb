# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Recurring task schedule with last-run / next-run times. Maps to
      # DataSource#recurring_tasks. The :arguments field is not part of the
      # list payload, so no redaction is needed here.
      class RecurringTool < BaseTool
        tool_name "pgbus_recurring"
        title "Pgbus Recurring Tasks"
        description <<~DESC
          List recurring (cron) tasks with their schedule, human-readable
          schedule, queue, enabled state, last_run_at and next_run_at. Use this
          to confirm scheduled work is firing on time. Returns every task unless
          page or per_page is given; then it is paginated (per_page defaults to
          100, capped at 100). The response carries page, per_page, total
          (capped at 10000) and has_more; per_page is null when unpaginated.
        DESC

        MAX_PER_PAGE = 100
        MAX_PAGE = 1_000

        input_schema(
          properties: {
            page: { type: "integer", description: "1-based page number (default 1, max 1000).", minimum: 1 },
            per_page: { type: "integer", description: "Rows per page (default 100 once paginating, max 100).", minimum: 1 }
          },
          required: []
        )

        def self.call(page: nil, per_page: nil, server_context: nil)
          data_source = data_source_from(server_context)
          return unpaged(data_source) if page.nil? && per_page.nil?

          per_page = (per_page || MAX_PER_PAGE).to_i.clamp(1, MAX_PER_PAGE)
          page = (page || 1).to_i.clamp(1, MAX_PAGE)
          tasks = data_source.recurring_tasks(page: page, per_page: per_page)
          count = data_source.list_count(:recurring_tasks)

          json_response(
            { recurring_tasks: tasks, page: page, per_page: per_page, total: count.total,
              has_more: more_pages?(count, page: page, per_page: per_page, shown: tasks.size) }
          )
        end

        def self.unpaged(data_source)
          tasks = data_source.recurring_tasks
          json_response({ recurring_tasks: tasks, page: 1, per_page: nil, total: tasks.size, has_more: false })
        end
        private_class_method :unpaged
      end
    end
  end
end
