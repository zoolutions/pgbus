# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Lists active job uniqueness locks (the leaked-uniqueness-lock
      # diagnostic). Maps to DataSource#job_locks — lock_key, queue_name,
      # msg_id, created_at, age_seconds. No payloads involved.
      class LocksTool < BaseTool
        tool_name "pgbus_locks"
        title "Pgbus Locks"
        description <<~DESC
          List active job uniqueness locks with their lock_key, queue_name,
          msg_id, and age in seconds. A lock that outlives its message
          indicates a leaked uniqueness key blocking re-enqueues. Paginated
          (default 100 per page, capped at 100); the response carries page,
          per_page, total (capped at 10000) and has_more.
        DESC

        MAX_PER_PAGE = 100
        MAX_PAGE = 1_000

        input_schema(
          properties: {
            page: { type: "integer", description: "1-based page number (default 1, max 1000).", minimum: 1 },
            per_page: { type: "integer", description: "Rows per page (default 100, max 100).", minimum: 1 }
          },
          required: []
        )

        def self.call(page: 1, per_page: MAX_PER_PAGE, server_context: nil)
          data_source = data_source_from(server_context)
          per_page = per_page.to_i.clamp(1, MAX_PER_PAGE)
          page = page.to_i.clamp(1, MAX_PAGE)
          locks = data_source.job_locks(page: page, per_page: per_page)
          count = data_source.list_count(:job_locks)

          json_response(
            { locks: locks, page: page, per_page: per_page, total: count.total,
              has_more: count.capped? || (page * per_page) < count.total }
          )
        end
      end
    end
  end
end
