# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Reports concurrency-key pressure: how many jobs are parked behind a
      # `limits_concurrency` key, how long the oldest has waited, how many slots
      # are held, and the per-key detail behind those numbers. Maps to
      # DataSource#concurrency_stats. Carries no job payloads — only key names,
      # counts and lease state — so there is nothing to redact.
      class ConcurrencyTool < BaseTool
        tool_name "pgbus_concurrency"
        title "Pgbus Concurrency"
        description <<~DESC
          Report concurrency keys and the jobs parked behind them: total parked
          jobs, the oldest parked job's wait in seconds, slots held, and keys at
          their limit — plus up to 100 key rows with value/limit, lease expiry,
          whether the lease is still fresh, parked count and oldest wait. A key
          with a stale lease and a parked backlog is a stuck pipeline: its holder
          died before releasing the slot. The key rows are paginated (default
          100 per page, capped at 100); the response also carries page,
          per_page, total (number of keys, capped at 10000) and has_more.
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
          stats = data_source.concurrency_stats(page: page, per_page: per_page)
          count = data_source.list_count(:concurrency_keys)

          json_response(
            stats.merge(page: page, per_page: per_page, total: count.total,
                        has_more: more_pages?(count, page: page, per_page: per_page, shown: stats[:keys].size))
          )
        end
      end
    end
  end
end
