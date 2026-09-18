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
          died before releasing the slot.
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response(data_source.concurrency_stats)
        end
      end
    end
  end
end
