# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Lists every queue with depth, visible count, oldest-message age and
      # throughput — the "are queues backed up?" diagnostic. Maps to
      # DataSource#queues_with_metrics. No payloads are involved.
      class QueuesTool < BaseTool
        tool_name "pgbus_queues"
        title "Pgbus Queues"
        description <<~DESC
          List all pgbus queues with their depth (queue_length), visible count
          (messages whose visibility timeout has expired and are ready to be
          claimed), oldest/newest message age in seconds, lifetime total, and
          paused state. Use this to answer "are any queues backed up?".
          oldest_claimable_age_sec is the age of the oldest message actually
          eligible for pickup — nil means no message is currently claimable:
          every remaining message is scheduled, backoff-parked, or in flight
          with a future visibility timeout. A queue whose oldest_msg_age_sec
          keeps growing while oldest_claimable_age_sec stays nil has no
          starving backlog — nothing is waiting for a worker.
          A queue with metrics_unavailable: true could not be measured (its
          metrics query failed or timed out): its counts and ages are null,
          meaning unknown, not zero; pgbus_health reports it as DEGRADED.
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response({ queues: data_source.queues_with_metrics })
        end
      end
    end
  end
end
