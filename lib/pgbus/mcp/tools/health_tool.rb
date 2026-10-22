# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Top-level health verdict: OK / DEGRADED / STALLED, with the reasons
      # that drove it. Delegates the actual analysis to HealthAnalyzer, which
      # reads the same DataSource surface the dashboard uses. This is the
      # single tool that catches the silent-worker-wedge class of incident.
      class HealthTool < BaseTool
        tool_name "pgbus_health"
        title "Pgbus Health"
        description <<~DESC
          Top-level pgbus health verdict. Returns OK, DEGRADED, or STALLED with
          the specific reasons. STALLED means the silent-wedge condition:
          queues hold a visible, never-claimed backlog older than
          health_unclaimed_grace (default 60s) while workers are heart-beating
          but not claiming (or a worker's claim loop has stalled); the summary's
          oldest_unread_claimable_age_sec tells a wedge from a spike. DEGRADED covers
          stale processes, paused queues holding a backlog, dead-letter
          buildup, or a long-running transaction pinning the MVCC horizon. Use
          this first when triaging — it summarizes the whole system in one call
          and is suitable for automated alerting.
        DESC

        input_schema(properties: {}, required: [])

        def self.call(server_context: nil)
          data_source = data_source_from(server_context)
          json_response(HealthAnalyzer.new(data_source).verdict)
        end
      end
    end
  end
end
