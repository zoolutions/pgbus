# frozen_string_literal: true

module Pgbus
  module MCP
    module Tools
      # Deploy tasks with their state and the rollover the scheduler waits on.
      # Maps to DataSource#deploy_tasks and #deploy_rollover. Job arguments are
      # not part of the list payload (and are dropped here if a data source
      # ever adds them), so the tool never carries them.
      class DeployTasksTool < BaseTool
        tool_name "pgbus_deploy_tasks"
        title "Pgbus Deploy Tasks"
        description <<~DESC
          Deploy tasks: one-time jobs the scheduler enqueues once every live pgbus process runs the new app revision. Lists each task's state (disabled, waiting, due, ran, never) and the rollover status the scheduler waits on.
          Returns every task unless page or per_page is given; then it is
          paginated (per_page defaults to 100, capped at 100). The response
          carries page, per_page, total (capped at 10000 when paginated; the
          exact row count when not) and has_more; per_page is null when
          unpaginated.
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
          tasks = data_source.deploy_tasks(page: page, per_page: per_page)
          count = data_source.list_count(:deploy_tasks)

          json_response(
            payload(data_source, tasks).merge(
              page: page, per_page: per_page, total: count.total,
              has_more: more_pages?(count, page: page, per_page: per_page, shown: tasks.size)
            )
          )
        end

        def self.unpaged(data_source)
          tasks = data_source.deploy_tasks
          json_response(
            payload(data_source, tasks).merge(page: 1, per_page: nil, total: tasks.size, has_more: false)
          )
        end
        private_class_method :unpaged

        def self.payload(data_source, tasks)
          rollover = data_source.deploy_rollover
          { rollover: serialize(rollover), tasks: tasks.map { |row| task_hash(row, rollover) } }
        end
        private_class_method :payload

        def self.task_hash(row, rollover)
          state = Pgbus::Web::DeployTaskState.present(row, rollover)
          serialize(row.except(:arguments)).merge(
            "state" => state.state, "reason" => state.reason_key, "reason_args" => serialize(state.reason_args)
          )
        end
        private_class_method :task_hash

        # Times as ISO 8601 UTC, symbols as strings, string keys throughout.
        def self.serialize(value)
          case value
          when Hash then value.to_h { |key, val| [key.to_s, serialize(val)] }
          when Array then value.map { |val| serialize(val) }
          when Symbol then value.to_s
          when Time, DateTime, ActiveSupport::TimeWithZone then value.utc.iso8601
          else value
          end
        end
        private_class_method :serialize
      end
    end
  end
end
