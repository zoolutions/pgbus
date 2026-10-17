# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The workflow insights' reads (issue #549): one row per workflow class
      # for /pgbus/workflows/definitions, and one class's pipeline, step
      # performance, throughput, durations, funnel and per-revision numbers
      # for /pgbus/workflows/definitions/:workflow_class and the MCP tools.
      # Read from the step and run rows inside a WorkflowWindow
      # (WorkflowInsightsQueries); WorkflowInsightsReport shapes the answer.
      #
      # A class is the registered Pgbus::Workflow subclass of that name, or,
      # for a name present in pgbus_workflows, its safe_constantize when that
      # is a Pgbus::Workflow subclass. A name from a URL that is neither is
      # never constantized: the class is unknown.
      module WorkflowInsights
        # A workflow class name: a constant path (OrderWorkflow,
        # Billing::OrderWorkflow). The route constrains to the same shape.
        CLASS_NAME = /\A[A-Z][A-Za-z0-9_]*(?:::[A-Z][A-Za-z0-9_]*)*\z/

        def workflow_definitions(window:)
          return [] unless Workflow::Sweep.tables_ready?

          since = window.since(Time.current)
          ongoing = WorkflowInsightsQueries.ongoing.group_by { |row| row[:workflow_class] }
          ended = WorkflowInsightsQueries.ended(since: since).index_by { |row| row[:workflow_class] }
          stuck = attention_by_class
          names = (registered_workflows.keys + WorkflowInsightsQueries.class_names + ongoing.keys + ended.keys + stuck.keys)
          names.uniq.sort.map do |name|
            WorkflowInsightsReport.summary(name, insights_class(name), ongoing.fetch(name, []), ended[name], stuck.fetch(name, 0))
          end
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading workflow definitions: #{e.class}: #{e.message}" }
          []
        end

        # nil for a name that is not a constant path, that no class or run
        # has, or when the read failed.
        def workflow_definition(workflow_class, window:)
          return nil unless Workflow::Sweep.tables_ready?

          name = workflow_class.to_s
          return nil unless CLASS_NAME.match?(name)

          registered = registered_workflows[name]
          return nil unless registered || WorkflowInsightsQueries.class_known?(name)

          read_definition(name, registered || insights_class(name), window)
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading workflow definition #{workflow_class}: #{e.class}: #{e.message}" }
          nil
        end

        private

        def read_definition(name, klass, window)
          now = Time.current
          since = window.since(now)
          queries = WorkflowInsightsQueries
          WorkflowInsightsReport.detail(
            name, klass,
            window: window, since: since, now: now, ongoing: queries.ongoing(workflow_class: name),
            ended: queries.ended(since: since, workflow_class: name).first, attention: attention_by_class.fetch(name, 0),
            stats: queries.step_stats(name, since: since), histogram: queries.duration_histogram(name, since: since),
            throughput: queries.throughput(name, since: since, bucket: window.bucket_seconds),
            reached: queries.funnel_reached(name, since: since), stopped: queries.funnel_stopped(name, since: since),
            revisions: queries.by_revision(name, since: since)
          )
        end

        # { name => class } of every registered Pgbus::Workflow subclass.
        def registered_workflows = Workflow.registered_classes.to_h { |klass| [klass.name, klass] }

        # The class a run's workflow_class names, or nil when it does not
        # load or is not a Pgbus::Workflow subclass. Only for names from the
        # registry or the rows.
        def insights_class(name)
          registered_workflows.fetch(name) do
            klass = name.safe_constantize
            klass if klass.is_a?(Class) && klass < Workflow
          end
        end

        # { workflow_class => runs needing a person } (stuck_reason_counts).
        def attention_by_class
          WorkflowEntry.stuck_reason_counts(**attention_thresholds).each_with_object(Hash.new(0)) do |((klass, _), runs), sums|
            sums[klass] += runs
          end
        end
      end
    end
  end
end
