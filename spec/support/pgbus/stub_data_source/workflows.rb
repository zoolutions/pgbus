# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The Workflows pages (issue #542), the way DataSource::WorkflowList and
      # WorkflowActions answer them: the list filtered and paged in memory,
      # the detail derived from the row (timeline and durations through the
      # real Web::WorkflowTimeline), and every action recorded with its actor
      # and reason, answering workflow_action_result.
      module Workflows
        DETAIL_DEFAULTS = { arguments: {}, steps: [], signals: [], events: [], definition_outdated: false,
                            class_loaded: true, step_names: [], resume_action: nil, resume_step: nil,
                            signal_names: [], reruns: [] }.freeze
        FILTER_COLUMNS = { workflow_class: :workflow_class, step: :current_step, error_class: :error_class,
                           subject_type: :subject_type, subject_id: :subject_id, workflow_id: :workflow_id }.freeze

        def workflows(status: "ongoing", page: 1, per_page: 25, **filters)
          record(:workflows, { status: status, page: page, per_page: per_page, **filters })
          slice(filtered_workflows(status, filters), page, per_page)
        end

        def workflows_count(status: "ongoing", **filters)
          counts = Pgbus::Web::DataSource::ListCounts
          rows = filtered_workflows(status, filters).size
          counts::Count.new(total: [rows, counts::COUNT_CAP].min, capped: rows > counts::COUNT_CAP)
        end

        def workflow_detail(workflow_id)
          row = @workflows_list.find { |w| w[:workflow_id] == workflow_id }
          return nil unless row

          now = Time.now
          detail = DETAIL_DEFAULTS.merge(available_actions: Pgbus::Workflow::States.available_actions(row[:status])).merge(row)
          steps = detail[:steps].map { |step| step.merge(durations: Pgbus::Web::WorkflowTimeline.durations(step, now: now)) }
          detail.merge(steps: steps,
                       timeline: Pgbus::Web::WorkflowTimeline.chart(detail[:steps], detail[:events], status: row[:status], now: now))
        end

        def ongoing_workflows_count = @workflows_list.count { |w| Pgbus::Workflow::States::ONGOING.include?(w[:status]) }
        def paused_workflows_count = @workflows_list.count { |w| w[:status] == "paused" }
        def workflow_attention_count = @workflow_attention_groups_list.sum { |g| g[:count] }

        def workflow_attention_groups(workflow_class: nil)
          groups = @workflow_attention_groups_list
          groups = groups.select { |group| group[:workflow_class] == workflow_class } if workflow_class
          groups.map do |group|
            status = Pgbus::Web::WorkflowActionPlan.group_status(group[:reason])
            group.merge(status: status, available_actions: Pgbus::Workflow::States.available_actions(status))
          end
        end

        %i[resume_workflow cancel_workflow skip_workflow_step run_workflow_step_now].each do |action|
          define_method(action) do |workflow_id, actor:, reason: nil|
            record(action, workflow_id, { actor: actor, reason: reason })
            @workflow_action_result
          end
        end

        def resume_workflow_at(workflow_id, step:, actor:, reason: nil)
          record(:resume_workflow_at, workflow_id, { step: step, actor: actor, reason: reason })
          @workflow_action_result
        end

        def signal_workflow(workflow_id, name:, payload:, idempotency_key:, actor:)
          record(:signal_workflow, workflow_id, { name: name, payload: payload, idempotency_key: idempotency_key,
                                                  actor: actor })
          @workflow_action_result[:ok] ? @workflow_action_result.merge(duplicate: false) : @workflow_action_result
        end

        # The new run is a sample row that names this one as its rerun_of, else a made-up id.
        def rerun_workflow(workflow_id, actor:, reason: nil)
          record(:rerun_workflow, workflow_id, { actor: actor, reason: reason })
          return @workflow_action_result unless @workflow_action_result[:ok]

          rerun = @workflows_list.find { |w| w[:rerun_of_workflow_id] == workflow_id }
          @workflow_action_result.merge(workflow_id: rerun ? rerun[:workflow_id] : "rerun-of-#{workflow_id}")
        end

        %i[bulk_resume_workflows bulk_cancel_workflows].each do |action|
          define_method(action) do |group:, actor:, reason: nil|
            record(action, { group: group, actor: actor, reason: reason })
            @workflow_bulk_result || { ok: true, done: workflow_group_size(group), skipped: 0, failed: 0 }
          end
        end

        private

        def filtered_workflows(status, filters)
          statuses = case status.to_s
                     when "ongoing" then Pgbus::Workflow::States::ONGOING
                     when "all" then Pgbus::Workflow::States::WORKFLOW
                     else [status.to_s]
                     end
          rows = @workflows_list.select { |w| statuses.include?(w[:status]) }
          filters.compact.each do |filter, value|
            rows = case filter
                   when :created_from then rows.select { |w| w[:created_at] >= value }
                   when :created_to then rows.select { |w| w[:created_at] <= value }
                   when :ended_from then rows.select { |w| (ended = w[:finished_at] || w[:cancelled_at]) && ended >= value }
                   when :ended_to then rows.select { |w| (ended = w[:finished_at] || w[:cancelled_at]) && ended <= value }
                   else rows.select { |w| w[FILTER_COLUMNS.fetch(filter)] == value }
                   end
          end
          rows.sort_by { |w| -w[:created_at].to_f }
        end

        def workflow_group_size(group)
          keys = %i[workflow_class step_name reason error_class]
          @workflow_attention_groups_list.find { |g| g.slice(*keys) == group.slice(*keys) }&.fetch(:count) || 0
        end
      end
    end
  end
end
