# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The Workflows pages' reads (issue #542): the list with its filters,
      # one run's detail (attempts, signals, audit trail, what can be done to
      # it), the needs-attention groups and the home-page counts. An install
      # without the workflow tables (pgbus:add_workflows not run) gets empty
      # lists and zero counts, not an error per refresh.
      module WorkflowList
        STATUSES = (%w[ongoing] + Workflow::States::WORKFLOW + %w[all]).freeze
        FILTERS = %i[workflow_class step error_class subject_type subject_id workflow_id created_from created_to
                     ended_from ended_to].freeze
        STEP_HISTORY = 100
        EVENT_HISTORY = 200
        # The pause events the timeline pairs, read on their own so a long
        # audit trail never drops a run's earliest pauses from the chart.
        PAUSE_HISTORY = 1_000
        SIGNAL_HISTORY = 100
        RERUN_LINKS = 10
        ATTENTION_GROUPS = 200

        # status – one of STATUSES ("ongoing": ready, performing, waiting or
        # paused). Filters (FILTERS) combine with AND. An unknown status or
        # filter raises ArgumentError (the controller answers 400).
        def workflows(status: "ongoing", page: 1, per_page: 25, **filters)
          check_workflow_query!(status, filters)
          return [] unless Workflow::Sweep.tables_ready?

          fetch_workflows(status, filters, page, per_page)
        end

        def workflows_count(status: "ongoing", **filters)
          check_workflow_query!(status, filters)
          return ListCounts::EMPTY_COUNT unless Workflow::Sweep.tables_ready?

          count_workflows(status, filters)
        end

        def workflow_detail(workflow_id)
          return nil unless Workflow::Sweep.tables_ready?

          record = WorkflowEntry.find_by(workflow_id: workflow_id)
          return nil unless record

          steps = WorkflowStep.where(workflow_id: workflow_id).order(id: :desc).limit(STEP_HISTORY).to_a
          progress = batch_progress([step_attribute(steps.first, :batch_id)].compact)
          workflow_row(record, steps.first, progress).merge(workflow_extras(record, steps))
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error fetching workflow #{workflow_id}: #{e.class}: #{e.message}" }
          nil
        end

        def ongoing_workflows_count = workflow_status_count(Workflow::States::ONGOING)

        def paused_workflows_count = workflow_status_count("paused")

        def workflow_attention_count
          return 0 unless Workflow::Sweep.tables_ready?

          WorkflowEntry.stuck_reason_counts(**attention_thresholds).values.sum
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error counting stuck workflows: #{e.class}: #{e.message}" }
          0
        end

        # The stuck runs grouped by (class, step, reason, error class), each
        # with the status its runs are in and the actions that status allows;
        # with +workflow_class+, that class's groups only (issue #549).
        def workflow_attention_groups(workflow_class: nil)
          return [] unless Workflow::Sweep.tables_ready?

          groups = WorkflowEntry.attention_groups(**attention_thresholds, limit: ATTENTION_GROUPS, workflow_class: workflow_class)
          groups.map do |group|
            status = WorkflowActionPlan.group_status(group[:reason])
            group.merge(status: status, available_actions: Workflow::States.available_actions(status))
          end
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error fetching stuck workflows: #{e.class}: #{e.message}" }
          []
        end

        private

        def check_workflow_query!(status, filters)
          raise ArgumentError, "unknown workflow status: #{status.inspect}" unless STATUSES.include?(status.to_s)

          unknown = filters.keys - FILTERS
          raise ArgumentError, "unknown workflow filter(s): #{unknown.join(", ")}" if unknown.any?
        end

        def fetch_workflows(status, filters, page, per_page)
          records = workflow_scope(status, filters).order(created_at: :desc, id: :desc)
                                                   .limit(per_page).offset((page - 1) * per_page).to_a
          latest = latest_steps(records.map(&:workflow_id))
          waiting = latest.values.select { |step| step.status == "waiting" }
          progress = batch_progress(waiting.filter_map { |step| step_attribute(step, :batch_id) })
          records.map { |record| workflow_row(record, latest[record.workflow_id], progress) }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error fetching workflows: #{e.class}: #{e.message}" }
          []
        end

        def count_workflows(status, filters)
          rows = workflow_scope(status, filters).limit(ListCounts::COUNT_CAP + 1).count
          ListCounts::Count.new(total: [rows, ListCounts::COUNT_CAP].min, capped: rows > ListCounts::COUNT_CAP)
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error counting workflows: #{e.class}: #{e.message}" }
          ListCounts::EMPTY_COUNT
        end

        def workflow_scope(status, filters)
          scope = WorkflowEntry.all
          status = status.to_s
          scope = scope.where(status: status == "ongoing" ? Workflow::States::ONGOING : status) unless status == "all"
          filters = filters.compact
          { workflow_class: :workflow_class, step: :current_step, error_class: :error_class, subject_type: :subject_type,
            subject_id: :subject_id, workflow_id: :workflow_id }.each do |filter, column|
            scope = scope.where(column => filters[filter]) if filters.key?(filter)
          end
          scope = scope.where(created_at: filters[:created_from]..) if filters[:created_from]
          scope = scope.where(created_at: ..filters[:created_to]) if filters[:created_to]
          ended_scope(scope, filters)
        end

        # When a finished or cancelled run ended (issue #549): what the
        # insights' "finished / cancelled in the window" counts link to. A run
        # ends in the UPDATE that sets its updated_at, so the updated_at bound
        # keeps every match and lets idx_pgbus_workflows_status serve it.
        def ended_scope(scope, filters)
          table = WorkflowEntry.arel_table
          ended_at = Arel::Nodes::NamedFunction.new("COALESCE", [table[:finished_at], table[:cancelled_at]])
          if filters[:ended_from]
            scope = scope.where(table[:updated_at].gteq(filters[:ended_from])).where(ended_at.gteq(filters[:ended_from]))
          end
          scope = scope.where(ended_at.lteq(filters[:ended_to])) if filters[:ended_to]
          scope
        end

        # { workflow_id => its newest step row }, one query for the page.
        def latest_steps(workflow_ids)
          return {} if workflow_ids.empty?

          WorkflowStep.where(workflow_id: workflow_ids).select("DISTINCT ON (workflow_id) #{WorkflowStep.table_name}.*")
                      .order(Arel.sql("workflow_id, id DESC")).index_by(&:workflow_id)
        end

        # { batch_id => { done:, total: } } for waiting fan-out steps, one query.
        def batch_progress(batch_ids)
          return {} if batch_ids.empty?

          BatchEntry.where(batch_id: batch_ids).to_a.to_h do |batch|
            [batch.batch_id, { done: batch.completed_jobs.to_i + failure_job_count(batch), total: batch.total_jobs.to_i }]
          end
        end

        def workflow_row(record, step, progress)
          waiting = step&.status == "waiting"
          { workflow_id: record.workflow_id, workflow_class: record.workflow_class, subject_type: record.subject_type,
            subject_id: record.subject_id, status: record.status, current_step: record.current_step,
            current_attempt: step&.attempt, step_status: step&.status,
            waiting_for: (step_attribute(step, :wait_for) if waiting),
            waiting_since: (step_attribute(step, :waiting_since) if waiting),
            timeout_at: (step_attribute(step, :timeout_at) if waiting),
            batch_id: step_attribute(step, :batch_id),
            batch_progress: (progress[step_attribute(step, :batch_id)] if waiting),
            scheduled_at: (step.scheduled_at if step&.status == "scheduled"),
            error_class: record.error_class, error_message: record.error_message,
            paused_at: record.paused_at, finished_at: record.finished_at, cancelled_at: record.cancelled_at,
            rerun_of_workflow_id: record.rerun_of_workflow_id, created_at: record.created_at, updated_at: record.updated_at }
        end

        # The waiting-step columns exist only once pgbus:add_workflow_signals ran.
        def step_attribute(step, name)
          step.public_send(name) if step&.has_attribute?(name)
        end

        def workflow_extras(record, steps)
          step_hashes = steps.map { |step| workflow_step_hash(step) }
          events = WorkflowEvent.where(workflow_id: record.workflow_id).order(id: :desc).limit(EVENT_HISTORY).map do |event|
            { id: event.id, event: event.event, actor: event.actor, reason: event.reason, step_name: event.step_name,
              attempt: event.attempt, details: event.details, created_at: event.created_at }
          end
          klass = record.workflow_klass
          resume_action, resume_step = WorkflowActionPlan.resume_plan(record, steps.first, klass)
          now = Time.current
          { arguments: WorkflowPayload.display(record.arguments),
            steps: step_hashes.map { |step| step.merge(durations: WorkflowTimeline.durations(step, now: now)) },
            signals: workflow_signals(record.workflow_id), events: events,
            available_actions: Workflow::States.available_actions(record.status),
            definition_outdated: record.definition_outdated?, class_loaded: !klass.nil?,
            step_names: klass ? klass.workflow_steps.names : [], resume_action: resume_action, resume_step: resume_step,
            signal_names: WorkflowActionPlan.signal_names(steps.first, klass), reruns: workflow_reruns(record.workflow_id),
            timeline: WorkflowTimeline.chart(step_hashes, pause_events(record.workflow_id), status: record.status, now: now) }
        end

        def workflow_step_hash(step)
          { id: step.id, step_name: step.step_name, attempt: step.attempt, status: step.status, outcome: step.outcome,
            created_at: step.created_at, scheduled_at: step.scheduled_at, enqueued_at: step.enqueued_at,
            started_at: step.started_at, finished_at: step.finished_at, job_id: step.job_id, msg_id: step.msg_id,
            worker: step.worker, app_revision: step.app_revision, error_class: step.error_class,
            error_message: step.error_message, error_backtrace: step.error_backtrace, error_causes: step.error_causes || [],
            output: step.output && WorkflowPayload.display(step.output),
            waiting_since: step_attribute(step, :waiting_since), wait_for: step_attribute(step, :wait_for),
            timeout_at: step_attribute(step, :timeout_at), batch_id: step_attribute(step, :batch_id),
            signal_id: step_attribute(step, :signal_id) }
        end

        def pause_events(workflow_id)
          WorkflowEvent.where(workflow_id: workflow_id, event: WorkflowTimeline::PAUSE_EVENTS).order(:id).limit(PAUSE_HISTORY)
                       .map { |event| { event: event.event, created_at: event.created_at } }
        end

        def workflow_signals(workflow_id)
          return [] unless Workflow::Sweep.waiting_ready?

          WorkflowSignal.where(workflow_id: workflow_id).order(id: :desc).limit(SIGNAL_HISTORY).map do |signal|
            { id: signal.id, name: signal.name, status: signal.status, idempotency_key: signal.idempotency_key,
              payload: WorkflowPayload.display(signal.read_attribute(:payload)), created_at: signal.created_at,
              claimed_at: signal.claimed_at, consumed_at: signal.consumed_at }
          end
        end

        def workflow_reruns(workflow_id)
          WorkflowEntry.where(rerun_of_workflow_id: workflow_id).order(created_at: :desc).limit(RERUN_LINKS)
                       .pluck(:workflow_id, :status, :created_at).map do |id, status, created_at|
            { workflow_id: id, status: status, created_at: created_at }
          end
        end

        def workflow_status_count(status)
          return 0 unless Workflow::Sweep.tables_ready?

          WorkflowEntry.where(status: status).count
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error counting workflows: #{e.class}: #{e.message}" }
          0
        end

        def attention_thresholds(config: Pgbus.configuration)
          waiting = Workflow::Sweep.waiting_ready? ? config.workflow_waiting_visibility_threshold : nil
          { threshold: config.workflow_stall_threshold, waiting_threshold: waiting }
        end
      end
    end
  end
end
