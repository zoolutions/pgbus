# frozen_string_literal: true

require "json"

module Pgbus
  # One workflow run (pgbus_workflows). Pgbus::Workflow is the DSL and the
  # handle; this is its row. Every status change goes through
  # Pgbus::Workflow::Runner, as a guarded UPDATE under the row lock.
  class WorkflowEntry < BusRecord
    self.table_name = "pgbus_workflows"

    scope :ongoing, -> { where(status: Workflow::States::ONGOING) }
    scope :terminal, -> { where(status: Workflow::States::TERMINAL) }
    scope :stale, ->(before:) { terminal.where(updated_at: ...before) }

    # The run's row, locked FOR UPDATE for the rest of the transaction.
    def self.lock_for(workflow_id)
      lock.find_by(workflow_id: workflow_id)
    end

    # Single-winner status change: 1 when this call moved the row, 0 when
    # its status was not in +from+.
    def self.transition!(workflow_id, from:, to:, **attrs) # rubocop:disable Naming/MethodParameterName
      where(workflow_id: workflow_id, status: from).update_all(attrs.merge(status: to, updated_at: Time.current))
    end

    # Inserts a run. With an idempotency_key a conflict on
    # (workflow_class, idempotency_key) inserts nothing and returns false; a
    # conflict on the one-ongoing-per-subject index raises
    # ActiveRecord::RecordNotUnique. Raw SQL rather than
    # insert_all(unique_by:), which resolves the index through the schema
    # cache (issue #401).
    def self.insert_run!(attrs) # rubocop:disable Naming/PredicateMethod
      columns = attrs.keys
      values = attrs.values.map { |value| value.is_a?(Hash) ? JSON.generate(value) : value }
      placeholders = (1..columns.size).map { |i| "$#{i}" }.join(", ")
      conflict = (" ON CONFLICT (workflow_class, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING" if attrs[:idempotency_key])
      result = connection.exec_query(
        "INSERT INTO #{table_name} (#{columns.join(", ")}) VALUES (#{placeholders})#{conflict} RETURNING id",
        "WorkflowEntry Insert", values
      )
      result.rows.any?
    end

    # { [workflow_class, status] => count } over every run.
    def self.status_counts
      group(:workflow_class, :status).count
    end

    # Why ongoing runs need a person, as { [workflow_class, reason] => count }
    # (Workflow::States::STUCK_REASONS). One query: each paused or ready run
    # (and, with +waiting_threshold+, each waiting run) with its latest step row.
    #   stalled       paused on a step that failed with StepStalled
    #   paused_error  paused on a step that failed with any other error
    #   paused_manual paused otherwise (the step called pause!, or an outside pause!)
    #   overdue       ready, its scheduled step due more than +threshold+ ago
    #   waiting_overdue waiting longer than +waiting_threshold+ on a step
    #                 without a wait_timeout: (only with +waiting_threshold+,
    #                 which needs the waiting-step columns)
    def self.stuck_reason_counts(threshold:, now: Time.current, waiting_threshold: nil)
      waiting = !waiting_threshold.nil?
      sql = <<~SQL
        SELECT stuck.workflow_class, stuck.reason, COUNT(*) AS runs
        FROM (
          SELECT w.workflow_class,
                 CASE
                   WHEN w.status = 'paused' AND s.status = 'failed' AND s.error_class = 'Pgbus::Workflow::StepStalled' THEN 'stalled'
                   WHEN w.status = 'paused' AND s.status = 'failed' THEN 'paused_error'
                   WHEN w.status = 'paused' THEN 'paused_manual'
                   WHEN w.status = 'ready' AND s.status = 'scheduled' AND s.scheduled_at < $1 THEN 'overdue'
                   #{"WHEN w.status = 'waiting' AND s.status = 'waiting' AND s.timeout_at IS NULL AND s.waiting_since < $2 THEN 'waiting_overdue'" if waiting}
                 END AS reason
          FROM #{table_name} w
          LEFT JOIN LATERAL (
            SELECT status, error_class, scheduled_at#{", timeout_at, waiting_since" if waiting} FROM #{WorkflowStep.table_name}
            WHERE workflow_id = w.workflow_id ORDER BY id DESC LIMIT 1
          ) s ON TRUE
          WHERE w.status IN ('paused', 'ready'#{", 'waiting'" if waiting})
        ) stuck
        WHERE stuck.reason IS NOT NULL
        GROUP BY stuck.workflow_class, stuck.reason
      SQL
      binds = waiting ? [now - threshold, now - waiting_threshold] : [now - threshold]
      connection.exec_query(sql, "WorkflowEntry Stuck", binds).rows.to_h do |klass, reason, count|
        [[klass, reason], count.to_i]
      end
    end

    # The Pgbus::Workflow subclass that started this run, or nil when it no
    # longer exists.
    def workflow_klass
      klass = workflow_class.safe_constantize
      klass if klass.is_a?(Class) && klass < Workflow
    end

    # True when the run started on a different step list than its class
    # declares now (a step added, removed or reordered since).
    def definition_outdated?
      klass = workflow_klass
      klass ? klass.definition_digest != definition_digest : false
    end
  end
end
