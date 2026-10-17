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
      sql = <<~SQL
        SELECT stuck.workflow_class, stuck.reason, COUNT(*) AS runs
        FROM (#{stuck_runs_sql(waiting: !waiting_threshold.nil?)}) stuck
        WHERE stuck.reason IS NOT NULL
        GROUP BY stuck.workflow_class, stuck.reason
      SQL
      connection.exec_query(sql, "WorkflowEntry Stuck", stuck_binds(threshold, now, waiting_threshold)).rows.to_h do |klass, reason, count|
        [[klass, reason], count.to_i]
      end
    end

    # The dashboard's needs-attention view (issue #542): the stuck runs of
    # stuck_reason_counts grouped by (class, step, reason, error class), the
    # biggest group first, at most +limit+ groups. first_seen_at/last_seen_at
    # bound when the group's runs got stuck (paused_at, the overdue step's
    # scheduled_at, or the waiting step's waiting_since); error_message is
    # the newest one's.
    def self.attention_groups(threshold:, now: Time.current, waiting_threshold: nil, limit: 200)
      sql = <<~SQL
        SELECT stuck.workflow_class, stuck.step_name, stuck.reason, stuck.error_class, COUNT(*) AS runs,
               MIN(stuck.since) AS first_seen_at, MAX(stuck.since) AS last_seen_at,
               (ARRAY_AGG(stuck.error_message ORDER BY stuck.since DESC NULLS LAST, stuck.id DESC))[1] AS error_message
        FROM (#{stuck_runs_sql(waiting: !waiting_threshold.nil?)}) stuck
        WHERE stuck.reason IS NOT NULL
        GROUP BY stuck.workflow_class, stuck.step_name, stuck.reason, stuck.error_class
        ORDER BY runs DESC, last_seen_at DESC NULLS LAST, stuck.workflow_class
        LIMIT #{Integer(limit)}
      SQL
      result = connection.exec_query(sql, "WorkflowEntry Attention", stuck_binds(threshold, now, waiting_threshold))
      result.cast_values.map do |klass, step_name, reason, error_class, count, first_seen, last_seen, message|
        { workflow_class: klass, step_name: step_name, reason: reason, error_class: error_class, count: count.to_i,
          first_seen_at: first_seen, last_seen_at: last_seen, error_message: message }
      end
    end

    # The workflow_ids of one attention group (+group+: workflow_class,
    # step_name, reason, error_class; a nil error_class matches runs without
    # one), oldest stuck first, at most +limit+: what a bulk action walks.
    def self.attention_ids(group, threshold:, now: Time.current, waiting_threshold: nil, limit: 500)
      binds = stuck_binds(threshold, now, waiting_threshold)
      first = binds.size + 1
      sql = <<~SQL
        SELECT stuck.workflow_id
        FROM (#{stuck_runs_sql(waiting: !waiting_threshold.nil?)}) stuck
        WHERE stuck.workflow_class = $#{first} AND stuck.step_name = $#{first + 1} AND stuck.reason = $#{first + 2}
          AND stuck.error_class IS NOT DISTINCT FROM $#{first + 3}
        ORDER BY stuck.since NULLS LAST, stuck.id
        LIMIT #{Integer(limit)}
      SQL
      values = binds + group.values_at(:workflow_class, :step_name, :reason, :error_class)
      connection.exec_query(sql, "WorkflowEntry Attention Ids", values).rows.map(&:first)
    end

    # Each paused or ready run (and, when +waiting+, each waiting run) with
    # its latest step row and why it is stuck (NULL when it is not). Binds
    # $1 (the overdue cutoff) and, when +waiting+, $2 (the waiting cutoff).
    def self.stuck_runs_sql(waiting:)
      <<~SQL
        SELECT w.id, w.workflow_id, w.workflow_class, COALESCE(s.step_name, w.current_step) AS step_name,
               s.error_class, s.error_message, stuck_reason.reason,
               CASE
                 WHEN stuck_reason.reason IN ('stalled', 'paused_error', 'paused_manual') THEN w.paused_at
                 WHEN stuck_reason.reason = 'overdue' THEN s.scheduled_at
                 #{"WHEN stuck_reason.reason = 'waiting_overdue' THEN s.waiting_since" if waiting}
               END AS since
        FROM #{table_name} w
        LEFT JOIN LATERAL (
          SELECT step_name, status, error_class, error_message, scheduled_at#{", timeout_at, waiting_since" if waiting}
          FROM #{WorkflowStep.table_name}
          WHERE workflow_id = w.workflow_id ORDER BY id DESC LIMIT 1
        ) s ON TRUE
        CROSS JOIN LATERAL (
          SELECT CASE
                   WHEN w.status = 'paused' AND s.status = 'failed' AND s.error_class = 'Pgbus::Workflow::StepStalled' THEN 'stalled'
                   WHEN w.status = 'paused' AND s.status = 'failed' THEN 'paused_error'
                   WHEN w.status = 'paused' THEN 'paused_manual'
                   WHEN w.status = 'ready' AND s.status = 'scheduled' AND s.scheduled_at < $1 THEN 'overdue'
                   #{"WHEN w.status = 'waiting' AND s.status = 'waiting' AND s.timeout_at IS NULL AND s.waiting_since < $2 THEN 'waiting_overdue'" if waiting}
                 END AS reason
        ) stuck_reason
        WHERE w.status IN ('paused', 'ready'#{", 'waiting'" if waiting})
      SQL
    end
    private_class_method :stuck_runs_sql

    def self.stuck_binds(threshold, now, waiting_threshold)
      waiting_threshold.nil? ? [now - threshold] : [now - threshold, now - waiting_threshold]
    end
    private_class_method :stuck_binds

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
