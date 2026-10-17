# frozen_string_literal: true

module Pgbus
  # The audit trail (pgbus_workflow_events): one row per transition and per
  # operator action, with who did it (actor) and why (reason). Append-only:
  # written by Pgbus::Workflow::Runner inside the transaction of the
  # transition it records, read-only once inserted, removed with its run.
  #
  # Actors: "app" (an outside verb called without actor:), "system" (the
  # StepJob, the sweep, crash detection), or what the caller passes
  # ("dashboard:<name>", "mcp", "alice", ...).
  class WorkflowEvent < BusRecord
    self.table_name = "pgbus_workflow_events"

    REASON_LIMIT = 1_000
    DETAIL_LIMIT = 1_000

    EVENTS = %w[
      started step_scheduled step_started step_completed step_failed step_skipped step_reattempted
      paused resumed cancelled finished run_now rescheduled rerun stall_detected late_completion_ignored
      signal_received signal_claimed step_waiting wait_timed_out fan_out_opened fan_out_finished
    ].freeze

    def self.record!(workflow_id:, event:, actor:, reason: nil, step: nil, details: {})
      create!(
        workflow_id: workflow_id, event: event.to_s, actor: actor.to_s,
        reason: reason && Workflow::ErrorCapture.truncate(reason.to_s, REASON_LIMIT),
        step_id: step&.id, step_name: step&.step_name, attempt: step&.attempt,
        details: cap(details), created_at: Time.current
      )
    end

    # String values in details are capped, so one large error message cannot
    # bloat the trail.
    def self.cap(details)
      details.to_h do |key, value|
        [key.to_s, value.is_a?(String) ? Workflow::ErrorCapture.truncate(value, DETAIL_LIMIT) : value]
      end
    end

    def readonly? = persisted? || super
  end
end
