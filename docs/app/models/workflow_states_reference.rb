# frozen_string_literal: true

# The workflow transition table (Pgbus::Workflow::States, issue #540) for the
# Workflows page. The docs app loads only pgbus/version, so the rows live
# here; spec/workflow_states_reference_spec.rb compares them with the real
# Pgbus::Workflow::States::TRANSITIONS in both directions, so the page cannot
# drift from the gem.
module WorkflowStatesReference
  # verb => [from states, to state, what happens to the steps]
  OUTSIDE = {
    pause: [ %w[ready performing waiting], "paused",
             "a running step finishes and records its outcome; nothing new is claimed" ],
    resume: [ %w[paused], "ready",
              "a failed step runs again as a new attempt; a completed or skipped one is followed by the next step; a scheduled one is sent again" ],
    resume_at: [ %w[paused], "ready", "the named step runs as a new attempt" ],
    cancel: [ %w[ready performing waiting paused], "cancelled",
              "scheduled and in_progress steps are cancelled; a running step's outcome is ignored" ],
    run_now: [ %w[ready], "ready", "the scheduled step's wait is cut short and it is sent now" ],
    reschedule: [ %w[ready], "ready", "the scheduled step moves to the given time" ],
    rerun: [ %w[finished cancelled], "ready", "a new run starts with the same subject and arguments" ]
  }.freeze

  RUNNER = {
    claim: [ %w[ready], "performing", "the scheduled step moves to in_progress" ],
    advance: [ %w[performing], "ready", "the step is completed or skipped and the next step is scheduled" ],
    finish: [ %w[performing paused], "finished", "the last step (or one that called finished!) completed" ],
    reattempt: [ %w[performing], "ready", "the step failed and its next attempt is scheduled" ],
    step_pause: [ %w[performing], "paused", "the step completed after pause!, or failed (an exception or a stall)" ],
    step_cancel: [ %w[performing paused], "cancelled", "the step called cancel!, or cancel_if or a policy cancelled it" ],
    unpause: [ %w[paused], "performing",
               "resume! while the step that was running at the outside pause! is still running: the run goes on performing it" ],
    hold: [ %w[paused], "paused",
            "an outcome that arrived after an outside pause!: recorded on the step; a completed, skipped or reattempted step has its next row scheduled; the workflow stays paused" ]
  }.freeze

  def self.all = OUTSIDE.merge(RUNNER)

  def self.rows(table)
    table.map { |verb, (from, to, effect)| [ [ :code, verb.to_s ], from.join(", "), to, effect ] }
  end
end
