# frozen_string_literal: true

module Pgbus
  class Workflow
    # The one transition table (issue #540). Every verb, the workflow states
    # it is legal from, the state it moves the workflow to, and what it does
    # to the step rows. The runner's guarded UPDATEs take their WHERE status
    # IN (...) from here, available_actions (dashboard buttons, MCP tools)
    # reads it, and the docs page lists it. Nothing else decides whether a
    # transition is legal.
    #
    # Outside verbs are the ones a person or a program calls on a run
    # (console, dashboard, MCP); runner verbs are the StepJob's and the
    # sweep's own moves, never offered as actions.
    module States
      WORKFLOW = %w[ready performing waiting paused finished cancelled].freeze
      ONGOING = %w[ready performing waiting paused].freeze
      TERMINAL = %w[finished cancelled].freeze
      STEP = %w[scheduled waiting in_progress completed failed cancelled skipped].freeze
      OPEN_STEP = %w[scheduled waiting in_progress].freeze

      # Why an ongoing run needs a person (the sweep's gauge, and the
      # dashboard's needs-attention view later).
      STUCK_REASONS = %w[paused_error stalled paused_manual overdue].freeze

      Transition = Data.define(:verb, :from, :to, :outside, :error, :effect)

      TRANSITIONS = Ractor.make_shareable(
        [
          # Outside verbs
          Transition.new(verb: :pause, from: %w[ready performing waiting], to: "paused", outside: true,
                         error: :InvalidTransition,
                         effect: "a running step finishes and records its outcome; nothing new is claimed"),
          Transition.new(verb: :resume, from: %w[paused], to: "ready", outside: true, error: :NotPaused,
                         effect: "a failed step runs again as a new attempt; a completed or skipped one is " \
                                 "followed by the next step; a scheduled one is sent again"),
          Transition.new(verb: :resume_at, from: %w[paused], to: "ready", outside: true, error: :NotPaused,
                         effect: "the named step runs as a new attempt"),
          Transition.new(verb: :cancel, from: ONGOING, to: "cancelled", outside: true, error: :InvalidTransition,
                         effect: "scheduled and in_progress steps are cancelled; a running step's outcome is ignored"),
          Transition.new(verb: :run_now, from: %w[ready], to: "ready", outside: true, error: :InvalidTransition,
                         effect: "the scheduled step's wait is cut short and it is sent now"),
          Transition.new(verb: :reschedule, from: %w[ready], to: "ready", outside: true, error: :InvalidTransition,
                         effect: "the scheduled step moves to the given time"),
          Transition.new(verb: :rerun, from: TERMINAL, to: "ready", outside: true, error: :InvalidTransition,
                         effect: "a new run starts with the same subject and arguments"),
          # Runner verbs
          Transition.new(verb: :claim, from: %w[ready], to: "performing", outside: false, error: :InvalidTransition,
                         effect: "the scheduled step moves to in_progress"),
          Transition.new(verb: :advance, from: %w[performing], to: "ready", outside: false, error: :InvalidTransition,
                         effect: "the step is completed or skipped and the next step is scheduled"),
          Transition.new(verb: :finish, from: %w[performing paused], to: "finished", outside: false,
                         error: :InvalidTransition, effect: "the last step (or one that called finished!) completed"),
          Transition.new(verb: :reattempt, from: %w[performing], to: "ready", outside: false, error: :InvalidTransition,
                         effect: "the step failed and its next attempt is scheduled"),
          Transition.new(verb: :step_pause, from: %w[performing], to: "paused", outside: false,
                         error: :InvalidTransition,
                         effect: "the step completed after pause!, or failed (an exception or a stall)"),
          Transition.new(verb: :step_cancel, from: %w[performing paused], to: "cancelled", outside: false,
                         error: :InvalidTransition, effect: "the step called cancel!, or cancel_if or a policy cancelled it"),
          Transition.new(verb: :unpause, from: %w[paused], to: "performing", outside: false, error: :NotPaused,
                         effect: "resume! while the step that was running at the outside pause! is still running: " \
                                 "the run goes on performing it"),
          Transition.new(verb: :hold, from: %w[paused], to: "paused", outside: false, error: :InvalidTransition,
                         effect: "an outcome that arrived after an outside pause!: recorded, the next step is " \
                                 "scheduled, and the workflow stays paused")
        ].to_h { |transition| [transition.verb, transition] }
      )

      module_function

      def fetch(verb)
        TRANSITIONS.fetch(verb.to_sym) { raise ArgumentError, "unknown workflow verb #{verb.inspect}" }
      end

      def from(verb) = fetch(verb).from

      def to(verb) = fetch(verb).to

      def legal?(verb, status) = from(verb).include?(status.to_s)

      # Returns the target state, or raises the verb's documented error.
      def check!(verb, status)
        transition = fetch(verb)
        return transition.to if transition.from.include?(status.to_s)

        raise Workflow.const_get(transition.error),
              "cannot #{verb.to_s.tr("_", " ")} a #{status} workflow (allowed from: #{transition.from.join(", ")})"
      end

      # The outside verbs legal in +status+: the dashboard's buttons and the
      # MCP write tools.
      def available_actions(status)
        state = status.to_s
        TRANSITIONS.each_value.select { |transition| transition.outside && transition.from.include?(state) }.map(&:verb)
      end
    end
  end
end
