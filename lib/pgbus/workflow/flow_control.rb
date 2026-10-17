# frozen_string_literal: true

module Pgbus
  class Workflow
    # The flow-control verbs. Inside a step body each one ends the body at
    # once (`throw :pgbus_workflow_flow, Outcome`) and the StepJob applies the
    # outcome under the workflow lock. Outside a step, `pause!` and `cancel!`
    # are operator verbs (they change the run through the Runner, with
    # actor:/reason: on the audit trail); the step-only verbs raise NotInStep.
    module FlowControl
      THROW_TAG = :pgbus_workflow_flow
      ACTIONS = %i[complete reattempt pause cancel skip finish].freeze

      # What a step attempt ended with. +wait+ is the reattempt delay in
      # seconds (nil = now).
      Outcome = Data.define(:action, :wait) do
        def self.complete = new(action: :complete, wait: nil)
        def self.pause = new(action: :pause, wait: nil)
        def self.cancel = new(action: :cancel, wait: nil)
        def self.skip = new(action: :skip, wait: nil)
        def self.finish = new(action: :finish, wait: nil)
        def self.reattempt(wait: nil) = new(action: :reattempt, wait: StepDefinition.seconds(wait, "wait", positive: false))

        # An ExceptionPolicy action (:pause!, :reattempt!, :cancel!, :skip!).
        def self.for(verb, wait: nil)
          case verb.to_s.delete_suffix("!").to_sym
          when :pause then pause
          when :cancel then cancel
          when :skip then skip
          when :reattempt then reattempt(wait: wait)
          when :finished, :finish then finish
          else raise ArgumentError, "unknown flow-control verb #{verb.inspect}"
          end
        end

        def reattempt? = action == :reattempt
      end

      # Inside a step: end it here, completed, with the workflow paused.
      # Outside: pause the run (ready, performing or waiting → paused).
      def pause!(actor: nil, reason: nil)
        return throw_outcome(Outcome.pause) if in_step?

        apply_outside_verb(:pause, actor: actor, reason: reason)
      end

      # Inside a step: end it here and cancel the workflow.
      # Outside: cancel the run from any ongoing state.
      def cancel!(actor: nil, reason: nil)
        return throw_outcome(Outcome.cancel) if in_step?

        apply_outside_verb(:cancel, actor: actor, reason: reason)
      end

      # Ends this attempt as failed and schedules the next one, +wait+
      # seconds from now. Step bodies only.
      def reattempt!(wait: nil)
        throw_outcome(Outcome.reattempt(wait: wait), verb: :reattempt!)
      end

      # Ends the step as skipped; the workflow moves on. Step bodies only.
      def skip!
        throw_outcome(Outcome.skip, verb: :skip!)
      end

      # Ends the step as completed and finishes the workflow, skipping the
      # steps after it. Step bodies only.
      def finished!
        throw_outcome(Outcome.finish, verb: :finished!)
      end

      private

      def in_step? = !step_context.nil?

      def throw_outcome(outcome, verb: nil)
        raise NotInStep, "#{verb} can only be called inside a step body" if verb && !in_step?

        throw THROW_TAG, outcome
      end
    end
  end
end
