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
      ACTIONS = %i[complete reattempt pause cancel skip finish fan_out].freeze

      # What a step attempt ended with. +wait+ is the reattempt delay in
      # seconds (nil = now); +actor+ and +reason+ are what a step's own
      # pause!/cancel! (or an operator's skip!) passed, for the audit trail;
      # +details+ carries a fan_out's batch_id and size.
      Outcome = Data.define(:action, :wait, :actor, :reason, :details) do
        def initialize(action:, wait: nil, actor: nil, reason: nil, details: nil) = super

        def self.complete = new(action: :complete, wait: nil)
        def self.pause(actor: nil, reason: nil) = new(action: :pause, actor: actor, reason: reason)
        def self.cancel(actor: nil, reason: nil) = new(action: :cancel, actor: actor, reason: reason)
        def self.skip(actor: nil, reason: nil) = new(action: :skip, actor: actor, reason: reason)
        def self.finish = new(action: :finish, wait: nil)
        def self.reattempt(wait: nil) = new(action: :reattempt, wait: StepDefinition.seconds(wait, "wait", positive: false))
        # A fan_out body returned and its batch is open: the step parks on it.
        def self.fan_out(batch_id:, size:) = new(action: :fan_out, details: { batch_id: batch_id, size: size }.freeze)

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
        return throw_outcome(Outcome.pause(actor: actor, reason: reason)) if in_step?

        apply_outside_verb(:pause, actor: actor, reason: reason)
      end

      # Inside a step: end it here and cancel the workflow.
      # Outside: cancel the run from any ongoing state.
      def cancel!(actor: nil, reason: nil)
        return throw_outcome(Outcome.cancel(actor: actor, reason: reason)) if in_step?

        apply_outside_verb(:cancel, actor: actor, reason: reason)
      end

      # Ends this attempt as failed and schedules the next one, +wait+
      # seconds from now. Step bodies only.
      def reattempt!(wait: nil)
        throw_outcome(Outcome.reattempt(wait: wait), verb: :reattempt!)
      end

      # Inside a step: end it here, skipped; the workflow moves on.
      # Outside: skip the step a waiting run waits on (a signal it holds is
      # consumed) and schedule the next one.
      def skip!(actor: nil, reason: nil)
        return throw_outcome(Outcome.skip) if in_step?

        apply_outside_verb(:skip, actor: actor, reason: reason)
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
        raise NotInStep, "flow-control verbs end a step from its body or an on_exception block, not from cancel_if or skip_if" if @guarding

        throw THROW_TAG, outcome
      end
    end
  end
end
