# frozen_string_literal: true

module Pgbus
  class Workflow
    # The waiting state wait_for: steps and fan_out steps share (issue #541),
    # every move under the run's row lock:
    #
    #   park!    in_progress → waiting   the gate found no signal, or the
    #                                    fan_out batch opened (run: park)
    #   wake!    waiting → scheduled     a signal it matches arrived, or the
    #                                    wait timed out with on_timeout: :run
    #                                    (run: wake); the model callback sends
    #                                    the StepJob once the write commits
    #   resolve! waiting → (an outcome)  the batch finished, the wait timed
    #                                    out, or an operator skipped it: the
    #                                    outcome is applied by StepOutcome as
    #                                    for any finished attempt (run: resolve,
    #                                    then advance, finish, step_pause, ...)
    #
    # A run an outside pause! reached first stays paused (hold): the step row
    # moves, nothing runs, and resume! picks it up.
    module Waiting
      module_function

      # False when the attempt had already lost its in_progress row (stalled
      # or cancelled while it ran).
      # A fan_out's +output+ (the body's output(...)) waits on the row.
      def park!(record, step, now:, batch_id: nil, wait_for: nil, timeout_at: nil, output: nil) # rubocop:disable Naming/PredicateMethod
        attrs = { waiting_since: now, stall_at: nil, batch_id: batch_id, wait_for: wait_for, timeout_at: timeout_at }
        attrs[:output] = output if output
        return false if WorkflowStep.park!(step.id, **attrs).zero?

        step.assign_attributes(status: "waiting", **attrs)
        Runner.transition!(record, paused?(record) ? :hold : :park)
        Runner.event!(record, "step_waiting", actor: Runner::SYSTEM_ACTOR, step: step,
                                              details: { wait_for: wait_for, batch_id: batch_id,
                                                         timeout_at: timeout_at&.utc&.iso8601(3) }.compact)
        true
      end

      # A paused run's step is written without the model callback, so no
      # StepJob goes out that its claim would only drop; resume! sends it.
      def wake!(record, step, now:, **attrs)
        if paused?(record)
          step.update_columns(status: "scheduled", scheduled_at: now, **attrs)
          Runner.transition!(record, :hold)
        else
          step.update!(status: "scheduled", scheduled_at: now, **attrs)
          Runner.transition!(record, :wake)
        end
      end

      # Returns false when the row was no longer waiting.
      def resolve!(record, step, outcome, notes:, error: nil)
        return false if WorkflowStep.unpark!(step.id).zero?

        step.assign_attributes(status: "in_progress")
        Runner.transition!(record, :resolve) unless paused?(record)
        output = step.respond_to?(:output) ? step.output : nil
        StepOutcome.apply(record, step, outcome, notes: notes, error: error, output: output)
      end

      # start! (and rerun!) of a class with waiting steps needs their schema.
      def check_schema!(klass)
        definitions = klass.workflow_steps.definitions
        fan_out = definitions.any? { |definition| definition.kind == "fan_out" }
        return unless fan_out || definitions.any?(&:waits_for_signal?)

        unless Sweep.waiting_ready?
          raise WaitingStepsUnmigrated, "#{klass.name} has waiting steps (fan_out or wait_for:): run " \
                                        "`rails generate pgbus:add_workflow_signals` and migrate first"
        end
        return unless fan_out && !Batch.callback_jobs_migrated?

        raise BatchCallbacksUnmigrated, "#{klass.name} has a fan_out step, whose batch resumes the run through a " \
                                        "callback job: run `rails generate pgbus:add_batch_callback_jobs` and migrate first"
      end

      def paused?(record) = record.status == "paused"
    end
  end
end
