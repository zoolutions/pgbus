# frozen_string_literal: true

module Pgbus
  class Workflow
    # The instance side of a workflow: the run's readers, its outputs and
    # idempotency keys, and the outside verbs. An instance wraps one
    # Pgbus::WorkflowEntry row; inside a step it also knows which step
    # attempt it is running (the step context).
    module Instance
      # Which attempt a step body is running. Set by the StepJob on the fresh
      # instance it runs the body on, never by app code.
      StepContext = Data.define(:step_id, :step_name, :attempt)

      def workflow_id = record.workflow_id

      def workflow_class = record.workflow_class

      def status = record.status

      def current_step = record.current_step

      # True when start!(…, idempotency_key:) found an existing run with that
      # key and returned it instead of starting another.
      def duplicate_start? = @duplicate_start

      # The record the run was started for, loaded through its GlobalID;
      # nil for a subject-less run or when the record is gone.
      def subject
        return @subject if defined?(@subject)

        @subject = Payload.locate_subject(record.subject_gid)
      end

      # start!'s keyword arguments, with indifferent access.
      def arguments
        return @arguments if defined?(@arguments)

        @arguments = Payload.load(record.arguments) || {}.with_indifferent_access
      end

      def reload
        @record = WorkflowEntry.find_by!(workflow_id: workflow_id)
        remove_instance_variable(:@subject) if defined?(@subject)
        remove_instance_variable(:@arguments) if defined?(@arguments)
        self
      end

      # The audit trail, oldest first.
      def events = WorkflowEvent.where(workflow_id: workflow_id).order(:id).to_a

      # The outside verbs legal in the current status (States).
      def available_actions = States.available_actions(status)

      # Inside a step: record durable output values. They are written in the
      # same UPDATE that completes the step, so they exist exactly when the
      # step does; later steps read them with output_of. Merges across calls.
      def output(**values)
        context = step_context!(:output)
        merged = (@output_values || {}).merge(values)
        @pending_output = Payload.dump(merged, what: "output of step #{context.step_name}")
        @output_values = merged
        nil
      end

      # What this attempt's output(...) calls recorded, serialized (or nil).
      def pending_output = @pending_output

      # The output of +step_name+'s latest completed attempt, or nil. A
      # failed or reattempted attempt's output is never returned.
      def output_of(step_name)
        Payload.load(WorkflowStep.completed_output(workflow_id, step_name))
      end

      # { "step_name" => output } for every step with a completed attempt.
      def outputs
        WorkflowStep.completed_outputs(workflow_id).transform_values { |output| Payload.load(output) }
      end

      # The same on every attempt of this step (and on a resume_at! re-run):
      # hand it to an external API so a repeated side effect is deduplicated.
      def step_idempotency_key
        "pgbus-wf:#{workflow_id}:#{step_context!(:step_idempotency_key).step_name}"
      end

      # Differs per attempt, for the rare call that must not be deduplicated
      # across attempts.
      def attempt_idempotency_key
        "#{step_idempotency_key}:#{step_context!(:attempt_idempotency_key).attempt}"
      end

      # paused → ready. A failed step runs again as a new attempt, a
      # completed one is followed by the next step.
      def resume!(actor: nil, reason: nil) = apply_outside_verb(:resume, actor: actor, reason: reason)

      # paused → ready, running +step_name+ next.
      def resume_at!(step_name, actor: nil, reason: nil)
        apply_outside_verb(:resume_at, actor: actor, reason: reason, step_name: step_name)
      end

      # The scheduled step runs now instead of after its wait.
      def run_now!(actor: nil, reason: nil) = apply_outside_verb(:run_now, actor: actor, reason: reason)

      # The scheduled step runs at +at+ instead.
      def reschedule!(at:, actor: nil, reason: nil) # rubocop:disable Naming/MethodParameterName
        apply_outside_verb(:reschedule, actor: actor, reason: reason, at: at)
      end

      # A finished or cancelled run, started again as a new run with the same
      # subject and arguments. Returns the new run.
      def rerun!(actor: nil, reason: nil)
        Runner.rerun(self, actor: actor || Runner::DEFAULT_ACTOR, reason: reason)
      end

      def inspect
        "#<#{self.class.name} workflow_id=#{workflow_id.inspect} status=#{status.inspect} current_step=#{current_step.inspect}>"
      end

      private

      attr_reader :step_context

      def apply_outside_verb(verb, actor:, reason:, **)
        @record = Runner.apply(self, verb, actor: actor || Runner::DEFAULT_ACTOR, reason: reason, **)
        self
      end

      def step_context!(reader)
        step_context || raise(NotInStep, "#{reader} can only be called inside a step body")
      end

      # Used by the StepJob only: cancel_if and skip_if run inside this, where
      # the flow-control verbs raise NotInStep (only the body and an
      # on_exception block can end a step).
      def guarding
        @guarding = true
        yield
      ensure
        @guarding = false
      end

      # Used by the StepJob only.
      def enter_step(context)
        @step_context = context
        @output_values = nil
        @pending_output = nil
      end
    end
  end
end
