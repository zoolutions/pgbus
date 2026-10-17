# frozen_string_literal: true

module Pgbus
  class Workflow
    # The outside verbs, each called by Runner.apply with the run's row
    # locked and the transition already checked against the States table.
    module OutsideVerbs
      module_function

      def pause(record, notes, actor:, reason:)
        from = record.status
        Runner.transition!(record, :pause, paused_at: Time.current)
        Runner.event!(record, "paused", actor: actor, reason: reason, details: { from: from })
        notes.instrument("pgbus.workflow_paused", Runner.payload_for(record, step_name: record.current_step, error_class: nil))
      end

      def cancel(record, notes, actor:, reason:)
        now = Time.current
        from = record.status
        Runner.transition!(record, :cancel, cancelled_at: now)
        WorkflowStep.cancel_open!(record.workflow_id, now: now)
        Runner.event!(record, "cancelled", actor: actor, reason: reason, details: { from: from })
        notes.instrument("pgbus.workflow_cancelled", Runner.payload_for(record, step_name: record.current_step))
      end

      # What runs next depends on the row the run paused on: a failed one
      # (an exception or a stall) runs again as a new attempt; a completed or
      # skipped one (the step called pause!) is followed by the next step, or
      # finishes the run when it was the last; a scheduled one (an outside
      # pause! before it ran) is sent again; one still running goes on.
      def resume(record, notes, actor:, reason:)
        latest = WorkflowStep.latest_for(record.workflow_id)
        resumed = { paused_at: nil, **ErrorCapture.cleared }
        case latest&.status
        when "in_progress"
          Runner.transition!(record, :unpause, **resumed)
          resumed_event(record, actor, reason, latest)
        when "scheduled", "waiting"
          Runner.transition!(record, :resume, **resumed)
          resumed_event(record, actor, reason, latest)
          latest.update!(enqueued_at: Time.current) if latest.status == "scheduled"
        when "failed"
          Runner.transition!(record, :resume, **resumed)
          resumed_event(record, actor, reason, latest)
          Runner.schedule!(record, latest.step_name, at: Time.current, actor: actor)
        else
          resume_after(record, notes, latest, resumed, actor: actor, reason: reason)
        end
      end

      def resume_at(record, _notes, actor:, reason:, step_name:)
        definition = definition!(record, step_name)
        now = Time.current
        WorkflowStep.cancel_open!(record.workflow_id, now: now)
        Runner.transition!(record, :resume_at, current_step: definition.name, paused_at: nil, **ErrorCapture.cleared)
        Runner.event!(record, "resumed", actor: actor, reason: reason, details: { resume_at: definition.name })
        Runner.schedule!(record, definition.name, at: now, actor: actor)
      end

      def run_now(record, _notes, actor:, reason:)
        reschedule(record, nil, actor: actor, reason: reason, at: Time.current, event: "run_now")
      end

      def reschedule(record, _notes, actor:, reason:, at:, event: "rescheduled") # rubocop:disable Naming/MethodParameterName
        time = at.respond_to?(:to_time) && !at.is_a?(String) ? at.to_time : nil
        raise ArgumentError, "reschedule!(at:) takes a Time, got #{at.inspect}" unless time

        step = WorkflowStep.where(workflow_id: record.workflow_id, status: "scheduled").order(id: :desc).first
        raise InvalidTransition, "workflow #{record.workflow_id} has no scheduled step to #{event.tr("_", " ")}" unless step

        verb = event == "run_now" ? :run_now : :reschedule
        from = step.scheduled_at
        Runner.transition!(record, verb)
        step.update!(scheduled_at: time, enqueued_at: Time.current)
        Runner.event!(record, event, actor: actor, reason: reason, step: step,
                                     details: { from: from.utc.iso8601(3), to: step.scheduled_at.utc.iso8601(3) })
      end

      def resume_after(record, notes, latest, resumed, actor:, reason:)
        klass = record.workflow_klass || raise(StepNotDefined, "#{record.workflow_class} is not a Pgbus::Workflow class any more")
        steps = klass.workflow_steps
        if latest && steps.entries.none? { |entry| entry.name == latest.step_name }
          raise StepNotDefined, "#{klass.name} has no step #{latest.step_name} (declare `removed_step :#{latest.step_name}`)"
        end

        following = latest ? steps.next_after(latest.step_name) : steps.first
        if following.nil?
          Runner.transition!(record, :finish, finished_at: Time.current, **resumed)
          resumed_event(record, actor, reason, latest)
          Runner.event!(record, "finished", actor: actor)
          notes.instrument("pgbus.workflow_finished", Runner.payload_for(record))
          return
        end

        Runner.transition!(record, :resume, current_step: following.name, **resumed)
        resumed_event(record, actor, reason, latest)
        Runner.schedule!(record, following.name, at: Time.current + following.wait, actor: actor)
      end

      def resumed_event(record, actor, reason, step)
        Runner.event!(record, "resumed", actor: actor, reason: reason, step: step)
      end

      def definition!(record, step_name)
        klass = record.workflow_klass || raise(StepNotDefined, "#{record.workflow_class} is not a Pgbus::Workflow class any more")
        klass.workflow_steps.fetch(step_name) || raise(StepNotDefined, "#{klass.name} has no step #{step_name}")
      end
    end
  end
end
