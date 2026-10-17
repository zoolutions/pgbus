# frozen_string_literal: true

module Pgbus
  class Workflow
    # Applies how a step attempt ended to its row and to the run, under the
    # run's row lock: the StepJob after a body, a guard (cancel_if, skip_if,
    # a removed or unknown step), crash detection and the sweep.
    #
    # The step row is written first, guarded on status = 'in_progress'. A
    # row that is no longer in_progress was declared stalled (or the run was
    # cancelled) while this attempt was still running: the outcome and its
    # output are discarded and only a late_completion_ignored event is
    # recorded. Otherwise the run moves by the States table: advance (the
    # next step is scheduled), finish, reattempt, step_pause, step_cancel, or
    # hold when an outside pause! got there first.
    module StepOutcome
      STEP_STATUS = { complete: "completed", finish: "completed", pause: "completed",
                      skip: "skipped", cancel: "cancelled", reattempt: "failed" }.freeze
      STEP_OUTCOME = { complete: "completed", finish: "finished_early", pause: "paused",
                       skip: "skipped", cancel: "cancelled", reattempt: "reattempted" }.freeze

      module_function

      # Returns false when the attempt had lost its in_progress guard.
      def apply(record, step, outcome, notes:, error: nil, output: nil, report: true, duration: nil, klass: record.workflow_klass) # rubocop:disable Naming/PredicateMethod
        now = Time.current
        status = error ? "failed" : STEP_STATUS.fetch(outcome.action)
        column = STEP_OUTCOME.fetch(outcome.action)
        captured = error && ErrorCapture.capture(error)
        attrs = { status: status, outcome: column, finished_at: now, output: output, **captured.to_h }
        if WorkflowStep.finish!(step.id, **attrs).zero?
          ignore_late_completion(record, step, status, column, error)
          notes.report(error, error_context(record, step, "late_completion_ignored")) if error && report
          return false
        end

        step_events(record, step, outcome, column, error)
        move_run(record, step, outcome, notes: notes, captured: captured, klass: klass, now: now)
        notes.instrument("pgbus.workflow.step", step_payload(record, step, column, status, error, duration))
        notes.report(error, error_context(record, step, column)) if error && report
        true
      end

      # A step whose worker died (detection: "redelivery") or that outlived
      # its stall_at (detection: "sweep"): config.workflow_stall_action
      # applies, with Pgbus::Workflow::StepStalled recorded on the row.
      def stall!(record, step, detection:, notes:, config: Pgbus.configuration)
        why = if detection == "redelivery"
                "its message came back while it was in_progress: the worker #{step.worker} died"
              else
                "it was still in_progress at its stall_at #{step.stall_at&.utc&.iso8601} (worker #{step.worker})"
              end
        error = StepStalled.new("#{step.step_name} attempt #{step.attempt} stalled: #{why}")
        Runner.event!(record, "stall_detected", actor: Runner::SYSTEM_ACTOR, step: step,
                                                details: { detection: detection, worker: step.worker,
                                                           stall_at: step.stall_at&.utc&.iso8601(3) }.compact)
        notes.instrument("pgbus.workflow.stall_detected",
                         Runner.payload_for(record, step_name: step.step_name, attempt: step.attempt, detection: detection))
        apply(record, step, stall_outcome(config.workflow_stall_action), notes: notes, error: error)
      end

      def stall_outcome(action)
        case action
        when :reattempt then FlowControl::Outcome.reattempt
        when :cancel then FlowControl::Outcome.cancel
        else FlowControl::Outcome.pause
        end
      end

      def move_run(record, step, outcome, notes:, captured:, klass:, now:)
        paused = record.status == "paused"
        case outcome.action
        when :complete, :skip
          following = klass&.workflow_steps&.next_after(step.step_name)
          following ? advance(record, following, paused: paused, now: now) : finish(record, notes)
        when :finish then finish(record, notes)
        when :reattempt
          Runner.schedule!(record, step.step_name, at: now + (outcome.wait || 0), actor: Runner::SYSTEM_ACTOR)
          Runner.transition!(record, paused ? :hold : :reattempt, current_step: step.step_name)
        when :pause then pause(record, step, notes, captured: captured, paused: paused, now: now)
        when :cancel then cancel(record, step, notes, now: now)
        end
      end

      def advance(record, following, paused:, now:)
        Runner.schedule!(record, following.name, at: now + following.wait, actor: Runner::SYSTEM_ACTOR)
        Runner.transition!(record, paused ? :hold : :advance, current_step: following.name)
      end

      def finish(record, notes)
        Runner.transition!(record, :finish, finished_at: Time.current)
        Runner.event!(record, "finished", actor: Runner::SYSTEM_ACTOR)
        notes.instrument("pgbus.workflow_finished", Runner.payload_for(record))
      end

      # The run row keeps the error that paused it (cleared by resume!).
      def pause(record, step, notes, captured:, paused:, now:)
        run_error = captured ? captured.except(:error_causes) : {}
        return Runner.transition!(record, :hold, **run_error) if paused

        error_class = captured&.fetch(:error_class)
        Runner.transition!(record, :step_pause, paused_at: now, **run_error)
        Runner.event!(record, "paused", actor: Runner::SYSTEM_ACTOR, step: step, details: { error_class: error_class }.compact)
        notes.instrument("pgbus.workflow_paused", Runner.payload_for(record, step_name: step.step_name, error_class: error_class))
      end

      def cancel(record, step, notes, now:)
        Runner.transition!(record, :step_cancel, cancelled_at: now)
        WorkflowStep.cancel_open!(record.workflow_id, now: now)
        Runner.event!(record, "cancelled", actor: Runner::SYSTEM_ACTOR, step: step)
        notes.instrument("pgbus.workflow_cancelled", Runner.payload_for(record, step_name: step.step_name))
      end

      def step_events(record, step, outcome, column, error)
        actor = Runner::SYSTEM_ACTOR
        if error
          Runner.event!(record, "step_failed", actor: actor, step: step,
                                               details: { error_class: error.class.name, error_message: error.message.to_s })
        end
        case outcome.action
        when :complete, :finish, :pause
          Runner.event!(record, "step_completed", actor: actor, step: step, details: { outcome: column }) unless error
        when :skip then Runner.event!(record, "step_skipped", actor: actor, step: step)
        when :reattempt then Runner.event!(record, "step_reattempted", actor: actor, step: step, details: { wait: outcome.wait }.compact)
        end
      end

      def ignore_late_completion(record, step, status, column, error)
        details = { status: status, outcome: column, error_class: error&.class&.name }.compact
        Runner.event!(record, "late_completion_ignored", actor: Runner::SYSTEM_ACTOR, step: step, details: details)
        Pgbus.logger.warn do
          "[Pgbus] Workflow #{record.workflow_class} #{record.workflow_id}: step #{step.step_name} attempt #{step.attempt} " \
            "finished after it stopped being in_progress (stalled or cancelled); its outcome (#{column}) and output were discarded"
        end
      end

      def step_payload(record, step, column, status, error, duration)
        pickup = step.started_at && step.scheduled_at ? [step.started_at - step.scheduled_at, 0].max.to_f : nil
        Runner.payload_for(record, step_name: step.step_name, attempt: step.attempt, outcome: column, status: status,
                                   error_class: error&.class&.name, duration: duration, pickup_latency: pickup)
      end

      def error_context(record, step, column)
        { workflow_class: record.workflow_class, workflow_id: record.workflow_id, step_name: step.step_name,
          attempt: step.attempt, outcome: column }
      end
    end
  end
end
