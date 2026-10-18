# frozen_string_literal: true

module Pgbus
  class Workflow
    # wait_timeout: on a wait_for: step (issue #541). The dispatcher's
    # workflow sweep finds waiting steps of waiting runs whose timeout_at
    # passed and applies their on_timeout under the run's row lock, so a
    # timeout fires within config.workflow_sweep_interval of its deadline:
    #
    #   :pause!  the step fails with WaitTimedOut and the run pauses
    #   :cancel! the run is cancelled
    #   :skip!   the step is skipped and the run moves on
    #   :run     the step is scheduled again; its body runs with
    #            received_signal nil and timed_out? true
    #   :notify  only the event and the instrumentation (issue #565): the
    #            same attempt keeps waiting and timeout_at moves to its next
    #            deadline, one wait_timeout on (see next_deadline); a per-run
    #            wait_timeout: is evaluated again for it (see notify_deadline)
    #
    # A signal that arrives after the timeout fired stays pending, like any
    # signal no step waits for, except under :notify, whose step still waits
    # for it. A paused run's wait does not time out until it is resumed.
    module WaitTimeout
      module_function

      # Drains every due wait, Sweep::BATCH_SIZE at a time (at most
      # +max_batches+ of them per sweep, so one sweep cannot run unbounded).
      def expire_due(now: Time.current, limit: Sweep::BATCH_SIZE, max_batches: 20)
        total = 0
        max_batches.times do
          batch = WorkflowStep.wait_expired(now: now).order(:id).limit(limit).pluck(:id, :workflow_id)
          total += expire_batch(batch, now: now)
          break if batch.size < limit
        end
        total
      end

      def expire_batch(batch, now:)
        batch.count do |id, workflow_id|
          Runner.locked(workflow_id) do |record, notes|
            step = record && WorkflowStep.find_by(id: id)
            next false unless record&.status == "waiting" && step&.status == "waiting" && step.timeout_at && step.timeout_at < now

            fire!(record, step, notes: notes, now: now)
            true
          end
        end
      end

      # Applies the step's on_timeout now (the sweep, and expire_wait in
      # TestHelpers). Returns the action applied.
      def fire!(record, step, notes:, now: Time.current)
        definition = record.workflow_klass&.workflow_steps&.fetch(step.step_name)
        action = definition&.on_timeout || :pause!
        Runner.event!(record, "wait_timed_out", actor: Runner::SYSTEM_ACTOR, step: step,
                                                details: { wait_for: step.wait_for, action: action.to_s,
                                                           timeout_at: step.timeout_at&.utc&.iso8601(3) }.compact)
        notes.instrument("pgbus.workflow.wait_timed_out",
                         Runner.payload_for(record, step_name: step.step_name, attempt: step.attempt, action: action.to_s))
        apply(record, step, action, notes: notes, now: now, definition: definition)
        action
      end

      def apply(record, step, action, notes:, now:, definition: nil)
        return notify!(record, step, definition, notes: notes, now: now) if action == :notify
        return Waiting.wake!(record, step, now: now, timeout_at: [step.timeout_at, now].compact.min) if action == :run

        error = action == :pause! ? timed_out_error(step, now) : nil
        Waiting.resolve!(record, step, FlowControl::Outcome.for(action), notes: notes, error: error)
      end

      # The run and the step row stay as they are (no outcome, no StepJob);
      # only timeout_at moves. A per-run wait_timeout: that raises here, or
      # returns what is no deadline, pauses the run with that error: the
      # sweep holds the run's row lock, under which no on_exception block
      # may run, so the exception policies do not apply.
      def notify!(record, step, definition, notes:, now:)
        timeout_at = notify_deadline(record, step, definition, now)
      rescue StandardError => e
        Waiting.resolve!(record, step, FlowControl::Outcome.pause, notes: notes, error: e)
      else
        step.timeout_at = timeout_at if WorkflowStep.move_deadline!(step.id, timeout_at: timeout_at).positive?
      end

      # The next deadline of a :notify wait. A per-run wait_timeout: (issue
      # #574) is evaluated again: a Time after now is the next deadline, a
      # duration moves it one period on as a static one does, and nil or a
      # Time not after now leaves the wait without a deadline (it was
      # reported once and keeps waiting).
      def notify_deadline(record, step, definition, now)
        return next_deadline(step.timeout_at, definition.wait_timeout, now) unless definition.dynamic_wait_timeout?

        deadline = definition.resolve_wait_timeout(instance_for(record, step))
        case deadline
        when nil then nil
        when Time then deadline if deadline > now
        else next_deadline(step.timeout_at, deadline, now)
        end
      end

      # The workflow instance a per-run wait_timeout: is evaluated on, as the
      # StepJob builds it for the attempt.
      def instance_for(record, step)
        record.workflow_klass.new(record).tap do |workflow|
          workflow.send(:enter_step, StepContext::Attempt.new(step_id: step.id, step_name: step.step_name, attempt: step.attempt))
        end
      end

      # The first deadline after +now+ on the wait's wait_timeout grid: one
      # wait_timeout on when the sweep is within a period of the deadline
      # (and for expire_wait ahead of it). A sweep that missed several
      # deadlines (the dispatcher was down) reports them once: a deadline
      # still behind now would be selected again by the next batch.
      def next_deadline(timeout_at, wait_timeout, now)
        missed = timeout_at > now ? 0 : ((now - timeout_at) / wait_timeout).floor
        timeout_at + ((missed + 1) * wait_timeout)
      end

      def timed_out_error(step, now)
        WaitTimedOut.new("#{step.step_name} waited for #{step.wait_for} since #{step.waiting_since&.utc&.iso8601} " \
                         "and timed out at #{(step.timeout_at || now).utc.iso8601}")
      end
    end
  end
end
