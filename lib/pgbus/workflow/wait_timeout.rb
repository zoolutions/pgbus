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
    #
    # A signal that arrives after the timeout fired stays pending, like any
    # signal no step waits for. A paused run's wait does not time out until
    # it is resumed.
    module WaitTimeout
      module_function

      def expire_due(now: Time.current, limit: Sweep::BATCH_SIZE)
        WorkflowStep.wait_expired(now: now).order(:id).limit(limit).pluck(:id, :workflow_id).count do |id, workflow_id|
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
        apply(record, step, action, notes: notes, now: now)
        action
      end

      def apply(record, step, action, notes:, now:)
        return Waiting.wake!(record, step, now: now, timeout_at: [step.timeout_at, now].compact.min) if action == :run

        error = action == :pause! ? timed_out_error(step, now) : nil
        Waiting.resolve!(record, step, FlowControl::Outcome.for(action), notes: notes, error: error)
      end

      def timed_out_error(step, now)
        WaitTimedOut.new("#{step.step_name} waited for #{step.wait_for} since #{step.waiting_since&.utc&.iso8601} " \
                         "and timed out at #{(step.timeout_at || now).utc.iso8601}")
      end
    end
  end
end
