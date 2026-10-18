# frozen_string_literal: true

module Pgbus
  class Workflow
    # One StepJob delivery: claim the step row, check the guards, run the
    # body in a fresh workflow instance, apply the outcome.
    #
    # 1. Claim, under the run's row lock: scheduled → in_progress (only once
    #    due) and the run ready → performing. A row that is not claimable
    #    (already running elsewhere, finished, early, or its run is paused or
    #    gone) makes this delivery a no-op, except a redelivery of the very
    #    message that claimed an in_progress row while the visibility
    #    heartbeat is on: its worker died, and the stall action applies now.
    # 2. Guards, still under the lock: the class or the step no longer
    #    defined (pause with StepNotDefined, or skip a removed_step),
    #    cancel_if, skip_if (which read woken? and signal_ready? through the
    #    attempt's SignalPeek), then a wait_for: step's signal gate.
    # 3. The body, outside any transaction, inside `catch` for the
    #    flow-control verbs and `rescue StandardError` for the policies.
    # 4. The outcome, under the lock again (StepOutcome).
    #
    # Whatever the step's outcome, the job itself completes normally: the
    # step row and the pgbus.workflow.* events carry the truth.
    class StepExecution
      # +adopt_held+: a guard raised before the gate, so the signal the step
      # holds is put on this attempt's row before its outcome (Signals.adopt_held).
      Claim = Data.define(:record, :step, :klass, :definition, :workflow, :guard_error, :adopt_held)

      def initialize(step_id, delivery: Pgbus::ActiveJob::Delivery.current, config: Pgbus.configuration)
        @step_id = step_id
        @delivery = delivery
        @config = config
      end

      def call
        step = WorkflowStep.find_by(id: @step_id)
        return debug("step #{@step_id} no longer exists (its run was deleted)") unless step

        StepContext.with(workflow_id: step.workflow_id, workflow_class: step.workflow_class, step_name: step.step_name,
                         attempt: step.attempt) do
          claim = claim(step)
          run(claim) if claim
        end
        nil
      end

      private

      attr_reader :config

      # The worker tag and the app revision (which may call the app's
      # app_revision callable or read a file) are resolved before the run's
      # row lock is taken.
      def claim(step)
        @stamp = { worker: Runner.worker_tag, app_revision: Deploy::Revision.current(config) }
        Runner.locked(step.workflow_id) do |record, notes|
          step = WorkflowStep.find_by(id: step.id) if record
          if record.nil? || step.nil?
            debug("step #{@step_id}'s run no longer exists")
          elsif step.status == "in_progress"
            check_redelivery(record, step, notes)
          elsif claimable?(record, step)
            claim_step(record, step, notes)
          else
            debug("step #{step.id} is not claimable (step #{step.status}, run #{record.status}, due #{step.scheduled_at})")
          end
        end
      end

      def claimable?(record, step)
        step.status == "scheduled" && States.legal?(:claim, record.status) && step.scheduled_at <= Time.current + 1
      end

      # A live worker keeps its message invisible (VisibilityHeartbeat), so
      # the same message coming back for a row it claimed means that worker
      # is gone. Without the heartbeat a long live step can outlive its
      # visibility timeout, so the sweep's stall_at decides instead.
      def check_redelivery(record, step, notes)
        msg_id = @delivery && @delivery[:msg_id]
        redelivered = !msg_id.nil? && step.msg_id == msg_id.to_i && @delivery[:read_ct].to_i > 1 && config.visibility_heartbeat
        return debug("step #{step.id} is already in progress elsewhere") unless redelivered

        StepOutcome.stall!(record, step, detection: "redelivery", notes: notes, config: config)
        nil
      end

      def claim_step(record, step, notes)
        now = Time.current
        klass = record.workflow_klass
        definition = klass&.workflow_steps&.fetch(step.step_name)
        attrs = { stall_at: now + (definition&.timeout || config.workflow_stall_threshold), msg_id: @delivery&.dig(:msg_id),
                  **@stamp }
        return nil if WorkflowStep.claim!(step.id, now: now, **attrs).zero?

        step.assign_attributes(status: "in_progress", started_at: now, **attrs)
        Runner.transition!(record, :claim, current_step: step.step_name)
        Runner.event!(record, "step_started", actor: Runner::SYSTEM_ACTOR, step: step,
                                              details: attrs.slice(:worker, :app_revision, :msg_id).compact)
        guard(record, step, klass, definition, notes)
      end

      # Returns the Claim to run, or nil when a guard already ended the
      # attempt. A guard that raises is handed to the exception policies,
      # like a body that raises.
      def guard(record, step, klass, definition, notes)
        return undefined!(record, step, notes, "#{record.workflow_class} is not a Pgbus::Workflow class any more") unless klass

        unless definition
          if klass.workflow_steps.removed?(step.step_name)
            # The definition is gone, so whether it waited is unknown: look for a
            # held signal only where the signal schema exists.
            Signals.adopt_held(step, WorkflowSignal.held_by_step(record.workflow_id, step.step_name)) if Sweep.waiting_ready?
            return skip!(record, step, notes, klass)
          end

          return undefined!(record, step, notes, "#{klass.name} has no step #{step.step_name} " \
                                                 "(declare `removed_step :#{step.step_name}` to skip it)")
        end

        workflow = klass.new(record)
        peek = SignalPeek.new(record, step, definition)
        workflow.send(:enter_step, StepContext::Attempt.new(step_id: step.id, step_name: step.step_name, attempt: step.attempt),
                      signals: peek)
        claim = Claim.new(record: record, step: step, klass: klass, definition: definition, workflow: workflow, guard_error: nil,
                          adopt_held: false)
        begin
          verdict = workflow.send(:guarding) do
            next FlowControl::Outcome.cancel if cancel?(klass, workflow)

            FlowControl::Outcome.skip if definition.skip?(workflow)
          end
        rescue StandardError => e
          return claim.with(guard_error: e, adopt_held: definition.waits_for_signal?)
        end
        if verdict
          Signals.adopt_held(step, peek.held)
          return outcome!(record, step, notes, klass, verdict)
        end

        gate(record, step, definition, workflow, claim, peek)
      end

      # A wait_for: step runs only with a signal (or once its wait timed
      # out); without one it parks and this delivery ends. A matcher that
      # raises is handed to the exception policies, like a guard.
      def gate(record, step, definition, workflow, claim, peek)
        return claim unless definition.waits_for_signal?

        Signals.gate(record, step, definition, workflow, peek: peek) ? claim : nil
      rescue StandardError => e
        claim.with(guard_error: e)
      end

      def cancel?(klass, workflow)
        klass.cancel_conditions.any? do |condition|
          condition.is_a?(Symbol) ? workflow.send(condition) : workflow.instance_exec(&condition)
        end
      end

      def undefined!(record, step, notes, message)
        StepOutcome.apply(record, step, FlowControl::Outcome.pause, notes: notes, error: StepNotDefined.new(message), klass: nil)
        nil
      end

      def skip!(record, step, notes, klass) = outcome!(record, step, notes, klass, FlowControl::Outcome.skip)

      def outcome!(record, step, notes, klass, outcome)
        StepOutcome.apply(record, step, outcome, notes: notes, klass: klass, duration: 0.0)
        nil
      end

      def run(claim)
        started = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        outcome, error = claim.guard_error ? [nil, claim.guard_error] : execute(claim)
        duration = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) - started
        finish(claim, outcome, error, duration)
      end

      def execute(claim)
        outcome = catch(FlowControl::THROW_TAG) do
          claim.definition.invoke(claim.workflow)
          nil
        end
        [outcome || FlowControl::Outcome.complete, nil]
      rescue StandardError => e
        [nil, e]
      end

      # The exception policy is decided before the run's lock is taken
      # again: an imperative on_exception block is app code, and must not
      # hold the row lock (and an open transaction) while it runs.
      def finish(claim, outcome, error, duration)
        return finish_late(claim, error) if error && !attempt_active?(claim)

        decision = error && decide(claim, error)
        outcome = decision.outcome if decision
        Runner.locked(claim.record.workflow_id) do |record, notes|
          next debug("step #{claim.step.id}'s run was deleted while the step ran") unless record
          next park_fan_out(record, claim, outcome, notes) if outcome&.action == :fan_out

          notes.report(decision.handler_error, handler_context(claim)) if decision&.handler_error
          # Here rather than in the claim's transaction, which the statement
          # a guard failed on may have aborted.
          Signals.adopt_held(claim.step, SignalPeek.new(record, claim.step, claim.definition).held) if claim.adopt_held
          StepOutcome.apply(record, claim.step, outcome, notes: notes, error: error, output: claim.workflow.pending_output,
                                                         report: decision ? decision.report : true, duration: duration,
                                                         klass: claim.klass)
        end
      end

      def park_fan_out(record, claim, outcome, notes)
        FanOut.park(record, claim.step, outcome, notes: notes, output: claim.workflow.pending_output)
      end

      # While its body runs, a row leaves in_progress only for a terminal
      # status (a parked row went waiting before any body ran), so an attempt
      # that finds it gone has been declared stalled (or its run cancelled): late.
      def attempt_active?(claim) = WorkflowStep.where(id: claim.step.id, status: "in_progress").exists?

      # A late attempt that raised: its outcome is ignored either way, so an
      # imperative on_exception block must not run (and cause side effects)
      # for it. A declarative policy still names the outcome it would have
      # applied and decides the report; an imperative one records none.
      def finish_late(claim, error)
        policy = policy_for(claim, error)
        decision = policy.handler ? nil : decide(claim, error, policy: policy)
        column = decision && StepOutcome::STEP_OUTCOME.fetch(decision.outcome.action)
        report = decision ? decision.report : policy.report != :never
        Runner.locked(claim.record.workflow_id) do |record, notes|
          next debug("step #{claim.step.id}'s run was deleted while the step ran") unless record

          StepOutcome.ignore_late_completion(record, claim.step, "failed", column, error)
          notes.report(error, StepOutcome.error_context(record, claim.step, "late_completion_ignored")) if report
        end
      end

      def policy_for(claim, error)
        ExceptionPolicy.resolve(error, step_policies: claim.definition.exception_policies,
                                       class_policies: claim.klass.exception_policies)
      end

      def decide(claim, error, policy: policy_for(claim, error))
        step = claim.step
        policy.decide(error, workflow: claim.workflow,
                             consecutive_reattempts: -> { WorkflowStep.consecutive_reattempts(step.workflow_id, step.step_name) })
      end

      def handler_context(claim)
        { action: "workflow_on_exception", workflow_class: claim.klass.name, workflow_id: claim.step.workflow_id,
          step_name: claim.step.step_name, attempt: claim.step.attempt }
      end

      def debug(message)
        Pgbus.logger.debug { "[Pgbus::Workflow] #{message} #{StepContext.log_tag}".rstrip }
        nil
      end
    end
  end
end
