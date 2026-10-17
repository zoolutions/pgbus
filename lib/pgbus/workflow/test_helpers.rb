# frozen_string_literal: true

module Pgbus
  class Workflow
    # Run workflow steps inline in a spec, ignoring their wait:.
    #
    #   RSpec.configure { |config| config.include Pgbus::Workflow::TestHelpers }
    #
    #   wf = SignupWorkflow.start!(user)
    #   perform_next_step(wf)           # runs :welcome now
    #   run_step_inline(wf, :reminder)  # runs :reminder, raising if another step is next
    #   speedrun_workflow(wf)           # runs every step until the run stops being ready
    #   expect(wf.status).to eq("finished")
    #
    #   deliver_signal(wf, :shipped, payload: { at: Time.current })  # signal!, then the step it wakes
    #   complete_fan_out(wf)            # every member of the waiting fan_out's batch completes
    #   expire_wait(wf)                 # the waiting step's on_timeout applies now
    #
    # Each step runs through the real StepJob (claim, guards, policies, the
    # audit trail) against the test database; the StepJob messages the
    # model callback sends meanwhile go to whatever adapter the test uses
    # and are not needed.
    module TestHelpers
      class Error < Workflow::Error; end

      # Runs the run's scheduled step now. Returns the reloaded workflow.
      def perform_next_step(workflow)
        TestHelpers.ensure_ready!(workflow)
        step = TestHelpers.scheduled_step(workflow) || raise(Error, "#{workflow.inspect} has no scheduled step")
        TestHelpers.perform(step, workflow)
        workflow.reload
      end

      # Runs the run's scheduled step when it is +step_name+; raises when the
      # run is on another step.
      def run_step_inline(workflow, step_name)
        TestHelpers.ensure_ready!(workflow)
        step = TestHelpers.scheduled_step(workflow)
        unless step&.step_name == step_name.to_s
          raise Error, "#{workflow.inspect} is not about to run #{step_name} (scheduled: #{step&.step_name || "nothing"})"
        end

        TestHelpers.perform(step, workflow)
        workflow.reload
      end

      # Runs scheduled steps one after another until the run is no longer
      # ready: it stops at paused, finished and cancelled, and raises
      # Pgbus::Workflow::StepWaiting on waiting, naming what the step waits
      # for (deliver_signal, complete_fan_out or expire_wait moves it on).
      def speedrun_workflow(workflow, max_steps: 1_000)
        max_steps.times do
          case workflow.reload.status
          when "ready" then perform_next_step(workflow)
          when "waiting" then raise StepWaiting, "#{workflow.inspect} #{TestHelpers.waiting_for(workflow)}"
          when "performing"
            raise Error, "#{workflow.inspect} is performing: speedrun_workflow cannot move it on"
          else return workflow
          end
        end
        return workflow unless %w[ready waiting performing].include?(workflow.reload.status)

        raise Error, "#{workflow.inspect} was still #{workflow.status} after #{max_steps} steps"
      end

      # signal! the run, then run the step it scheduled (the waiting step it
      # woke, or the step that claims a buffered signal). Returns the
      # reloaded workflow.
      def deliver_signal(workflow, name, payload: {}, idempotency_key: nil)
        workflow.signal!(name, payload: payload, idempotency_key: idempotency_key, actor: "test_helpers")
        perform_next_step(workflow)
      end

      # Completes every member of the batch the run's waiting fan_out step
      # waits on (Pgbus::Batch.job_completed, as the executor does), then
      # runs the batch's BatchFinishedJob inline. The members do not run.
      # Returns the reloaded workflow.
      def complete_fan_out(workflow)
        step = TestHelpers.waiting_step(workflow)
        raise Error, "#{workflow.inspect} is not waiting on a fan_out batch" unless step&.batch_id

        if Pgbus::Batch.executions_migrated?
          Pgbus::BatchExecution.where(batch_id: step.batch_id).order(:id).pluck(:job_id).each do |job_id|
            Pgbus::Batch.job_completed(step.batch_id, job_id: job_id)
          end
        else
          Pgbus::Batch.find(step.batch_id)&.pending_jobs.to_i.times { Pgbus::Batch.job_completed(step.batch_id) }
        end
        BatchFinishedJob.new(step.id).perform_now
        workflow.reload
      end

      # Applies the waiting step's on_timeout now, whatever its timeout_at.
      # Returns the reloaded workflow.
      def expire_wait(workflow)
        step = TestHelpers.waiting_step(workflow)
        raise Error, "#{workflow.inspect} has no waiting step with a wait_timeout:" unless step&.timeout_at

        Runner.locked(workflow.workflow_id) do |record, notes|
          WaitTimeout.fire!(record, WorkflowStep.find(step.id), notes: notes)
        end
        workflow.reload
      end

      # A paused or finished run's scheduled step is not claimable: running
      # it would silently do nothing (and pull its wait forward for resume!).
      def self.ensure_ready!(workflow)
        status = workflow.reload.status
        return if status == "ready"
        raise StepWaiting, "#{workflow.inspect} #{waiting_for(workflow)}" if status == "waiting"

        hint = case status
               when "paused" then "resume! it first"
               when "finished", "cancelled" then "it is over; rerun! starts a new run"
               when "performing" then "a step is still running"
               else "nothing inline can move it on"
               end
        raise Error, "#{workflow.inspect} is #{status}, not ready: #{hint}"
      end

      def self.waiting_step(workflow)
        WorkflowStep.where(workflow_id: workflow.workflow_id, status: "waiting").order(:id).last
      end

      # "is waiting for signal shipped (deliver_signal …)" / "… batch (complete_fan_out …)".
      def self.waiting_for(workflow)
        step = waiting_step(workflow)
        return "is waiting" unless step
        return "is waiting on step #{step.step_name}'s batch #{step.batch_id}: complete_fan_out moves it on" if step.batch_id

        "is waiting on step #{step.step_name} for signal #{step.wait_for}: deliver_signal moves it on" \
          "#{", expire_wait times it out" if step.timeout_at}"
      end

      def self.scheduled_step(workflow)
        WorkflowStep.where(workflow_id: workflow.workflow_id, status: "scheduled").order(:id).last
      end

      # A step still waiting is pulled forward through run_now!, so the audit
      # trail records the skipped wait (actor "test_helpers").
      def self.perform(step, workflow)
        workflow.run_now!(actor: "test_helpers", reason: "run inline by a test helper") if step.scheduled_at > Time.current
        StepJob.new(step.id).perform_now
      end
    end
  end
end
