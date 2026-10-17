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
        TestHelpers.perform(step)
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

        TestHelpers.perform(step)
        workflow.reload
      end

      # Runs scheduled steps one after another until the run is no longer
      # ready: it stops at paused, finished and cancelled, and raises on
      # waiting (nothing inline can deliver what it waits for).
      def speedrun_workflow(workflow, max_steps: 1_000)
        max_steps.times do
          case workflow.reload.status
          when "ready" then perform_next_step(workflow)
          when "waiting", "performing"
            raise Error, "#{workflow.inspect} is #{workflow.status}: speedrun_workflow cannot move it on"
          else return workflow
          end
        end
        return workflow unless %w[ready waiting performing].include?(workflow.reload.status)

        raise Error, "#{workflow.inspect} was still #{workflow.status} after #{max_steps} steps"
      end

      # A paused or finished run's scheduled step is not claimable: running
      # it would silently do nothing (and pull its wait forward for resume!).
      def self.ensure_ready!(workflow)
        status = workflow.reload.status
        return if status == "ready"

        hint = case status
               when "paused" then "resume! it first"
               when "finished", "cancelled" then "it is over; rerun! starts a new run"
               when "performing" then "a step is still running"
               else "nothing inline can move it on"
               end
        raise Error, "#{workflow.inspect} is #{status}, not ready: #{hint}"
      end

      def self.scheduled_step(workflow)
        WorkflowStep.where(workflow_id: workflow.workflow_id, status: "scheduled").order(:id).last
      end

      def self.perform(step)
        step.update_columns(scheduled_at: Time.current) if step.scheduled_at > Time.current
        StepJob.new(step.id).perform_now
      end
    end
  end
end
