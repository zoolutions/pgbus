# frozen_string_literal: true

module Pgbus
  module Web
    # What a Workflows dashboard action will do (issue #542), so its button
    # can say so before anyone presses it. Which actions exist at all is
    # Workflow::States.available_actions; this only names their effect.
    module WorkflowActionPlan
      # The status the runs of a needs-attention group are in.
      GROUP_STATUS = { "stalled" => "paused", "paused_error" => "paused", "paused_manual" => "paused",
                       "overdue" => "ready", "waiting_overdue" => "waiting" }.freeze

      module_function

      def group_status(reason) = GROUP_STATUS.fetch(reason.to_s)

      # [action, step_name] for resume! of a paused run, as OutsideVerbs.resume
      # decides it from the run's latest step row:
      #   :retry    the step failed: it runs again as a new attempt
      #   :continue the step completed or was skipped: the next step runs
      #   :finish   …and it was the last: the run finishes
      #   :resend   the step was scheduled when the run paused: it is sent again
      #   :rewait   the step was waiting: it waits again
      #   :carry_on the step was still running: the run goes on performing it
      # [nil, nil] when the run is not paused, or its class is gone.
      def resume_plan(record, latest, klass)
        return [nil, nil] unless record.status == "paused" && latest

        case latest.status
        when "failed" then [:retry, latest.step_name]
        when "scheduled" then [:resend, latest.step_name]
        when "waiting" then [:rewait, latest.step_name]
        when "in_progress" then [:carry_on, latest.step_name]
        when "completed", "skipped" then following_plan(latest, klass)
        else [nil, nil]
        end
      end

      def following_plan(latest, klass)
        return [nil, nil] unless klass

        following = klass.workflow_steps.next_after(latest.step_name)
        following ? [:continue, following.name] : [:finish, nil]
      end

      # The signal names a waiting step takes, for the Send signal select:
      # [name] for a wait_for: name or SignalMatcher, [] for a custom matcher
      # (the form then takes free text) or a step that is not waiting on one.
      def signal_names(latest, klass)
        return [] unless latest&.status == "waiting" && klass
        return [] if latest.has_attribute?(:batch_id) && latest.batch_id

        matcher = klass.workflow_steps.fetch(latest.step_name)&.wait_for
        name = matcher.respond_to?(:signal_name) ? matcher.signal_name : nil
        name ? [name.to_s] : []
      end
    end
  end
end
