# frozen_string_literal: true

module Pgbus
  class Workflow
    # The workflow step running in this fiber, while its body runs:
    #
    #   Pgbus::Workflow::StepContext.workflow_id   # => "6f1c…" (nil outside a step)
    #   Rails.logger.tagged("wf=#{Pgbus::Workflow::StepContext.workflow_id}") { ... }
    #
    # Not named Current: every constant under Pgbus::Workflow is visible from
    # a workflow subclass's step bodies, and a bare `Current` there must stay
    # the app's ActiveSupport::CurrentAttributes.
    #
    # Fiber-local (Thread.current[] is per fiber), saved and restored around
    # each step so a nested or concurrent step never sees another's context.
    # Pgbus's own log lines inside a step carry workflow=… step=… attempt=…
    # through Pgbus::LogFormatter's context.
    module StepContext
      KEY = :pgbus_workflow_step_context

      # The running step, as the fiber-local value.
      Frame = Data.define(:workflow_id, :workflow_class, :step_name, :attempt)

      # Which attempt a workflow instance is running its body for. Set by the
      # StepJob on the fresh instance it runs the body on, never by app code.
      Attempt = Data.define(:step_id, :step_name, :attempt)

      module_function

      def with(workflow_id:, workflow_class:, step_name:, attempt:, &)
        previous = Thread.current[KEY]
        Thread.current[KEY] = Frame.new(workflow_id: workflow_id, workflow_class: workflow_class,
                                        step_name: step_name, attempt: attempt)
        LogFormatter.with_context(workflow: workflow_id, step: step_name, attempt: attempt, &)
      ensure
        Thread.current[KEY] = previous
      end

      def context = Thread.current[KEY]

      def workflow_id = context&.workflow_id

      def workflow_class = context&.workflow_class

      def step_name = context&.step_name

      def attempt = context&.attempt

      # "workflow=… step=… attempt=…" for a log line, or "" outside a step.
      def log_tag
        current = context
        return "" unless current

        "workflow=#{current.workflow_id} step=#{current.step_name} attempt=#{current.attempt}"
      end
    end
  end
end
