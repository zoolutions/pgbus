# frozen_string_literal: true

require "active_job"

module Pgbus
  class Workflow
    # The job behind every step attempt: perform(step_id) runs one
    # StepExecution. Enqueued only by Pgbus::WorkflowStep's after-commit
    # callback, on config.workflow_queue unless the class
    # (set_step_job_options) or the step (job_options:) says otherwise.
    #
    # No retry_on and no discard_on: a step's failure is the exception
    # policies' business, and the job returns normally whatever the step's
    # outcome. Redelivery is reserved for a worker that died mid-step, which
    # the claim recognises. The visibility heartbeat must stay on for this
    # class (never `pgbus_visibility_heartbeat false`): fast crash detection
    # depends on a live worker keeping its message invisible.
    class StepJob < ::ActiveJob::Base
      # Always on, whatever ActiveJob::Base or the app set: the executor reads
      # this, and a redelivery means a dead worker only while it is true.
      def self.pgbus_visibility_heartbeat_enabled = true # rubocop:disable Naming/PredicateMethod -- JobMixin's reader

      def perform(step_id)
        StepExecution.new(step_id).call
      end
    end
  end
end
