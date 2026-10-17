# frozen_string_literal: true

require "active_job"

module Pgbus
  class Workflow
    # The on_complete callback of a fan_out step's batch (issue #541):
    # perform(step_id) resumes the run once every member finished (FanOut).
    # Sent on the step's StepJob queue (config.workflow_queue unless the
    # class or step says otherwise). A callback is never a member of the
    # batch it reports on, and the StepJob it leads to is sent outside any
    # Pgbus.batch block, so neither joins a batch.
    class BatchFinishedJob < ::ActiveJob::Base
      queue_as { Pgbus.configuration.workflow_queue }

      def perform(step_id)
        FanOut.finished(step_id)
      end
    end
  end
end
