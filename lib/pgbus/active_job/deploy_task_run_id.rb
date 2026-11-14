# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # The deploy task run a job was enqueued for (issue #660). Deploy::Runner
    # sets it before the enqueue; it rides the serialized job data under
    # METADATA_KEY, so a retry_on re-enqueue keeps it, and the executor stamps
    # that run's outcome (DeployTaskRun.mark_completed! / mark_failed!).
    #
    # Omitted when unset: an unmarked job's payload is byte-for-byte what it
    # was before this mixin existed. Plain methods, no Concern, so the runner
    # can also extend a single instance of a class that does not include it.
    module DeployTaskRunId
      METADATA_KEY = "pgbus_deploy_task_run_id"

      attr_accessor :deploy_task_run_id

      def serialize
        data = super
        data[METADATA_KEY] = deploy_task_run_id if deploy_task_run_id
        data
      end

      def deserialize(job_data)
        super
        self.deploy_task_run_id = job_data[METADATA_KEY]
      end
    end
  end
end
