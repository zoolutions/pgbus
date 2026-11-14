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
    #
    # deploy_task_run_enqueued is transient, never serialized: the pgbus
    # adapter sets it on a marked instance it was asked to enqueue, true once
    # the message is live (sent, or parked behind its concurrency limit),
    # false when it dropped the job (a uniqueness on_conflict :discard / :log
    # on a first enqueue, or a concurrency on_conflict :discard on any). nil means no enqueue of this instance was attempted.
    #
    # pgbus_discarded? is transient too (issue #662): true once ActiveJob
    # discarded this instance. Every discard (discard_on, a retry_on that
    # gives up) runs the job's after_discard procs through
    # run_after_discard_procs, so overriding it here sees them all, and a
    # subclass's own after_discard procs keep running as declared. discard_on
    # and a retry_on with a block then return normally, and the executor
    # stamps failed_at, not completed_at.
    module DeployTaskRunId
      METADATA_KEY = "pgbus_deploy_task_run_id"

      attr_accessor :deploy_task_run_id, :deploy_task_run_enqueued

      def serialize
        data = super
        data[METADATA_KEY] = deploy_task_run_id if deploy_task_run_id
        data
      end

      def deserialize(job_data)
        super
        self.deploy_task_run_id = job_data[METADATA_KEY]
      end

      def pgbus_discarded? = @pgbus_discarded == true

      private

      def run_after_discard_procs(exception)
        super
        @pgbus_discarded = true
      end
    end
  end
end
