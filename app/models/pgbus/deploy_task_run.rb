# frozen_string_literal: true

module Pgbus
  # One enqueue of a deploy task (issue #536): by the scheduler after a
  # rollover, or by "Run now" on the dashboard. Rollover runs are what
  # DeployTask.owed? reads; manual runs never satisfy an obligation.
  class DeployTaskRun < BusRecord
    self.table_name = "pgbus_deploy_task_runs"

    ROLLOVER = "rollover"
    MANUAL = "manual"

    validates :task_key, :version, :triggered_by, :run_at, presence: true

    scope :for_task, ->(key) { where(task_key: key) }
    scope :rollover, -> { where(triggered_by: ROLLOVER) }
    scope :manual, -> { where(triggered_by: MANUAL) }
    scope :recent, ->(limit = 25) { order(id: :desc).limit(limit) }
    scope :older_than, ->(time) { where(run_at: ...time) }

    def self.last_rollover_for(key)
      rollover.for_task(key).order(id: :desc).first
    end

    def self.rollover_run_for?(key, version)
      rollover.for_task(key).exists?(version: version.to_s)
    end
  end
end
