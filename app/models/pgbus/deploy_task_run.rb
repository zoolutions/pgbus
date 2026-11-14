# frozen_string_literal: true

module Pgbus
  # One enqueue of a deploy task (issue #536): by the scheduler after a
  # rollover, or by "Run now" on the dashboard. Rollover runs are what
  # DeployTask.owed? reads; manual runs never satisfy an obligation.
  #
  # The row is created before its job is enqueued and carries the job's
  # queue_name once the enqueue went through (issue #660), so a row without
  # one (a crash between the two) counts as no run. The executor stamps
  # completed_at or failed_at; a completed run at a task's current version is
  # what an after: dependency waits for (Deploy::Dependencies).
  class DeployTaskRun < BusRecord
    self.table_name = "pgbus_deploy_task_runs"

    ROLLOVER = "rollover"
    MANUAL = "manual"

    validates :task_key, :version, :triggered_by, :run_at, presence: true

    scope :for_task, ->(key) { where(task_key: key) }
    scope :enqueued, -> { where.not(queue_name: nil) }
    scope :completed, -> { where.not(completed_at: nil) }
    scope :rollover, -> { enqueued.where(triggered_by: ROLLOVER) }
    scope :manual, -> { where(triggered_by: MANUAL) }
    scope :recent, ->(limit = 25) { order(id: :desc).limit(limit) }
    scope :older_than, ->(time) { where(run_at: ...time) }

    # Runs the dispatcher may prune: older than +time+, except the newest
    # rollover run per (task_key, version). That run is what marks a version
    # done (DeployTask.owed?); pruning it would re-arm the task on the next
    # scheduler boot and run it again.
    # The newest completed run per (task_key, version) is kept too: it is
    # what satisfies an after: dependency on that version.
    def self.prunable_before(time)
      scope = older_than(time).where.not(id: rollover.group(:task_key, :version).select("MAX(id)"))
      return scope unless Deploy::Schema.outcomes?

      scope.where.not(id: completed.group(:task_key, :version).select("MAX(id)"))
    end

    def self.last_rollover_for(key)
      rollover.for_task(key).order(id: :desc).first
    end

    # The newest run per (task_key, triggered_by) for +keys+: the dashboard's
    # "last rollover run" and "last manual run" columns, in one query.
    def self.latest_for(keys)
      enqueued.where(task_key: keys)
              .select("DISTINCT ON (task_key, triggered_by) #{quoted_table_name}.*")
              .order(:task_key, :triggered_by, id: :desc)
    end

    def self.rollover_run_for?(key, version)
      rollover.for_task(key).exists?(version: version.to_s)
    end

    # The executor's outcome writes: true when this call stamped the run.
    def self.mark_completed!(id, now: Time.current) # rubocop:disable Naming/PredicateMethod -- reports whether it stamped
      where(id: id, completed_at: nil).update_all(completed_at: now) == 1
    end

    def self.mark_failed!(id, now: Time.current) # rubocop:disable Naming/PredicateMethod -- reports whether it stamped
      where(id: id, failed_at: nil).update_all(failed_at: now) == 1
    end

    Outcome = Data.define(:task_key, :version, :revision, :completed, :latest_failed, :latest_id)

    # One Outcome per (task_key, version, revision) of +keys+: whether any run
    # completed, whether the newest one failed without completing, and that
    # newest run's id.
    def self.outcomes_for(keys)
      return [] if keys.empty?

      enqueued.where(task_key: keys).group(:task_key, :version, :revision).pluck(
        :task_key, :version, :revision, Arel.sql("bool_or(completed_at IS NOT NULL)"),
        Arel.sql("(array_agg(failed_at IS NOT NULL AND completed_at IS NULL ORDER BY id DESC))[1]"),
        Arel.sql("MAX(id)")
      ).map { |row| Outcome.new(*row) }
    end
  end
end
