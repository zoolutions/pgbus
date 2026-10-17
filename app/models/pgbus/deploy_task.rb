# frozen_string_literal: true

module Pgbus
  # A declared deploy task and its pending arm (issue #536). armed_at/armed_for
  # is a row-level compare-and-set: sync arms an owed task, the scheduler's
  # claim! clears the arm before it enqueues, and a failed enqueue re-arms it.
  class DeployTask < BusRecord
    self.table_name = "pgbus_deploy_tasks"

    validates :key, presence: true, uniqueness: true
    validates :version, presence: true

    scope :static_tasks, -> { where(static: true) }
    scope :enabled, -> { where(enabled: true) }
    scope :armed, -> { where.not(armed_at: nil) }

    # Any enabled task waiting to fire: the runner's cheap per-tick check.
    def self.pending?
      enabled.armed.exists?
    end

    # Upserts every declared task (valid Deploy::Task objects) and arms the
    # owed ones, then removes static rows no longer declared. Never disarms
    # and never touches enabled; an arm already held keeps its armed_at and
    # takes the new armed_for (a later bump supersedes an unfired one).
    def self.sync_from_config!(tasks, revision:, now: Time.current)
      transaction do
        tasks.each { |task| sync_task!(task, revision: revision, now: now) }
        static_tasks.where.not(key: tasks.map(&:key)).delete_all
      end
    end

    # Owed-ness comes from the rollover runs, never from the stored row: any
    # scheduler that boots syncs, including an old-revision one.
    def self.owed?(task, revision)
      return !DeployTaskRun.rollover_run_for?(task.key, task.version) unless task.every_deploy
      return false if revision.nil?

      DeployTaskRun.last_rollover_for(task.key)&.revision != revision
    end

    def self.sync_task!(task, revision:, now:)
      record = find_or_initialize_by(key: task.key)
      record.assign_attributes(
        class_name: task.class_name, command: task.command, arguments: JSON.generate(task.arguments),
        queue_name: task.queue_name, priority: task.priority, version: task.version,
        every_deploy: task.every_deploy, description: task.description, static: true
      )
      if owed?(task, revision)
        record.armed_at ||= now
        record.armed_for = task.armed_for(revision)
      end
      record.save!
    end
    private_class_method :sync_task!

    # true when this caller took the arm; false when another scheduler took
    # it first or the arm is for another version or revision.
    def claim!(armed_for:, now: Time.current) # rubocop:disable Naming/PredicateMethod -- compare-and-set that reports whether it won
      self.class.armed.where(id: id, armed_for: armed_for).update_all(armed_at: nil, updated_at: now) == 1
    end

    # Re-stamps a held arm for +armed_for+, if it is still armed for what
    # this row was read with. The runner does this when its gate is open on
    # its own revision and its own version is owed: the other arm belongs to
    # code no live process runs.
    def take_over!(armed_for:, now: Time.current) # rubocop:disable Naming/PredicateMethod -- compare-and-set, like #claim!
      self.class.armed.where(id: id, armed_for: self.armed_for).update_all(armed_for: armed_for, updated_at: now) == 1
    end

    # Restores this caller's claimed arm after a failed enqueue. Compare-and-set:
    # only while the arm is still cleared and still for +armed_for+, so a
    # failed enqueue never overwrites an arm another scheduler set meanwhile.
    def rearm!(armed_for:, now: Time.current)
      self.class.where(id: id, armed_at: nil, armed_for: armed_for).update_all(armed_at: now, updated_at: now)
    end

    def armed?
      !armed_at.nil?
    end

    # For display: a corrupt value shows as no arguments.
    def parsed_arguments
      decoded_arguments
    rescue JSON::ParserError
      []
    end

    # For Run now: a corrupt value raises (the enqueue reports :failed and
    # logs it) rather than running the job without its arguments.
    def to_task
      Deploy::Task.new(
        key: key, class_name: class_name, command: command, version: version, every_deploy: every_deploy,
        queue_name: queue_name, arguments: decoded_arguments, priority: priority, description: description
      )
    end

    private

    def decoded_arguments
      arguments.blank? ? [] : Array(JSON.parse(arguments))
    end
  end
end
