# frozen_string_literal: true

module Pgbus
  module Deploy
    # Fires deploy tasks once the fleet has rolled over (issue #536). Ticked
    # by the recurring scheduler's loop; one instance per scheduler process,
    # so its throttle and log state are plain instance state on that loop.
    #
    # A tick: cheap pending check → the rollover gate (at most every
    # ROLLOVER_CHECK_INTERVAL seconds) → for each declared, enabled, armed
    # task in key order whose after: dependencies have completed (issue
    # #660): claim the arm → record the run → perform_later. A task still
    # waiting on a dependency keeps its arm and is not counted as a failure.
    # A failed enqueue deletes its run, re-arms the task, logs and reports.
    # No surrounding transaction: the adapter defers an enqueue inside one to
    # its commit.
    class Runner
      ROLLOVER_CHECK_INTERVAL = 5 # seconds
      # A task whose enqueue failed is retried after 5 s, then 10, 20, … up to
      # 5 minutes, so a deterministic failure (a before_enqueue that always
      # aborts, a uniqueness reject while the duplicate is queued) does not
      # retry and report on every tick.
      RETRY_BACKOFF_BASE = 5 # seconds
      RETRY_BACKOFF_MAX = 300 # seconds
      ROLLOVER = "rollover"
      MANUAL = "manual"

      attr_reader :revision

      def initialize(config: Pgbus.configuration, revision: Revision.current(config))
        @config = config
        @revision = revision
        @tasks = nil
        @status = nil
        @checked_at = nil
        @logged_wait = nil
        @failures = {}
        @synced = nil
        @sync_failures = 0
        @sync_retry_at = nil
        @logged_dependencies = {}
      end

      # The valid declared tasks, in key order. A class: that does not resolve
      # to an ActiveJob is skipped with a warning, like an invalid task. A
      # failed schema probe (Deploy::Schema) raises out of here and leaves
      # nothing memoized, so the next tick or sync retry loads them again.
      def tasks
        @tasks ||= load_tasks
      end

      # The scheduler's heartbeat and boot log read this before the first
      # tick: a failed schema probe must not stop it from booting, so it
      # counts the declared entries instead.
      def task_count
        tasks.size
      rescue StandardError
        (@config.deploy_tasks || {}).size
      end

      # true once synced. A failure (a database blip at boot) is retried from
      # tick with the enqueue backoff, so the scheduler keeps no unarmed tasks.
      def sync!(now: Time.current)
        if revision.nil? && @synced.nil?
          Pgbus.logger.warn do
            "[Pgbus] No app revision configured (config.app_revision, PGBUS_REVISION, DASH_VERSION, KAMAL_VERSION " \
              "or a REVISION file): deploy tasks wait until one is set"
          end
        end
        DeployTask.sync_from_config!(tasks, revision: revision, now: now)
        Pgbus.logger.info { "[Pgbus] Synced #{tasks.size} deploy task(s) for revision #{revision.inspect}" }
        @synced = true
      rescue StandardError => e
        @sync_failures += 1
        delay = retry_delay(@sync_failures)
        @sync_retry_at = monotonic_now + delay
        # Reported (and logged at error by the reporter) once; repeats of a
        # persistent failure drop to debug instead of reporting every retry.
        if @sync_failures == 1
          ErrorReporter.report(e, { action: "deploy_tasks_sync", retry_in: delay })
        else
          Pgbus.logger.debug { "[Pgbus] Deploy task sync still failing (retrying in #{delay}s): #{e.class}: #{e.message}" }
        end
        @synced = false
      end

      def tick(now = Time.current)
        return if tasks.empty?
        return if @synced == false && !retry_sync(now)
        return unless DeployTask.pending?

        status = gate_status
        return log_waiting(status) unless status.rolled_over?

        @logged_wait = nil
        rows = DeployTask.where(key: tasks.map(&:key)).index_by(&:key)
        due = tasks.select do |task|
          row = rows[task.key]
          row&.enabled && row.armed? && !backing_off?(task.key)
        end
        dependencies = load_dependencies(due)
        due.each { |task| fire(task, rows[task.key], now) unless waiting_on_dependencies?(task, dependencies) }
      end

      # Record a run of +task+ and enqueue it through ActiveJob, carrying the
      # run's id so the executor stamps its outcome. :enqueued, or :failed
      # (logged and reported, the run deleted) when the run could not be
      # created or perform_later raised or did not enqueue the job. Run now
      # calls this directly: no gate, no claim, no dependency check.
      def enqueue(task, triggered_by:, now: Time.current)
        run = DeployTaskRun.create!(task_key: task.key, version: task.version, revision: revision,
                                    triggered_by: triggered_by, run_at: now)
        payload = { task: task.key, version: task.version, revision: revision, triggered_by: triggered_by }
        job = Instrumentation.instrument("pgbus.deploy_task.enqueue", payload) { submit(task, run) }
        record_enqueue(task, run, job, triggered_by)
        :enqueued
      rescue StandardError => e
        discard_run(task, run) if run
        Pgbus.logger.error do
          "[Pgbus] Deploy task #{task.key} (version #{task.version}, #{triggered_by}) was not enqueued: " \
            "#{e.class}: #{e.message}"
        end
        ErrorReporter.report(e, { action: "deploy_task_enqueue", task: task.key, version: task.version,
                                  revision: revision, triggered_by: triggered_by })
        :failed
      end

      private

      def load_tasks
        declared = @config.deploy_tasks || {}
        built = declared.filter_map { |key, options| build_task(key, options) }
        built = reject_dependencies_without_schema(built)
        valid, skipped = Dependencies.prune(built, declared.keys.map(&:to_s))
        skipped.each { |key, reason| skip_task(key, reason) }
        valid.sort_by(&:key)
      end

      # after: needs the run outcomes, so until they are migrated a task that
      # declares it is skipped (and its dependents with it, by prune).
      def reject_dependencies_without_schema(tasks)
        return tasks if tasks.none? { |task| task.dependencies.any? } || Schema.outcomes?

        tasks.reject do |task|
          next false if task.dependencies.empty?

          skip_task(task.key, "after: needs the deploy task run outcomes; run `rails g pgbus:add_deploy_task_run_outcomes` " \
                              "(or pgbus:update) and migrate")
          true
        end
      end

      # The outcomes of the dependencies of +due+ (one query), or nil when
      # none of them declares after:.
      def load_dependencies(due)
        keys = due.flat_map(&:dependencies).uniq
        return if keys.empty?

        Dependencies.load(keys.filter_map { |key| tasks_by_key[key] }, revision: revision)
      end

      def waiting_on_dependencies?(task, dependencies)
        return false if task.dependencies.empty?

        waiting = dependencies.waiting_on(task.dependencies, tasks_by_key)
        log_dependencies(task, waiting)
        waiting.any?
      end

      def tasks_by_key
        @tasks_by_key ||= tasks.index_by(&:key)
      end

      def log_dependencies(task, waiting)
        summary = waiting.map { |dep| "#{dep[:key]} (#{dep[:state].tr("_", " ")})" }.join(", ")
        return if @logged_dependencies[task.key] == summary

        @logged_dependencies[task.key] = summary
        Pgbus.logger.info { "[Pgbus] Deploy task #{task.key} waits on #{summary}" } unless summary.empty?
      end

      def build_task(key, options)
        options = options.is_a?(Hash) ? options.transform_keys(&:to_sym) : {}
        task = Task.from_configuration(key, **options)
        return skip_task(key, task.errors.join(", ")) unless task.valid?
        return skip_task(key, "#{task.class_name} is not an ActiveJob") if task.class_name && !active_job?(task.job_class)

        task.warnings.each { |warning| Pgbus.logger.warn { "[Pgbus] Deploy task '#{key}': #{warning}" } }
        task
      rescue StandardError => e
        # A malformed entry (a Hash where a scalar belongs) skips that task
        # instead of crashing the scheduler at boot.
        skip_task(key, "#{e.class}: #{e.message}")
      end

      def active_job?(klass)
        klass.is_a?(Class) && defined?(::ActiveJob::Base) && klass <= ::ActiveJob::Base
      end

      def skip_task(key, reason)
        Pgbus.logger.warn { "[Pgbus] Skipping invalid deploy task '#{key}': #{reason}" }
        nil
      end

      def gate_status
        at = monotonic_now
        return @status if @checked_at && at - @checked_at < ROLLOVER_CHECK_INTERVAL

        # Stamped only after a successful check: a raising check must not
        # leave a nil status cached for the next ticks.
        @status = Rollover.check(revision: revision)
        @checked_at = at
        @status
      end

      def retry_sync(now)
        return false if monotonic_now < @sync_retry_at

        sync!(now: now)
      end

      def log_waiting(status)
        summary = waiting_summary(status)
        return if summary == @logged_wait

        @logged_wait = summary
        Pgbus.logger.info { "[Pgbus] Deploy tasks waiting for rollover: #{summary}" }
      end

      def waiting_summary(status)
        case status.reason
        when :no_revision then "app revision not configured"
        when :no_workers then "no worker on #{revision} yet"
        else
          processes = status.lagging.map { |p| "#{p[:kind]}@#{p[:hostname]} pid #{p[:pid]} (#{p[:revision] || "no revision"})" }
          "#{status.lagging.size} process(es) not on #{revision}: #{processes.join(", ")}"
        end
      end

      # An arm for another version or revision belongs to code no live
      # process runs any more (the gate is open on this revision), so when
      # this runner's own version is owed it takes the arm over; otherwise
      # the claim below fails and nothing fires.
      def fire(task, row, now)
        armed_for = task.armed_for(revision)
        row.take_over!(armed_for: armed_for, now: now) if row.armed_for != armed_for && DeployTask.owed?(task, revision)
        return unless row.claim!(armed_for: armed_for, now: now)
        return @failures.delete(task.key) if enqueue(task, triggered_by: ROLLOVER, now: now) == :enqueued

        rearm(task, row, armed_for)
      end

      def rearm(task, row, armed_for)
        delay = note_failure(task.key)
        row.rearm!(armed_for: armed_for, now: Time.current)
        Pgbus.logger.warn { "[Pgbus] Deploy task #{task.key} re-armed; retrying in #{delay}s" }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Could not re-arm deploy task #{task.key}: #{e.class}: #{e.message}" }
        ErrorReporter.report(e, { action: "deploy_task_rearm", task: task.key, armed_for: armed_for })
      end

      def backing_off?(key)
        failure = @failures[key]
        !failure.nil? && monotonic_now < failure[:retry_at]
      end

      # Records one more failed enqueue for +key+ and returns the seconds until its retry.
      def note_failure(key)
        count = @failures.fetch(key, { count: 0 })[:count] + 1
        delay = retry_delay(count)
        @failures[key] = { count: count, retry_at: monotonic_now + delay }
        delay
      end

      def retry_delay(count)
        [RETRY_BACKOFF_BASE * (2**(count - 1)), RETRY_BACKOFF_MAX].min
      end

      def monotonic_now
        ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
      end

      # Builds the job and enqueues it directly (what perform_later does), so
      # the job, and its enqueue_error, is in hand on every Rails version:
      # perform_later returns false on failure, and on Rails 7.1 a .set(...)
      # job never yields it to perform_later's block.
      def submit(task, run)
        job = job_class_for(task).new(*job_arguments(task))
        # The engine includes DeployTaskRunId on every job; without it (no
        # engine loaded) this instance alone is marked.
        job.extend(ActiveJob::DeployTaskRunId) unless job.respond_to?(:deploy_task_run_id=)
        job.deploy_task_run_id = run.id
        enqueued = job.enqueue(job_options(task))
        # The pgbus adapter returns a job its uniqueness or concurrency
        # on_conflict dropped without raising; nothing would run for this run.
        raise EnqueueError, "the job was dropped by its uniqueness or concurrency on_conflict" if
          enqueued && job.deploy_task_run_enqueued == false
        return job if enqueued

        error = job.enqueue_error
        raise EnqueueError, error ? "#{error.class}: #{error.message}" : "the job was not enqueued (a before_enqueue callback aborted)"
      end

      def job_class_for(task)
        return Recurring::CommandJob if task.command

        klass = task.job_class
        raise EnqueueError, "#{task.class_name} does not resolve to a job class" unless active_job?(klass)

        klass
      end

      def job_options(task)
        priority = task.command || task.priority.zero? ? nil : task.priority
        { queue: task.queue_name, priority: priority }.compact
      end

      def job_arguments(task)
        task.command ? [task.command] : task.arguments
      end

      # queue_name marks the run as enqueued: until it is set, the run counts
      # as no run (DeployTaskRun.enqueued), so a failure here leaves the
      # version owed, as a missing run row did before #660.
      def record_enqueue(task, run, job, triggered_by)
        run.update_columns(queue_name: job.queue_name, msg_id: job.provider_job_id)
      rescue StandardError => e
        Pgbus.logger.error do
          "[Pgbus] Deploy task #{task.key} was enqueued but its run was not recorded: #{e.class}: #{e.message}"
        end
        ErrorReporter.report(e, { action: "deploy_task_record_run", task: task.key, triggered_by: triggered_by })
      end

      # A run whose enqueue failed. Left behind (this delete failed too) it
      # still counts as no run, having no queue_name.
      def discard_run(task, run)
        run.delete
      rescue StandardError => e
        ErrorReporter.report(e, { action: "deploy_task_discard_run", task: task.key, run_id: run.id })
      end
    end
  end
end
