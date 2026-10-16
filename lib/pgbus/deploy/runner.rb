# frozen_string_literal: true

module Pgbus
  module Deploy
    # Fires deploy tasks once the fleet has rolled over (issue #536). Ticked
    # by the recurring scheduler's loop; one instance per scheduler process,
    # so its throttle and log state are plain instance state on that loop.
    #
    # A tick: cheap pending check → the rollover gate (at most every
    # ROLLOVER_CHECK_INTERVAL seconds) → for each declared, enabled, armed
    # task in key order: claim the arm → perform_later → record the run. A
    # failed enqueue re-arms the task, logs and reports. No surrounding
    # transaction: the adapter defers an enqueue inside one to its commit.
    class Runner
      ROLLOVER_CHECK_INTERVAL = 5 # seconds
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
      end

      # The valid declared tasks, in key order. A class: that does not resolve
      # to an ActiveJob is skipped with a warning, like an invalid task.
      def tasks
        @tasks ||= load_tasks
      end

      def task_count
        tasks.size
      end

      def sync!(now: Time.current)
        if revision.nil?
          Pgbus.logger.warn do
            "[Pgbus] No app revision configured (config.app_revision, PGBUS_REVISION, DASH_VERSION, KAMAL_VERSION " \
              "or a REVISION file): deploy tasks wait until one is set"
          end
        end
        DeployTask.sync_from_config!(tasks, revision: revision, now: now)
        Pgbus.logger.info { "[Pgbus] Synced #{tasks.size} deploy task(s) for revision #{revision.inspect}" }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to sync deploy tasks: #{e.class}: #{e.message}" }
        ErrorReporter.report(e, { action: "deploy_tasks_sync" })
      end

      def tick(now = Time.current)
        return if tasks.empty? || !DeployTask.pending?

        status = gate_status
        return log_waiting(status) unless status.rolled_over?

        @logged_wait = nil
        rows = DeployTask.where(key: tasks.map(&:key)).index_by(&:key)
        tasks.each do |task|
          row = rows[task.key]
          fire(task, row, now) if row&.enabled && row.armed?
        end
      end

      # Enqueue +task+ through ActiveJob and record the run. :enqueued, or
      # :failed (logged and reported) when perform_later raised or did not
      # enqueue the job. Run now calls this directly: no gate, no claim.
      def enqueue(task, triggered_by:, now: Time.current)
        payload = { task: task.key, version: task.version, revision: revision, triggered_by: triggered_by }
        job = Instrumentation.instrument("pgbus.deploy_task.enqueue", payload) { submit(task) }
        record_run(task, job, triggered_by, now)
        :enqueued
      rescue StandardError => e
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
        (@config.deploy_tasks || {}).filter_map { |key, options| build_task(key, options) }.sort_by(&:key)
      end

      def build_task(key, options)
        options = options.is_a?(Hash) ? options.transform_keys(&:to_sym) : {}
        task = Task.from_configuration(key, **options)
        return skip_task(key, task.errors.join(", ")) unless task.valid?
        return skip_task(key, "#{task.class_name} is not an ActiveJob") if task.class_name && !active_job?(task.job_class)

        task.warnings.each { |warning| Pgbus.logger.warn { "[Pgbus] Deploy task '#{key}': #{warning}" } }
        task
      end

      def active_job?(klass)
        klass.is_a?(Class) && defined?(::ActiveJob::Base) && klass <= ::ActiveJob::Base
      end

      def skip_task(key, reason)
        Pgbus.logger.warn { "[Pgbus] Skipping invalid deploy task '#{key}': #{reason}" }
        nil
      end

      def gate_status
        at = ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        return @status if @checked_at && at - @checked_at < ROLLOVER_CHECK_INTERVAL

        @checked_at = at
        @status = Rollover.check(revision: revision)
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
        return unless enqueue(task, triggered_by: ROLLOVER, now: now) == :failed

        rearm(task, row, armed_for)
      end

      def rearm(task, row, armed_for)
        row.rearm!(armed_for: armed_for, now: Time.current)
        Pgbus.logger.warn { "[Pgbus] Deploy task #{task.key} re-armed; it fires again on a later tick" }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Could not re-arm deploy task #{task.key}: #{e.class}: #{e.message}" }
        ErrorReporter.report(e, { action: "deploy_task_rearm", task: task.key, armed_for: armed_for })
      end

      def submit(task)
        captured = nil
        job = job_for(task).perform_later(*job_arguments(task)) { |j| captured = j }
        return job if job

        error = captured&.enqueue_error
        raise EnqueueError, error ? "#{error.class}: #{error.message}" : "the job was not enqueued (a before_enqueue callback aborted)"
      end

      def job_for(task)
        if task.command
          Recurring::CommandJob.set(**{ queue: task.queue_name }.compact)
        else
          klass = task.job_class
          raise EnqueueError, "#{task.class_name} does not resolve to a job class" unless active_job?(klass)

          klass.set(**{ queue: task.queue_name, priority: task.priority.zero? ? nil : task.priority }.compact)
        end
      end

      def job_arguments(task)
        task.command ? [task.command] : task.arguments
      end

      def record_run(task, job, triggered_by, now)
        DeployTaskRun.create!(task_key: task.key, version: task.version, revision: revision,
                              triggered_by: triggered_by, queue_name: job.queue_name,
                              msg_id: job.provider_job_id, run_at: now)
      rescue StandardError => e
        Pgbus.logger.error do
          "[Pgbus] Deploy task #{task.key} was enqueued but its run was not recorded: #{e.class}: #{e.message}"
        end
        ErrorReporter.report(e, { action: "deploy_task_record_run", task: task.key, triggered_by: triggered_by })
      end
    end
  end
end
