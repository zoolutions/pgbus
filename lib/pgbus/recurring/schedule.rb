# frozen_string_literal: true

module Pgbus
  module Recurring
    class Schedule
      attr_reader :tasks

      def initialize(config: Pgbus.configuration)
        @config = config
        @tasks = load_tasks
      end

      def due_tasks(time = Time.current)
        tasks.filter_map do |task|
          run_at = canonical_run_at(task, time)
          [task, run_at] if run_at
        end
      end

      def enqueue_task(task, run_at:)
        queue = resolve_queue(task)
        acquired_key = acquire_uniqueness_lock(task)

        return if acquired_key == :already_locked

        RecurringExecution.record(task.key, run_at) do
          payload = build_payload(task)
          headers = build_headers(task, run_at)
          payload = inject_uniqueness_metadata(task, payload)

          Pgbus.client.ensure_queue(queue)
          msg_id = Pgbus.client.send_message(queue, payload, headers: headers)
          bind_uniqueness_lock(acquired_key, queue, msg_id)

          Pgbus.logger.info do
            "[Pgbus] Enqueued recurring task #{task.key} (#{task.class_name || task.command}) " \
              "for run_at=#{run_at.iso8601}"
          end
        end
      rescue AlreadyRecorded
        # AlreadyRecorded means this (task_key, run_at) was already enqueued.
        # If we acquired a NEW lock (prior lock was already released because the
        # job completed), release it — no message will use it. If we didn't
        # acquire a lock (nil or :already_locked), there's nothing to release.
        # In either case, we are NOT opening a race window because the job for
        # this run_at already ran or is running.
        release_uniqueness_lock(acquired_key)
        Pgbus.logger.debug { "[Pgbus] Recurring task #{task.key} already enqueued for #{run_at.iso8601}" }
      rescue StandardError
        release_uniqueness_lock(acquired_key)
        raise
      end

      def build_payload(task)
        if task.command
          {
            "job_class" => "Pgbus::Recurring::CommandJob",
            "arguments" => [task.command],
            "queue_name" => task.queue_name || @config.default_queue,
            "priority" => nil
          }
        else
          {
            "job_class" => task.class_name,
            "arguments" => task.arguments,
            "queue_name" => task.queue_name || @config.default_queue,
            "priority" => task.priority.zero? ? nil : task.priority
          }
        end
      end

      private

      def load_tasks
        raw = @config.recurring_tasks || {}
        raw.filter_map do |key, options|
          options = options.transform_keys(&:to_s).transform_keys(&:to_sym) if options.is_a?(Hash)
          task = Task.from_configuration(key, **(options || {}))
          if task.valid?
            task
          else
            Pgbus.logger.warn { "[Pgbus] Skipping invalid recurring task '#{key}': #{task.errors.join(", ")}" }
            nil
          end
        end
      end

      # Returns the canonical run_at time if the task is due, or nil if not.
      # This ensures a consistent run_at regardless of which tick detects
      # the cron occurrence — fixing a bug where match?(t) at the exact
      # boundary returns previous_time=T-1, while a tick 1s later gets
      # previous_time=T, producing different run_at values for the same
      # cron occurrence and bypassing RecurringExecution deduplication.
      def canonical_run_at(task, time)
        cron = task.parsed_schedule
        return nil unless cron

        # Exact boundary hit: cron.match?(time) is true.
        # previous_time returns the PRIOR occurrence here, but the cron
        # time that fired is `time` itself (truncated to the minute).
        if cron.match?(time)
          # Fugit next_time from 1 second before gives us the current cron time
          return cron.next_time(time - 1).to_t
        end

        # Within the scheduler interval window after the cron time.
        prev = task.previous_time(time)
        return nil unless prev

        return prev if (time - prev) <= @config.recurring_schedule_interval

        nil
      end

      def resolve_queue(task)
        task.queue_name || @config.default_queue
      end

      def build_headers(task, run_at)
        {
          "pgbus.recurring_key" => task.key,
          "pgbus.recurring_run_at" => run_at.iso8601,
          "pgbus.recurring_schedule" => task.schedule
        }
      end

      # Acquire the uniqueness lock for a recurring task.
      # Returns:
      #   nil              — no uniqueness configured, proceed without lock
      #   :already_locked  — lock held by a previous instance, caller should skip enqueue
      #   String           — the lock key (lock was acquired, caller must release on failure)
      def acquire_uniqueness_lock(task)
        return nil unless task.class_name

        job_class = task.class_name.safe_constantize
        return nil unless job_class
        return nil unless job_class.respond_to?(:pgbus_uniqueness)

        config = job_class.pgbus_uniqueness
        return nil unless config
        return nil unless config[:strategy] == :until_executed

        key = resolve_uniqueness_key(job_class, config, task)
        return nil unless key

        acquired = UniquenessKey.acquire!(key, queue_name: resolve_queue(task), msg_id: 0)

        if acquired
          key
        else
          Pgbus.logger.debug { "[Pgbus] Recurring task #{task.key} skipped: uniqueness lock held" }
          :already_locked
        end
      rescue StandardError => e
        Pgbus.logger.error do
          "[Pgbus] Uniqueness lock check failed for #{task.key}: #{e.class}: #{e.message} — " \
            "skipping enqueue to prevent duplicates"
        end
        ErrorReporter.report(e, { action: "recurring_uniqueness_lock", task: task.key })
        :already_locked # Fail closed — skip enqueue when lock check errors
      end

      # Point the pre-produce lock at the real PGMQ msg_id. Fail-soft: a bind
      # error must not roll back an already-produced message (issue #418).
      def bind_uniqueness_lock(key, queue, msg_id)
        return unless key.is_a?(String)

        Uniqueness.bind_lock(key, queue_name: queue, msg_id: msg_id)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Uniqueness bind failed: #{e.message}" }
      end

      # Release a uniqueness lock. Safe to call with nil or :already_locked.
      def release_uniqueness_lock(key)
        return if key.nil? || key == :already_locked

        UniquenessKey.release!(key)
      rescue StandardError => e
        Pgbus.logger.warn { "[Pgbus] Lock rollback failed: #{e.message}" }
      end

      # Resolve the uniqueness key for a recurring task.
      # With no explicit key: the key is the SCHEDULED job class's name —
      # resolved here, not via a stored proc, so a declaration inherited from
      # a base class keys each subclass separately (issue #357) — qualified
      # with the task's arguments so two recurring tasks pointing at the same
      # job class with different args: don't share one lock (the scheduler
      # can't use #333's raise-at-enqueue guard, so it disambiguates instead).
      #
      # Every failure here — default path or user-supplied key proc — must
      # propagate to acquire_uniqueness_lock's fail-closed rescue (skip this
      # tick, log, report). Returning nil instead would mean "no key
      # configured", and the task would be enqueued WITHOUT a lock: a broken
      # key proc silently disabling the very protection it configures. The
      # warn adds the failing-proc context before re-raising.
      def resolve_uniqueness_key(job_class, config, task)
        key_proc = config[:key]
        args = task.arguments || []
        return default_uniqueness_key(job_class, args) unless key_proc

        begin
          args.empty? ? key_proc.call : key_proc.call(*args)
        rescue StandardError => e
          Pgbus.logger.warn { "[Pgbus] Could not resolve uniqueness key for #{task.key}: #{e.message}" }
          raise
        end
      end

      # Class-name default for the scheduler path, args-qualified so distinct
      # argument sets get distinct locks. JSON keeps the key deterministic
      # and readable in the dashboard/locks table.
      def default_uniqueness_key(job_class, args)
        return job_class.name if args.empty?

        "#{job_class.name}:#{JSON.generate(args)}"
      end

      # Inject uniqueness metadata into the payload so the executor releases
      # the lock after the job completes.
      # Only inject for :until_executed strategy — :while_executing locks are
      # acquired at execution time by the executor, not by the scheduler.
      def inject_uniqueness_metadata(task, payload)
        return payload unless task.class_name

        job_class = task.class_name.safe_constantize
        return payload unless job_class.respond_to?(:pgbus_uniqueness)

        config = job_class.pgbus_uniqueness
        return payload unless config
        return payload unless config[:strategy] == :until_executed

        key = resolve_uniqueness_key(job_class, config, task)
        return payload unless key

        payload.merge(
          Pgbus::Uniqueness::METADATA_KEY => key,
          Pgbus::Uniqueness::STRATEGY_KEY => config[:strategy].to_s
        )
      end
    end
  end
end
