# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Recurring
    class Scheduler
      include Process::SignalHandler

      attr_reader :schedule, :config, :deploy_runner

      def initialize(config: Pgbus.configuration)
        @config = config
        @schedule = Schedule.new(config: config)
        # Deploy tasks (issue #536) ride this loop: the scheduler is already
        # the one-per-deployment process, dedup'd across the overlap window.
        @deploy_runner = Deploy::Runner.new(config: config) if config.deploy_tasks_enabled && config.deploy_tasks&.any?
        @shutting_down = false
        @last_runs = {}
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
        @deploy_error = nil
      end

      # The order is load-bearing: the heartbeat registers this scheduler's row
      # before it arms deploy tasks, so an old-revision scheduler in the
      # overlap window sees it as lagging. A failed registration is logged and
      # sync still runs; the new release's workers keep the old gate closed.
      def run
        setup_signals
        start_heartbeat
        sync_recurring_tasks
        deploy_runner&.sync!

        Pgbus.logger.info do
          "[Pgbus] Scheduler started: #{schedule.tasks.size} recurring tasks, " \
            "#{deploy_task_count} deploy tasks, interval=#{config.recurring_schedule_interval}s"
        end

        loop do
          stamp_loop_tick
          break if @shutting_down

          process_signals
          break if @shutting_down

          tick(Time.current)
          break if @shutting_down

          interruptible_sleep(config.recurring_schedule_interval)
        end

        shutdown
      end

      def tick(now)
        tick_recurring(now) if config.recurring_enabled
        tick_deploy_tasks(now)
      end

      def deploy_task_count
        deploy_runner&.task_count || 0
      end

      def last_run_at(key)
        @last_runs[key]
      end

      def task_statuses
        schedule.tasks.map do |task|
          {
            key: task.key,
            class_name: task.class_name,
            command: task.command,
            schedule: task.schedule,
            human_schedule: task.human_schedule,
            queue_name: task.queue_name,
            arguments: task.arguments,
            priority: task.priority,
            description: task.description,
            next_run_at: task.next_time,
            last_run_at: @last_runs[task.key]
          }
        end
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
      end

      private

      def tick_recurring(now)
        schedule.due_tasks(now).each do |task, run_at|
          Pgbus::Instrumentation.instrument(
            "pgbus.recurring.enqueue",
            task: task.key,
            class_name: task.class_name,
            queue: task.queue_name,
            run_at: run_at
          ) do
            schedule.enqueue_task(task, run_at: run_at)
            @last_runs[task.key] = now
          end
        rescue StandardError => e
          Pgbus.logger.error do
            "[Pgbus] Error scheduling recurring task #{task.key}: #{e.class}: #{e.message}"
          end
        end
      end

      # A persistent failure (the tables missing, the database down) is logged
      # and reported once per distinct error, then at debug, not every tick.
      def tick_deploy_tasks(now)
        deploy_runner&.tick(now)
        @deploy_error = nil
      rescue StandardError => e
        signature = "#{e.class}: #{e.message}"
        if signature == @deploy_error
          Pgbus.logger.debug { "[Pgbus] Error running deploy tasks (repeated): #{signature}" }
        else
          @deploy_error = signature
          Pgbus.logger.error { "[Pgbus] Error running deploy tasks: #{signature}" }
          ErrorReporter.report(e, { action: "deploy_tasks_tick" })
        end
      end

      def sync_recurring_tasks
        tasks = config.recurring_tasks
        return if tasks.nil? || !config.recurring_enabled

        RecurringTask.sync_from_config!(tasks)
        Pgbus.logger.info { "[Pgbus] Synced #{tasks.size} recurring task(s) from configuration" }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus] Failed to sync recurring tasks: #{e.class}: #{e.message}" }
      end

      # Wall clock (not monotonic) so the dashboard can compare the beacon
      # against Time.now across processes; the heartbeat timer thread reads it
      # via loop_tick_supplier. A scheduler wedged inside tick stops advancing
      # this while heartbeats keep firing, so the beacon ages and the processes
      # page can surface the stall.
      def stamp_loop_tick
        @loop_tick_at.set(Time.now.to_f)
      end

      def start_heartbeat
        @heartbeat = Process::Heartbeat.new(
          kind: "scheduler",
          config: @config,
          metadata: { pid: ::Process.pid, tasks: schedule.tasks.size, started_at: Time.now.to_f,
                      supervisor_pid: ::Process.ppid }.merge(deploy_runner ? { deploy_tasks: deploy_task_count } : {}),
          loop_tick_supplier: -> { @loop_tick_at.get }
        )
        @heartbeat.start
      end

      def shutdown
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Scheduler stopped" }
      end
    end
  end
end
