# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Recurring
    class Scheduler
      include Process::SignalHandler

      attr_reader :schedule, :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @schedule = Schedule.new(config: config)
        @shutting_down = false
        @last_runs = {}
        @loop_tick_at = Concurrent::AtomicReference.new(nil)
      end

      def run
        setup_signals
        start_heartbeat
        sync_recurring_tasks

        Pgbus.logger.info do
          "[Pgbus] Scheduler started: #{schedule.tasks.size} recurring tasks, " \
            "interval=#{config.recurring_schedule_interval}s"
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

      def sync_recurring_tasks
        tasks = config.recurring_tasks
        return if tasks.nil?

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
          metadata: { pid: ::Process.pid, tasks: schedule.tasks.size, started_at: Time.now.to_f,
                      supervisor_pid: ::Process.ppid },
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
