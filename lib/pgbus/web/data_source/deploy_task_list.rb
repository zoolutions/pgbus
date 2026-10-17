# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The Deploy tasks page (issue #536): declared tasks with their last
      # rollover and manual runs, one task's run history, the rollover the
      # scheduler acts on, and the two actions (Run now, Enable/Disable).
      module DeployTaskList
        RUN_HISTORY = 25

        # page/per_page nil returns every task (the MCP tool's default). An
        # install without the tables (pgbus:add_deploy_tasks not run) gets the
        # empty state, whose hint names the generator, not an error per refresh.
        def deploy_tasks(page: nil, per_page: nil)
          return [] unless DeployTask.table_exists?

          scope = DeployTask.order(:key)
          scope = scope.limit(per_page).offset(((page || 1) - 1) * per_page) if per_page
          records = scope.to_a
          runs = DeployTaskRun.latest_for(records.map(&:key)).group_by(&:task_key)
          records.map { |record| deploy_task_row(record, runs.fetch(record.key, [])) }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error fetching deploy tasks: #{e.class}: #{e.message}" }
          []
        end

        def deploy_task(id)
          return nil unless DeployTask.table_exists?

          record = DeployTask.find_by(id: id)
          return nil unless record

          # The state reads the same unbounded latest-per-trigger runs as the
          # list, so a run older than the 25 shown still counts.
          runs = DeployTaskRun.for_task(record.key).recent(RUN_HISTORY).to_a
          deploy_task_row(record, DeployTaskRun.latest_for([record.key]).to_a).merge(
            arguments: record.parsed_arguments,
            runs: runs.map { |run| deploy_run_hash(run).merge(queue_name: run.queue_name, msg_id: run.msg_id) }
          )
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error fetching deploy task #{id}: #{e.class}: #{e.message}" }
          nil
        end

        def toggle_deploy_task(id)
          record = DeployTask.find_by(id: id)
          return nil unless record

          record.update!(enabled: !record.enabled)
          record.enabled ? :enabled : :disabled
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error toggling deploy task #{id}: #{e.message}" }
          nil
        end

        # Run now: the scheduler's enqueue path with no gate and no claim. It
        # leaves a pending rollover arm alone and records a manual run.
        # :enqueued or :failed, the real outcome.
        def enqueue_deploy_task_now(id)
          record = DeployTask.find_by(id: id)
          return :failed unless record
          # The hidden button is not the boundary: a disabled task is not run.
          return disabled_enqueue(record) unless record.enabled

          Deploy::Runner.new(config: Pgbus.configuration).enqueue(record.to_task, triggered_by: Deploy::Runner::MANUAL)
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error enqueuing deploy task #{id}: #{e.class}: #{e.message}" }
          :failed
        end

        # The rollover judged against the newest live scheduler's revision,
        # not this web process's; that one only when no scheduler is live
        # (scheduler: false says so). nil when it cannot be read.
        def deploy_rollover(now: Time.current)
          scheduler, revision = Deploy::Rollover.scheduler_revision(now: now)
          revision = Deploy::Revision.current unless scheduler
          status = Deploy::Rollover.check(revision: revision, now: now)
          { revision: revision, reason: status.reason, lagging: status.lagging, workers_live: status.workers_live,
            live_by_revision: status.live_by_revision, scheduler: scheduler }
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::Web] Error reading the deploy rollover: #{e.class}: #{e.message}" }
          nil
        end

        private

        def disabled_enqueue(record)
          Pgbus.logger.warn { "[Pgbus::Web] Deploy task #{record.key} is disabled; Run now refused" }
          :failed
        end

        def deploy_task_row(record, latest_runs)
          by_trigger = latest_runs.index_by(&:triggered_by)
          {
            id: record.id, key: record.key, class_name: record.class_name, command: record.command,
            version: record.version, every_deploy: record.every_deploy, queue_name: record.queue_name,
            priority: record.priority, description: record.description, enabled: record.enabled,
            static: record.static, armed_at: record.armed_at, armed_for: record.armed_for,
            last_rollover_run: deploy_run_hash(by_trigger[Deploy::Runner::ROLLOVER]),
            last_manual_run: deploy_run_hash(by_trigger[Deploy::Runner::MANUAL]),
            created_at: record.created_at, updated_at: record.updated_at
          }
        end

        def deploy_run_hash(run)
          return nil unless run

          { run_at: run.run_at, revision: run.revision, version: run.version, triggered_by: run.triggered_by }
        end
      end
    end
  end
end
