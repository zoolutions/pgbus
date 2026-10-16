# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # The Deploy tasks page's sample data (issue #536): a deploy mid-rollover
      # with two processes still on the old revision, and a task in every state
      # (ran, waiting, never, disabled, an every_deploy task, a manual run),
      # padded past one page. Part of fill_sample_data!.
      module SampleDeployTasks
        OLD_REVISION = "3f2a9c1"
        NEW_REVISION = "8d41b07"
        DEPLOY_KEYS = %w[backfill_order_totals warm_fragment_caches fix_currency_rounding purge_legacy_tokens
                         reindex_search rebuild_counters].freeze

        private

        def fill_sample_deploy_tasks!(now)
          @deploy_rollover_hash = {
            revision: NEW_REVISION, reason: :lagging, workers_live: 3, scheduler: true,
            live_by_revision: { NEW_REVISION => 6, OLD_REVISION => 2 },
            lagging: [{ kind: "worker", hostname: "app-2", pid: 4102, revision: OLD_REVISION },
                      { kind: "worker", hostname: "app-2", pid: 4103, revision: OLD_REVISION }]
          }
          @deploy_tasks_list = sample_deploy_tasks(now)
          @deploy_task_runs_list = sample_deploy_task_runs(now)
        end

        def sample_deploy_tasks(now)
          [
            deploy_task_row(now, 1, version: "2", armed_at: now - 240, armed_for: "2", queue_name: "maintenance",
                                    description: "Recompute totals after the currency migration",
                                    last_rollover_run: deploy_run(now - (3 * 86_400), OLD_REVISION, "1")),
            deploy_task_row(now, 2, class_name: nil, command: "FragmentCacheWarmer.run", every_deploy: true,
                                    armed_at: now - 240, armed_for: NEW_REVISION,
                                    last_rollover_run: deploy_run(now - (3 * 86_400), OLD_REVISION, "1"),
                                    last_manual_run: deploy_run(now - 90, NEW_REVISION, "1", "manual")),
            deploy_task_row(now, 3, last_rollover_run: deploy_run(now - 3600, OLD_REVISION, "1")),
            deploy_task_row(now, 4, enabled: false),
            deploy_task_row(now, 5, last_manual_run: deploy_run(now - 7200, OLD_REVISION, "1", "manual")),
            deploy_task_row(now, 6, version: "3", last_rollover_run: deploy_run(now - 86_400, OLD_REVISION, "3"))
          ] + Array.new(SampleData::PAGED_ROWS - DEPLOY_KEYS.size) do |i|
            deploy_task_row(now, DEPLOY_KEYS.size + i + 1, key: format("tenant_%02d_backfill", i + 1),
                                                           class_name: "TenantBackfillJob", static: false,
                                                           last_rollover_run: deploy_run(now - 86_400, OLD_REVISION, "1"))
          end
        end

        def deploy_task_row(now, id, **attrs)
          key = attrs.delete(:key) || DEPLOY_KEYS[id - 1]
          { id: id, key: key, class_name: "#{key.split("_").map(&:capitalize).join}Job", command: nil, version: "1",
            every_deploy: false, queue_name: "default", priority: 0, description: nil, enabled: true, static: true,
            armed_at: nil, armed_for: nil, last_rollover_run: nil, last_manual_run: nil,
            created_at: now - (30 * 86_400), updated_at: now - 3600 }.merge(attrs)
        end

        def deploy_run(run_at, revision, version, triggered_by = "rollover")
          { run_at: run_at, revision: revision, version: version, triggered_by: triggered_by }
        end

        def sample_deploy_task_runs(now)
          [deploy_run(now - 90, NEW_REVISION, "1", "manual"), deploy_run(now - (3 * 86_400), OLD_REVISION, "1"),
           deploy_run(now - (9 * 86_400), "c09e4d2", "1")].each_with_index.map do |run, i|
            run.merge(queue_name: "default", msg_id: 9100 - i)
          end
        end
      end
    end
  end
end
