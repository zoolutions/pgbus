# frozen_string_literal: true

module Pgbus
  # The Deploy tasks page's words (issue #536): DeployTaskState results and
  # the rollover banner as badges and sentences. Moments go through
  # pgbus_time; the _html keys let the <time> and <code> through, translate
  # escapes every other argument.
  module DeployTasksHelper
    BANNER_TONE_CSS = {
      green: "bg-green-50 text-green-800 ring-green-200 dark:bg-green-900/30 dark:text-green-200 dark:ring-green-800",
      yellow: "bg-yellow-50 text-yellow-800 ring-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-200 dark:ring-yellow-800",
      red: "bg-red-50 text-red-800 ring-red-200 dark:bg-red-900/30 dark:text-red-200 dark:ring-red-800",
      gray: "bg-gray-50 text-gray-700 ring-gray-200 dark:bg-gray-900/40 dark:text-gray-200 dark:ring-gray-700"
    }.freeze

    def pgbus_deploy_state_badge(result)
      tag.span(t("pgbus.deploy_tasks.states.#{result.state}"),
               class: "#{ApplicationHelper::BATCH_BADGE_BASE} #{ApplicationHelper::JOB_STATE_BADGE_CSS[result.badge_tone]}")
    end

    # "Waiting for rollover: 2 processes are not on abc123 yet. · ran 1m ago (manual)"
    def pgbus_deploy_task_reason(result)
      reason = translate("pgbus.deploy_tasks.reasons.#{result.reason_key}_html", **pgbus_reason_args(result.reason_args))
      return reason unless result.manual_args

      safe_join([reason, " ", translate("pgbus.deploy_tasks.reasons.manual_html", **pgbus_reason_args(result.manual_args))])
    end

    def pgbus_deploy_rollover_key(rollover)
      return "unavailable" unless rollover
      return "no_scheduler" unless rollover[:scheduler]

      rollover[:reason].to_s
    end

    def pgbus_deploy_rollover_tone(rollover)
      case pgbus_deploy_rollover_key(rollover)
      when "ready" then :green
      when "no_scheduler", "unavailable" then :red
      when "no_revision" then :gray
      else :yellow
      end
    end

    def pgbus_deploy_rollover_sentence(rollover)
      key = pgbus_deploy_rollover_key(rollover)
      args = { revision: rollover&.dig(:revision) }
      args[:count] = rollover[:lagging].size if key == "lagging"
      translate("pgbus.deploy_tasks.banner.#{key}_html", **args)
    end

    def pgbus_deploy_lagging_process(process)
      t("pgbus.deploy_tasks.banner.lagging_process",
        kind: process[:kind], hostname: process[:hostname], pid: process[:pid],
        revision: process[:revision] || t("pgbus.deploy_tasks.banner.no_revision_label"))
    end

    # "6 on 8d41b07 · 2 on 3f2a9c1", the most common revision first.
    def pgbus_deploy_live_counts(rollover)
      counts = rollover&.dig(:live_by_revision) || {}
      parts = counts.sort_by { |revision, count| [-count, revision.to_s] }.map do |revision, count|
        t("pgbus.deploy_tasks.banner.live_count", count: count,
                                                  revision: revision || t("pgbus.deploy_tasks.banner.no_revision_label"))
      end
      safe_join(parts, ProcessesHelper::PART_SEPARATOR)
    end

    def pgbus_deploy_version(task)
      task[:every_deploy] ? t("pgbus.deploy_tasks.every_deploy") : t("pgbus.deploy_tasks.version", version: task[:version])
    end

    # The newer of the last rollover and the last manual run.
    def pgbus_deploy_last_run(task)
      [task[:last_rollover_run], task[:last_manual_run]].compact.max_by { |run| Web::JobState.time(run[:run_at]).to_f }
    end
  end
end
