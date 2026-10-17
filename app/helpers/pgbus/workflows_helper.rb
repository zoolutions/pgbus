# frozen_string_literal: true

require "json"
require "uri"

module Pgbus
  # The Workflows pages' words (issue #542): status badges, the "what happens
  # next" sentence, the subject link, the Resume label, the audit-trail
  # sentence and the attention reasons. Moments go through pgbus_time; the
  # _html keys let the <time>, <code> and links through, translate escapes
  # every other argument.
  module WorkflowsHelper
    BADGE_CSS = {
      blue: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
      gray: "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
      yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400",
      green: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400",
      red: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400"
    }.freeze

    # The outside verbs the show page has a button for (Send signal is its
    # own form; pause and reschedule stay in the console for now).
    DASHBOARD_ACTIONS = %i[resume resume_at run_now skip rerun cancel].freeze

    # A link inside a sentence: inline (the button helper's inline-flex would
    # break the sentence) and underlined; WCAG 2.5.8 exempts inline targets.
    INLINE_LINK_CSS = "font-medium underline text-indigo-700 hover:text-indigo-900 dark:text-indigo-300 dark:hover:text-indigo-200"

    # Text inputs, selects and textareas on the Workflows pages.
    INPUT_CSS = "mt-1 block w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-900 " \
                "px-2 py-1.5 text-sm text-gray-900 dark:text-white focus:border-indigo-500 focus:ring-indigo-500"

    STEP_TONES = { "scheduled" => :gray, "waiting" => :yellow, "in_progress" => :blue, "completed" => :green,
                   "failed" => :red, "cancelled" => :gray, "skipped" => :gray }.freeze

    SIGNAL_TONES = { "pending" => :yellow, "claimed" => :blue, "consumed" => :green }.freeze

    # The show page's buttons: available_actions it has a button for, without
    # Resume and Resume at when the run's class is not loaded here (resume!
    # needs its step list and raises StepNotDefined without it).
    def pgbus_workflow_dashboard_actions(workflow)
      actions = Array(workflow[:available_actions]) & DASHBOARD_ACTIONS
      workflow[:class_loaded] == false ? actions - %i[resume resume_at] : actions
    end

    def pgbus_workflow_state(row)
      Web::WorkflowState.present(row, threshold: Pgbus.configuration.workflow_stall_threshold)
    end

    def pgbus_workflow_badge(result)
      pgbus_workflow_tone_badge(t("pgbus.workflows.statuses.#{result.badge}"), result.badge_tone)
    end

    def pgbus_workflow_step_badge(status)
      pgbus_workflow_tone_badge(t("pgbus.workflows.step_statuses.#{status}"), STEP_TONES.fetch(status.to_s, :gray))
    end

    def pgbus_workflow_signal_badge(status)
      pgbus_workflow_tone_badge(t("pgbus.workflows.signal_statuses.#{status}"), SIGNAL_TONES.fetch(status.to_s, :gray))
    end

    def pgbus_workflow_tone_badge(label, tone)
      tag.span(label, class: "#{ApplicationHelper::BATCH_BADGE_BASE} #{BADGE_CSS.fetch(tone)}")
    end

    # "Waiting for batch OrderWorkflow#notify_vendors (7 of 12 jobs done)."
    def pgbus_workflow_next(row, result = pgbus_workflow_state(row))
      args = result.next_args.to_h do |key, value|
        case key
        when :step, :signal then [key, tag.code(value, class: "font-mono")]
        when :batch then [key, pgbus_workflow_batch_link(value, row[:batch_id])]
        when :ago then [key, pgbus_time(value)]
        when :time then [key, pgbus_time(value, clock: true)]
        else [key, value]
        end
      end
      translate("pgbus.workflows.next.#{result.next_key}_html", **args)
    end

    def pgbus_workflow_batch_link(name, batch_id)
      label = tag.code(name, class: "font-mono")
      batch_id ? link_to(label, pgbus.batch_path(batch_id), class: INLINE_LINK_CSS, data: { turbo_frame: "_top" }) : label
    end

    # "Order 1042", linked through config.workflow_subject_url when it gives
    # an http(s) URL or a same-site path; plain text otherwise, and "—"
    # for a subject-less run.
    def pgbus_workflow_subject(row)
      return Web::TimeFormat::NONE unless row[:subject_type]

      label = "#{row[:subject_type]} #{row[:subject_id]}"
      url = pgbus_workflow_subject_url(row[:subject_type], row[:subject_id])
      url ? pgbus_link_to(label, url, data: { turbo_frame: "_top" }) : label
    end

    def pgbus_workflow_subject_url(type, id)
      builder = Pgbus.configuration.workflow_subject_url
      return nil unless builder

      pgbus_workflow_safe_url(builder.call(type, id))
    rescue StandardError => e
      Pgbus.logger.error { "[Pgbus::Web] workflow_subject_url raised #{e.class}: #{e.message}" }
      nil
    end

    # An absolute http(s) URL with a host, or a path on this site ("/…",
    # never "//host" or "/\host"). Anything else (javascript:, data:, a
    # relative path) is not linked.
    def pgbus_workflow_safe_url(value)
      url = value.to_s.strip
      return nil if url.empty?
      return url if url.start_with?("/") && !url.start_with?("//", "/\\")

      uri = URI.parse(url)
      %w[http https].include?(uri.scheme&.downcase) && uri.host.present? ? url : nil
    rescue URI::InvalidURIError
      nil
    end

    # The Resume button's label: what resume! will do (WorkflowActionPlan).
    def pgbus_workflow_resume_label(workflow)
      action = workflow[:resume_action]
      return t("pgbus.workflows.actions.resume") unless action

      t("pgbus.workflows.resume.#{action}", step: workflow[:resume_step])
    end

    # "Resumed by dashboard:alice: SMTP fixed", "Step failed by system (capture #2)".
    def pgbus_workflow_event_sentence(event)
      what = t("pgbus.workflows.events.#{event[:event]}", default: event[:event].to_s.humanize)
      sentence = t("pgbus.workflows.audit.by", event: what, actor: event[:actor])
      if event[:step_name]
        sentence = t("pgbus.workflows.audit.with_step", sentence: sentence, step: event[:step_name], attempt: event[:attempt])
      end
      event[:reason].present? ? t("pgbus.workflows.audit.with_reason", sentence: sentence, reason: event[:reason]) : sentence
    end

    def pgbus_workflow_value(value)
      value.is_a?(Hash) || value.is_a?(Array) ? JSON.generate(value) : value.to_s
    end

    # The bulk confirm: "Resume all 3 runs paused at capture on Timeout::Error?"
    def pgbus_workflow_bulk_confirm(action, group)
      key = group[:error_class] ? "#{action}_error" : action.to_s
      t("pgbus.workflows.attention.confirm.#{key}", count: group[:count], step: group[:step_name], error: group[:error_class])
    end

    # The index URL with the current query, merged with +extra+ (nil drops a key).
    def pgbus_workflows_path_with(query, **extra)
      pgbus.workflows_path(query.to_param(**extra))
    end

    # Series names and words the timeline chart prints (charts.js renderTimeline).
    def pgbus_workflow_chart_i18n
      series = %w[waited queued waiting paused ran_ok ran_failed ran_open ran_cancelled]
      { series: series.index_with { |name| t("pgbus.workflows.timeline.series.#{name}") },
        paused_row: t("pgbus.workflows.timeline.paused_row"),
        statuses: Workflow::States::STEP.index_with { |status| t("pgbus.workflows.step_statuses.#{status}") } }
    end
  end
end
