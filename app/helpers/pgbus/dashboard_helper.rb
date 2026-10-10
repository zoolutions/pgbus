# frozen_string_literal: true

module Pgbus
  # The home page's words (issue #492): the health verdict strip, the card
  # sentences built from DataSource#dashboard_signals, the queue rows'
  # QueueSummary line and the Queue Health thresholds from HealthVerdict.
  module DashboardHelper
    PART_SEPARATOR = " · "

    # Every home card is a link to the page that explains it.
    CARD_CSS = "block rounded-lg bg-white dark:bg-gray-800 p-5 shadow ring-1 ring-gray-200 dark:ring-gray-700 " \
               "hover:ring-gray-300 dark:hover:ring-gray-600"
    CARD_NUMBER_CSS = "mt-1 text-3xl font-semibold text-gray-900 dark:text-white"

    VERDICT_CSS = {
      "OK" => "bg-green-50 text-green-800 ring-green-200 dark:bg-green-900/30 dark:text-green-200 dark:ring-green-800",
      "DEGRADED" => "bg-yellow-50 text-yellow-800 ring-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-200 " \
                    "dark:ring-yellow-800",
      "STALLED" => "bg-red-50 text-red-800 ring-red-200 dark:bg-red-900/30 dark:text-red-200 dark:ring-red-800"
    }.freeze
    VERDICT_UNKNOWN_CSS = "bg-gray-50 text-gray-700 ring-gray-200 dark:bg-gray-900/40 dark:text-gray-200 dark:ring-gray-700"

    # Stat numbers are text-3xl, so the -600 shades meet the 3:1 large-text ratio.
    HEALTH_NUMBER_CSS = {
      ok: "mt-1 text-3xl font-semibold text-green-600 dark:text-green-400",
      warn: "mt-1 text-3xl font-semibold text-amber-600 dark:text-amber-400",
      critical: "mt-1 text-3xl font-semibold text-red-600 dark:text-red-400",
      unknown: "mt-1 text-3xl font-semibold text-gray-900 dark:text-white"
    }.freeze
    HEALTH_WORDS_CSS = {
      ok: "text-xs text-gray-600 dark:text-gray-300",
      warn: "text-xs text-amber-700 dark:text-amber-300",
      critical: "text-xs text-red-700 dark:text-red-300",
      unknown: "text-xs text-gray-600 dark:text-gray-300"
    }.freeze
    QUEUE_STATUS_CSS = {
      red: "text-red-700 dark:text-red-300",
      yellow: "text-yellow-800 dark:text-yellow-200",
      gray: "text-gray-600 dark:text-gray-300"
    }.freeze

    def pgbus_verdict_css(verdict)
      VERDICT_CSS.fetch(verdict&.dig(:status).to_s, VERDICT_UNKNOWN_CSS)
    end

    # Where the verdict's reasons are explained, from the signals rather than
    # the (English, unlocalised) reason strings.
    def pgbus_verdict_links(signals)
      processes = signals[:processes]
      links = []
      if (processes[:stalled] + processes[:stale]).positive?
        links << pgbus_link_to(t("pgbus.dashboard.verdict.links.processes"), pgbus.processes_path,
                               data: { turbo_frame: "_top" })
      end
      if signals[:dlq][:depth].positive?
        links << pgbus_link_to(t("pgbus.dashboard.verdict.links.dlq"), pgbus.dead_letter_index_path, data: { turbo_frame: "_top" })
      end
      links
    end

    def pgbus_queue_card_words(queues)
      parts = [t("pgbus.dashboard.stats_cards.queues_detail", paused: queues[:paused], dlq: queues[:dlq])]
      parts << t("pgbus.dashboard.stats_cards.queues_undrained", count: queues[:undrained]) if queues[:undrained].positive?
      safe_join(parts, PART_SEPARATOR)
    end

    def pgbus_process_card_words(processes)
      return t("pgbus.dashboard.stats_cards.processes_none") if processes[:total].zero?

      words = t("pgbus.dashboard.stats_cards.processes_detail", **processes.slice(:healthy, :stalled, :stale))
      return words unless processes[:stale].positive?

      safe_join([words, t("pgbus.dashboard.stats_cards.processes_stale")], " — ")
    end

    def pgbus_retrying_card_words(retrying)
      return t("pgbus.dashboard.stats_cards.retrying_none") if retrying[:count].zero?

      parts = []
      if retrying[:next_attempt_at]
        parts << t("pgbus.dashboard.stats_cards.retrying_next_html", time: pgbus_time(retrying[:next_attempt_at], clock: true))
      end
      parts << t("pgbus.dashboard.stats_cards.retrying_due", count: retrying[:due_now_count]) if retrying[:due_now_count].positive?
      parts << t("pgbus.dashboard.stats_cards.retrying_newest", error_class: retrying[:newest_error_class]) if retrying[:newest_error_class]
      safe_join(parts, PART_SEPARATOR)
    end

    def pgbus_retrying_count(retrying)
      "#{number_with_delimiter(retrying[:count])}#{"+" if retrying[:capped]}"
    end

    # The newest dead letter's reason, read as the DLQ page reads it (#495).
    def pgbus_dlq_card_words(dlq)
      return t("pgbus.dashboard.stats_cards.dlq_none") if dlq[:depth].zero?
      return t("pgbus.dashboard.stats_cards.dlq_see_why") unless dlq[:newest]

      reason = Web::DeadLetterReason.present(dlq[:newest])
      args = { job: pgbus_dead_letter_job(dlq[:newest][:message]), ago: pgbus_time(reason.died_at) }
      return t("pgbus.dashboard.stats_cards.dlq_newest_legacy_html", **args) if reason.legacy?
      return t("pgbus.dashboard.stats_cards.dlq_newest_no_error_html", **args) unless reason.error_class

      t("pgbus.dashboard.stats_cards.dlq_newest_html", error_class: reason.error_class, **args)
    end

    def pgbus_queue_status(line)
      return Web::TimeFormat::NONE unless line

      tag.span(pgbus_queue_summary_line(line), class: QUEUE_STATUS_CSS.fetch(line.tone, QUEUE_STATUS_CSS[:gray]))
    end

    # A HealthVerdict::Entry in words. Bloat thresholds are ratios, the
    # vacuum and transaction ones seconds.
    def pgbus_health_words(entry)
      percent = entry.key.start_with?("bloat.")
      args = entry.args.to_h do |key, value|
        case key
        when :table then [key, value]
        when :count, :limit then [key, number_with_delimiter(value)]
        else [key, percent ? pgbus_percent(value) : pgbus_duration(value)]
        end
      end
      tag.p(t("pgbus.dashboard.queue_health.words.#{entry.key}", **args), class: HEALTH_WORDS_CSS.fetch(entry.level))
    end

    def pgbus_health_number_css(entry) = HEALTH_NUMBER_CSS.fetch(entry.level)

    def pgbus_percent(ratio)
      number_to_percentage(ratio.to_f * 100, precision: 1, strip_insignificant_zeros: true)
    end

    # "4 / 5" for a failed row: the attempt that failed against max_retries.
    def pgbus_failure_attempt(row)
      "#{row["retry_count"].to_i + 1} / #{Pgbus.configuration.max_retries}"
    end

    def pgbus_failure_job(row)
      payload = JSON.parse(row["payload"].to_s)
      (payload["job_class"] if payload.is_a?(Hash)).presence || row["handler_class"].presence || Web::TimeFormat::NONE
    rescue JSON::ParserError
      row["handler_class"].presence || Web::TimeFormat::NONE
    end
  end
end
