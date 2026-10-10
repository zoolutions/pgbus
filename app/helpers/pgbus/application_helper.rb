# frozen_string_literal: true

module Pgbus
  module ApplicationHelper
    # A moment as "5m ago" / "in 2h" / "now", with the exact value in the
    # tooltip and the UTC value in datetime (issue #497). clock: true adds
    # the wall-clock time: "in 2h (21:40)".
    def pgbus_time(value, clock: false)
      return Web::TimeFormat::NONE if value.blank?

      relative = Web::TimeFormat.relative(value)
      return value.to_s unless relative

      text = clock ? "#{relative} (#{Web::TimeFormat.clock(value)})" : relative
      tag.time(text, datetime: Web::TimeFormat.iso(value), title: Web::TimeFormat.absolute(value))
    end

    # The exact moment, then the relative one: for expanded rows and show
    # pages, where the absolute is what an operator debugs with.
    def pgbus_timestamp(value)
      return Web::TimeFormat::NONE if value.blank?
      return value.to_s unless Web::TimeFormat.coerce(value)

      safe_join([pgbus_absolute_time(value), " (#{Web::TimeFormat.relative(value)})"])
    end

    def pgbus_absolute_time(value)
      return Web::TimeFormat::NONE if value.blank?
      return value.to_s unless Web::TimeFormat.coerce(value)

      tag.time(Web::TimeFormat.absolute(value), datetime: Web::TimeFormat.iso(value))
    end

    def pgbus_number(n)
      return "0" unless n

      n = n.to_i
      case n
      when 0..999 then n.to_s
      when 1_000..999_999 then "#{(n / 1_000.0).round(1)}K"
      else "#{(n / 1_000_000.0).round(1)}M"
      end
    end

    # Order the rate keys are rendered in, and their i18n label suffixes.
    WORKER_RATE_KEYS = %w[processed failed dequeued].freeze

    # Metadata keys rendered specially elsewhere (throughput badge, health
    # status) and therefore excluded from the generic key/value dump.
    WORKER_INTERNAL_METADATA_KEYS = %w[rates jobs_processed jobs_failed in_flight loop_tick_at].freeze

    # The subset of a process's metadata to render as generic key/value badges:
    # everything except the throughput/health keys that have dedicated rendering.
    def pgbus_display_metadata(metadata)
      return {} unless metadata.is_a?(Hash)

      metadata.reject { |k, _| WORKER_INTERNAL_METADATA_KEYS.include?(k.to_s) }
    end

    # Render a worker's per-second throughput rates (from heartbeat metadata)
    # as a compact human-readable string, e.g. "12.4/s processed · 0.2/s failed".
    # Zero rates are omitted; returns nil when there are no non-zero rates so
    # callers can fall back to the raw metadata rendering.
    def pgbus_worker_rates(metadata)
      return nil unless metadata.is_a?(Hash)

      rates = metadata["rates"]
      return nil unless rates.is_a?(Hash)

      parts = WORKER_RATE_KEYS.filter_map do |key|
        value = rates[key].to_f
        next if value.zero?

        label = I18n.t("pgbus.processes.processes_table.rates.#{key}", default: key)
        "#{value}/s #{label}"
      end
      return nil if parts.empty?

      parts.join(" · ")
    end

    def pgbus_status_badge(healthy_or_status)
      status = case healthy_or_status
               when true then :healthy
               when Symbol, String then healthy_or_status.to_sym
               else :stale
               end

      case status
      when :healthy
        tag.span(I18n.t("pgbus.helpers.status_badge.healthy"),
                 class: "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium " \
                        "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400")
      when :stalled
        tag.span(I18n.t("pgbus.helpers.status_badge.stalled"),
                 class: "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium " \
                        "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400")
      else
        tag.span(I18n.t("pgbus.helpers.status_badge.stale"),
                 class: "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium " \
                        "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400")
      end
    end

    def pgbus_queue_badge(name)
      if name.to_s.end_with?("_dlq")
        tag.span(I18n.t("pgbus.helpers.queue_badge.dlq"),
                 class: "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium " \
                        "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400")
      else
        tag.span(I18n.t("pgbus.helpers.queue_badge.queue"),
                 class: "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium " \
                        "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400")
      end
    end

    def pgbus_duration(seconds)
      Web::TimeFormat.duration(seconds)
    end

    def pgbus_ms_duration(millis)
      Web::TimeFormat.ms_duration(millis)
    end

    def pgbus_paused_badge(paused)
      return unless paused

      tag.span(I18n.t("pgbus.helpers.paused_badge"),
               class: "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium " \
                      "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400")
    end

    BATCH_BADGE_BASE = "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
    BATCH_BADGE_CSS = {
      "finished" => "#{BATCH_BADGE_BASE} bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400",
      "processing" => "#{BATCH_BADGE_BASE} bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
      "pending" => "#{BATCH_BADGE_BASE} bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400"
    }.freeze

    def pgbus_batch_status_badge(status)
      css = BATCH_BADGE_CSS[status] || BATCH_BADGE_CSS["pending"]
      tag.span(I18n.t("pgbus.helpers.batch_status.#{status}", default: status), class: css)
    end

    JOB_STATE_BADGE_CSS = {
      blue: "bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-400",
      gray: "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300",
      indigo: "bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300",
      yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400",
      purple: "bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400",
      green: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400"
    }.freeze

    def pgbus_job_state_badge(state)
      tone = Pgbus::Web::JobState::BADGE_TONES.fetch(state.to_s, :gray)
      tag.span(I18n.t("pgbus.jobs.list.states.#{state}"), class: "#{BATCH_BADGE_BASE} #{JOB_STATE_BADGE_CSS[tone]}")
    end

    # A JobState::Result's reason in words. :time is a future moment
    # ("in 2h (21:40)"), :ago a past one ("5m ago"), both as <time>. The
    # _html key lets them through; translate escapes every other argument.
    def pgbus_job_reason(result)
      translate("pgbus.jobs.list.reasons.#{result.reason_key}_html", **pgbus_reason_args(result.reason_args))
    end

    # Reason arguments ready for an _html key: :time (future) and :ago (past)
    # as <time>, everything else as given for translate to escape.
    def pgbus_reason_args(args)
      args.to_h do |key, value|
        case key
        when :time then [key, pgbus_time(value, clock: true)]
        when :ago then [key, pgbus_time(value)]
        else [key, value]
        end
      end
    end

    # A QueueSummary::Line in words. :ago is a past moment and :time a future
    # one, both as <time>; :age is a wait in seconds. The _html key lets the
    # <time> elements through; translate escapes every other argument.
    def pgbus_queue_summary_line(line)
      args = line.args.to_h do |key, value|
        case key
        when :ago then [key, pgbus_time(value)]
        when :time then [key, pgbus_time(value, clock: true)]
        when :age then [key, pgbus_duration(value)]
        else [key, value]
        end
      end
      translate("pgbus.queues.show.summary.#{line.key}_html", **args)
    end

    QUEUE_SUMMARY_TONE_CSS = {
      yellow: "bg-yellow-50 text-yellow-800 ring-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-200 dark:ring-yellow-800",
      red: "bg-red-50 text-red-800 ring-red-200 dark:bg-red-900/30 dark:text-red-200 dark:ring-red-800",
      gray: "bg-gray-50 text-gray-700 ring-gray-200 dark:bg-gray-900/40 dark:text-gray-200 dark:ring-gray-700"
    }.freeze

    def pgbus_queue_summary_classes(tone)
      QUEUE_SUMMARY_TONE_CSS.fetch(tone, QUEUE_SUMMARY_TONE_CSS[:gray])
    end

    # "2/5" for a job's delivery attempts against max_retries; "—" when the
    # row was never delivered (blocked).
    def pgbus_job_attempts(row, max_retries)
      return "—" if row[:read_ct].nil?

      "#{row[:read_ct]}/#{max_retries}"
    end

    # A DeadLetterReason::Result in words (issue #495). With filter_path
    # (->(extra) { url }) the error class becomes a link that filters the list
    # to it. The _html keys let that link through; translate escapes the
    # error message and every other argument.
    def pgbus_dead_letter_reason(result, filter_path: nil)
      args = result.reason_args.dup
      if args[:error_class] && filter_path
        args[:error_class] = pgbus_link_to(
          result.error_class, filter_path.call(error_class: result.error_class, page: nil),
          title: t("pgbus.dead_letter.filters.filter_error_class", error_class: result.error_class),
          data: { turbo_frame: "_top" }
        )
      end
      sentence = translate("pgbus.dead_letter.reasons.#{result.reason_key}_html", **args)
      return sentence unless result.retried_before.to_i.positive?

      note = t("pgbus.dead_letter.reasons.retried_before", count: result.retried_before)
      safe_join([sentence, tag.span(note, class: "mt-1 block text-xs text-gray-600 dark:text-gray-300")])
    end

    def pgbus_dead_letter_attempts(result)
      return "—" if result.attempts.nil?

      "#{result.attempts}/#{result.max_retries}"
    end

    # The Job column: an ActiveJob's class, or a dead event's routing key.
    # Reads the raw message like the Events page: both are identifiers, and
    # PayloadFilter's default /_key$/ pattern would mask routing_key.
    def pgbus_dead_letter_job(message)
      payload = message.is_a?(String) ? JSON.parse(message) : message
      return "—" unless payload.is_a?(Hash)

      headers = payload["headers"]
      payload["job_class"].presence || (headers["routing_key"].presence if headers.is_a?(Hash)) ||
        payload["routing_key"].presence || "—"
    rescue JSON::ParserError
      "—"
    end

    # A tab's count, with "+" when a capped fragment made it a lower bound.
    def pgbus_job_count(counts, state)
      "#{number_with_delimiter(counts[state])}#{"+" if counts.capped?(state)}"
    end

    # Persisted Current attributes for a job payload, as
    # { "Current" => { "tenant" => "gid://…", ... } } or nil (issue #430).
    # Goes through pgbus_parse_message so PayloadFilter redaction applies.
    def pgbus_job_context(message)
      Pgbus::Web::JobContext.from_payload(pgbus_parse_message(message))
    end

    def pgbus_parse_message(message)
      return {} unless message

      parsed = case message
               when Hash then message
               when String then JSON.parse(message)
               else {}
               end
      Pgbus::Web::PayloadFilter.filter(parsed)
    rescue JSON::ParserError
      {}
    end

    def pgbus_json_preview(json_string, max_length: 120)
      return "—" unless json_string

      filtered = Pgbus::Web::PayloadFilter.filter_json(json_string)
      text = filtered.is_a?(String) ? filtered : JSON.generate(filtered)
      text.length > max_length ? "#{text[0...max_length]}..." : text
    end

    def pgbus_refresh_interval
      Pgbus.configuration.web_refresh_interval
    end

    def pgbus_recurring_health_badge(task)
      if task[:last_run_at].nil?
        tag.span(I18n.t("pgbus.helpers.recurring_health.pending"),
                 class: "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium " \
                        "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400")
      else
        tag.span(I18n.t("pgbus.helpers.recurring_health.active"),
                 class: "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium " \
                        "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400")
      end
    end

    def pgbus_time_range_label(minutes)
      Web::TimeFormat.range_label(minutes)
    end

    def pgbus_nav_link(label, path)
      active = request.path == path || (path != pgbus.root_path && request.path.start_with?(path))
      css = if active
              "rounded-md px-3 py-2 text-sm font-medium text-white bg-gray-800"
            else
              "rounded-md px-3 py-2 text-sm font-medium text-gray-300 hover:text-white hover:bg-gray-700"
            end
      link_to label, path, class: css
    end

    def pgbus_mobile_nav_link(label, path)
      active = request.path == path || (path != pgbus.root_path && request.path.start_with?(path))
      css = if active
              "block rounded-md px-3 py-2 text-base font-medium text-white bg-gray-800"
            else
              "block rounded-md px-3 py-2 text-base font-medium text-gray-300 hover:text-white hover:bg-gray-700"
            end
      link_to label, path, class: css
    end

    LOCALE_NAMES = {
      da: "Dansk",
      de: "Deutsch",
      en: "English",
      es: "Espa\u00f1ol",
      fi: "Suomi",
      fr: "Fran\u00e7ais",
      it: "Italiano",
      ja: "\u65E5\u672C\u8A9E",
      nb: "Norsk",
      nl: "Nederlands",
      pt: "Portugu\u00eas",
      sv: "Svenska"
    }.freeze

    def pgbus_locale_name(code)
      LOCALE_NAMES[code.to_sym] || code.to_s.upcase
    end

    def pgbus_locale_flag(code)
      case code.to_sym
      when :da then "\u{1F1E9}\u{1F1F0}"
      when :de then "\u{1F1E9}\u{1F1EA}"
      when :en then "\u{1F1EC}\u{1F1E7}"
      when :es then "\u{1F1EA}\u{1F1F8}"
      when :fi then "\u{1F1EB}\u{1F1EE}"
      when :fr then "\u{1F1EB}\u{1F1F7}"
      when :it then "\u{1F1EE}\u{1F1F9}"
      when :ja then "\u{1F1EF}\u{1F1F5}"
      when :nb then "\u{1F1F3}\u{1F1F4}"
      when :nl then "\u{1F1F3}\u{1F1F1}"
      when :pt then "\u{1F1F5}\u{1F1F9}"
      when :sv then "\u{1F1F8}\u{1F1EA}"
      else "\u{1F310}"
      end
    end
  end
end
