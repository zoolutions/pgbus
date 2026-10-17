# frozen_string_literal: true

module Pgbus
  # The Processes page's words (issue #493): ProcessState results as
  # sentences. Moments go through pgbus_time, seconds through pgbus_duration,
  # counts through number_with_delimiter.
  module ProcessesHelper
    PART_SEPARATOR = " · "

    # Metadata keys the row always renders (status, activity, recycle limits,
    # Process cell, uptime), left out of the expansion's raw chips. Counters
    # and RSS stay: Recycle shows them only when a limit is configured.
    RENDERED_METADATA_KEYS = %w[
      rates in_flight loop_tick_at buffered capacity busy started_at supervisor_pid max_jobs max_memory_mb
      max_lifetime queues topics tasks threads capsule process pid hostname
    ].freeze

    SECONDS_ARGS = %i[limit within interval age].freeze
    NUMBER_ARGS = %i[number busy capacity].freeze

    # "Loop last ticked 2m ago (limit 1m 30s) — the supervisor's watchdog
    # restarts it". The _html key lets the <time> through.
    def pgbus_process_status(result)
      reason = translate("pgbus.processes.state.reasons.#{result.reason_key}_html", **pgbus_process_args(result.reason_args))
      return reason unless result.next_key

      safe_join([reason, " — ", t("pgbus.processes.state.next.#{result.next_key}", **pgbus_process_args(result.next_args))])
    end

    def pgbus_process_activity(result)
      parts = result.activity.map do |key, args|
        next t("pgbus.processes.state.activity.supervises", children: pgbus_process_children(args[:children])) if key == "supervises"

        t("pgbus.processes.state.activity.#{key}", **pgbus_process_args(args))
      end
      pgbus_process_join(parts)
    end

    def pgbus_process_recycle(result)
      parts = Array(result.recycle).map do |key, args|
        values = if key == "lifetime"
                   { uptime: pgbus_duration(args[:uptime]), limit: pgbus_duration(args[:limit]) }
                 else
                   args.transform_values { |v| v.nil? ? Web::TimeFormat::NONE : number_with_delimiter(v) }
                 end
        t("pgbus.processes.state.recycle.#{key}", **values)
      end
      pgbus_process_join(parts)
    end

    def pgbus_process_raw_metadata(metadata)
      return {} unless metadata.is_a?(Hash)

      metadata.reject { |k, v| v.nil? || RENDERED_METADATA_KEYS.include?(k.to_s) }
    end

    private

    def pgbus_process_args(args)
      args.to_h do |key, value|
        case key
        when :ago then [key, pgbus_time(value)]
        when *SECONDS_ARGS then [key, pgbus_duration(value)]
        when *NUMBER_ARGS then [key, number_with_delimiter(value)]
        else [key, value]
        end
      end
    end

    def pgbus_process_children(children)
      children.map { |kind, count| t("pgbus.processes.state.kinds.#{kind}", count: count, default: "#{count} #{kind}") }
              .join(", ")
    end

    def pgbus_process_join(parts)
      parts.empty? ? Web::TimeFormat::NONE : safe_join(parts, PART_SEPARATOR)
    end
  end
end
