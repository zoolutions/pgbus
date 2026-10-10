# frozen_string_literal: true

module Pgbus
  # The Events page's words (issue #494): EventState results as badges and
  # sentences, in the jobs list's vocabulary and colours.
  module EventsHelper
    def pgbus_event_state_badge(state)
      tone = Pgbus::Web::JobState::BADGE_TONES.fetch(state.to_s, :gray)
      tag.span(t("pgbus.events.list.states.#{state}"),
               class: "#{ApplicationHelper::BATCH_BADGE_BASE} #{ApplicationHelper::JOB_STATE_BADGE_CSS[tone]}")
    end

    # An EventState::Result in words. The _html key lets the <time> elements
    # through; translate escapes the handler, pattern and error.
    def pgbus_event_reason(result)
      translate("pgbus.events.list.reasons.#{result.reason_key}_html", **pgbus_reason_args(result.reason_args))
    end

    def pgbus_processed_event_badge(result)
      tag.span(t("pgbus.events.processed.states.#{result.state}"),
               class: "#{ApplicationHelper::BATCH_BADGE_BASE} #{ApplicationHelper::JOB_STATE_BADGE_CSS[result.badge_tone]}")
    end

    def pgbus_processed_event_reason(result)
      translate("pgbus.events.processed.reasons.#{result.reason_key}_html", **pgbus_reason_args(result.reason_args))
    end

    # The routing key from the raw message: PayloadFilter's default /_key$/
    # pattern would mask it in the parsed (filtered) payload.
    def pgbus_event_routing_key(message)
      payload = message.is_a?(String) ? JSON.parse(message) : message
      return "—" unless payload.is_a?(Hash)

      headers = payload["headers"]
      (headers["routing_key"].presence if headers.is_a?(Hash)) || payload["routing_key"].presence || "—"
    rescue JSON::ParserError, TypeError
      "—"
    end
  end
end
