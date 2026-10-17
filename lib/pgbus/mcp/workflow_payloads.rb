# frozen_string_literal: true

module Pgbus
  module MCP
    # What the workflow tools (issue #542) send back. A run's arguments, its
    # steps' outputs, its signals' payloads and its error text (message,
    # backtrace, cause messages, an event's error_message) can carry
    # application data, and the Redactor only blanks known payload keys
    # holding a leaf value, so these tools gate them explicitly: replaced by
    # Redactor::REDACTED unless BaseTool.payloads_allowed? says otherwise.
    # Times go out as ISO 8601 UTC, symbols as strings, keys as strings.
    module WorkflowPayloads
      REDACTED = Redactor::REDACTED
      STEP_SECRETS = %i[output error_message error_backtrace].freeze

      module_function

      def row(row, allowed:)
        serialize(allowed ? row : row.merge(redact(row, :error_message)))
      end

      def detail(detail, allowed:)
        return serialize(detail.except(:timeline)) if allowed

        gated = detail.except(:timeline).merge(arguments: REDACTED, **redact(detail, :error_message))
        gated[:steps] = Array(detail[:steps]).map { |step| gate_step(step) }
        gated[:signals] = Array(detail[:signals]).map { |signal| signal.merge(payload: REDACTED) }
        gated[:events] = Array(detail[:events]).map { |event| gate_event(event) }
        serialize(gated)
      end

      def group(group, allowed:)
        serialize(allowed ? group : group.merge(redact(group, :error_message)))
      end

      def gate_step(step)
        gated = step.merge(STEP_SECRETS.to_h { |key| [key, step[key].nil? ? nil : REDACTED] })
        gated[:error_causes] = Array(step[:error_causes]).map { |cause| cause.slice("class", :class) }
        gated
      end

      def gate_event(event)
        details = event[:details] || {}
        event.merge(details: details.reject { |key, _| key.to_s == "error_message" })
      end

      # { key => REDACTED } when the value is there, so a nil stays nil.
      def redact(hash, key) = hash[key].nil? ? {} : { key => REDACTED }

      def serialize(value)
        case value
        when Hash then value.to_h { |key, nested| [key.to_s, serialize(nested)] }
        when Array then value.map { |nested| serialize(nested) }
        when Symbol then value.to_s
        when Time, DateTime, ActiveSupport::TimeWithZone then value.utc.iso8601
        else value
        end
      end
    end
  end
end
