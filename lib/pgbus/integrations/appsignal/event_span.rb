# frozen_string_literal: true

require "pgbus/integrations/appsignal/span_subscriber"

module Pgbus
  module Integrations
    module Appsignal
      # The `<Handler>#handle` transaction around pgbus.event_processed, and
      # the fallback for an error pgbus.event_failed reports from outside it
      # (an event that could not be built never reaches the span).
      class EventSpan < SpanSubscriber
        private

        def frame_key = :pgbus_appsignal_event_span

        def describe(transaction, payload)
          transaction.set_action_if_nil("#{payload[:handler] || "UnknownHandler"}#handle")
          apply_queue_start(transaction, payload[:published_at])
          transaction.add_tags(handler_tags(payload))
        end

        # The processed count is taken at finish whether or not the handler
        # raised, as Metrics::Subscriber does: a raised handler counts as
        # processed and, through pgbus.event_failed, as failed.
        def finished(payload, duration)
          tags = { handler: payload[:handler], routing_key: payload[:routing_key] }
          ::Appsignal.add_distribution_value("pgbus_event_duration_ms", duration, tags)
          ::Appsignal.increment_counter("pgbus_event_count", 1, tags.merge(status: "processed"))
        end

        def handler_tags(payload)
          {
            "handler" => payload[:handler],
            "routing_key" => payload[:routing_key],
            "attempts" => payload[:read_ct]
          }.compact
        end
      end
    end
  end
end
