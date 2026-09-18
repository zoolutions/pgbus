# frozen_string_literal: true

require "time"

module Pgbus
  module EventBus
    module Publisher
      module_function

      def publish(routing_key, payload, headers: nil, delay: 0)
        event_data = build_event_data(payload, routing_key: routing_key)
        event_data = tag_fair_share(event_data, payload, routing_key: routing_key, headers: headers)
        event_data = tag_current(event_data)

        if defined?(Pgbus::Testing) && !Pgbus::Testing.disabled?
          event = Pgbus::Event.new(
            event_id: event_data["event_id"],
            payload: event_data["payload"],
            published_at: event_data["published_at"] ? Time.parse(event_data["published_at"]) : nil,
            routing_key: routing_key,
            headers: headers,
            context: event_data[Pgbus::CurrentAttributes::METADATA_KEY]
          )

          Pgbus::Testing.store.push_event(event)

          if Pgbus::Testing.inline? && delay.to_i <= 0
            Pgbus::EventBus::Registry.instance.subscribers_matching(routing_key).each do |subscriber|
              Pgbus::CurrentAttributes.restore(event.context) { subscriber.handler_class.new.handle(event) }
            end
          end

          return event_data
        end

        Pgbus.client.publish_to_topic(routing_key, event_data, headers: headers, delay: delay)
      end

      def publish_later(routing_key, payload, delay:, headers: nil)
        publish(routing_key, payload, headers: headers, delay: delay)
      end

      # Fair share for consumers (issue #427): when config.event_fair_share is
      # set, hand the callable a Pgbus::Event (routing key, the payload object
      # as passed to publish — not its serialized form — and headers) and merge
      # the resolved key/weight into the envelope. Shared with Outbox.publish_event
      # so the key is resolved where the publisher's context (Current.*) exists
      # and rides the outbox row to the bus. Returns event_data itself when off.
      def tag_fair_share(event_data, payload, routing_key:, headers:)
        return event_data unless FairShare.event_enabled?

        event = Pgbus::Event.new(
          event_id: event_data["event_id"],
          payload: payload,
          routing_key: routing_key,
          headers: headers
        )
        FairShare.inject_event_metadata(event, event_data)
      end

      # Current attributes publish → handler (issue #431): when
      # config.current_attributes is set, snapshot the publisher's persisted
      # Current classes (Pgbus::CurrentAttributes.capture — same filters,
      # serialization and allowlist gating as jobs) into the envelope under
      # the same +pgbus_current+ key jobs use. Shared with Outbox.publish_event
      # so the capture happens where Current is set, not at relay time.
      # Returns event_data itself when off or nothing is assigned.
      def tag_current(event_data)
        captured = Pgbus::CurrentAttributes.capture
        return event_data unless captured

        event_data.merge(Pgbus::CurrentAttributes::METADATA_KEY => captured)
      end

      def build_event_data(payload, routing_key: nil)
        event_id = SecureRandom.uuid

        serialized_payload = if payload.respond_to?(:to_global_id)
                               { "_global_id" => payload.to_global_id.to_s }
                             elsif payload.is_a?(Hash)
                               payload
                             else
                               { "value" => payload }
                             end

        data = {
          "event_id" => event_id,
          "payload" => serialized_payload,
          "published_at" => Time.now.utc.iso8601(6)
        }
        data["routing_key"] = routing_key if routing_key
        data
      end
    end
  end
end
