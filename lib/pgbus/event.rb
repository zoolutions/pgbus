# frozen_string_literal: true

require "time"

module Pgbus
  class Event
    # +context+ is the publisher's persisted ActiveSupport::CurrentAttributes
    # (issue #431) in their stored form — { "Current" => serialized attrs } —
    # or nil. Handlers normally just read +Current+ (it is restored around
    # +handle+); the raw form is here for tests and explicit access.
    attr_reader :event_id, :payload, :published_at, :routing_key, :headers, :context

    def initialize(event_id:, payload:, published_at: nil, routing_key: nil, headers: nil, context: nil)
      @event_id = event_id
      @payload = payload
      @published_at = published_at || Time.now.utc
      @routing_key = routing_key
      @headers = headers || {}
      @context = context
    end

    def [](key)
      payload.is_a?(Hash) ? payload[key.to_s] : nil
    end

    # Nested counterpart of #[]: the first key is read as a String, later
    # Symbol keys too (payloads are JSON-decoded); Integer indexes are kept.
    # A non-Hash payload gives nil; an intermediate value without #dig raises
    # TypeError, as Hash#dig does.
    def dig(key, *rest)
      return nil unless payload.is_a?(Hash)

      payload.dig(key.to_s, *rest.map { |k| k.is_a?(Symbol) ? k.to_s : k })
    end

    def to_h
      {
        "event_id" => event_id,
        "payload" => payload,
        "published_at" => published_at.iso8601(6),
        "routing_key" => routing_key,
        "headers" => headers
      }
    end
  end
end
