# frozen_string_literal: true

module Pgbus
  module Streams
    # Parses an SSE replay cursor from a Rack request. The cursor is the highest
    # PGMQ msg_id the client has already seen; the streamer replays everything
    # strictly greater on (re)connect.
    #
    # Two sources, by precedence:
    #   1. Last-Event-ID header (sent automatically by EventSource on reconnect)
    #   2. ?since= query param (sent by the custom element on first connect,
    #      because EventSource cannot send custom headers on the initial request)
    #
    # Returns 0 when no cursor is present (i.e. "deliver everything from now").
    #
    # A negative Last-Event-ID is not a cursor: ephemeral broadcasts carry a
    # negative `id:` (they have no archive position), and a native EventSource
    # that saw one last echoes it on reconnect. It is skipped, so the
    # element's ?since= (its last durable id) decides (issue #621). A negative
    # ?since= is still rejected: the element never sends one.
    module Cursor
      # PGMQ msg_id is BIGINT — strictly within signed 64-bit range.
      MAX_MSG_ID = (2**63) - 1

      class InvalidCursor < ArgumentError
      end

      # Strict integer pattern: optional leading minus, then digits. No plus prefix,
      # no decimal, no whitespace. Negatives are caught by validate! with a clearer
      # error message than "must be numeric".
      INTEGER_PATTERN = /\A-?\d+\z/

      def self.parse(query_since:, last_event_id:)
        raw = pick(last_event_id, query_since)
        return 0 if raw.nil?

        value = coerce(raw)
        validate!(value)
        value
      end

      def self.pick(last_event_id, query_since)
        return last_event_id if present?(last_event_id) && !ephemeral_id?(last_event_id)
        return query_since   if present?(query_since)

        nil
      end

      def self.present?(value)
        return false if value.nil?
        # Any Integer is "present"; pick skips a negative last_event_id (an
        # ephemeral frame's id) and validate! rejects a negative ?since=.
        return true if value.is_a?(Integer)

        !value.to_s.strip.empty?
      end

      # An ephemeral frame's id: a negative Integer, or its decimal text.
      def self.ephemeral_id?(value)
        return value.negative? if value.is_a?(Integer)

        value.to_s.strip.match?(/\A-\d+\z/)
      end

      def self.coerce(raw)
        return raw if raw.is_a?(Integer)

        str = raw.to_s
        raise InvalidCursor, "cursor must be numeric (got #{raw.inspect})" unless str.match?(INTEGER_PATTERN)

        Integer(str, 10)
      end

      def self.validate!(value)
        raise InvalidCursor, "cursor must not be negative (got #{value})" if value.negative?
        raise InvalidCursor, "cursor #{value} is out of BIGINT range" if value > MAX_MSG_ID
      end

      private_class_method :pick, :present?, :ephemeral_id?, :coerce, :validate!
    end
  end
end
