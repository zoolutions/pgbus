# frozen_string_literal: true

module Pgbus
  class Configuration
    # Seat expiry and the change hook for connection-driven presence
    # (issue #515). EXPERIMENTAL, like the other streams_presence_* settings.
    #
    #   config.streams_presence_ttl = 60                                   # every stream
    #   config.streams_presence_ttl = { /^doc:/ => 5.minutes, default: 60 } # per pattern
    #   config.streams_presence_on_change = ->(stream_name, change) { ... }
    #
    # `change` is { type: :join | :leave, connection_id:, seat:, member_id: }.
    # An expired seat is reported as a :leave.
    module PresenceSettings
      DEFAULT_PRESENCE_TTL = 60

      attr_accessor :streams_presence_on_change
      attr_writer :streams_presence_ttl

      def streams_presence_ttl
        @streams_presence_ttl.nil? ? DEFAULT_PRESENCE_TTL : @streams_presence_ttl
      end

      # Seconds a seat may stay silent on `stream_name` before the heartbeat
      # expires it. A pattern matches like streams_presence_patterns (exact
      # String or Regexp); the first match wins.
      def presence_ttl_for(stream_name)
        ttl = streams_presence_ttl
        return ttl.to_i unless ttl.is_a?(Hash)

        match = ttl.find { |pattern, _| pattern_matches?(pattern, stream_name) }
        (match ? match.last : ttl.fetch(:default, DEFAULT_PRESENCE_TTL)).to_i
      end

      # The smallest ttl configured: the expiry scan's candidate cutoff.
      def presence_min_ttl
        ttl = streams_presence_ttl
        return ttl.to_i unless ttl.is_a?(Hash)

        [*ttl.values, ttl.fetch(:default, DEFAULT_PRESENCE_TTL)].map(&:to_i).min
      end

      # Reports one join, leave or expiry to streams_presence_on_change.
      # A raising hook is logged and swallowed, like the rest of the
      # presence path: it must not break a connection or a sweep.
      def notify_presence_change(stream_name, change)
        hook = streams_presence_on_change
        return unless hook

        hook.call(stream_name, change)
      rescue StandardError => e
        Pgbus.logger.error do
          "[Pgbus] streams_presence_on_change raised for #{stream_name}: #{e.class}: #{e.message}"
        end
      end

      private

      def validate_presence_settings!
        if !streams_presence_on_change.nil? && !streams_presence_on_change.respond_to?(:call)
          raise Pgbus::ConfigurationError,
                "streams_presence_on_change must respond to #call (a Proc/lambda) or be nil"
        end

        ttl = streams_presence_ttl
        values = ttl.is_a?(Hash) ? ttl.values : [ttl]
        return if values.all? { |v| v.is_a?(Numeric) && v.to_i.positive? }

        raise Pgbus::ConfigurationError,
              "streams_presence_ttl must be a positive number of seconds or a Hash of pattern => seconds"
      end

      def pattern_matches?(pattern, stream_name)
        case pattern
        when Regexp then pattern.match?(stream_name)
        when :default then false
        else pattern.to_s == stream_name
        end
      end
    end
  end
end
