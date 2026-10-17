# frozen_string_literal: true

require "time"

module Pgbus
  module Web
    # The window the workflow insights read (issue #549): 1 h, 24 h (the
    # default), 7 d or 30 d, never longer than config.workflow_retention
    # (finished and cancelled runs older than that are gone, so a longer
    # window would undercount). Each length has its chart bucket, at most 30
    # per window: 5 min, 1 h, 6 h, 1 d; a capped window takes the bucket of
    # the shortest choice that covers it. Anything else raises Invalid (400
    # on the page, an error on the MCP tool).
    class WorkflowWindow
      class Invalid < StandardError; end

      # name => [seconds, bucket seconds]
      CHOICES = Ractor.make_shareable({ "1h" => [3_600, 300], "24h" => [86_400, 3_600], "7d" => [604_800, 21_600],
                                        "30d" => [2_592_000, 86_400] })
      DEFAULT = "24h"

      attr_reader :name, :seconds, :bucket_seconds

      def self.parse(value, retention: Pgbus.configuration.workflow_retention)
        name = value.nil? || value == "" ? DEFAULT : value
        raise Invalid, "unknown window #{value.inspect} (one of #{CHOICES.keys.join(", ")})" unless name.is_a?(String) && CHOICES.key?(name)

        new(name, retention: retention)
      end

      def initialize(name, retention:)
        @name = name
        requested = CHOICES.fetch(name).first
        @seconds = retention ? [requested, retention.to_i].min : requested
        @capped = @seconds < requested
        @bucket_seconds = CHOICES.values.find { |length, _| length >= @seconds }.last
        freeze
      end

      def capped? = @capped

      def since(now = Time.now) = now - seconds

      def to_h = { name: name, seconds: seconds, bucket_seconds: bucket_seconds, capped: capped? }
    end
  end
end
