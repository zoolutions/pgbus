# frozen_string_literal: true

module Pgbus
  class Workflow
    # Which signal a `wait_for:` step takes:
    #
    #   step :capture, wait_for: :payment_confirmed do ... end
    #   step :ship, wait_for: Pgbus::Workflow::SignalMatcher.new(:shipped) { |payload| payload[:region] == "eu" }
    #   step :ship, wait_for: EuShipment.new          # any object answering matches?(signal)
    #
    # A matcher runs under the workflow row lock (when the step reaches its
    # gate, and when signal! looks for a waiting step), so it must be fast
    # and free of side effects. It gets a Pgbus::WorkflowSignal whose
    # +payload+ has indifferent access.
    class SignalMatcher
      attr_reader :signal_name, :condition

      # A name becomes a matcher; a matcher, or anything answering matches?,
      # is kept as it is.
      def self.coerce(value)
        return value if value.respond_to?(:matches?)
        return new(value) if (value.is_a?(Symbol) || value.is_a?(String)) && !value.to_s.strip.empty?

        raise ArgumentError, "wait_for: takes a signal name, a Pgbus::Workflow::SignalMatcher or an object " \
                             "answering matches?(signal), got #{value.inspect}"
      end

      # What the step row's wait_for column shows: the signal name, else the
      # matcher's class.
      def self.label(matcher)
        return matcher.signal_name.to_s if matcher.respond_to?(:signal_name) && matcher.signal_name

        matcher.class.name.to_s
      end

      def initialize(name, &condition)
        raise ArgumentError, "a SignalMatcher needs a signal name, got #{name.inspect}" if name.to_s.strip.empty?

        @signal_name = -name.to_s
        @condition = condition
        freeze
      end

      def matches?(signal)
        return false unless signal.name.to_s == signal_name

        condition.nil? || (condition.call(signal.payload) ? true : false)
      end

      def to_s = signal_name
    end
  end
end
