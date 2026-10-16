# frozen_string_literal: true

require "concurrent"

module Pgbus
  module Process
    # Thread-safe worker lifecycle state machine inspired by LavinMQ's QueueState.
    #
    # States:
    #   :starting  → initial state, setting up
    #   :running   → actively processing messages
    #   :paused    → temporarily stopped (manual or circuit breaker)
    #   :draining  → finishing in-flight work before stopping
    #   :stopped   → terminal state
    #
    # Transitions:
    #   starting → running
    #   running  → paused | draining | stopped
    #   paused   → running | draining | stopped
    #   draining → stopped
    class Lifecycle
      STATES = %i[starting running paused draining stopped].freeze

      TRANSITIONS = {
        starting: %i[running stopped].freeze,
        running: %i[paused draining stopped].freeze,
        paused: %i[running draining stopped].freeze,
        draining: %i[stopped].freeze,
        stopped: [].freeze
      }.freeze

      attr_reader :state

      def initialize
        @state = :starting
        @mutex = Mutex.new
        @callbacks = Hash.new { |h, k| h[k] = [] }
      end

      def transition_to!(new_state)
        @mutex.synchronize do
          validate_transition!(new_state)
          old_state = @state
          @state = new_state
          fire_callbacks(old_state, new_state)
          new_state
        end
      end

      def transition_to(new_state)
        transition_to!(new_state)
      rescue InvalidTransition
        false
      end

      def on(event, &block)
        @callbacks[event] << block
      end

      def starting?
        @state == :starting
      end

      def running?
        @state == :running
      end

      def paused?
        @state == :paused
      end

      def draining?
        @state == :draining
      end

      def stopped?
        @state == :stopped
      end

      def active?
        running? || paused?
      end

      def can_process?
        running?
      end

      def terminal?
        stopped?
      end

      private

      def validate_transition!(new_state)
        raise ArgumentError, "Unknown state: #{new_state}. Valid states: #{STATES.join(", ")}" unless STATES.include?(new_state)

        return if TRANSITIONS[@state].include?(new_state)

        raise InvalidTransition, "Cannot transition from #{@state} to #{new_state}. " \
                                 "Valid transitions: #{TRANSITIONS[@state].join(", ")}"
      end

      def fire_callbacks(old_state, new_state)
        @callbacks[:"#{old_state}_to_#{new_state}"].each(&:call)
        @callbacks[:any].each { |cb| cb.call(old_state, new_state) }
      end
    end

    class InvalidTransition < Pgbus::Error; end
  end
end
