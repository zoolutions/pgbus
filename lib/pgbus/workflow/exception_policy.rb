# frozen_string_literal: true

module Pgbus
  class Workflow
    # What happens when a step body raises (issue #540). A policy matches an
    # exception (by class, class name, or a list of either; none = every
    # StandardError) and names an action:
    #
    #   on_exception Net::OpenTimeout, action: :reattempt!, wait: 15.seconds, max_reattempts: 5
    #   on_exception PaymentDeclined, action: :cancel!, report: :never
    #   on_exception action: :pause!                        # blanket
    #   on_exception Timeout::Error do |error|              # imperative: must call a verb
    #     error.message.include?("read") ? reattempt!(wait: 5) : skip!
    #   end
    #
    # Resolution: the step's own policies, then the class's specific ones,
    # then the class's blanket ones, then the default pause!. The error is
    # recorded on the step row either way; +report+ decides whether it also
    # goes to Pgbus::ErrorReporter (:always, :never, or :terminal_only: only
    # when the action ends the retrying). max_reattempts counts consecutive
    # reattempted attempts of the same step; once spent, terminal_action
    # (default pause!) applies.
    class ExceptionPolicy
      ACTIONS = %i[pause! reattempt! cancel! skip!].freeze
      TERMINAL_ACTIONS = %i[pause! cancel! skip!].freeze
      REPORTS = %i[always never terminal_only].freeze

      # +outcome+ is a FlowControl::Outcome; +report+ whether ErrorReporter
      # hears of it; +handler_error+ is what an imperative block raised.
      Decision = Data.define(:outcome, :report, :terminal, :handler_error)

      attr_reader :action, :matching, :wait, :max_reattempts, :terminal_action, :report, :handler

      class << self
        # The step-level `on_exception:` option: a policy, an action
        # (`:reattempt!`), a Hash of policy options, or an Array of those.
        def coerce_all(value)
          Array(value.is_a?(Hash) ? [value] : value).map { |entry| coerce(entry) }.freeze
        end

        def coerce(value)
          case value
          when ExceptionPolicy then value
          when Symbol, String then new(value.to_sym)
          when Hash
            options = value.transform_keys(&:to_sym)
            new(options.delete(:action), **options)
          else
            raise ArgumentError, "on_exception: takes a Pgbus::Workflow::ExceptionPolicy, an action, a Hash or an Array, " \
                                 "got #{value.inspect}"
          end
        end

        def resolve(error, step_policies: [], class_policies: [])
          step_policies.find { |policy| policy.matches?(error) } ||
            class_policies.find { |policy| policy.specific? && policy.matches?(error) } ||
            class_policies.find { |policy| !policy.specific? && policy.matches?(error) } ||
            DEFAULT
        end
      end

      def initialize(action = nil, matching: nil, wait: nil, max_reattempts: nil, terminal_action: nil, report: :always, &handler)
        raise ArgumentError, "on_exception takes action: or a block, not both" if action && handler
        raise ArgumentError, "on_exception needs action: (#{ACTIONS.join(", ")}) or a block" unless action || handler

        @action = action && normalize_action(action, ACTIONS, "action")
        reattempt_only!(wait: wait, terminal_action: terminal_action)
        @matching = normalize_matching(matching)
        @wait = StepDefinition.seconds(wait, "wait", positive: false)
        @max_reattempts = validate_max_reattempts(max_reattempts)
        @terminal_action = normalize_action(terminal_action.nil? ? :pause! : terminal_action, TERMINAL_ACTIONS, "terminal_action")
        @report = validate_report(report)
        @handler = handler
        freeze
      end

      def specific? = !matching.nil?

      def matches?(error)
        return error.is_a?(StandardError) if matching.nil?

        matching.any? do |matcher|
          matcher.is_a?(String) ? error.class.ancestors.any? { |ancestor| ancestor.name == matcher } : error.is_a?(matcher)
        end
      end

      # +consecutive_reattempts+ is a callable (it reads the step rows), only
      # called when max_reattempts needs it.
      def decide(error, workflow:, consecutive_reattempts: -> { 0 })
        return decide_with_handler(error, workflow) if handler
        return decide_reattempt(consecutive_reattempts) if action == :reattempt!

        Decision.new(outcome: FlowControl::Outcome.for(action), report: report != :never, terminal: true, handler_error: nil)
      end

      private

      def decide_reattempt(consecutive_reattempts)
        if max_reattempts && consecutive_reattempts.call >= max_reattempts
          return Decision.new(outcome: FlowControl::Outcome.for(terminal_action), report: report != :never,
                              terminal: true, handler_error: nil)
        end

        Decision.new(outcome: FlowControl::Outcome.reattempt(wait: wait), report: report == :always,
                     terminal: false, handler_error: nil)
      end

      # The block runs on the workflow instance and must call a verb; one
      # that returns without calling any pauses the workflow. A block that
      # raises pauses it too, and its own error is handed back for reporting.
      def decide_with_handler(error, workflow)
        outcome = catch(FlowControl::THROW_TAG) do
          workflow.instance_exec(error, &handler)
          nil
        end
        terminal = !outcome&.reattempt?
        Decision.new(outcome: outcome || FlowControl::Outcome.pause,
                     report: report == :always || (report == :terminal_only && terminal),
                     terminal: terminal, handler_error: nil)
      rescue StandardError => e
        Decision.new(outcome: FlowControl::Outcome.pause, report: true, terminal: true, handler_error: e)
      end

      def normalize_action(value, allowed, option)
        verb = :"#{value.to_s.delete_suffix("!")}!"
        return verb if allowed.include?(verb)

        raise ArgumentError, "#{option}: must be one of #{allowed.map(&:inspect).join(", ")}, got #{value.inspect}"
      end

      def normalize_matching(matching)
        return nil if matching.nil? || (matching.respond_to?(:empty?) && matching.empty?)

        Array(matching).map do |matcher|
          next -matcher.to_s if matcher.is_a?(String) || matcher.is_a?(Symbol)
          next matcher if matcher.is_a?(Module)

          raise ArgumentError, "matching: takes exception classes or class names, got #{matcher.inspect}"
        end.freeze
      end

      # wait: and terminal_action: shape a reattempt!; anywhere else (another
      # action, an imperative block, which passes its own wait) they would be
      # silently ignored.
      def reattempt_only!(**options)
        given = options.compact.keys
        return if given.empty? || action == :reattempt!

        raise ArgumentError, "#{given.map { |key| "#{key}:" }.join(" and ")} only applies to action: :reattempt!"
      end

      def validate_max_reattempts(value)
        return nil if value.nil?
        raise ArgumentError, "max_reattempts: only applies to action: :reattempt!" unless action == :reattempt!
        return value if value.is_a?(Integer) && value.positive?

        raise ArgumentError, "max_reattempts: must be a positive Integer, got #{value.inspect}"
      end

      def validate_report(value)
        return value if REPORTS.include?(value)

        raise ArgumentError, "report: must be one of #{REPORTS.map(&:inspect).join(", ")}, got #{value.inspect}"
      end
    end

    # What applies when no policy matches: pause!, reported.
    ExceptionPolicy::DEFAULT = ExceptionPolicy.new(:pause!)
  end
end
