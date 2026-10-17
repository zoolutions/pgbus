# frozen_string_literal: true

module Pgbus
  class Workflow
    # One declared step: its name, its body (a block or the name of an
    # instance method) and its options. Built and validated when the class
    # body runs, so a bad option fails at load, not on the first run.
    class StepDefinition
      OPTIONS = %i[wait skip_if job_options timeout on_exception wait_for wait_timeout on_timeout].freeze
      # What a wait_for: step does when its wait_timeout: passes first.
      ON_TIMEOUT = %i[pause! cancel! skip! run].freeze
      # The StepJob options a class (set_step_job_options) or a step
      # (job_options:) may set. Anything else raises at class load.
      JOB_OPTIONS = %i[queue priority wait wait_until].freeze

      attr_reader :name, :body, :wait, :skip_if, :job_options, :timeout, :exception_policies,
                  :wait_for, :wait_for_name, :wait_timeout, :on_timeout

      def self.validate_job_options!(options)
        raise ArgumentError, "job_options: must be a Hash, got #{options.inspect}" unless options.is_a?(Hash)

        options.to_h do |key, value|
          key = key.to_sym
          unless JOB_OPTIONS.include?(key)
            raise ArgumentError,
                  "unknown job_options: #{key.inspect} — accepted: #{JOB_OPTIONS.map(&:inspect).join(", ")}"
          end

          [key, job_option_value(key, value)]
        end.freeze
      end

      def self.job_option_value(key, value)
        case key
        when :queue
          raise ArgumentError, "job_options: queue must be a queue name, got #{value.inspect}" if value.to_s.strip.empty?

          -value.to_s
        when :priority
          return value if value.is_a?(Integer)

          raise ArgumentError, "job_options: priority must be an Integer, got #{value.inspect}"
        when :wait then seconds(value, "job_options: wait", positive: false)
        when :wait_until
          # Time, DateTime and TimeWithZone; not a String, which ActiveSupport
          # also gives #to_time (nil for an unparseable one).
          return value.to_time if value.respond_to?(:to_time) && !value.is_a?(String) && value.to_time

          raise ArgumentError, "job_options: wait_until must be a Time, got #{value.inspect}"
        else value
        end
      end

      # Seconds as an Integer when whole, else a Float. Durations are
      # accepted; nil stays nil.
      def self.seconds(value, option, positive:)
        return nil if value.nil?

        seconds = duration?(value) ? value.to_f : value
        unless seconds.is_a?(Numeric) && seconds.real? && seconds.finite? && (positive ? seconds.positive? : !seconds.negative?)
          requirement = positive ? "a positive" : "a non-negative"
          raise ArgumentError, "#{option}: must be #{requirement} number of seconds or a Duration, got #{value.inspect}"
        end

        seconds == seconds.to_i ? seconds.to_i : seconds.to_f
      end

      def self.duration?(value)
        defined?(ActiveSupport::Duration) && value.is_a?(ActiveSupport::Duration)
      end

      def initialize(name:, body:, wait: nil, skip_if: nil, job_options: {}, timeout: nil, on_exception: nil,
                     wait_for: nil, wait_timeout: nil, on_timeout: nil)
        @name = -name.to_s
        @body = body
        @wait = self.class.seconds(wait, "wait", positive: false) || 0
        @skip_if = validate_skip_if(skip_if)
        @job_options = self.class.validate_job_options!(job_options.nil? ? {} : job_options)
        @timeout = self.class.seconds(timeout, "timeout", positive: true)
        @exception_policies = ExceptionPolicy.coerce_all(on_exception)
        assign_wait(wait_for, wait_timeout, on_timeout)
        freeze
      end

      def kind = "step"

      # True for a wait_for: step: it parks until a matching signal arrives.
      def waits_for_signal? = !wait_for.nil?

      def removed? = false

      # Runs the body on +workflow+; the return value is ignored.
      def invoke(workflow)
        body.is_a?(Symbol) ? workflow.send(body) : workflow.instance_exec(&body)
        nil
      end

      def skip?(workflow)
        return false if skip_if.nil?

        result = skip_if.is_a?(Symbol) ? workflow.send(skip_if) : workflow.instance_exec(&skip_if)
        result ? true : false
      end

      private

      def assign_wait(wait_for, wait_timeout, on_timeout)
        @wait_for = wait_for.nil? ? nil : SignalMatcher.coerce(wait_for)
        @wait_for_name = @wait_for && SignalMatcher.label(@wait_for)
        raise ArgumentError, "on_timeout: needs wait_for: and wait_timeout:" if on_timeout && (@wait_for.nil? || wait_timeout.nil?)
        raise ArgumentError, "wait_timeout: needs wait_for: (it bounds the wait for a signal)" if wait_timeout && @wait_for.nil?

        @wait_timeout = self.class.seconds(wait_timeout, "wait_timeout", positive: true)
        @on_timeout = @wait_timeout && normalize_on_timeout(on_timeout.nil? ? :pause! : on_timeout)
      end

      def normalize_on_timeout(value)
        verb = value.to_s == "run" ? :run : :"#{value.to_s.delete_suffix("!")}!"
        return verb if ON_TIMEOUT.include?(verb)

        raise ArgumentError, "on_timeout: must be one of #{ON_TIMEOUT.map(&:inspect).join(", ")}, got #{value.inspect}"
      end

      def validate_skip_if(value)
        return value if value.nil? || value.is_a?(Symbol) || value.is_a?(Proc)

        raise ArgumentError, "skip_if: must be a lambda, a Proc or a method name, got #{value.inspect}"
      end
    end
  end
end
