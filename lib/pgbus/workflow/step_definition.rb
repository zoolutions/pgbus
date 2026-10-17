# frozen_string_literal: true

module Pgbus
  class Workflow
    # One declared step: its name, its body (a block or the name of an
    # instance method) and its options. Built and validated when the class
    # body runs, so a bad option fails at load, not on the first run.
    class StepDefinition
      OPTIONS = %i[wait skip_if job_options timeout on_exception].freeze
      # The StepJob options a class (set_step_job_options) or a step
      # (job_options:) may set. Anything else raises at class load.
      JOB_OPTIONS = %i[queue priority wait wait_until].freeze

      attr_reader :name, :body, :wait, :skip_if, :job_options, :timeout, :exception_policies

      def self.validate_job_options!(options)
        raise ArgumentError, "job_options: must be a Hash, got #{options.inspect}" unless options.is_a?(Hash)

        options.to_h do |key, value|
          key = key.to_sym
          unless JOB_OPTIONS.include?(key)
            raise ArgumentError,
                  "unknown job_options: #{key.inspect} — accepted: #{JOB_OPTIONS.map(&:inspect).join(", ")}"
          end

          [key, key == :queue ? value.to_s : value]
        end.freeze
      end

      # Seconds as an Integer when whole, else a Float. Durations are
      # accepted; nil stays nil.
      def self.seconds(value, option, positive:)
        return nil if value.nil?

        seconds = duration?(value) ? value.to_f : value
        unless seconds.is_a?(Numeric) && seconds.real? && (positive ? seconds.positive? : !seconds.negative?)
          requirement = positive ? "a positive" : "a non-negative"
          raise ArgumentError, "#{option}: must be #{requirement} number of seconds or a Duration, got #{value.inspect}"
        end

        seconds == seconds.to_i ? seconds.to_i : seconds.to_f
      end

      def self.duration?(value)
        defined?(ActiveSupport::Duration) && value.is_a?(ActiveSupport::Duration)
      end

      def initialize(name:, body:, wait: nil, skip_if: nil, job_options: {}, timeout: nil, on_exception: nil)
        @name = name.to_s
        @body = body
        @wait = self.class.seconds(wait, "wait", positive: false) || 0
        @skip_if = validate_skip_if(skip_if)
        @job_options = self.class.validate_job_options!(job_options || {})
        @timeout = self.class.seconds(timeout, "timeout", positive: true)
        @exception_policies = ExceptionPolicy.coerce_all(on_exception)
        freeze
      end

      def kind = "step"

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

      def validate_skip_if(value)
        return value if value.nil? || value.is_a?(Symbol) || value.is_a?(Proc)

        raise ArgumentError, "skip_if: must be a lambda, a Proc or a method name, got #{value.inspect}"
      end
    end
  end
end
