# frozen_string_literal: true

module Pgbus
  class Configuration
    # Workflows (issue #540): durable multi-step Pgbus::Workflow runs.
    #
    #   config.workflow_queue = "workflows"             # where StepJob messages go
    #   config.workflow_retention = 30.days             # finished/cancelled runs kept this long; nil keeps them
    #   config.workflow_sweep_interval = 5.minutes      # how often the dispatcher repairs stuck steps
    #   config.workflow_stall_threshold = 1.hour        # an in_progress step older than this is stalled
    #   config.workflow_stall_action = :pause           # :pause, :reattempt or :cancel a stalled step
    #   config.workflow_max_payload_size = 131_072      # bytes: arguments and step outputs
    #   config.workflow_error_backtrace_lines = 50      # backtrace lines kept on a failed step
    #   config.workflow_waiting_visibility_threshold = 7.days # a step waiting this long without a
    #                                                   # wait_timeout: counts as stuck (waiting_overdue)
    #   config.workflow_subject_url = ->(type, id) { "/admin/#{type.underscore.pluralize}/#{id}" }
    #                                                   # the dashboard's subject link (nil: plain text)
    module WorkflowSettings
      WORKFLOW_STALL_ACTIONS = %i[pause reattempt cancel].freeze

      attr_reader :workflow_queue, :workflow_retention, :workflow_sweep_interval, :workflow_stall_threshold,
                  :workflow_stall_action, :workflow_waiting_visibility_threshold
      attr_accessor :workflow_max_payload_size, :workflow_error_backtrace_lines, :workflow_subject_url

      def workflow_queue=(value)
        name = value.to_s
        raise Pgbus::ConfigurationError, "workflow_queue must be a non-empty queue name" if value.nil? || name.strip.empty?

        @workflow_queue = name
      end

      def workflow_retention=(value)
        @workflow_retention = coerce_duration!(value, :workflow_retention)
      end

      def workflow_sweep_interval=(value)
        @workflow_sweep_interval = coerce_duration!(value, :workflow_sweep_interval)
      end

      def workflow_stall_threshold=(value)
        @workflow_stall_threshold = coerce_duration!(value, :workflow_stall_threshold)
      end

      def workflow_waiting_visibility_threshold=(value)
        @workflow_waiting_visibility_threshold = coerce_duration!(value, :workflow_waiting_visibility_threshold)
      end

      def workflow_stall_action=(value)
        action = value.respond_to?(:to_sym) ? value.to_sym : value
        unless WORKFLOW_STALL_ACTIONS.include?(action)
          raise Pgbus::ConfigurationError,
                "workflow_stall_action must be one of #{WORKFLOW_STALL_ACTIONS.map(&:inspect).join(", ")}, got #{value.inspect}"
        end

        @workflow_stall_action = action
      end

      private

      def initialize_workflow_settings
        @workflow_queue = "default"
        @workflow_retention = 30 * 24 * 3600 # 30 days
        @workflow_sweep_interval = 300 # 5 minutes
        @workflow_stall_threshold = 3600 # 1 hour
        @workflow_stall_action = :pause
        @workflow_max_payload_size = 131_072 # 128 KB
        @workflow_error_backtrace_lines = 50
        @workflow_waiting_visibility_threshold = 7 * 24 * 3600 # 7 days
        @workflow_subject_url = nil
      end

      def validate_workflow_settings!
        { workflow_sweep_interval: workflow_sweep_interval, workflow_stall_threshold: workflow_stall_threshold,
          workflow_waiting_visibility_threshold: workflow_waiting_visibility_threshold,
          workflow_retention: workflow_retention }.each do |name, value|
          next if value.nil? && name == :workflow_retention
          next if value.is_a?(Numeric) && value.positive? && value.finite?

          raise Pgbus::ConfigurationError, "#{name} must be a finite number of seconds > 0"
        end

        begin
          QueueNameValidator.validate!(queue_name(workflow_queue))
        rescue ArgumentError => e
          raise Pgbus::ConfigurationError, "workflow_queue #{workflow_queue.inspect}: #{e.message}"
        end

        { workflow_max_payload_size: workflow_max_payload_size,
          workflow_error_backtrace_lines: workflow_error_backtrace_lines }.each do |name, value|
          raise Pgbus::ConfigurationError, "#{name} must be a positive integer" unless value.is_a?(Integer) && value.positive?
        end

        return if workflow_subject_url.nil? || workflow_subject_url.respond_to?(:call)

        raise Pgbus::ConfigurationError, "workflow_subject_url must respond to #call (a Proc/lambda) or be nil"
      end
    end
  end
end
