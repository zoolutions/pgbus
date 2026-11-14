# frozen_string_literal: true

module Pgbus
  module Deploy
    # One declared deploy task (issue #536): an entry of
    # config/deploy_tasks.yml. The sibling of Recurring::Task without a
    # schedule: it runs once per (key, version) after a rollover, or after
    # every rollover when every_deploy is set. after: names the tasks it
    # waits for (issue #660): see Deploy::Dependencies.
    class Task
      DEFAULT_VERSION = "1"

      attr_reader :key, :class_name, :command, :version, :every_deploy, :queue_name,
                  :arguments, :priority, :description, :dependencies

      def self.from_configuration(key, **options)
        options = options.transform_keys(&:to_sym)
        new(
          key: key.to_s,
          class_name: options[:class],
          command: options[:command],
          version: options.fetch(:version, DEFAULT_VERSION),
          explicit_version: options.key?(:version),
          every_deploy: options.fetch(:every_deploy, false),
          queue_name: options[:queue],
          arguments: options[:args].is_a?(Hash) ? [options[:args]] : Array(options[:args]),
          priority: options.fetch(:priority, 0).to_i,
          description: options[:description],
          dependencies: options[:after]
        )
      end

      def initialize(key:, class_name: nil, command: nil, version: DEFAULT_VERSION, explicit_version: false,
                     every_deploy: false, queue_name: nil, arguments: [], priority: 0, description: nil,
                     dependencies: nil)
        @key = key
        @class_name = class_name
        @command = command
        @version = version.to_s.strip
        @explicit_version = explicit_version
        @every_deploy = every_deploy
        @queue_name = queue_name
        @arguments = arguments || []
        @priority = priority || 0
        @description = description
        @dependencies = dependency_list(dependencies)
        @errors = []
        @warnings = []
      end

      def valid?
        @errors = []
        @warnings = []
        validate!
        @errors.empty?
      end

      def errors
        valid? unless @validated
        @errors
      end

      def warnings
        valid? unless @validated
        @warnings
      end

      # What an arm for this task is stamped with: its version, or the
      # revision it runs on for an every_deploy task.
      def armed_for(revision)
        every_deploy ? revision : version
      end

      def job_class
        class_name&.safe_constantize
      end

      def to_h
        {
          key: key, class_name: class_name, command: command, version: version, every_deploy: every_deploy,
          queue_name: queue_name, arguments: arguments, priority: priority, description: description,
          dependencies: dependencies
        }
      end

      private

      def validate!
        @validated = true
        @errors << "Key must not be blank" if key.to_s.strip.empty?
        validate_target!
        @errors << "Version must not be blank" if version.empty?
        @errors << "every_deploy must be true or false" unless [true, false].include?(every_deploy)
        validate_dependencies!
        return unless every_deploy == true && @explicit_version

        @warnings << "every_deploy is set, so version is ignored (it runs after every rollover)"
      end

      def validate_dependencies!
        @errors << "after: must name task keys" unless dependencies.all? { |dep| dep.is_a?(String) && !dep.strip.empty? }
        @errors << "after: cycle #{key} \u2192 #{key}" if dependencies.include?(key)
      end

      # nil, one key, or a list; a Symbol reads as its name. Anything else is
      # kept as given, for validate_dependencies! to reject.
      def dependency_list(value)
        list = value.is_a?(Array) ? value : [value].compact
        list.map { |dep| dep.is_a?(Symbol) ? dep.to_s : dep }.uniq.freeze
      end

      def validate_target!
        { class: class_name, command: command }.each do |name, value|
          @errors << "#{name} must be a String" unless value.nil? || value.is_a?(String)
        end
        has_class = !class_name.to_s.strip.empty?
        has_command = !command.to_s.strip.empty?
        @errors << "Either class or command is required" unless has_class || has_command
        @errors << "Declare either class or command, not both" if has_class && has_command
      end
    end
  end
end
