# frozen_string_literal: true

module Pgbus
  class Configuration
    # Deploy tasks (issue #536): one-time jobs the scheduler enqueues once
    # every live pgbus process runs the new app revision.
    #
    #   config.app_revision = -> { File.read("REVISION").strip } # or a String
    #   config.deploy_tasks_files = ["config/deploy_tasks.yml"]
    #   config.deploy_task_run_retention = 30.days
    module DeploySettings
      DEFAULT_DEPLOY_TASK_RUN_RETENTION = 90 * 24 * 3600

      attr_accessor :app_revision, :deploy_tasks, :deploy_tasks_files, :deploy_tasks_enabled
      attr_reader :deploy_task_run_retention

      def deploy_task_run_retention=(value)
        @deploy_task_run_retention = coerce_duration!(value, :deploy_task_run_retention)
      end

      private

      def initialize_deploy_settings
        @app_revision = nil
        @deploy_tasks = nil
        @deploy_tasks_files = nil
        @deploy_tasks_enabled = true
        @deploy_task_run_retention = DEFAULT_DEPLOY_TASK_RUN_RETENTION
      end

      def validate_deploy_settings!
        unless app_revision.nil? || app_revision.is_a?(String) || app_revision.respond_to?(:call)
          raise Pgbus::ConfigurationError, "app_revision must be a String, respond to #call, or be nil"
        end

        raise Pgbus::ConfigurationError, "deploy_tasks must be a Hash or nil" unless deploy_tasks.nil? || deploy_tasks.is_a?(Hash)
        unless deploy_tasks_files.nil? || deploy_tasks_files.is_a?(Array)
          raise Pgbus::ConfigurationError, "deploy_tasks_files must be an Array of paths or nil"
        end
        return if [true, false].include?(deploy_tasks_enabled)

        raise Pgbus::ConfigurationError, "deploy_tasks_enabled must be true or false"
      end
    end
  end
end
