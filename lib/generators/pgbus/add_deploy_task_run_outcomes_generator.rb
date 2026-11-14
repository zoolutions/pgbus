# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    # Issue #660: run outcomes (completed_at, failed_at) on
    # pgbus_deploy_task_runs and after: dependencies on pgbus_deploy_tasks,
    # for an install whose deploy task tables predate them.
    class AddDeployTaskRunOutcomesGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add deploy task run outcome columns (completed_at, failed_at) and after: dependencies"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_deploy_task_run_outcomes.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_deploy_task_run_outcomes.rb")
      end

      def display_post_install
        say ""
        say "Pgbus deploy task run outcomes installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Deploy task runs now record completed_at / failed_at"
        say "  3. A deploy task can wait on others with after: in config/deploy_tasks.yml"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
