# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddDeployTasksGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add deploy tasks tables (one-time jobs run after a rollover) to an existing Pgbus installation"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_deploy_tasks_tables.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_deploy_tasks_tables.rb")
      end

      def create_deploy_tasks_config
        template "deploy_tasks.yml.erb", "config/deploy_tasks.yml"
      end

      def display_post_install
        say ""
        say "Pgbus deploy tasks installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Edit config/deploy_tasks.yml to declare your one-time tasks"
        say "  3. Make sure every pgbus process knows the app revision (config.app_revision,"
        say "     PGBUS_REVISION, DASH_VERSION, KAMAL_VERSION or a REVISION file)"
        say "  4. Restart pgbus: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
