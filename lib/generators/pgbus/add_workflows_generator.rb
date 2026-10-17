# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddWorkflowsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add the workflow tables (pgbus_workflows, pgbus_workflow_steps, pgbus_workflow_events) " \
           "to an existing Pgbus installation"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_workflows.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_workflows.rb")
      end

      def display_post_install
        say ""
        say "Pgbus workflows installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
        say "Define a workflow:"
        say "  class SignupWorkflow < Pgbus::Workflow"
        say "    step :welcome do"
        say "      WelcomeMailer.with(user: subject).welcome.deliver_later"
        say "    end"
        say "  end"
        say ""
        say "  SignupWorkflow.start!(user)"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
