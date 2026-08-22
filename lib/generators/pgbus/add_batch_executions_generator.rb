# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddBatchExecutionsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add pgbus_batch_executions and rename batch failure columns (issue #414)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_batch_executions.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_batch_executions.rb")
      end

      def display_post_install
        say ""
        say "Pgbus batch execution-row tracking installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
