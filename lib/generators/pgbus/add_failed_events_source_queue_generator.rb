# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddFailedEventsSourceQueueGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add source_queue to pgbus_failed_events so each priority sub-queue's failure keeps its own row"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_failed_events_source_queue.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_failed_events_source_queue.rb")
      end

      def display_post_install
        say ""
        say "Pgbus failed events source_queue added!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus processes so they record failures per physical queue"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
