# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddBatchOwnerGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add owner_process_id to pgbus_batches, so a batch can be built by a watched process (issue #683)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_batch_owner.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_batch_owner.rb")
      end

      def display_post_install
        say ""
        say "Pgbus batch owner column installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
        say "A Pgbus.batch opened in a pgbus process (a worker, consumer, dispatcher or"
        say "scheduler) is now built: its perform_later calls are held and sent together"
        say "when the block returns."
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
