# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddPresenceSeatsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add per-connection seats (connection_id, seat, state) to pgbus_presence_members"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_presence_seats.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_presence_seats.rb")
      end

      def display_post_install
        say ""
        say "Pgbus presence seats installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart the app: connection-driven presence (streams_presence_patterns)"
        say "     then keeps one row per SSE connection, expired after streams_presence_ttl"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
