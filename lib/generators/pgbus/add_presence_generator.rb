# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddPresenceGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add presence_members table for Pgbus::Streams presence tracking"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_presence.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_presence.rb")
      end

      def display_post_install
        say ""
        say "Pgbus presence installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Use in your code:"
        say "       Pgbus.stream(@room).presence.join(member_id: current_user.id.to_s)"
        say "       Pgbus.stream(@room).presence.members"
        say "  3. Connection-driven presence (streams_presence_patterns) expires silent"
        say "     seats on its own. Rows you join by member_id need a periodic sweeper:"
        say "       Pgbus.stream(@room).presence.sweep!(older_than: 60.seconds.ago)"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
