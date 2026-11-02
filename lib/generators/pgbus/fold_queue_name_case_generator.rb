# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    # Issue #633: PGMQ lower-cases its table names, so a queue pgbus created
    # as "pgbus_Orders" before 0.23.0 lives in pgmq.q_pgbus_orders while
    # pgmq.meta records the mixed-case spelling. pgbus now spells every queue
    # in lower case; this migration brings an install's existing rows to that
    # spelling. No table is dropped, renamed or copied.
    class FoldQueueNameCaseGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Spell existing PGMQ queues (pgmq.meta, NOTIFY triggers, pgbus's queue-name columns) in lower case, " \
           "the way their tables are"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "fold_queue_name_case.rb.erb",
                           File.join(pgbus_migrate_path, "fold_pgbus_queue_name_case.rb")
      end

      def display_post_install
        say ""
        say "Pgbus queue-name fold migration created!", :green
        say ""
        say "PGMQ spells every queue table in lower case. This migration renames the"
        say "pgmq.meta rows (and NOTIFY triggers) of queues created with upper-case"
        say "letters to the lower-case spelling their tables already use. Messages stay"
        say "where they are."
        say ""
        say "Next steps:"
        say "  1. Deploy pgbus 0.23.0+ everywhere first: an older process recreates its mixed-case row"
        say "  2. Run: rails db:migrate#{migrate_command_suffix}"
        say "  3. Check: rake pgbus:doctor (\"Queue name case\" is ok)"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
