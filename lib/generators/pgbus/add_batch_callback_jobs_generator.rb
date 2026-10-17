# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddBatchCallbackJobsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add jsonb callback-job columns to pgbus_batches (issue #415)"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_batch_callback_jobs.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_batch_callback_jobs.rb")
      end

      def display_post_install
        say ""
        say "Pgbus configured batch callbacks installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
        say "You can now pass a configured ActiveJob instance to a batch:"
        say "  Pgbus.batch do |b|"
        say "    b.on_complete ReportJob.new.set(queue: :critical, wait: 5.minutes)"
        say "    b.add(*rows.map { |row| ImportRowJob.new(row.id) })"
        say "  end"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
