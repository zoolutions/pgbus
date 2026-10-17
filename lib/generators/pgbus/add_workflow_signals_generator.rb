# frozen_string_literal: true

require "rails/generators"
require "rails/generators/active_record"
require_relative "migration_path"

module Pgbus
  module Generators
    class AddWorkflowSignalsGenerator < Rails::Generators::Base
      include ActiveRecord::Generators::Migration
      include MigrationPath

      source_root File.expand_path("templates", __dir__)

      desc "Add waiting workflow steps (fan_out, wait_for: signals, wait timeouts) to an installation " \
           "that ran pgbus:add_workflows before them: the pgbus_workflow_signals table and five step columns"

      class_option :database,
                   type: :string,
                   default: nil,
                   desc: "Use a separate database for pgbus tables (e.g. --database=pgbus)"

      def create_migration_file
        migration_template "add_workflow_signals.rb.erb",
                           File.join(pgbus_migrate_path, "add_pgbus_workflow_signals.rb")
      end

      def display_post_install
        say ""
        say "Pgbus workflow signals installed!", :green
        say ""
        say "Next steps:"
        say "  1. Run: rails db:migrate#{migrate_command_suffix}"
        say "  2. Restart pgbus: bin/pgbus start"
        say ""
        say "Wait for an outside event, or for a group of jobs:"
        say "  step :capture, wait_for: :payment_confirmed, wait_timeout: 2.days do"
        say "    PaymentGateway.capture(subject, received_signal.payload[\"amount\"])"
        say "  end"
        say ""
        say "  fan_out :notify_vendors do |b|"
        say "    b.add(*subject.vendors.map { |vendor| NotifyVendorJob.new(vendor.id) })"
        say "  end"
        say ""
        say "  workflow.signal!(:payment_confirmed, payload: { amount: 42 }, idempotency_key: event_id)"
        say ""
      end

      private

      def migration_version
        "[#{ActiveRecord::Migration.current_version}]"
      end
    end
  end
end
