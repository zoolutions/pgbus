# frozen_string_literal: true

module Pgbus
  module ActiveJob
    class Coalesce
      # Whether a Pgbus.coalesce scope (the request middleware's, a batch's)
      # may hold a job class (issue #689). Included on ActiveJob::Base by the
      # engine:
      #
      #   class ProcessItemJob < ApplicationJob
      #     pgbus_coalesce false   # sent at its perform_later, inside a scope too
      #   end
      #
      # A class that declares nothing follows config.coalesce_default (true:
      # held). With the default false, only classes that declare
      # `pgbus_coalesce true` are held. Subclasses inherit the setting.
      module JobMixin
        extend ActiveSupport::Concern

        included do
          # A per-class setting the job's own class body writes at load
          # (`pgbus_coalesce false`); the adapter only reads it at enqueue.
          class_attribute :pgbus_coalesce_enabled, instance_writer: false, default: nil # rubocop:disable ThreadSafety/ClassAndModuleAttributes
        end

        class_methods do
          def pgbus_coalesce(enabled = true) # rubocop:disable Style/OptionalBooleanParameter
            self.pgbus_coalesce_enabled = enabled ? true : false
          end

          # The adapter's check: the class's own setting, else the configured default.
          def pgbus_coalesce?
            enabled = pgbus_coalesce_enabled
            enabled.nil? ? Pgbus.configuration.coalesce_default : enabled
          end
        end
      end
    end
  end
end
