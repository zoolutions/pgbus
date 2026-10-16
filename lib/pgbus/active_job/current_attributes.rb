# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # Carries ActiveSupport::CurrentAttributes across enqueue → perform
    # (issue #430). Included on ActiveJob::Base by the engine next to BatchId.
    #
    # * +serialize+ snapshots the persisted Current classes
    #   (Pgbus::CurrentAttributes.capture) under +pgbus_current+. A job that
    #   was itself deserialized re-serializes the context it was enqueued
    #   with — so a +retry_on+ re-enqueue keeps the original context even if
    #   Current changed during the attempt. Nothing is added when the feature
    #   is off or no attribute is assigned: the payload is byte-for-byte what
    #   it was before this mixin existed.
    # * +perform_now+ is wrapped (not an +around_perform+) so the restored
    #   context also covers +rescue_from+ / +retry_on+ / +discard_on+ blocks,
    #   which run in +perform_now+'s rescue outside the perform callbacks —
    #   and it works identically under the pgbus worker, Rails' :test and
    #   :inline adapters, and a bare +job.perform_now+.
    #
    # Per-class control: +self.pgbus_persist_current_attributes = false+
    # (never persist for this job class) or a spec in the same shapes as
    # +config.current_attributes+ (an Array / Hash) to replace the config's
    # list for this class. +nil+ (default) follows the config.
    module CurrentAttributes
      extend ActiveSupport::Concern

      included do
        attr_accessor :pgbus_current_attributes

        # A per-class setting the job's own class body writes at load; jobs
        # only read it while they serialize.
        class_attribute :pgbus_persist_current_attributes, instance_writer: false, default: nil # rubocop:disable ThreadSafety/ClassAndModuleAttributes
      end

      def serialize
        data = super
        captured = pgbus_current_attributes ||
                   Pgbus::CurrentAttributes.capture(override: self.class.pgbus_persist_current_attributes)
        data[Pgbus::CurrentAttributes::METADATA_KEY] = captured if captured
        data
      end

      def deserialize(job_data)
        super
        self.pgbus_current_attributes = job_data[Pgbus::CurrentAttributes::METADATA_KEY]
      end

      def perform_now
        Pgbus::CurrentAttributes.restore(pgbus_current_attributes) { super }
      end
    end
  end
end
