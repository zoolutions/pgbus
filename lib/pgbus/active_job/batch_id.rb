# frozen_string_literal: true

module Pgbus
  module ActiveJob
    # Gives every ActiveJob a handle on the batch it belongs to.
    #
    # Two distinct ids, mirroring solid_queue's ActiveJob::BatchId:
    #
    # * +batch_id+ — the batch this job is a *member* of. Assigned by the
    #   executor from the payload's +pgbus_batch_id+ before +perform+, so a
    #   running job can call +batch.add+ to add siblings.
    # * +callback_batch_id+ — the batch this job *reports on*. Set on
    #   +on_complete+/+on_success+/+on_failure+ jobs at fire time. A callback is
    #   never a member of the batch it reports on, so its +batch_id+ is nil.
    #
    # Both round-trip through +serialize+/+deserialize+, and both are omitted
    # from the serialized hash when unset — an unbatched job's payload is
    # byte-for-byte what it was before this mixin existed.
    module BatchId
      extend ActiveSupport::Concern

      included do
        attr_accessor :batch_id, :callback_batch_id
      end

      def serialize
        data = super
        data["batch_id"] = batch_id if batch_id
        data["callback_batch_id"] = callback_batch_id if callback_batch_id
        data
      end

      def deserialize(job_data)
        super
        self.batch_id = job_data["batch_id"]
        self.callback_batch_id = job_data["callback_batch_id"]
      end

      # Without a block: the batch this job reports on, or is a member of; nil
      # when neither. With a block: open a new, independent batch through
      # Pgbus.batch (this job's own membership is untouched).
      #
      #   batch(description: "Import") { |b| b.add(*rows.map { |r| RowJob.new(r.id) }) }
      def batch(**options, &block)
        return Pgbus.batch(**options, &block) if block
        raise ArgumentError, "batch(...) with options needs a block: batch(description: ...) { |b| ... }" if options.any?
        return @batch if defined?(@batch)

        id = callback_batch_id || batch_id
        @batch = id && Pgbus::Batch.find(id)
      end
    end
  end
end
