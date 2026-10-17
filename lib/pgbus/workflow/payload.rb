# frozen_string_literal: true

require "json"

module Pgbus
  class Workflow
    # Arguments, step outputs and the subject, to and from the database.
    # Values go through ActiveJob::Arguments (JSON, GlobalID-aware, never
    # Marshal), are capped at config.workflow_max_payload_size, and come back
    # with indifferent access. GlobalIDs are gated by
    # config.allowed_global_id_models, as job arguments are.
    module Payload
      module_function

      def dump(values, what:, limit: Pgbus.configuration.workflow_max_payload_size)
        serialized = ::ActiveJob::Arguments.serialize([values.to_h]).first
        size = JSON.generate(serialized).bytesize
        raise PayloadTooLarge, "#{what} is #{size} bytes, over workflow_max_payload_size (#{limit})" if size > limit

        serialized
      end

      def load(serialized, configuration: Pgbus.configuration)
        return nil if serialized.nil?

        Serializer.assert_job_global_ids_allowed!(serialized, configuration: configuration)
        ::ActiveJob::Arguments.deserialize([serialized]).first.with_indifferent_access
      end

      # The subject columns of a run: the polymorphic type and id the
      # one-ongoing index keys on, and the GlobalID +subject+ loads through.
      def subject_attributes(subject)
        return { subject_type: nil, subject_id: nil, subject_gid: nil } if subject.nil?
        unless subject.respond_to?(:to_global_id) && subject.respond_to?(:id)
          raise ArgumentError, "a workflow subject must be a GlobalID record (an ActiveRecord model), got #{subject.class}"
        end

        klass = subject.class
        type = klass.respond_to?(:base_class) ? klass.base_class.name : klass.name
        { subject_type: type, subject_id: subject.id.to_s, subject_gid: subject.to_global_id.to_s }
      end

      # nil when the run has no subject or the record is gone.
      def locate_subject(gid, configuration: Pgbus.configuration)
        return nil if gid.nil?

        Serializer.locate_global_id(gid, configuration: configuration)
      rescue ActiveRecord::RecordNotFound
        nil
      end
    end
  end
end
