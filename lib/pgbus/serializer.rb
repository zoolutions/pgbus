# frozen_string_literal: true

require "json"
require "time"

module Pgbus
  module Serializer
    module_function

    # ActiveJob encodes GlobalID job arguments with this key (private constant
    # on ActiveJob::Arguments). Walked by the job-path allowlist gate (#368).
    AJ_GLOBALID_KEY = "_aj_globalid"
    private_constant :AJ_GLOBALID_KEY

    def serialize_job(active_job)
      Instrumentation.instrument("pgbus.serializer.serialize", kind: :job) do
        data = active_job.serialize
        # GlobalID is handled by ActiveJob's serialize — it converts AR objects
        # to GlobalID URIs automatically. We just JSON-encode the result.
        JSON.generate(data)
      end
    end

    def serialize_job_hash(active_job)
      Instrumentation.instrument("pgbus.serializer.serialize", kind: :job) do
        active_job.serialize
      end
    end

    def deserialize_job(json_string, configuration: Pgbus.configuration)
      Instrumentation.instrument("pgbus.serializer.deserialize", kind: :job) do
        deserialize_job_data(JSON.parse(json_string), configuration: configuration)
      end
    end

    # Job-hash entry point used by the executor (payload already parsed) and by
    # `deserialize_job`. When `allowed_global_id_models` is configured, every
    # `_aj_globalid` in the tree is checked before Rails' unrestricted
    # GlobalID::Locator runs (issue #368). Nil allowlist = zero-cost allow-all.
    # Prefer the caller's `configuration` (e.g. Executor's injected config) so a
    # non-global allowlist is not silently ignored.
    def deserialize_job_data(data, configuration: Pgbus.configuration)
      assert_job_global_ids_allowed!(data, configuration: configuration)
      # Top-level constant: bare ActiveJob::Base can resolve to
      # Pgbus::ActiveJob::Base under Zeitwerk's Pgbus::ActiveJob namespace.
      ::ActiveJob::Base.deserialize(data)
    end

    def serialize_event(event)
      payload = event.respond_to?(:to_global_id) ? { "_global_id" => event.to_global_id.to_s } : event
      JSON.generate({
                      "event_id" => event.respond_to?(:event_id) ? event.event_id : SecureRandom.uuid,
                      "payload" => payload,
                      "published_at" => Time.now.utc.iso8601(6)
                    })
    end

    def deserialize_event(json_string, configuration: Pgbus.configuration)
      data = JSON.parse(json_string)
      payload = data["payload"]

      if payload.is_a?(Hash) && payload["_global_id"]
        data["payload"] = locate_global_id(payload["_global_id"], configuration: configuration)
      end

      Event.new(
        event_id: data["event_id"],
        payload: data["payload"],
        published_at: Time.parse(data["published_at"])
      )
    end

    # Locate a GlobalID with optional type restriction.
    # When allowed_global_id_models is configured, only those model classes
    # can be resolved — prevents loading arbitrary objects from crafted payloads.
    # Shared by EventBus payloads (`_global_id`) and job arguments (`_aj_globalid`).
    def locate_global_id(gid_string, configuration: Pgbus.configuration)
      gid = assert_allowed_global_id!(gid_string, configuration: configuration)
      GlobalID::Locator.locate(gid)
    end

    # Raises SerializationError unless the GlobalID's model is permitted.
    # Returns the parsed GlobalID on success (so locate can skip re-parse).
    def assert_allowed_global_id!(gid_string, configuration: Pgbus.configuration)
      gid = GlobalID.parse(gid_string)
      raise Pgbus::SerializationError, "Invalid GlobalID: #{gid_string.inspect}" unless gid

      allowed = configuration.allowed_global_id_models
      if allowed && allowed.empty?
        raise Pgbus::SerializationError,
              "GlobalID deserialization is disabled (allowed_global_id_models is empty). " \
              "Set to nil to allow all models, or add permitted classes."
      end
      if allowed&.any? { |entry| !entry.is_a?(Class) && !entry.is_a?(Module) }
        raise Pgbus::SerializationError,
              "allowed_global_id_models must contain Class/Module objects, " \
              "got: #{allowed.map(&:class).uniq.join(", ")}"
      end
      if allowed&.none? { |klass| gid.model_class <= klass }
        raise Pgbus::SerializationError,
              "GlobalID model #{gid.model_class} is not in allowed_global_id_models. " \
              "Add it to Pgbus.configuration.allowed_global_id_models to permit deserialization."
      end

      gid
    end

    # Walk a job payload (or any nested structure) and enforce the allowlist on
    # every ActiveJob `_aj_globalid` value. No-op when allowlist is nil.
    def assert_job_global_ids_allowed!(data, configuration: Pgbus.configuration)
      return if configuration.allowed_global_id_models.nil?

      walk_job_global_ids(data, configuration)
    end

    def walk_job_global_ids(value, configuration)
      case value
      when Hash
        assert_allowed_global_id!(value[AJ_GLOBALID_KEY], configuration: configuration) if value.key?(AJ_GLOBALID_KEY)
        value.each_value { |child| walk_job_global_ids(child, configuration) }
      when Array
        value.each { |child| walk_job_global_ids(child, configuration) }
      end
    end
    private_class_method :walk_job_global_ids
  end
end
