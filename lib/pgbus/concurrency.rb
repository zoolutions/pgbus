# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  module Concurrency
    extend ActiveSupport::Concern

    METADATA_KEY = "pgbus_concurrency_key"
    # How long a slot holder may go silent (no visibility heartbeat) before
    # its slot is presumed dead. Also the limit/duration used to promote a
    # parked job whose class no longer resolves.
    DEFAULT_DURATION = 15 * 60

    class_methods do
      # Limit concurrent execution of jobs with the same key.
      #
      #   limits_concurrency to: 1, key: ->(order) { "ProcessOrder-#{order.id}" }
      #   limits_concurrency to: 3, key: ->(user_id) { "ImportUser-#{user_id}" }, on_conflict: :discard
      #
      # Options:
      #   to:          Maximum concurrent jobs for the same key (required)
      #   key:         Proc receiving job arguments, returns a string key.
      #                Default: the ENQUEUED job's class name (resolved at
      #                resolve time, so an inherited declaration keys each
      #                subclass separately — issue #357).
      #   duration:    How long a running holder may go without a visibility
      #                heartbeat before its slot is presumed dead and swept
      #                (default: 15 minutes). Not a cap on run time: the
      #                heartbeat keeps the semaphore alive while the job runs.
      #   on_conflict: What to do when limit is reached — :block, :discard, or :raise (default: :block)
      def limits_concurrency(to:, key: nil, duration: DEFAULT_DURATION, on_conflict: :block) # rubocop:disable Naming/MethodParameterName
        raise ArgumentError, "to: must be a positive integer" unless to.is_a?(Integer) && to.positive?
        raise ArgumentError, "on_conflict must be :block, :discard, or :raise" unless %i[block discard raise].include?(on_conflict)
        raise ArgumentError, "duration must be a positive number" unless duration.is_a?(Numeric) && duration.positive?
        raise ArgumentError, "key must be callable (Proc or lambda)" if !key.nil? && !key.respond_to?(:call)

        @pgbus_concurrency = {
          limit: to,
          key: key,
          duration: duration,
          on_conflict: on_conflict
        }.freeze
      end

      # The nearest declaration in the ancestor chain wins — same inheritance
      # contract as Uniqueness#pgbus_uniqueness (issue #357).
      def pgbus_concurrency
        @pgbus_concurrency || (superclass.pgbus_concurrency if superclass.respond_to?(:pgbus_concurrency))
      end
    end

    class << self
      # Resolve the concurrency key for a given job instance.
      # Returns nil if the job class has no concurrency config.
      def resolve_key(active_job)
        return nil unless active_job.class.respond_to?(:pgbus_concurrency)

        config = active_job.class.pgbus_concurrency
        return nil unless config

        # Class-name default, resolved from the ENQUEUED job's class so an
        # inherited declaration keys each subclass separately (#357).
        return active_job.class.name unless config[:key]

        Support.call_key_proc(config[:key], active_job.arguments)
      end

      # Inject the resolved concurrency key into the job's serialized payload.
      def inject_metadata(active_job, payload_hash)
        key = resolve_key(active_job)
        return payload_hash unless key

        payload_hash.merge(METADATA_KEY => key)
      end

      # Extract the concurrency key from a deserialized payload.
      def extract_key(payload)
        payload[METADATA_KEY]
      end

      # Limit and duration for a job class, or the defaults when the class
      # has no concurrency config or no longer resolves (a parked job must
      # still be promoted; the executor dead-letters a missing class).
      def config_for(job_class)
        config = job_class.respond_to?(:pgbus_concurrency) && job_class.pgbus_concurrency
        return { limit: 1, duration: DEFAULT_DURATION } unless config

        { limit: config[:limit], duration: config[:duration] }
      end

      def config_for_payload(payload)
        config_for(Object.const_get(payload["job_class"].to_s))
      rescue NameError
        config_for(nil)
      end
    end
  end
end
