# frozen_string_literal: true

require "active_support/concern"

module Pgbus
  # Job uniqueness guarantees: prevent duplicate jobs from running concurrently.
  #
  # Unlike concurrency limits (which allow N concurrent jobs for the same key),
  # uniqueness ensures AT MOST ONE job with a given key exists in the system
  # at any time — from enqueue through completion.
  #
  # Lock lifecycle (advisory lock + thin lookup table):
  #   1. Enqueue: INSERT INTO pgbus_uniqueness_keys ON CONFLICT DO NOTHING
  #      (logical queue, msg_id=0). After send_message, bind! writes the
  #      real msg_id. The lock row lives as long as the job is in the queue
  #      or executing.
  #   2. Execution: PGMQ's visibility timeout is the execution lock —
  #      no separate claim_for_execution step needed.
  #   3. Completion/DLQ: DELETE FROM pgbus_uniqueness_keys WHERE lock_key = ?.
  #   4. Crash recovery: if a worker dies, VT expires, the message becomes
  #      readable again. The uniqueness key row stays (correctly — the job
  #      hasn't finished). The next worker picks it up and executes.
  #
  # Strategies:
  #   :until_executed  — Lock acquired at enqueue, held through execution, released on
  #                      completion or DLQ. Prevents duplicate enqueue AND duplicate execution.
  #
  #   :while_executing — Lock acquired at execution start, released on completion.
  #                      Allows duplicate enqueue (multiple copies in queue) but only one
  #                      executes at a time.
  #
  # Usage:
  #   class ImportOrderJob < ApplicationJob
  #     ensures_uniqueness strategy: :until_executed,
  #                        key: ->(order_id) { "import-order-#{order_id}" },
  #                        on_conflict: :reject
  #
  #     def perform(order_id)
  #       # Only one instance of this job per order_id can exist at a time
  #     end
  #   end
  module Uniqueness
    extend ActiveSupport::Concern

    METADATA_KEY = "pgbus_uniqueness_key"
    STRATEGY_KEY = "pgbus_uniqueness_strategy"
    # Synthetic queue stored before produce when the caller does not know the
    # real queue yet. Never a live PGMQ queue — the reaper must not probe
    # pgmq.q_<prefix>_pending for these rows (issue #418).
    PLACEHOLDER_QUEUE = "pending"

    VALID_STRATEGIES = %i[until_executed while_executing].freeze
    VALID_CONFLICTS = %i[reject discard log].freeze

    class_methods do
      def ensures_uniqueness(strategy: :until_executed, key: nil, on_conflict: :reject, **opts)
        # lock_ttl was validated and stored in message metadata but never read
        # by the lock lifecycle (the lock lives until the job completes or is
        # dead-lettered, not until a TTL expires). Removed in 1.0.0.
        if opts.key?(:lock_ttl)
          raise ArgumentError,
                "lock_ttl: was removed in pgbus 1.0.0 — it was validated but never read by anything. " \
                "Remove it from ensures_uniqueness. See https://pgbus.dev/docs/upgrading-pgbus"
        end
        raise ArgumentError, "unknown keyword: #{opts.keys.first.inspect}" if opts.any?
        raise ArgumentError, "strategy must be one of: #{VALID_STRATEGIES.join(", ")}" unless VALID_STRATEGIES.include?(strategy)
        raise ArgumentError, "on_conflict must be one of: #{VALID_CONFLICTS.join(", ")}" unless VALID_CONFLICTS.include?(on_conflict)
        raise ArgumentError, "key must be callable (Proc or lambda)" if !key.nil? && !key.respond_to?(:call)

        # Record whether an explicit key was given. With NO explicit key the key
        # defaults to the class name; that is safe for a no-argument job (one
        # logical instance — e.g. a recurring CleanupJob that must not overlap
        # itself) but a silent-correctness footgun for a job that takes per-record
        # arguments: `ImportOrderJob.perform_later(order_id)` would collapse every
        # order into ONE per-class singleton. resolve_key raises at resolve time
        # when the class-name default meets non-empty arguments (see #333); the
        # no-arg case keeps working. explicit_key marks which is which.
        #
        # No proc is stored for the default — resolve_key derives it from the
        # ENQUEUED job's class. A stored `->(*) { name }` would capture the
        # DECLARING class, so a base-class declaration would collapse every
        # subclass into one shared key once configs became inheritable (#357).
        @pgbus_uniqueness = {
          strategy: strategy,
          key: key,
          explicit_key: !key.nil?,
          on_conflict: on_conflict
        }.freeze
      end

      # The nearest declaration in the ancestor chain wins: a class's own
      # `ensures_uniqueness` beats an inherited one, and a base-class
      # declaration reaches every subclass (issue #357 — class-level ivars
      # don't inherit, so without the superclass walk a base declaration was
      # silently inert for subclasses).
      def pgbus_uniqueness
        @pgbus_uniqueness || (superclass.pgbus_uniqueness if superclass.respond_to?(:pgbus_uniqueness))
      end
    end

    class << self
      def resolve_key(active_job)
        config = uniqueness_config(active_job)
        return nil unless config

        args = active_job.arguments
        guard_class_name_default!(active_job, config, args)
        key = if config[:explicit_key]
                Support.call_key_proc(config[:key], args)
              else
                # Class-name default, resolved from the ENQUEUED job's class so
                # an inherited declaration keys each subclass separately (#357).
                active_job.class.name
              end

        # Automatically serialize GlobalID-compatible objects (e.g. ActiveRecord models)
        # so users can pass model instances directly without manual .to_global_id.to_s
        key = key.to_global_id.to_s if key.respond_to?(:to_global_id)
        key
      end

      # Guards the class-name default key against the per-record collapse
      # footgun (#333). When an :until_executed job was declared with NO explicit
      # key (so the key is the class name) AND is enqueued WITH arguments, every
      # distinct argument set would resolve to the same class-name key and
      # collapse into one per-class singleton — almost never what the caller
      # wants. Raise with an actionable message. A no-argument job keeps the
      # class-name default (one logical instance, e.g. a recurring task that must
      # not overlap itself), and :while_executing is unaffected (it acquires
      # per-invocation at execution start, not by class-name identity at enqueue).
      def guard_class_name_default!(active_job, config, args)
        return if config[:explicit_key]
        return unless config[:strategy] == :until_executed
        return if args.nil? || args.empty?

        raise ArgumentError,
              "#{active_job.class.name} uses ensures_uniqueness strategy: :until_executed with no key: " \
              "but is enqueued with arguments — the default key is the class name, which would collapse " \
              "every distinct argument set into one per-class singleton. Pass an explicit " \
              "key: ->(*args) { ... } that includes the arguments. See https://pgbus.dev/docs/upgrading-pgbus"
      end

      def inject_metadata(active_job, payload_hash)
        config = uniqueness_config(active_job)
        return payload_hash unless config

        key = resolve_key(active_job)
        return payload_hash unless key

        payload_hash.merge(
          METADATA_KEY => key,
          STRATEGY_KEY => config[:strategy].to_s
        )
      end

      def extract_key(payload)
        payload&.dig(METADATA_KEY)
      end

      def extract_strategy(payload)
        payload&.dig(STRATEGY_KEY)&.to_sym
      end

      def uniqueness_config(active_job)
        return nil unless active_job.class.respond_to?(:pgbus_uniqueness)

        active_job.class.pgbus_uniqueness
      end

      # Acquire the uniqueness lock at enqueue time (:until_executed only).
      # Uses pg_advisory_xact_lock to serialize concurrent attempts.
      # Returns :acquired, :locked, or :no_lock.
      def acquire_enqueue_lock(key, active_job, queue_name: nil, msg_id: nil)
        config = uniqueness_config(active_job)
        return :no_lock unless config
        return :no_lock unless config[:strategy] == :until_executed

        acquired = if msg_id && queue_name
                     UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: msg_id)
                   else
                     # Pre-produce check: use advisory lock + ON CONFLICT
                     UniquenessKey.acquire!(key, queue_name: queue_name || PLACEHOLDER_QUEUE, msg_id: msg_id || 0)
                   end
        acquired ? :acquired : :locked
      end

      # Acquire the uniqueness lock at execution time (:while_executing only).
      # Returns true if acquired, false if another instance is running.
      def acquire_execution_lock(key, payload)
        strategy = extract_strategy(payload)
        return true unless strategy == :while_executing

        queue_name = payload["queue_name"] || "unknown"
        UniquenessKey.acquire!(key, queue_name: queue_name, msg_id: 0)
      end

      # Release the uniqueness lock after execution completes.
      def release_lock(key)
        return unless key

        UniquenessKey.release!(key)
      end

      # Point a pre-produce lock at the real queue + PGMQ msg_id after send.
      # No-op when key is nil or msg_id is not a positive integer (the reaper
      # treats those rows as unbound and scans live queues instead).
      def bind_lock(key, queue_name:, msg_id:)
        return unless key
        return unless msg_id.to_i.positive?

        UniquenessKey.bind!(key, queue_name: queue_name, msg_id: msg_id)
      end

      # True when the uniqueness row is not yet bound to a real PGMQ message.
      # Covers the synthetic pending queue and any msg_id=0 placeholder
      # (recurring scheduler, bind-not-yet-run, bind failure).
      def placeholder?(queue_name:, msg_id:)
        msg_id.to_i <= 0 || queue_name.to_s == PLACEHOLDER_QUEUE
      end
    end
  end
end
