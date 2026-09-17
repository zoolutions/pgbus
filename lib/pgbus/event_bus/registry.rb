# frozen_string_literal: true

require "singleton"

module Pgbus
  module EventBus
    class Registry
      include Singleton

      attr_reader :subscribers

      def initialize
        @subscribers = []
        @mutex = Mutex.new
      end

      def subscribe(pattern, handler_class, queue_name: nil)
        subscriber = Subscriber.new(
          pattern: pattern,
          handler_class: handler_class,
          queue_name: queue_name
        )

        @mutex.synchronize do
          @subscribers << subscriber
        end

        subscriber
      end

      # Set up every registered subscriber (creates its queue + binds its topic
      # via Pgbus.client, which opens a PGMQ connection).
      #
      # safe: false (default) — set up unconditionally; a connection error
      #   propagates. Use when you know the database is up.
      # safe: true — boot/rake-safe: skip entirely in a schema/db: rake context
      #   (opening a PGMQ connection there would block DROP DATABASE and isn't
      #   wanted during schema load / asset precompile), and swallow a connection
      #   error with a warning instead of crashing boot when the DB isn't ready.
      #   This is the path host apps should call from an initializer so they stop
      #   hand-wrapping setup_all! in a multi-class rescue (issue #334).
      def setup_all!(safe: false)
        return if safe && schema_task_context?

        # Snapshot under the mutex — subscribe/clear! mutate @subscribers under
        # @mutex, and setup! does DB I/O we must NOT hold the lock across, so
        # iterate a copy taken atomically.
        subscribers = @mutex.synchronize { @subscribers.dup }

        subscribers.each do |subscriber|
          subscriber.setup!
        rescue PGMQ::Errors::ConnectionError, PG::ConnectionBad => e
          # Only a genuine CONNECTION failure ("database isn't up yet") is
          # tolerable under safe:; a PG::Error subclass like a syntax/permission/
          # missing-table error is a real setup bug and must still surface.
          raise unless safe

          Pgbus.logger.warn do
            "[Pgbus] EventBus subscriber setup skipped (#{e.class}: #{e.message}) — " \
              "the database isn't reachable yet; subscribers set up on the next attempt."
          end
        end
      end

      # Subscribers a message read from +queue_name+ must be dispatched to
      # (issue #469). Every subscriber gets its own queue, so a topic with N
      # matching subscribers produces N queue copies of each event; selecting by
      # pattern alone fanned every copy out to every match, running each handler
      # N times per event — and, across hosts, concurrently. Ownership is the
      # primary filter; the pattern check still applies because a routing key
      # the owner's pattern does not match means a stale pgmq.topic_bindings
      # row, and a stale binding must not run the handler.
      #
      # +queue_name+ is required on purpose: a keyword-less call would silently
      # restore the fan-out this closed. Callers that genuinely want the
      # pattern view (the Testing inline/drain paths, which never touch a
      # queue) use #subscribers_matching.
      def handlers_for(routing_key, queue_name:)
        @subscribers.select do |s|
          s.queue_name == queue_name && matches?(s.pattern, routing_key)
        end
      end

      # Pattern-only selection, with no queue in play. Used by the Testing
      # inline/drain paths, where the event never reaches PGMQ and each matching
      # subscriber is invoked exactly once — the same per-subscriber delivery
      # count owner-only dispatch produces in production.
      def subscribers_matching(routing_key)
        @subscribers.select { |s| matches?(s.pattern, routing_key) }
      end

      # Physical PGMQ queue names for every registered event subscriber, so a
      # wildcard (`queues: ['*']`) worker can exclude them — an event queue
      # carries event payloads, not ActiveJob jobs, and a job worker that adopts
      # one fails to deserialize and DLQ-moves the event (issue #333). Returns a
      # Set of prefixed names (`#{queue_prefix}_<subscriber>`), matching the
      # pgmq.meta rows the wildcard resolver diffs against.
      def event_queue_names
        @subscribers.to_set { |s| Pgbus.configuration.queue_name(s.queue_name) }
      end

      def clear!
        @mutex.synchronize { @subscribers.clear }
      end

      # Logical queue names a consumer subscribed to +topics+ reads from — the
      # derivation Consumer#setup_subscriptions uses, exposed here so the
      # supervisor-owned NotifyHub (issue #381) computes the same LISTEN set
      # the consumer forks actually read. The overlap check is deliberately
      # coarse: any topic filter ending in "#" claims every subscriber (read
      # more queues rather than risk an uncovered subscriber).
      def queue_names_for_topics(topics)
        subscribers = @mutex.synchronize { @subscribers.dup }
        subscribers
          .select { |s| topics.any? { |t| pattern_overlaps?(t, s.pattern) } }
          .map(&:queue_name)
          .uniq
      end

      # Preserved verbatim from Consumer#pattern_overlaps?: true when either
      # side is a superset of the other by the cheap prefix/suffix rules.
      def pattern_overlaps?(topic_filter, subscription_pattern)
        topic_filter == subscription_pattern ||
          topic_filter.end_with?("#") ||
          subscription_pattern.start_with?(topic_filter.delete_suffix(".#"))
      end

      private

      # True when running inside a rake schema/asset task, where setup_all!(safe:)
      # should skip rather than open a connection. Delegates to the shared,
      # public detector (issue #409) so there is a single implementation.
      def schema_task_context?
        Pgbus.database_task?
      end

      def matches?(pattern, routing_key)
        regex = pattern
                .gsub(".", "\\.")
                .gsub("*", "[^.]+")
                .gsub("#", ".*")
        routing_key.match?(/\A#{regex}\z/)
      end
    end
  end
end
