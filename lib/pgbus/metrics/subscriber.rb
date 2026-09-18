# frozen_string_literal: true

require "socket"

module Pgbus
  module Metrics
    # Consumes Pgbus::Instrumentation events and forwards them to the configured
    # metrics backend. The event→metric mapping (names, tags, counter vs
    # histogram) intentionally mirrors the AppSignal subscriber so a team can
    # switch backends and keep the same dashboards.
    #
    # Every handler runs inside `safely`: a raising backend (Prometheus registry
    # bug, StatsD socket down, custom backend) is logged and swallowed — a metrics
    # failure must never propagate into the producer thread that emitted the
    # event.
    module Subscriber
      METRIC_PREFIX = "pgbus_"
      private_constant :METRIC_PREFIX

      @subscriptions = []
      @backend = nil

      class << self
        def install!(backend:)
          return false if @installed

          @backend = backend
          @subscriptions = [
            subscribe("pgbus.executor.execute") { |event| on_executor_execute(event) },
            subscribe("pgbus.job_completed") { |event| on_job_completed(event) },
            subscribe("pgbus.job_failed") { |event| on_job_failed(event) },
            subscribe("pgbus.job_dead_lettered") { |event| on_job_dead_lettered(event) },
            subscribe("pgbus.job_visibility_extended") { |event| on_job_visibility_extended(event) },
            subscribe("pgbus.event_processed") { |event| on_event_processed(event) },
            subscribe("pgbus.event_failed") { |event| on_event_failed(event) },
            subscribe("pgbus.event_skipped") { |event| on_event_skipped(event) },
            subscribe("pgbus.client.send_message") { |event| on_send_message(event) },
            subscribe("pgbus.client.send_batch") { |event| on_send_batch(event) },
            subscribe("pgbus.client.read_batch") { |event| on_read_batch(event) },
            subscribe("pgbus.stream.broadcast") { |event| on_stream_broadcast(event) },
            subscribe("pgbus.outbox.publish") { |event| on_outbox_publish(event) },
            subscribe("pgbus.recurring.enqueue") { |event| on_recurring_enqueue(event) },
            subscribe("pgbus.worker.recycle") { |event| on_worker_recycle(event) },
            subscribe("pgbus.consumer.recycle") { |event| on_consumer_recycle(event) },
            subscribe("pgbus.client.pool") { |event| on_client_pool(event) }
          ]
          @installed = true
        end

        def installed?
          @installed == true
        end

        def reset!
          @subscriptions&.each { |s| ActiveSupport::Notifications.unsubscribe(s) }
          @subscriptions = []
          @backend = nil
          @installed = false
        end

        private

        attr_reader :backend

        def subscribe(name, &block)
          ActiveSupport::Notifications.subscribe(name) do |*args|
            event = ActiveSupport::Notifications::Event.new(*args)
            safely { block.call(event) }
          end
        end

        # Errors in the subscriber must never affect the producer thread.
        def safely
          yield
        rescue StandardError => e
          Pgbus.logger.warn do
            "[Pgbus::Metrics] subscriber error: #{e.class}: #{e.message}"
          end
        end

        # ── Job execution ─────────────────────────────────────────────────

        def on_executor_execute(event)
          payload = event.payload
          backend.histogram(
            "#{METRIC_PREFIX}job_duration_ms",
            event.duration,
            compact(queue: payload[:queue], job_class: payload[:job_class])
          )
        end

        def on_job_completed(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}queue_job_count", 1,
            compact(queue: payload[:queue], job_class: payload[:job_class], status: "processed")
          )
        end

        def on_job_failed(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}queue_job_count", 1,
            compact(queue: payload[:queue], job_class: payload[:job_class], status: "failed")
          )
        end

        def on_job_dead_lettered(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}queue_job_count", 1,
            compact(queue: payload[:queue], job_class: payload[:job_class], status: "dead_lettered")
          )
        end

        # One increment per re-armed visibility timeout: a job class that
        # shows up here is one that outlives visibility_timeout.
        def on_job_visibility_extended(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}visibility_extended", 1,
            compact(queue: payload[:queue], job_class: payload[:job_class])
          )
        end

        # ── Event handler ─────────────────────────────────────────────────

        def on_event_processed(event)
          payload = event.payload
          tags = compact(handler: payload[:handler], routing_key: payload[:routing_key])
          backend.histogram("#{METRIC_PREFIX}event_duration_ms", event.duration, tags)
          backend.increment("#{METRIC_PREFIX}event_count", 1, tags.merge(status: "processed"))
        end

        def on_event_failed(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}event_count", 1,
            compact(handler: payload[:handler], routing_key: payload[:routing_key], status: "failed")
          )
        end

        # An idempotent handler that did not run this delivery. `reason` is what
        # makes the skip readable: :completed/:cached is deduplication working,
        # :owned means a second delivery arrived while the holder was still
        # running and was deferred to it (issue #470).
        def on_event_skipped(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}event_count", 1,
            compact(handler: payload[:handler], routing_key: payload[:routing_key],
                    status: "skipped", reason: payload[:reason])
          )
        end

        # ── Client (PGMQ wrapper) ─────────────────────────────────────────

        def on_send_message(event)
          backend.increment("#{METRIC_PREFIX}messages_sent", 1, compact(queue: event.payload[:queue]))
        end

        def on_send_batch(event)
          payload = event.payload
          count = payload[:count] || payload[:batch_size] || 1
          backend.increment("#{METRIC_PREFIX}messages_sent", count, compact(queue: payload[:queue]))
        end

        def on_read_batch(event)
          payload = event.payload
          count = payload[:count] || payload[:fetched] || 0
          backend.increment("#{METRIC_PREFIX}messages_read", count, compact(queue: payload[:queue]))
        end

        # ── Streams ───────────────────────────────────────────────────────

        def on_stream_broadcast(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}stream_broadcast_count", 1,
            compact(stream: payload[:stream], deferred: payload[:deferred] ? "true" : "false")
          )
        end

        # ── Outbox ────────────────────────────────────────────────────────

        def on_outbox_publish(event)
          backend.increment(
            "#{METRIC_PREFIX}outbox_published", 1,
            compact(kind: event.payload[:kind] || "job")
          )
        end

        # ── Recurring scheduler ───────────────────────────────────────────

        def on_recurring_enqueue(event)
          payload = event.payload
          backend.increment(
            "#{METRIC_PREFIX}recurring_enqueued", 1,
            compact(task: payload[:task], class_name: payload[:class_name])
          )
        end

        # ── Worker lifecycle ──────────────────────────────────────────────

        def on_worker_recycle(event)
          backend.increment(
            "#{METRIC_PREFIX}worker_recycled", 1,
            compact(reason: event.payload[:reason], kind: "worker")
          )
        end

        def on_consumer_recycle(event)
          backend.increment(
            "#{METRIC_PREFIX}worker_recycled", 1,
            compact(reason: event.payload[:reason], kind: "consumer")
          )
        end

        # ── Connection pool ───────────────────────────────────────────────

        def on_client_pool(event)
          payload = event.payload
          tags = { hostname: hostname }
          backend.gauge("#{METRIC_PREFIX}pool_size", payload[:size], tags)
          backend.gauge("#{METRIC_PREFIX}pool_available", payload[:available], tags)
        end

        def hostname
          Socket.gethostname
        end

        # Drop nil-valued tags so backends don't emit empty labels.
        def compact(tags)
          tags.compact
        end
      end
    end
  end
end
