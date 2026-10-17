# frozen_string_literal: true

require "pgbus/integrations/appsignal/job_span"
require "pgbus/integrations/appsignal/event_span"

module Pgbus
  module Integrations
    module Appsignal
      # Translates Pgbus::Instrumentation events into AppSignal transactions
      # and custom metrics.
      #
      # Job and event-handler events open a BACKGROUND_JOB transaction so they
      # appear under AppSignal's "Performance > Background jobs" view, with
      # action `<JobClass>#perform` or `<HandlerClass>#handle`. The transaction
      # is opened when pgbus.executor.execute / pgbus.event_processed starts and
      # completed when it finishes (JobSpan, EventSpan), so it is current while the
      # job or handler runs and carries the error the execution raised. All
      # other events are reported as counters or distributions only.
      #
      # All metric names are prefixed `pgbus_`. Tag keys avoid high-cardinality
      # values (no msg_id, no event_id) so AppSignal's metric storage stays
      # efficient.
      module Subscriber
        METRIC_PREFIX = "pgbus_"

        # Built once, at load: a span keeps its state in a fiber-local frame,
        # never in itself.
        JOB_SPAN = JobSpan.new.freeze
        EVENT_SPAN = EventSpan.new.freeze
        private_constant :METRIC_PREFIX, :JOB_SPAN, :EVENT_SPAN

        # Tracked so we can detach in reset! (used by specs). Installed once
        # at boot (the engine's after_initialize); @install_lock makes the
        # check-then-install atomic so nothing subscribes twice (issue #527).
        @install_lock = Mutex.new
        @subscriptions = [].freeze
        @installed = false

        class << self
          def install!
            @install_lock.synchronize do
              next false if @installed

              @subscriptions = subscribe_all
              @installed = true
            end
          end

          def installed?
            @install_lock.synchronize { @installed }
          end

          def reset!
            @install_lock.synchronize do
              @subscriptions.each { |s| ActiveSupport::Notifications.unsubscribe(s) }
              @subscriptions = [].freeze
              @installed = false
            end
          end

          private

          def subscribe_all
            [
              ActiveSupport::Notifications.subscribe("pgbus.executor.execute", JOB_SPAN),
              subscribe("pgbus.job_completed") { |event| on_job_completed(event) },
              subscribe("pgbus.job_failed") { |event| on_job_failed(event) },
              subscribe("pgbus.job_dead_lettered") { |event| on_job_dead_lettered(event) },
              ActiveSupport::Notifications.subscribe("pgbus.event_processed", EVENT_SPAN),
              subscribe("pgbus.event_failed") { |event| on_event_failed(event) },
              subscribe("pgbus.event_dead_lettered") { |event| on_event_dead_lettered(event) },
              subscribe("pgbus.client.send_message") { |event| on_send_message(event) },
              subscribe("pgbus.client.send_batch") { |event| on_send_batch(event) },
              subscribe("pgbus.client.read_batch") { |event| on_read_batch(event) },
              subscribe("pgbus.stream.broadcast") { |event| on_stream_broadcast(event) },
              subscribe("pgbus.outbox.publish") { |event| on_outbox_publish(event) },
              subscribe("pgbus.recurring.enqueue") { |event| on_recurring_enqueue(event) },
              subscribe("pgbus.worker.recycle") { |event| on_worker_recycle(event) },
              subscribe("pgbus.consumer.recycle") { |event| on_consumer_recycle(event) }
              # pgbus.client.pool is deliberately NOT subscribed here — the
              # minutely Probe#track_pool already reports pgbus_pool_size /
              # pgbus_pool_available gauges for AppSignal (see probe.rb).
              # Subscribing here too would double-report on every heartbeat
              # and skew the gauges.
            ].freeze
          end

          def subscribe(name, &block)
            # silence rubocop unused
            ActiveSupport::Notifications.subscribe(name) do |*args|
              event = ActiveSupport::Notifications::Event.new(*args)
              safely { block.call(event) }
            end
          end

          # Errors in the subscriber must never affect the producer thread.
          # AppSignal can be misconfigured, the agent can be down, etc. — log
          # and move on.
          def safely
            yield
          rescue StandardError => e
            Pgbus.logger.warn do
              "[Pgbus::AppSignal] subscriber error: #{e.class}: #{e.message}"
            end
          end

          # ── Job execution ───────────────────────────────────────────────

          def on_job_completed(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}queue_job_count",
              1,
              { queue: payload[:queue], job_class: payload[:job_class], status: "processed" }
            )
          end

          # The counter has its own guard so a raise there cannot keep the
          # error from being reported.
          def on_job_failed(event)
            payload = event.payload
            safely do
              ::Appsignal.increment_counter(
                "#{METRIC_PREFIX}queue_job_count",
                1,
                { queue: payload[:queue], job_class: payload[:job_class], status: "failed" }
              )
            end
            JOB_SPAN.report_unattached(payload[:exception_object], payload)
          end

          def on_job_dead_lettered(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}queue_job_count",
              1,
              { queue: payload[:queue], job_class: payload[:job_class], status: "dead_lettered" }
            )
          end

          # ── Event handler ───────────────────────────────────────────────

          # The counter has its own guard so a raise there cannot keep the
          # error from being reported.
          def on_event_failed(event)
            payload = event.payload
            safely do
              ::Appsignal.increment_counter(
                "#{METRIC_PREFIX}event_count",
                1,
                { handler: payload[:handler], routing_key: payload[:routing_key], status: "failed" }
              )
            end
            EVENT_SPAN.report_unattached(payload[:exception_object], payload)
          end

          # A counter only, like on_job_dead_lettered: nothing is raised at
          # move time, and the handler's error already went out on the
          # attempt that raised (issues #543, #552). A body
          # that is not JSON leaves handler and routing_key nil; compact drops
          # those tags, as Metrics::Subscriber does.
          def on_event_dead_lettered(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}event_count",
              1,
              { handler: payload[:handler], routing_key: payload[:routing_key], status: "dead_lettered" }.compact
            )
          end

          # ── Client (PGMQ wrapper) ───────────────────────────────────────

          def on_send_message(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}messages_sent",
              1,
              { queue: payload[:queue] }
            )
            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}send_duration_ms",
              event.duration,
              { queue: payload[:queue] }
            )
          end

          def on_send_batch(event)
            payload = event.payload
            count = payload[:count] || payload[:batch_size] || 1
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}messages_sent",
              count,
              { queue: payload[:queue] }
            )
            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}send_batch_duration_ms",
              event.duration,
              { queue: payload[:queue] }
            )
          end

          def on_read_batch(event)
            payload = event.payload
            count = payload[:count] || payload[:fetched] || 0
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}messages_read",
              count,
              { queue: payload[:queue] }
            )
          end

          # ── Streams ─────────────────────────────────────────────────────

          def on_stream_broadcast(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}stream_broadcast_count",
              1,
              { stream: payload[:stream], deferred: payload[:deferred] ? "true" : "false" }
            )
            return unless payload[:bytes]

            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}stream_broadcast_bytes",
              payload[:bytes],
              { stream: payload[:stream] }
            )
          end

          # ── Outbox ──────────────────────────────────────────────────────

          def on_outbox_publish(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}outbox_published",
              1,
              { kind: payload[:kind] || "job" }
            )
            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}outbox_publish_duration_ms",
              event.duration,
              { kind: payload[:kind] || "job" }
            )
          end

          # ── Recurring scheduler ─────────────────────────────────────────

          def on_recurring_enqueue(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}recurring_enqueued",
              1,
              { task: payload[:task], class_name: payload[:class_name] }
            )
          end

          # ── Worker lifecycle ────────────────────────────────────────────

          def on_worker_recycle(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}worker_recycled",
              1,
              { reason: payload[:reason], kind: "worker" }
            )
          end

          def on_consumer_recycle(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}worker_recycled",
              1,
              { reason: payload[:reason], kind: "consumer" }
            )
          end
        end
      end
    end
  end
end
