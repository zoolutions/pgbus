# frozen_string_literal: true

require "time"

module Pgbus
  module Integrations
    module Appsignal
      # Translates Pgbus::Instrumentation events into AppSignal transactions
      # and custom metrics.
      #
      # Job and event-handler events open a BACKGROUND_JOB transaction so they
      # appear under AppSignal's "Performance > Background jobs" view, with
      # action `<JobClass>#perform` or `<HandlerClass>#handle`. All other
      # events are reported as counters or distributions only.
      #
      # All metric names are prefixed `pgbus_`. Tag keys avoid high-cardinality
      # values (no msg_id, no event_id) so AppSignal's metric storage stays
      # efficient.
      module Subscriber
        BACKGROUND_JOB = "background_job"
        METRIC_PREFIX = "pgbus_"
        private_constant :BACKGROUND_JOB, :METRIC_PREFIX

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
              subscribe("pgbus.executor.execute") { |event| on_executor_execute(event) },
              subscribe("pgbus.job_completed") { |event| on_job_completed(event) },
              subscribe("pgbus.job_failed") { |event| on_job_failed(event) },
              subscribe("pgbus.job_dead_lettered") { |event| on_job_dead_lettered(event) },
              subscribe("pgbus.event_processed") { |event| on_event_processed(event) },
              subscribe("pgbus.event_failed") { |event| on_event_failed(event) },
              subscribe("pgbus.event_dead_lettered") { |event| on_event_dead_lettered(event) },
              subscribe("pgbus.client.send_message") { |event| on_send_message(event) },
              subscribe("pgbus.client.send_batch") { |event| on_send_batch(event) },
              subscribe("pgbus.client.read_batch") { |event| on_read_batch(event) },
              subscribe("pgbus.stream.broadcast") { |event| on_stream_broadcast(event) },
              subscribe("pgbus.outbox.publish") { |event| on_outbox_publish(event) },
              subscribe("pgbus.recurring.enqueue") { |event| on_recurring_enqueue(event) },
              subscribe("pgbus.worker.recycle") { |event| on_worker_recycle(event) },
              subscribe("pgbus.consumer.recycle") { |event| on_consumer_recycle(event) },
              *Pgbus::Metrics::WorkflowMetrics::EVENTS.map { |name| subscribe(name) { |event| on_workflow(event) } }
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

          def on_executor_execute(event)
            payload = event.payload
            transaction = ::Appsignal::Transaction.create(BACKGROUND_JOB)
            transaction.set_action_if_nil("#{payload[:job_class] || "UnknownJob"}#perform")
            apply_queue_start(transaction, payload[:enqueued_at])
            transaction.add_tags(job_tags(payload))
            transaction.add_params_if_nil { { arguments: payload[:arguments] } }
            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}job_duration_ms",
              event.duration,
              { queue: payload[:queue], job_class: payload[:job_class] }
            )
          ensure
            ::Appsignal::Transaction.complete_current!
          end

          def on_job_completed(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}queue_job_count",
              1,
              { queue: payload[:queue], job_class: payload[:job_class], status: "processed" }
            )
          end

          def on_job_failed(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}queue_job_count",
              1,
              { queue: payload[:queue], job_class: payload[:job_class], status: "failed" }
            )
            err = payload[:exception_object]
            ::Appsignal.set_error(err) if err && ::Appsignal.respond_to?(:set_error)
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

          def on_event_processed(event)
            payload = event.payload
            transaction = ::Appsignal::Transaction.create(BACKGROUND_JOB)
            transaction.set_action_if_nil("#{payload[:handler] || "UnknownHandler"}#handle")
            apply_queue_start(transaction, payload[:published_at])
            transaction.add_tags(handler_tags(payload))
            ::Appsignal.add_distribution_value(
              "#{METRIC_PREFIX}event_duration_ms",
              event.duration,
              { handler: payload[:handler], routing_key: payload[:routing_key] }
            )
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}event_count",
              1,
              { handler: payload[:handler], routing_key: payload[:routing_key], status: "processed" }
            )
          ensure
            ::Appsignal::Transaction.complete_current!
          end

          def on_event_failed(event)
            payload = event.payload
            ::Appsignal.increment_counter(
              "#{METRIC_PREFIX}event_count",
              1,
              { handler: payload[:handler], routing_key: payload[:routing_key], status: "failed" }
            )
            err = payload[:exception_object]
            ::Appsignal.set_error(err) if err && ::Appsignal.respond_to?(:set_error)
          end

          # A counter only, like on_job_dead_lettered: nothing is raised at
          # move time, and the handler's error already went out through
          # pgbus.event_failed on the attempt that raised (issue #543). A body
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

          # ── Workflows (issue #540) ──────────────────────────────────────

          # The same names and labels as Metrics::Subscriber
          # (Pgbus::Metrics::WorkflowMetrics), as AppSignal custom metrics.
          def on_workflow(event)
            Pgbus::Metrics::WorkflowMetrics.samples(event.name, event.payload).each do |kind, name, value, tags|
              case kind
              when :histogram then ::Appsignal.add_distribution_value(name, value, tags)
              when :counter then ::Appsignal.increment_counter(name, value, tags)
              when :gauge then ::Appsignal.set_gauge(name, value, tags)
              end
            end
          end

          # ── Helpers ─────────────────────────────────────────────────────

          # AppSignal expects queue-start as Unix epoch milliseconds. Pgbus
          # carries it as either an ISO-8601 String or a Time — both happen
          # in practice (executor passes the JSON string, handler passes a
          # parsed Time).
          def apply_queue_start(transaction, value)
            return unless value

            millis =
              case value
              when Time
                (value.to_f * 1_000).to_i
              when String
                (Time.parse(value).to_f * 1_000).to_i
              when Numeric
                value.to_i
              end
            transaction.set_queue_start(millis) if millis
          rescue ArgumentError
            # Unparseable timestamp — skip rather than blow up.
          end

          def job_tags(payload)
            tags = {
              "queue" => payload[:queue],
              "job_class" => payload[:job_class],
              "attempts" => payload[:read_ct]
            }
            tags["active_job_id"] = payload[:job_id] if payload[:job_id]
            tags["provider_job_id"] = payload[:provider_job_id] if payload[:provider_job_id]
            tags["request_id"] = payload[:provider_job_id] || payload[:job_id]
            tags.compact
          end

          def handler_tags(payload)
            {
              "handler" => payload[:handler],
              "routing_key" => payload[:routing_key],
              "attempts" => payload[:read_ct]
            }.compact
          end
        end
      end
    end
  end
end
