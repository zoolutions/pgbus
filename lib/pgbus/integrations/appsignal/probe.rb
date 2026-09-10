# frozen_string_literal: true

require "socket"

module Pgbus
  module Integrations
    module Appsignal
      # Minutely probe that pushes pgbus-wide gauges into AppSignal.
      #
      # All readings come from Pgbus::Web::DataSource so the probe doesn't
      # duplicate query logic. DataSource is built to be resilient — every
      # method rescues StandardError and returns a safe default — but we
      # still wrap each section in our own rescue so a probe iteration
      # never raises out into the AppSignal probe runner.
      #
      # Tagging policy: most pgbus metrics are cluster-wide (the queue
      # depth in PostgreSQL is the same regardless of which host reads it),
      # so cluster-wide gauges are emitted WITHOUT a hostname tag — every
      # host sends the same value and AppSignal's last-write-wins
      # semantics keep the dashboard correct. The only gauge that is
      # genuinely per-host is `active_processes`, which the probe filters
      # to this host before tagging.
      module Probe
        METRIC_PREFIX = "pgbus_"
        private_constant :METRIC_PREFIX

        class << self
          def install! # rubocop:disable Naming/PredicateMethod
            return false if @installed

            ::Appsignal::Probes.register :pgbus, new_probe_instance
            @installed = true
            true
          end

          def installed?
            @installed == true
          end

          def reset!
            ::Appsignal::Probes.unregister(:pgbus) if defined?(::Appsignal::Probes) &&
                                                      ::Appsignal::Probes.respond_to?(:unregister)
            @installed = false
          end

          # Visible for testing — returns a fresh runnable probe.
          def new_probe_instance
            Runner.new
          end
        end

        # The actual probe object; AppSignal calls #call once per minute.
        class Runner
          def initialize(data_source: nil, client: nil)
            @data_source = data_source
            @client = client
            @hostname = Socket.gethostname
          end

          def call
            return unless data_source

            track_queues
            track_processes
            track_summary
            track_streams
            track_pool
          end

          private

          def data_source
            @data_source ||=
              (::Pgbus::Web::DataSource.new if defined?(::Pgbus::Web::DataSource))
          end

          def client
            @client ||= (::Pgbus.client if defined?(::Pgbus) && ::Pgbus.respond_to?(:client))
          end

          def track_queues
            data_source.queues_with_metrics.each do |q|
              tags = { queue: q[:name] }
              gauge "queue_depth", q[:queue_length], tags
              gauge "queue_visible_depth", q[:queue_visible_length], tags
              gauge "queue_paused", q[:paused] ? 1 : 0, tags
              age = q[:oldest_msg_age_sec]
              gauge "queue_oldest_message_age_seconds", age, tags if age
              claimable_age = q[:oldest_claimable_age_sec]
              gauge "queue_oldest_claimable_age_seconds", claimable_age, tags if claimable_age
              # Latency = time the oldest *claimable* message has waited for
              # pickup; a queue holding only vt-parked (scheduled/backoff)
              # messages is healthy, so 0 — not the raw enqueued_at age, which
              # grows at wall-clock rate on a parked message (issue #389).
              gauge "queue_latency", (claimable_age || 0) * 1_000, tags
            end
          rescue StandardError => e
            log_failure("queue metrics", e)
          end

          def track_processes
            local_count = data_source.processes.count { |p| p[:hostname] == @hostname }
            gauge "active_processes", local_count, { hostname: @hostname }
          rescue StandardError => e
            log_failure("process metrics", e)
          end

          def track_summary
            stats = data_source.summary_stats
            gauge "total_queues", stats[:total_queues]
            gauge "total_depth", stats[:total_depth]
            gauge "total_visible", stats[:total_visible]
            gauge "dlq_depth", stats[:dlq_depth]
            gauge "failed_events_total", stats[:failed_count]
            gauge "throughput_rate", stats[:throughput_rate]
            gauge "total_dead_tuples", stats[:total_dead_tuples]
            gauge "tables_needing_vacuum", stats[:tables_needing_vacuum]
            gauge "oldest_transaction_age_seconds", stats[:oldest_transaction_age_sec]
            # Unlabelled, same rationale as the /pgbus/api/metrics family:
            # concurrency keys are per-record and would be unbounded as tags.
            gauge "concurrency_blocked_executions", stats[:parked_total]
            gauge "concurrency_blocked_oldest_age_seconds", stats[:oldest_parked_age_sec]
            gauge "concurrency_slots_held", stats[:slots_held]
          rescue StandardError => e
            log_failure("summary metrics", e)
          end

          # The PGMQ connection pool is per-process (each host owns its own
          # pool), so — unlike the cluster-wide queue/summary gauges — these
          # are tagged with the hostname, same policy as active_processes.
          # pool_stats already rescues to {} internally; the outer rescue here
          # keeps a probe iteration alive if the client itself is unavailable.
          def track_pool
            stats = client&.pool_stats || {}
            return if stats.empty?

            tags = { hostname: @hostname }
            gauge "pool_size", stats[:size], tags
            gauge "pool_available", stats[:available], tags
          rescue StandardError => e
            log_failure("pool metrics", e)
          end

          def track_streams
            return unless data_source.respond_to?(:stream_stats_available?) &&
                          data_source.stream_stats_available?

            summary = data_source.stream_stats_summary
            gauge "stream_broadcasts_60m", summary[:broadcasts]
            gauge "stream_connects_60m", summary[:connects]
            gauge "stream_disconnects_60m", summary[:disconnects]
            gauge "stream_active_connections", summary[:active_estimate]
            gauge "stream_avg_fanout", summary[:avg_fanout]
            gauge "stream_avg_broadcast_ms", summary[:avg_broadcast_ms]
          rescue StandardError => e
            log_failure("stream metrics", e)
          end

          def gauge(key, value, tags = {})
            return if value.nil?

            ::Appsignal.set_gauge("#{METRIC_PREFIX}#{key}", value, tags)
          end

          def log_failure(label, error)
            Pgbus.logger.debug do
              "[Pgbus::AppSignal::Probe] #{label} failed: #{error.class}: #{error.message}"
            end
          end
        end
      end
    end
  end
end
