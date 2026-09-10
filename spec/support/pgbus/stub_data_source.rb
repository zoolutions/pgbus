# frozen_string_literal: true

module Pgbus
  module Test
    # In-memory stand-in for Pgbus::Web::DataSource. Injected via
    # Pgbus.configuration.web_data_source so dashboard/API request specs and
    # system specs run without a database. Every mutating call is recorded in
    # #calls so specs can assert the controller reached the data source with the
    # right arguments.
    class StubDataSource
      attr_accessor :stats, :queues, :processes_list, :failed_events_list,
                    :dlq_messages_list, :events_list, :subscribers_list, :jobs_list,
                    :paused_queues, :locks_list, :recurring_tasks_list,
                    :insights_summary, :insights_slowest, :insights_latency_by_queue,
                    :insights_latency_trend, :insights_throughput, :insights_status_counts,
                    :stream_stats_available, :stream_summary, :top_streams_list,
                    :pending_events_list, :outbox_stats_hash, :outbox_entries_list,
                    :batches_list, :batch_detail_hash, :concurrency_stats_hash,
                    :promoted_count
      attr_reader :calls

      def initialize
        @stats = default_stats
        @queues = default_queues
        @processes_list = default_processes
        @failed_events_list = []
        @dlq_messages_list = []
        @events_list = []
        @subscribers_list = []
        @jobs_list = []
        @paused_queues = []
        @locks_list = []
        @recurring_tasks_list = []
        @insights_summary = default_insights_summary
        @insights_slowest = []
        @insights_latency_by_queue = []
        @insights_latency_trend = []
        @insights_throughput = []
        @insights_status_counts = { "success" => 0, "failed" => 0, "dead_lettered" => 0 }
        @stream_stats_available = false
        @stream_summary = default_stream_summary
        @top_streams_list = []
        @pending_events_list = []
        @outbox_stats_hash = default_outbox_stats
        @outbox_entries_list = []
        @batches_list = []
        @batch_detail_hash = nil
        @concurrency_stats_hash = default_concurrency_stats
        @promoted_count = 0
        @calls = Hash.new { |h, k| h[k] = [] }
      end

      def summary_stats = @stats
      def reset_cache! = self
      def queues_with_metrics = @queues
      def queue_detail(name) = @queues.find { |q| q[:name].include?(name) }
      def processes = @processes_list
      def failed_events(page: 1, per_page: 25) = @failed_events_list
      def failed_events_count = @failed_events_list.size
      def failed_event(id) = @failed_events_list.find { |e| e["id"].to_s == id.to_s }

      def dlq_messages(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        @dlq_messages_list.slice(offset, per_page) || []
      end

      def dlq_total_count = @dlq_messages_list.size
      def dlq_message_detail(msg_id) = @dlq_messages_list.find { |m| m[:msg_id].to_s == msg_id.to_s }
      def processed_events(page: 1, per_page: 25) = @events_list
      def processed_events_count = @events_list.size
      def processed_event(id) = @events_list.find { |e| e["id"].to_s == id.to_s }
      def registered_subscribers = @subscribers_list
      def jobs(queue_name: nil, page: 1, per_page: 25) = @jobs_list
      def batches(limit: 100) = @batches_list
      def batch_detail(batch_id) = @batch_detail_hash
      def batches_count = @batches_list.size
      def active_batches_count = 0
      def job_locks = @locks_list
      def concurrency_stats = @concurrency_stats_hash
      def queue_paused?(name) = @paused_queues.include?(name)
      def recurring_tasks = @recurring_tasks_list

      def recurring_task(id)
        @recurring_tasks_list.find { |t| t[:id].to_s == id.to_s }&.merge(executions: [])
      end

      def toggle_recurring_task(id)
        task = @recurring_tasks_list.find { |t| t[:id].to_s == id.to_s }
        return nil unless task

        task[:enabled] = !task[:enabled]
        record(:toggle_recurring_task, id)
        task[:enabled] ? :enabled : :disabled
      end

      def enqueue_recurring_task_now(id) = record(:enqueue_recurring_task_now, id)

      def purge_queue(name)          = record(:purge_queue, name)
      def drop_queue(name)           = record(:drop_queue, name)
      def pause_queue(name, reason: nil) = record(:pause_queue, name, reason)
      def resume_queue(name) = record(:resume_queue, name)
      def retry_job(queue_name, msg_id)   = record(:retry_job, queue_name, msg_id)
      def discard_job(queue_name, msg_id) = record(:discard_job, queue_name, msg_id)
      def retry_failed_event(id)     = record(:retry_failed_event, id)
      def discard_failed_event(id)   = record(:discard_failed_event, id)
      def retry_all_failed           = record(:retry_all_failed) && @failed_events_list.size
      def discard_all_failed         = record(:discard_all_failed) && @failed_events_list.size
      def discard_all_enqueued       = record(:discard_all_enqueued) && @jobs_list.size
      def retry_dlq_message(queue_name, msg_id) = record(:retry_dlq_message, queue_name, msg_id)
      def discard_dlq_message(queue_name, msg_id) = record(:discard_dlq_message, queue_name, msg_id)
      def retry_all_dlq              = record(:retry_all_dlq) && @dlq_messages_list.size
      def discard_all_dlq            = record(:discard_all_dlq) && @dlq_messages_list.size
      def pending_events(page: 1, per_page: 25) = @pending_events_list
      def replay_event(event) = record(:replay_event, event)
      def discard_event(queue_name, msg_id) = record(:discard_event, queue_name, msg_id)
      def mark_event_handled(queue_name, msg_id, handler_class) = record(:mark_event_handled, queue_name, msg_id, handler_class)
      def edit_event_payload(queue_name, msg_id, payload) = record(:edit_event_payload, queue_name, msg_id, payload)
      def reroute_event(source_queue, msg_id, target_queue) = record(:reroute_event, source_queue, msg_id, target_queue)
      def discard_selected_events(selections) = record(:discard_selected_events, selections) && selections.size

      def handler_queue_physical_names
        @subscribers_list.map { |s| s[:physical_queue_name] || s[:queue_name] }.uniq
      end

      def handler_class_for_queue(queue_name)
        sub = @subscribers_list.find do |s|
          s[:physical_queue_name] == queue_name || s[:queue_name] == queue_name
        end
        sub && sub[:handler_class]
      end

      def discard_lock(key)          = record(:discard_lock, key) && 1
      def discard_locks(keys)        = record(:discard_locks, keys) && keys.size
      def discard_all_locks          = record(:discard_all_locks) && @locks_list.size
      # The real method returns the number of jobs PROMOTED (bounded by the
      # promote cap and slot availability), not the parked total — discard is
      # the one that drops every parked job.
      def release_concurrency_key(key) = record(:release_concurrency_key, key) && @promoted_count
      def discard_parked_jobs(key) = record(:discard_parked_jobs, key) && @concurrency_stats_hash[:parked_total]

      def called?(method_name) = @calls.key?(method_name)

      # Insights (job stats)
      def job_stats_summary(minutes: 60) = @insights_summary
      def slowest_job_classes(limit: 10, minutes: 60) = @insights_slowest.first(limit)
      def latency_by_queue(minutes: 60) = @insights_latency_by_queue
      def latency_trend(minutes: 60) = @insights_latency_trend
      def job_throughput(minutes: 60) = @insights_throughput
      def job_status_counts(minutes: 60) = @insights_status_counts

      # Live stream metrics (always-on in-memory counters)
      def live_stream_metrics
        { streams: {}, totals: { broadcasts: 0, active_connections: 0, total_connections: 0, streams: 0 } }
      end

      # Insights (stream stats — opt-in, default unavailable)
      def stream_stats_available? = @stream_stats_available
      def stream_stats_summary(minutes: 60) = @stream_summary
      def top_streams(limit: 10, minutes: 60) = @top_streams_list.first(limit)

      # Outbox
      def outbox_stats = @outbox_stats_hash
      def outbox_entries(page: 1, per_page: 25) = @outbox_entries_list

      # Queue health
      def queue_health_stats = default_health_stats
      def queue_health_detail(_name) = { tables: [], oldest_transaction_age_sec: nil }

      private

      def record(method_name, *args)
        @calls[method_name] << args
        true
      end

      def default_stats
        { total_queues: 2, total_depth: 15, total_visible: 12,
          active_processes: 1, failed_count: 3, dlq_depth: 2,
          recurring_count: 0, throughput_rate: 0.0,
          total_dead_tuples: 0, tables_needing_vacuum: 0,
          oldest_transaction_age_sec: nil,
          parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0 }
      end

      def default_insights_summary
        {
          total: 342, success: 335, failed: 5, dead_lettered: 2,
          avg_duration_ms: 127.4, max_duration_ms: 4521.0,
          avg_latency_ms: 0, p50_latency_ms: 0, p95_latency_ms: 0,
          p99_latency_ms: 0, avg_retries: 0
        }
      end

      def default_stream_summary
        {
          broadcasts: 0, connects: 0, disconnects: 0,
          active_estimate: 0, avg_fanout: 0,
          avg_broadcast_ms: 0, avg_connect_ms: 0
        }
      end

      def default_outbox_stats
        { unpublished: 0, total: 0, oldest_unpublished_age: nil }
      end

      def default_concurrency_stats
        { parked_total: 0, oldest_parked_age_sec: nil, slots_held: 0, keys_at_limit: 0, keys: [] }
      end

      def default_health_stats
        {
          total_dead_tuples: 0, total_live_tuples: 0, worst_bloat_ratio: 0.0,
          tables_needing_vacuum: 0, oldest_vacuum_ago_sec: nil,
          oldest_transaction_age_sec: nil, tables: []
        }
      end

      def default_queues
        [
          { name: "pgbus_default", queue_length: 10, queue_visible_length: 8, parked_length: 2,
            oldest_msg_age_sec: 120, oldest_claimable_age_sec: 90,
            newest_msg_age_sec: 5, total_messages: 500 },
          { name: "pgbus_default_dlq", queue_length: 2, queue_visible_length: 2, parked_length: 0,
            oldest_msg_age_sec: 3600, oldest_claimable_age_sec: 3600,
            newest_msg_age_sec: 1800, total_messages: 5 }
        ]
      end

      def default_processes
        [{ id: 1, kind: "worker", hostname: "test-host", pid: 12_345,
           metadata: { "queues" => "default" }, last_heartbeat_at: Time.now, healthy: true,
           created_at: Time.now }]
      end
    end
  end
end
