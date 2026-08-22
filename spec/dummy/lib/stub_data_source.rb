# frozen_string_literal: true

# Rich stub data source for dashboard QA without a database.
# Used by `rake dummy:server` (triggered via PGBUS_STUB_DATA=1).
module DummyApp
  class StubDataSource
    def self.build
      new
    end

    def summary_stats
      { total_queues: 4, total_depth: 127, total_visible: 98,
        active_processes: 2, failed_count: 5, dlq_depth: 3,
        recurring_count: 14, throughput_rate: 42.7,
        total_dead_tuples: 1_250, tables_needing_vacuum: 1,
        oldest_transaction_age_sec: 8 }
    end

    def queues_with_metrics
      [
        { name: "pgbus_default", queue_length: 85, queue_visible_length: 62, parked_length: 23,
          oldest_msg_age_sec: 300, oldest_claimable_age_sec: 240,
          newest_msg_age_sec: 2, total_messages: 12_450 },
        { name: "pgbus_mailers", queue_length: 22, queue_visible_length: 18, parked_length: 4,
          oldest_msg_age_sec: 45, oldest_claimable_age_sec: 30,
          newest_msg_age_sec: 1, total_messages: 8_320 },
        { name: "pgbus_events", queue_length: 15, queue_visible_length: 13, parked_length: 2,
          oldest_msg_age_sec: 120, oldest_claimable_age_sec: 100,
          newest_msg_age_sec: 5, total_messages: 45_000 },
        { name: "pgbus_default_dlq", queue_length: 3, queue_visible_length: 3, parked_length: 0,
          oldest_msg_age_sec: 7200, oldest_claimable_age_sec: 7200,
          newest_msg_age_sec: 3600, total_messages: 47 }
      ]
    end

    def queue_detail(name)
      queues_with_metrics.find { |q| q[:name] == name }
    end

    def queue_paused?(name)
      name == "pgbus_mailers"
    end

    def jobs(queue_name: nil, page: 1, per_page: 25) # rubocop:disable Lint/UnusedMethodArgument
      job_classes = %w[CaptureSpaceStatsJob SendWelcomeEmailJob ProcessPaymentJob SyncInventoryJob GenerateReportJob]
      Array.new(6) do |i|
        id = 900 - i
        job_class = job_classes[i % job_classes.size]
        {
          msg_id: id, queue_name: queue_name || "pgbus_default", read_ct: i,
          enqueued_at: (i * 5).minutes.ago.utc.iso8601,
          vt: (i * 3).minutes.ago.utc.iso8601,
          last_read_at: i.positive? ? (i * 2).minutes.ago.utc.iso8601 : nil,
          message: {
            job_class: job_class, job_id: "job-#{SecureRandom.hex(6)}",
            queue_name: queue_name || "default", priority: [1, 2, 5][i % 3],
            arguments: sample_arguments(job_class), locale: "en", timezone: "UTC",
            scheduled_at: i.even? ? 1.minute.from_now.utc.iso8601 : nil
          }.compact.to_json,
          headers: i.even? ? { "X-Request-Id" => SecureRandom.uuid }.to_json : nil
        }
      end
    end

    def failed_events(page: 1, per_page: 25) # rubocop:disable Lint/UnusedMethodArgument
      [
        { "id" => 1, "handler_class" => "Billing::InvoiceHandler", "event_type" => "invoice.created",
          "error_class" => "Stripe::InvalidRequestError", "error_message" => "No such customer: cus_xxx",
          "retry_count" => 3, "failed_at" => 30.minutes.ago.utc.iso8601,
          "queue_name" => "pgbus_default" },
        { "id" => 2, "handler_class" => "Notifications::SlackHandler", "event_type" => "user.signed_up",
          "error_class" => "Net::ReadTimeout", "error_message" => "execution expired",
          "retry_count" => 1, "failed_at" => 2.hours.ago.utc.iso8601,
          "queue_name" => "pgbus_events" }
      ]
    end

    def failed_events_count = 2
    def failed_event(id) = failed_events.find { |e| e["id"].to_s == id.to_s }

    def dlq_messages(page: 1, per_page: 25) # rubocop:disable Lint/UnusedMethodArgument
      [
        { msg_id: 501, queue_name: "pgbus_default_dlq", read_ct: 6,
          enqueued_at: 2.hours.ago.utc.iso8601, vt: 1.hour.ago.utc.iso8601,
          message: { job_class: "ProcessPaymentJob", job_id: "dlq-aaa",
                     arguments: [{ amount: 99.99 }], priority: 1 }.to_json },
        { msg_id: 502, queue_name: "pgbus_default_dlq", read_ct: 4,
          enqueued_at: 5.hours.ago.utc.iso8601, vt: 4.hours.ago.utc.iso8601,
          message: { job_class: "SyncInventoryJob", job_id: "dlq-bbb",
                     arguments: [{ sku: "WIDGET-42" }], priority: 2 }.to_json }
      ]
    end

    def dlq_message_detail(msg_id)
      dlq_messages.find { |m| m[:msg_id].to_s == msg_id.to_s }
    end

    def processes
      [
        { id: 1, kind: "worker", hostname: "web-01.prod", pid: 12_345,
          metadata: { "queues" => "default,mailers", "threads" => 5 },
          last_heartbeat_at: 10.seconds.ago, healthy: true, created_at: 2.hours.ago },
        { id: 2, kind: "worker", hostname: "worker-01.prod", pid: 67_890,
          metadata: { "queues" => "events", "threads" => 3 },
          last_heartbeat_at: 45.seconds.ago, healthy: true, created_at: 1.day.ago }
      ]
    end

    def recurring_tasks
      tasks = %w[capture_stats cleanup_old sync_exchange_rates generate_reports
                 purge_expired_tokens refresh_cache send_digest_emails
                 check_ssl_certs rotate_logs archive_events
                 update_search_index reindex_products vacuum_analyze health_check]
      tasks.each_with_index.map do |key, i|
        {
          id: i + 1, key: key, class_name: key.camelize.concat("Job"),
          command: nil, schedule: sample_schedule(i),
          human_schedule: sample_human_schedule(i),
          queue_name: %w[default maintenance events][i % 3],
          priority: [1, 2, 5][i % 3],
          description: "#{key.humanize} recurring task",
          enabled: i != 1, static: i < 10,
          next_run_at: i == 1 ? nil : ((i * 5) + 1).minutes.from_now,
          last_run_at: i.positive? ? (i * 3).minutes.ago : nil,
          created_at: 30.days.ago, updated_at: 1.day.ago
        }
      end
    end

    def recurring_task(id)
      task = recurring_tasks.find { |t| t[:id].to_s == id.to_s }
      return nil unless task

      task.merge(executions: Array.new(5) do |i|
        { run_at: (i * 5).minutes.ago, created_at: (i * 5).minutes.ago }
      end)
    end

    def processed_events(page: 1, per_page: 25) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def processed_events_count = 0
    def processed_event(_id) = nil
    def registered_subscribers = []

    def batches(limit: 100) # rubocop:disable Lint/UnusedMethodArgument
      [
        { batch_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890", description: "Import users from CSV",
          status: "processing", total_jobs: 250, completed_jobs: 180, discarded_jobs: 3, failed_jobs: 3,
          progress_pct: 73, created_at: 45.minutes.ago, finished_at: nil },
        { batch_id: "b2c3d4e5-f6a7-8901-bcde-f12345678901", description: "Send welcome emails",
          status: "finished", total_jobs: 50, completed_jobs: 50, discarded_jobs: 0, failed_jobs: 0,
          progress_pct: 100, created_at: 2.hours.ago, finished_at: 1.hour.ago },
        { batch_id: "c3d4e5f6-a7b8-9012-cdef-123456789012", description: nil,
          status: "pending", total_jobs: 0, completed_jobs: 0, discarded_jobs: 0, failed_jobs: 0,
          progress_pct: 100, created_at: 5.minutes.ago, finished_at: 5.minutes.ago }
      ]
    end

    def batch_detail(batch_id)
      batch = batches.find { |b| b[:batch_id] == batch_id }
      return nil unless batch

      batch.merge(
        properties: '{"source":"admin","user_id":42}',
        on_finish_class: "BatchFinishJob",
        on_success_class: "BatchSuccessNotifyJob",
        on_discard_class: nil
      )
    end

    def batches_count = 3
    def active_batches_count = 1

    def job_locks
      [
        { lock_key: "uniqueness:ProcessPaymentJob:abc123", queue_name: "pgbus_default",
          msg_id: 900, created_at: 5.minutes.ago, age_seconds: 300 },
        { lock_key: "uniqueness:SendWelcomeEmailJob:def456", queue_name: "pgbus_mailers",
          msg_id: 898, created_at: 2.minutes.ago, age_seconds: 120 }
      ]
    end

    def outbox_stats
      { unpublished: 3, total: 1250, oldest_unpublished_age: 45 }
    end

    def outbox_entries(page: 1, per_page: 25) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def job_stats_summary(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      { total: 342, success: 335, failed: 5, dead_lettered: 2,
        avg_duration_ms: 127.4, max_duration_ms: 4521.0 }
    end

    def slowest_job_classes(limit: 10, minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def latency_by_queue(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def latency_trend(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def job_throughput(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      []
    end

    def job_status_counts(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      { "success" => 335, "failed" => 5, "dead_lettered" => 2 }
    end

    def live_stream_metrics
      {
        streams: {
          "chat:lobby" => { broadcasts: 420, active_connections: 12, total_connections: 92 },
          "orders:dashboard" => { broadcasts: 312, active_connections: 3, total_connections: 45 },
          "notifications:admin" => { broadcasts: 214, active_connections: 1, total_connections: 18 }
        },
        totals: { broadcasts: 946, active_connections: 16, total_connections: 155, streams: 3 }
      }
    end

    # Stream stats — opt-in in production, always "available" in the
    # dummy app so the Insights QA surface demos the section.
    def stream_stats_available?
      true
    end

    def stream_stats_summary(minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      {
        broadcasts: 1_248, connects: 92, disconnects: 87,
        active_estimate: 5, avg_fanout: 7.3,
        avg_broadcast_ms: 4.1, avg_connect_ms: 12.6
      }
    end

    def top_streams(limit: 10, minutes: 60) # rubocop:disable Lint/UnusedMethodArgument
      [
        { stream_name: "chat:lobby", count: 420, avg_fanout: 18.2, avg_ms: 3.1 },
        { stream_name: "orders:dashboard", count: 312, avg_fanout: 4.5, avg_ms: 5.8 },
        { stream_name: "notifications:admin", count: 214, avg_fanout: 2.1, avg_ms: 2.4 }
      ].first(limit)
    end

    def queue_health_stats
      {
        total_dead_tuples: 1_250, total_live_tuples: 65_000,
        worst_bloat_ratio: 0.12, tables_needing_vacuum: 1,
        oldest_vacuum_ago_sec: 1800,
        oldest_transaction_age_sec: 8,
        tables: [
          { table: "pgmq.q_pgbus_default", kind: "queue",
            live_tuples: 45_000, dead_tuples: 800, bloat_ratio: 0.0175,
            last_vacuum_ago_sec: 120, last_vacuum: 2.minutes.ago, last_autovacuum: nil },
          { table: "pgmq.a_pgbus_default", kind: "archive",
            live_tuples: 12_000, dead_tuples: 350, bloat_ratio: 0.0283,
            last_vacuum_ago_sec: 1800, last_vacuum: nil, last_autovacuum: 30.minutes.ago },
          { table: "pgmq.q_pgbus_events", kind: "queue",
            live_tuples: 8_000, dead_tuples: 100, bloat_ratio: 0.1235,
            last_vacuum_ago_sec: 600, last_vacuum: 10.minutes.ago, last_autovacuum: nil }
        ]
      }
    end

    def queue_health_detail(_name)
      {
        tables: [
          { table: "pgmq.q_pgbus_default", kind: "queue",
            live_tuples: 45_000, dead_tuples: 800, bloat_ratio: 0.0175,
            last_vacuum_ago_sec: 120, last_vacuum: 2.minutes.ago, last_autovacuum: nil },
          { table: "pgmq.a_pgbus_default", kind: "archive",
            live_tuples: 12_000, dead_tuples: 350, bloat_ratio: 0.0283,
            last_vacuum_ago_sec: 1800, last_vacuum: nil, last_autovacuum: 30.minutes.ago }
        ],
        oldest_transaction_age_sec: 8
      }
    end

    # Mutating actions (no-ops for QA)
    def purge_queue(_name) = true
    def drop_queue(_name) = true
    def pause_queue(_name, reason: nil) = true # rubocop:disable Lint/UnusedMethodArgument
    def resume_queue(_name) = true
    def retry_job(_queue, _id) = true
    def discard_job(_queue, _id) = true
    def retry_failed_event(_id) = true
    def discard_failed_event(_id) = true
    def retry_all_failed = 2
    def discard_all_failed = 2
    def discard_all_enqueued = 6
    def retry_dlq_message(_queue, _id) = true
    def discard_dlq_message(_queue, _id) = true
    def retry_all_dlq = 2
    def discard_all_dlq = 2
    def replay_event(_event) = true
    def toggle_recurring_task(_id) = true
    def enqueue_recurring_task_now(_id) = true
    def discard_lock(_key) = 1
    def discard_locks(keys) = keys.size
    def discard_all_locks = 2

    private

    def sample_arguments(job_class)
      case job_class
      when "ProcessPaymentJob" then [{ amount: 49.99, currency: "USD" }]
      when "SendWelcomeEmailJob" then [{ user_id: 42 }]
      when "SyncInventoryJob" then [{ sku: "WIDGET-42", warehouse: "US-EAST" }]
      else []
      end
    end

    def sample_schedule(index)
      schedules = ["*/5 * * * *", "0 3 * * *", "0 */6 * * *", "0 0 * * 1",
                   "*/15 * * * *", "0 */2 * * *", "0 8 * * 1-5", "0 0 1 * *",
                   "30 4 * * *", "0 2 * * 0", "*/10 * * * *", "0 6 * * *",
                   "0 1 * * *", "*/1 * * * *"]
      schedules[index % schedules.size]
    end

    def sample_human_schedule(index)
      descriptions = ["Every 5 minutes", "Daily at 3:00 AM", "Every 6 hours", "Weekly on Monday",
                      "Every 15 minutes", "Every 2 hours", "Weekdays at 8:00 AM", "Monthly on the 1st",
                      "Daily at 4:30 AM", "Weekly on Sunday at 2:00 AM", "Every 10 minutes",
                      "Daily at 6:00 AM", "Daily at 1:00 AM", "Every minute"]
      descriptions[index % descriptions.size]
    end
  end
end
