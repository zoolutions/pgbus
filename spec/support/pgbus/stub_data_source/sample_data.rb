# frozen_string_literal: true

module Pgbus
  module Test
    class StubDataSource
      # Rich rows for every list the dashboard renders: the one dataset behind
      # rake dummy:server, Lighthouse, the PR screenshots and the accessibility
      # gate (#498). StubDataSource.new stays sparse for the empty-state specs;
      # call fill_sample_data! to get this.
      module SampleData
        OutboxRow = Struct.new(:id, :routing_key, :queue_name, :payload, :priority, :published_at,
                               :created_at, keyword_init: true)

        JOB_CLASSES = %w[CaptureSpaceStatsJob SendWelcomeEmailJob ProcessPaymentJob SyncInventoryJob
                         GenerateReportJob].freeze
        RECURRING_KEYS = %w[capture_stats cleanup_old sync_exchange_rates generate_reports purge_expired_tokens
                            refresh_cache send_digest_emails check_ssl_certs rotate_logs archive_events
                            update_search_index reindex_products vacuum_analyze health_check].freeze
        SCHEDULES = [
          ["*/5 * * * *", "Every 5 minutes"], ["0 3 * * *", "Daily at 3:00 AM"], ["0 */6 * * *", "Every 6 hours"],
          ["0 0 * * 1", "Weekly on Monday"], ["*/15 * * * *", "Every 15 minutes"], ["0 */2 * * *", "Every 2 hours"],
          ["0 8 * * 1-5", "Weekdays at 8:00 AM"], ["0 0 1 * *", "Monthly on the 1st"],
          ["30 4 * * *", "Daily at 4:30 AM"], ["0 2 * * 0", "Weekly on Sunday at 2:00 AM"],
          ["*/10 * * * *", "Every 10 minutes"], ["0 6 * * *", "Daily at 6:00 AM"], ["0 1 * * *", "Daily at 1:00 AM"],
          ["*/1 * * * *", "Every minute"]
        ].freeze

        def fill_sample_data!
          now = Time.now
          @stats = sample_stats
          @queues = sample_queues
          @paused_queues = ["pgbus_mailers"]
          @jobs_list = sample_jobs(now)
          @job_rows_list = sample_job_rows(now.utc)
          @jobs_ahead_hash = { ["pgbus_default", 905] => 0, ["pgbus_mailers", 906] => 14 }
          @job_context = @job_context.with(paused: Set["mailers"])
          @failed_events_list = sample_failed_events(now)
          @dlq_messages_list = sample_dlq_messages(now)
          @processes_list = sample_processes(now)
          @recurring_tasks_list = sample_recurring_tasks(now)
          @recurring_executions_list = Array.new(5) { |i| { run_at: now - (i * 300), created_at: now - (i * 300) } }
          @batches_list = sample_batches(now)
          @batch_detail_hash = @batches_list.first.merge(
            properties: '{"source":"admin","user_id":42}', on_finish_class: "BatchFinishJob",
            on_success_class: "BatchSuccessNotifyJob", on_discard_class: nil
          )
          @locks_list = sample_locks(now)
          @concurrency_stats_hash = sample_concurrency(now)
          @subscribers_list = sample_subscribers
          @pending_events_list = sample_pending_events(now)
          @events_list = sample_processed_events(now)
          @outbox_stats_hash = { unpublished: 1, total: 1250, oldest_unpublished_age: 45 }
          @outbox_entries_list = sample_outbox_entries(now)
          fill_sample_insights(now)
          @health_stats = sample_health_stats(now)
          @health_detail_hash = { tables: @health_stats[:tables].first(2), oldest_transaction_age_sec: 8 }
          self
        end

        private

        def fill_sample_insights(now)
          @insights_summary = { total: 342, success: 335, failed: 5, dead_lettered: 2,
                                avg_duration_ms: 127.4, max_duration_ms: 4521.0,
                                avg_latency_ms: 85.2, p50_latency_ms: 40.0, p95_latency_ms: 310.0,
                                p99_latency_ms: 920.0, avg_retries: 0.1 }
          @insights_slowest = [
            { job_class: "GenerateReportJob", count: 12, avg_ms: 2310.5, max_ms: 4521.0 },
            { job_class: "SyncInventoryJob", count: 48, avg_ms: 640.2, max_ms: 1820.0 },
            { job_class: "ProcessPaymentJob", count: 97, avg_ms: 210.7, max_ms: 990.0 }
          ]
          @insights_latency_by_queue = [
            { queue_name: "default", count: 210, avg_ms: 64.3, p95_ms: 280.0 },
            { queue_name: "mailers", count: 98, avg_ms: 120.9, p95_ms: 410.0 }
          ]
          minutes = (0...12).map { |i| (now - ((11 - i) * 300)).utc.iso8601 }
          @insights_latency_trend = minutes.each_with_index.map { |t, i| { time: t, avg_ms: 60 + (i * 3), p95_ms: 250 + (i * 7) } }
          @insights_throughput = minutes.each_with_index.map { |t, i| { time: t, count: 20 + ((i * 7) % 15) } }
          @insights_status_counts = { "success" => 335, "failed" => 5, "dead_lettered" => 2 }
          @stream_stats_available = true
          @stream_summary = { broadcasts: 1_248, connects: 92, disconnects: 87, active_estimate: 5,
                              avg_fanout: 7.3, avg_broadcast_ms: 4.1, avg_connect_ms: 12.6 }
          @top_streams_list = [
            { stream_name: "chat:lobby", count: 420, avg_fanout: 18.2, avg_ms: 3.1 },
            { stream_name: "orders:dashboard", count: 312, avg_fanout: 4.5, avg_ms: 5.8 },
            { stream_name: "notifications:admin", count: 214, avg_fanout: 2.1, avg_ms: 2.4 }
          ]
          @live_stream_metrics_hash = {
            streams: {
              "chat:lobby" => { broadcasts: 420, active_connections: 12, total_connections: 92 },
              "orders:dashboard" => { broadcasts: 312, active_connections: 3, total_connections: 45 },
              "notifications:admin" => { broadcasts: 214, active_connections: 1, total_connections: 18 }
            },
            totals: { broadcasts: 946, active_connections: 16, total_connections: 155, streams: 3 }
          }
        end

        def sample_stats
          { total_queues: 4, total_depth: 125, total_visible: 96, active_processes: 4, failed_count: 5, dlq_depth: 3,
            recurring_count: 14, throughput_rate: 42.7, total_dead_tuples: 1_250, tables_needing_vacuum: 1,
            oldest_transaction_age_sec: 8, parked_total: 5, oldest_parked_age_sec: 300, slots_held: 3,
            keys_at_limit: 1 }
        end

        # pgbus_default must precede pgbus_default_dlq: queue_detail matches by
        # substring, and /pgbus/queues/pgbus_default is an audited URL.
        def sample_queues
          [
            { name: "pgbus_default", queue_length: 85, queue_visible_length: 62, parked_length: 23,
              oldest_msg_age_sec: 300, oldest_claimable_age_sec: 240, newest_msg_age_sec: 2, total_messages: 12_450 },
            { name: "pgbus_mailers", queue_length: 22, queue_visible_length: 18, parked_length: 4,
              oldest_msg_age_sec: 45, oldest_claimable_age_sec: 30, newest_msg_age_sec: 1, total_messages: 8_320 },
            { name: "pgbus_events", queue_length: 15, queue_visible_length: 13, parked_length: 2,
              oldest_msg_age_sec: 120, oldest_claimable_age_sec: 100, newest_msg_age_sec: 5, total_messages: 45_000 },
            { name: "pgbus_default_dlq", queue_length: 3, queue_visible_length: 3, parked_length: 0,
              oldest_msg_age_sec: 7200, oldest_claimable_age_sec: 7200, newest_msg_age_sec: 3600, total_messages: 47 }
          ]
        end

        def sample_jobs(now)
          Array.new(6) do |i|
            job_class = JOB_CLASSES[i % JOB_CLASSES.size]
            { msg_id: 900 - i, queue_name: "pgbus_default", read_ct: i,
              enqueued_at: (now - (i * 300)).utc.iso8601, vt: i == 1 ? now + 900 : (now - (i * 180)).utc.iso8601,
              last_read_at: i.positive? ? (now - (i * 120)).utc.iso8601 : nil,
              message: { job_class: job_class, job_id: "job-#{900 - i}", queue_name: "default",
                         priority: [1, 2, 5][i % 3], arguments: sample_arguments(job_class), locale: "en",
                         timezone: "UTC", scheduled_at: sample_scheduled_at(now, i) }.compact.to_json,
              headers: i.even? ? { "X-Request-Id" => "req-#{900 - i}" }.to_json : nil }
          end
        end

        # One row per state, plus an orphaned failed row whose message has
        # left its queue and a blocked row waiting on a concurrency slot.
        def sample_job_rows(now)
          base = { source: "queue", queue_name: "pgbus_default", logical_queue: "default", read_ct: 0,
                   last_read_at: nil, error_class: nil, error_message: nil, failed_event_id: nil,
                   concurrency_key: nil, slots_held: nil, slots_max: nil, headers: nil }
          [
            base.merge(id: 905, msg_id: 905, job_class: "SendWelcomeEmailJob", state: "ready",
                       enqueued_at: now - 20, vt: now - 20),
            base.merge(id: 906, msg_id: 906, job_class: "SendWelcomeEmailJob", state: "ready",
                       queue_name: "pgbus_mailers", logical_queue: "mailers", enqueued_at: now - 30, vt: now - 30),
            base.merge(id: 904, msg_id: 904, job_class: "GenerateReportJob", state: "scheduled",
                       enqueued_at: now - 60, vt: now + 7200),
            base.merge(id: 903, msg_id: 903, job_class: "CaptureSpaceStatsJob", state: "running", read_ct: 1,
                       enqueued_at: now - 90, last_read_at: now - 12, vt: now + 48),
            base.merge(id: 902, msg_id: 902, job_class: "ProcessPaymentJob", state: "retrying", read_ct: 2,
                       enqueued_at: now - 600, last_read_at: now - 30, vt: now + 40, failed_event_id: 1,
                       error_class: "Net::ReadTimeout", error_message: "execution expired"),
            base.merge(id: 901, msg_id: 901, job_class: "SyncInventoryJob", state: "ready", read_ct: 1,
                       enqueued_at: now - 900, last_read_at: now - 400, vt: now - 100),
            base.merge(source: "failed", id: 2, msg_id: 870, job_class: "SyncInventoryJob", state: "retrying",
                       read_ct: 4, enqueued_at: nil, vt: nil, failed_event_id: 2, queue_name: "events",
                       logical_queue: "events", error_class: "Stripe::InvalidRequestError",
                       error_message: "No such customer: cus_xxx"),
            base.merge(source: "blocked", id: 31, msg_id: nil, job_class: "ImportCsvJob", state: "blocked",
                       read_ct: nil, queue_name: "default", enqueued_at: now - 300, vt: nil,
                       concurrency_key: "ImportCsvJob/account:42", slots_held: 2, slots_max: 2)
          ].map do |row|
            row.merge(payload: { job_class: row[:job_class], job_id: "job-#{row[:id]}",
                                 arguments: sample_arguments(row[:job_class]), locale: "en" }.to_json,
                      sort_at: row[:enqueued_at])
          end
        end

        def sample_failed_events(now)
          backtrace = ["app/handlers/billing/invoice_handler.rb:27:in `handle'",
                       "lib/pgbus/event_bus/handler.rb:41:in `process'"].to_json
          [
            { "id" => 1, "handler_class" => "Billing::InvoiceHandler", "event_type" => "invoice.created",
              "error_class" => "Stripe::InvalidRequestError", "error_message" => "No such customer: cus_xxx",
              "retry_count" => 3, "failed_at" => (now - 1800).utc.iso8601, "queue_name" => "pgbus_default",
              "payload" => { job_class: "ProcessPaymentJob", arguments: [{ amount: 49.99 }] }.to_json,
              "backtrace" => backtrace },
            { "id" => 2, "handler_class" => "Notifications::SlackHandler", "event_type" => "user.signed_up",
              "error_class" => "Net::ReadTimeout", "error_message" => "execution expired",
              "retry_count" => 1, "failed_at" => (now - 7200).utc.iso8601, "queue_name" => "pgbus_events",
              "payload" => { job_class: "SyncInventoryJob", arguments: [{ sku: "WIDGET-42" }] }.to_json,
              "backtrace" => backtrace }
          ]
        end

        # payload["scheduled_at"] in every shape ActiveJob has used: an ISO string
        # (Rails >= 7.1) and epoch seconds (before), or absent.
        def sample_scheduled_at(now, index)
          return (now + 60).utc.iso8601 if index.even?

          (now + 120).to_f if index == 3
        end

        def sample_dlq_messages(now)
          [
            { msg_id: 501, queue_name: "pgbus_default_dlq", read_ct: 6, enqueued_at: (now - 7200).utc.iso8601,
              vt: (now - 3600).utc.iso8601, last_read_at: (now - 3600).utc.iso8601, headers: nil,
              message: { job_class: "ProcessPaymentJob", job_id: "dlq-aaa", arguments: [{ amount: 99.99 }],
                         priority: 1 }.to_json },
            { msg_id: 502, queue_name: "pgbus_default_dlq", read_ct: 4, enqueued_at: now - 18_000,
              vt: now - 14_400, last_read_at: now - 14_400, headers: nil,
              message: { job_class: "SyncInventoryJob", job_id: "dlq-bbb", arguments: [{ sku: "WIDGET-42" }],
                         priority: 2 }.to_json }
          ]
        end

        def sample_processes(now)
          [
            { id: 1, kind: "worker", hostname: "web-01.prod", pid: 12_345,
              metadata: { "queues" => "default,mailers", "threads" => 5 },
              last_heartbeat_at: now - 10, healthy: true, created_at: now - 7200 },
            { id: 2, kind: "worker", hostname: "worker-01.prod", pid: 67_890,
              metadata: { "queues" => "events", "threads" => 3 },
              last_heartbeat_at: now - 45, healthy: true, created_at: now - 86_400 },
            # A processes: 2 capsule (issue #503): one row per fork.
            *[1, 2].map do |n|
              { id: 2 + n, kind: "worker", hostname: "worker-02.prod", pid: 70_000 + n,
                metadata: { "capsule" => "render", "process" => "#{n}/2", "queues" => "render", "threads" => 1 },
                last_heartbeat_at: now - (5 * n), healthy: true, created_at: now - 3600 }
            end
          ]
        end

        def sample_recurring_tasks(now)
          RECURRING_KEYS.each_with_index.map do |key, i|
            schedule, human = SCHEDULES[i]
            { id: i + 1, key: key, class_name: "#{key.split("_").map(&:capitalize).join}Job", command: nil,
              schedule: schedule, human_schedule: human, queue_name: %w[default maintenance events][i % 3],
              priority: [1, 2, 5][i % 3], description: "#{key.tr("_", " ").capitalize} recurring task",
              enabled: i != 1, static: i < 10, next_run_at: i == 1 ? nil : now + (((i * 5) + 1) * 60),
              last_run_at: i.positive? ? now - (i * 180) : nil, created_at: now - (30 * 86_400),
              updated_at: now - 86_400 }
          end
        end

        def sample_batches(now)
          [
            { batch_id: "a1b2c3d4-e5f6-7890-abcd-ef1234567890", description: "Import users from CSV",
              status: "processing", total_jobs: 250, completed_jobs: 180, discarded_jobs: 3, failed_jobs: 3,
              progress_pct: 73, created_at: now - 2700, finished_at: nil },
            { batch_id: "b2c3d4e5-f6a7-8901-bcde-f12345678901", description: "Send welcome emails",
              status: "finished", total_jobs: 50, completed_jobs: 50, discarded_jobs: 0, failed_jobs: 0,
              progress_pct: 100, created_at: now - 7200, finished_at: now - 3600 },
            { batch_id: "c3d4e5f6-a7b8-9012-cdef-123456789012", description: nil,
              status: "pending", total_jobs: 0, completed_jobs: 0, discarded_jobs: 0, failed_jobs: 0,
              progress_pct: 100, created_at: now - 300, finished_at: now - 300 }
          ]
        end

        def sample_locks(now)
          [
            { lock_key: "uniqueness:ProcessPaymentJob:abc123", queue_name: "pgbus_default",
              msg_id: 900, created_at: now - 300, age_seconds: 300 },
            { lock_key: "uniqueness:SendWelcomeEmailJob:def456", queue_name: "pgbus_mailers",
              msg_id: 898, created_at: now - 120, age_seconds: 120 }
          ]
        end

        # One key at its limit with a live lease and parked jobs, one whose
        # lease has expired.
        def sample_concurrency(now)
          { parked_total: 5, oldest_parked_age_sec: 300, slots_held: 3, keys_at_limit: 1,
            keys: [
              { key: "ImportCsvJob/account:42", value: 2, max_value: 2, lease_fresh: true,
                expires_at: now + 240, parked_count: 5, oldest_parked_age_sec: 300 },
              { key: "SyncInventoryJob/warehouse:US-EAST", value: 1, max_value: 3, lease_fresh: false,
                expires_at: now - 60, parked_count: 0, oldest_parked_age_sec: nil }
            ] }
        end

        def sample_subscribers
          [
            { pattern: "invoice.*", handler_class: "Billing::InvoiceHandler", queue_name: "billing_invoice_handler",
              physical_queue_name: "pgbus_billing_invoice_handler" },
            { pattern: "user.signed_up", handler_class: "Notifications::SlackHandler",
              queue_name: "notifications_slack_handler", physical_queue_name: "pgbus_notifications_slack_handler" }
          ]
        end

        def sample_pending_events(now)
          [
            ["evt-pending-1", "invoice.created", "pgbus_billing_invoice_handler", { invoice_id: 7 }],
            ["evt-pending-2", "user.signed_up", "pgbus_notifications_slack_handler", { user_id: 42 }]
          ].each_with_index.map do |(event_id, type, queue, payload), i|
            enqueued_at = now - (600 * (i + 1))
            { msg_id: 701 + i, read_ct: i + 1, queue_name: queue, enqueued_at: i.zero? ? enqueued_at.utc.iso8601 : enqueued_at,
              last_read_at: (now - 60).utc.iso8601, vt: (now + 30).utc.iso8601, headers: nil,
              message: { event_id: event_id, event_type: type, payload: payload,
                         published_at: (now - (600 * (i + 1))).utc.iso8601 }.to_json }
          end
        end

        def sample_processed_events(now)
          [["Billing::InvoiceHandler", 120], ["Notifications::SlackHandler", 900],
           ["Billing::InvoiceHandler", 3600]].each_with_index.map do |(handler, ago), i|
            { "id" => i + 1, "event_id" => "evt-processed-#{i + 1}", "handler_class" => handler,
              "processed_at" => i.zero? ? now - ago : (now - ago).utc.iso8601 }
          end
        end

        def sample_outbox_entries(now)
          [
            OutboxRow.new(id: 3, routing_key: "orders.created", queue_name: nil, payload: { order_id: 17 }.to_json,
                          priority: 0, published_at: nil, created_at: now - 45),
            OutboxRow.new(id: 2, routing_key: "orders.paid", queue_name: nil, payload: { order_id: 16 }.to_json,
                          priority: 0, published_at: now - 290, created_at: now - 300),
            OutboxRow.new(id: 1, routing_key: nil, queue_name: "mailers", payload: { user_id: 42 }.to_json,
                          priority: 5, published_at: now - 590, created_at: now - 600)
          ]
        end

        def sample_health_stats(now)
          { total_dead_tuples: 1_250, total_live_tuples: 65_000, worst_bloat_ratio: 0.12, tables_needing_vacuum: 1,
            oldest_vacuum_ago_sec: 1800, oldest_transaction_age_sec: 8,
            tables: [
              { table: "pgmq.q_pgbus_default", kind: "queue", live_tuples: 45_000, dead_tuples: 800,
                bloat_ratio: 0.0175, last_vacuum_ago_sec: 120, last_vacuum: now - 120, last_autovacuum: nil },
              { table: "pgmq.a_pgbus_default", kind: "archive", live_tuples: 12_000, dead_tuples: 350,
                bloat_ratio: 0.0283, last_vacuum_ago_sec: 1800, last_vacuum: nil, last_autovacuum: now - 1800 },
              { table: "pgmq.q_pgbus_events", kind: "queue", live_tuples: 8_000, dead_tuples: 100,
                bloat_ratio: 0.1235, last_vacuum_ago_sec: 600, last_vacuum: now - 600, last_autovacuum: nil }
            ] }
        end

        def sample_arguments(job_class)
          case job_class
          when "ProcessPaymentJob" then [{ amount: 49.99, currency: "USD" }]
          when "SendWelcomeEmailJob" then [{ user_id: 42 }]
          when "SyncInventoryJob" then [{ sku: "WIDGET-42", warehouse: "US-EAST" }]
          else []
          end
        end
      end
    end
  end
end
