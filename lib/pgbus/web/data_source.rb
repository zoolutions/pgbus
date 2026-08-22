# frozen_string_literal: true

require "time"

module Pgbus
  module Web
    class DataSource
      def initialize(client: Pgbus.client)
        @client = client
        @last_throughput_snapshot = nil
        @last_throughput_at = nil
      end

      # Dashboard summary
      def summary_stats
        queues = queues_with_metrics
        total_depth = queues.sum { |q| q[:queue_length] }
        total_visible = queues.sum { |q| q[:queue_visible_length] }
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        dlq_depth = queues.select { |q| q[:name].end_with?(dlq_suffix) }.sum { |q| q[:queue_length] }

        throughput = compute_throughput(queues)

        health = queue_health_stats

        {
          total_queues: queues.size,
          total_depth: total_depth,
          total_visible: total_visible,
          active_processes: processes.count,
          failed_count: failed_events_count,
          dlq_depth: dlq_depth,
          recurring_count: recurring_tasks_count,
          throughput_rate: throughput,
          total_dead_tuples: health[:total_dead_tuples],
          tables_needing_vacuum: health[:tables_needing_vacuum],
          oldest_transaction_age_sec: health[:oldest_transaction_age_sec]
        }
      end

      # Queues — query via ActiveRecord for reliability in web processes
      # (avoids PGMQ client connection issues when the web server uses a
      # different connection lifecycle than the worker processes).
      # Memoized for the lifetime of this data-source instance (one per web
      # request — see ApplicationController#data_source). A single page can ask
      # for queue metrics more than once (e.g. the DLQ page reads both the
      # message rows and the total count); without the memo each call re-runs
      # the meta query + batched metrics. Mutations redirect to a fresh request
      # with a new instance, so a per-request memo never serves stale data.
      def queues_with_metrics
        @queues_with_metrics ||= fetch_queues_with_metrics
      end

      def fetch_queues_with_metrics
        queue_names = connection.select_values("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
        return [] if queue_names.empty?

        paused_queues = paused_queue_names.to_set
        batched_queue_metrics(queue_names).map do |q|
          q.merge(paused: paused_queues.include?(logical_queue_name(q[:name])))
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching queue metrics: #{e.class}: #{e.message}" }
        []
      end
      private :fetch_queues_with_metrics

      # Physical queue names known to back streams: the pgbus_stream_queues
      # registry plus any dormant pre-registry queues discovered by the
      # archive msg_id index fingerprint (issue #366). Stream delivery is a
      # non-consuming peek, so stream messages sit visible with read_ct=0
      # forever — health verdicts must not read that as a wedge (issue #359).
      # Loaded fresh per call (reset + registry + fingerprint queries):
      # verdicts run on coarse intervals and a long-lived process must see
      # streams registered since the last check. Degrades to an empty Set
      # when the registry/pgmq schema is absent or unreadable.
      def stream_queue_names
        StreamQueue.reset_cache!
        StreamQueue.known_names
      end

      # Fingerprint-matched stream queues missing from the registry (issue #366).
      # Used by HealthAnalyzer to surface a DEGRADED hint pointing at
      # `rake pgbus:streams:backfill_registry`. Zero when fully backfilled or
      # when the registry table is absent.
      def unregistered_stream_queue_count
        return 0 unless StreamQueue.table_exists?

        (StreamQueue.fingerprint_matched_names - StreamQueue.all_names).size
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] unregistered_stream_queue_count failed: #{e.message}" }
        0
      end

      # The set of physical queue names some configured worker capsule or
      # EventBus handler is set up to drain. Used by the HealthAnalyzer to keep
      # the silent-worker-wedge verdict from flagging queues nobody drains
      # (ad-hoc queues, unregistered stream queues — issue #367, #366): live
      # workers prove nothing about a queue they can never claim from.
      #
      # Returns nil when a capsule uses the "*" wildcard — that capsule drains
      # every job queue (Worker#resolve_wildcard_queues), so there is nothing to
      # intersect against. Otherwise returns a Set of physical names: each
      # capsule's explicit logical queues expanded to the physical tables that
      # actually exist (via the client's queue strategy, so a priority queue's
      # _p0.._pN sub-tables are matched, not the bare prefixed name priority mode
      # never creates), unioned with the EventBus handler queues consumers drain.
      def drained_queue_names
        capsules = Array(Pgbus.configuration.workers)
        capsule_queues = capsules.flat_map { |c| c[:queues] || c["queues"] || [] }
        return nil if capsule_queues.include?("*")

        physical = capsule_queues.flat_map { |q| @client.physical_queue_names(q) }
        (physical + handler_queue_physical_names).to_set
      rescue StandardError => e
        # Fail open: nil means "a wildcard drains everything", which restores the
        # pre-#367 behavior of intersecting against the whole backlog. A raise
        # here (e.g. a malformed capsule queue name) must never break the
        # HealthAnalyzer's "always produces a verdict" contract.
        Pgbus.logger.debug { "[Pgbus::Web] Error computing drained queues: #{e.class}: #{e.message}" }
        nil
      end

      # name is the full PGMQ queue name (e.g. "pgbus_default") as returned
      # by queues_with_metrics. No prefix is added.
      def queue_detail(name)
        queue_metrics_via_sql(name)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching queue detail for #{name}: #{e.class}: #{e.message}" }
        nil
      end

      def purge_queue(name)
        release_uniqueness_keys_for_queue(name)
        @client.purge_queue(name, prefixed: false)
      end

      def drop_queue(name)
        release_uniqueness_keys_for_queue(name)
        @client.drop_queue(name, prefixed: false)
      end

      def pause_queue(name, reason: nil)
        QueueState.pause!(logical_queue_name(name), reason: reason)
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error pausing queue #{name}: #{e.message}" }
      end

      def resume_queue(name)
        QueueState.resume!(logical_queue_name(name))
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error resuming queue #{name}: #{e.message}" }
      end

      def queue_paused?(name)
        QueueState.paused?(logical_queue_name(name))
      rescue StandardError
        false
      end

      # Jobs (messages in queue tables)
      def jobs(queue_name: nil, page: 1, per_page: 25)
        offset = (page - 1) * per_page

        if queue_name
          query_queue_messages(queue_name, per_page, offset)
        else
          all_queue_messages(per_page, offset)
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error reading jobs: #{e.message}" }
        []
      end

      def job_detail(queue_name, msg_id)
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus Job Detail",
          [msg_id.to_i]
        )
        row ? format_message(row, queue_name) : nil
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching job detail: #{e.message}" }
        nil
      end

      def retry_job(queue_name, msg_id)
        @client.set_visibility_timeout(queue_name, msg_id.to_i, vt: 0, prefixed: false)
      end

      def discard_job(queue_name, msg_id)
        release_lock_for_message(queue_name, msg_id)
        @client.archive_message(queue_name, msg_id.to_i, prefixed: false)
      end

      def discard_all_enqueued
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        queues = queues_with_metrics.reject { |q| q[:name].end_with?(dlq_suffix) }
        total = 0

        queues.each do |q|
          messages = query_queue_messages_raw(q[:name], 10_000, 0)
          next if messages.empty?

          release_locks_for_messages(messages)

          ids = messages.map { |m| m[:msg_id].to_i }
          @client.archive_batch(q[:name], ids, prefixed: false)
          total += ids.size
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error discarding enqueued messages from #{q[:name]}: #{e.message}" }
        end

        total
      end

      # Failed events
      def failed_events(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        rows = connection.select_all(
          "SELECT * FROM pgbus_failed_events ORDER BY failed_at DESC LIMIT $1 OFFSET $2",
          "Pgbus Failed Events",
          [per_page, offset]
        )
        rows.to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching failed events: #{e.message}" }
        []
      end

      def failed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_failed_events")
        result.to_i
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting failed events: #{e.message}" }
        0
      end

      def failed_event(id)
        connection.select_one(
          "SELECT * FROM pgbus_failed_events WHERE id = $1",
          "Pgbus Failed Event",
          [id.to_i]
        )
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching failed event #{id}: #{e.message}" }
        nil
      end

      def retry_failed_event(id)
        event = failed_event(id)
        return false unless event

        # Prefer resetting the existing message's visibility timeout to 0
        # so the worker picks it up immediately. This avoids creating a
        # duplicate (the original is still in the queue waiting for retry).
        # Falls back to enqueueing a fresh copy only if the original is gone
        # (e.g., already moved to DLQ).
        msg_id = event["msg_id"]
        if msg_id && @client.message_exists?(event["queue_name"], msg_id: msg_id.to_i)
          @client.set_visibility_timeout(event["queue_name"], msg_id.to_i, vt: 0)
        else
          payload = JSON.parse(event["payload"])
          headers = event["headers"]
          headers = JSON.parse(headers) if headers.is_a?(String)
          @client.send_message(event["queue_name"], payload, headers: headers)
        end

        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [id.to_i]
        )
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error retrying failed event #{id}: #{e.message}" }
        false
      end

      def discard_failed_event(id)
        event = failed_event(id)
        if event
          release_lock_for_payload(event["payload"])
          archive_failed_message(event)
        end

        connection.exec_delete(
          "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [id.to_i]
        )
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding failed event #{id}: #{e.message}" }
        false
      end

      def retry_all_failed
        count = 0
        loop do
          batch = connection.select_all(
            "SELECT * FROM pgbus_failed_events ORDER BY id LIMIT 100", "Pgbus Retry Batch"
          ).to_a
          break if batch.empty?

          batch.each do |event|
            payload = JSON.parse(event["payload"])
            headers = event["headers"]
            headers = JSON.parse(headers) if headers.is_a?(String)

            connection.transaction do
              @client.send_message(event["queue_name"], payload, headers: headers)
              connection.exec_delete(
                "DELETE FROM pgbus_failed_events WHERE id = $1", "Pgbus Delete Failed Event", [event["id"].to_i]
              )
            end
            count += 1
          rescue StandardError => e
            Pgbus.logger.error { "[Pgbus::Web] Failed to retry event #{event["id"]}: #{e.message}" }
          end
        end
        count
      end

      def discard_all_failed
        release_locks_for_failed_events
        archive_all_failed_messages

        result = connection.execute("DELETE FROM pgbus_failed_events")
        result.cmd_tuples
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding all failed events: #{e.message}" }
        0
      end

      # Dead letter queue
      # Note: DLQ queue names from queues_with_metrics are already fully qualified
      # (e.g., "pgbus_default_dlq"), so we use them directly without re-prefixing.
      def dlq_messages(page: 1, per_page: 25)
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        queues = queues_with_metrics.select { |q| q[:name].end_with?(dlq_suffix) }
        offset = (page - 1) * per_page

        paginated_queue_messages(queues.map { |q| q[:name] }, per_page, offset)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ messages: #{e.message}" }
        []
      end

      def dlq_total_count
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        queues_with_metrics
          .select { |q| q[:name].end_with?(dlq_suffix) }
          .sum { |q| q[:queue_length] }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ count: #{e.message}" }
        0
      end

      def dlq_message_detail(msg_id)
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        queues = queues_with_metrics.select { |q| q[:name].end_with?(dlq_suffix) }
        queues.each do |q|
          row = connection.select_one(
            "SELECT * FROM pgmq.q_#{sanitize_name(q[:name])} WHERE msg_id = $1",
            "Pgbus DLQ Detail",
            [msg_id.to_i]
          )
          return format_message(row, q[:name]) if row
        end
        nil
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ message #{msg_id}: #{e.message}" }
        nil
      end

      def retry_dlq_message(queue_name, msg_id)
        # queue_name here is the full DLQ name (already prefixed)
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        original_queue = queue_name.delete_suffix(dlq_suffix)

        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus DLQ Read",
          [msg_id.to_i]
        )
        return false unless row

        @client.transaction do |txn|
          txn.produce(original_queue, row["message"], headers: row["headers"])
          txn.delete(queue_name, msg_id.to_i)
        end
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error retrying DLQ message #{msg_id}: #{e.message}" }
        false
      end

      def discard_dlq_message(queue_name, msg_id)
        # queue_name here is the full DLQ name (already prefixed)
        release_lock_for_message(queue_name, msg_id)
        @client.delete_message(queue_name, msg_id.to_i, prefixed: false)
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding DLQ message #{msg_id}: #{e.message}" }
        false
      end

      def retry_all_dlq
        messages = dlq_messages(page: 1, per_page: 1000)
        count = 0
        messages.each do |m|
          retry_dlq_message(m[:queue_name], m[:msg_id]) && count += 1
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error retrying DLQ message #{m[:msg_id]}: #{e.message}" }
          next
        end
        count
      end

      def discard_all_dlq
        messages = dlq_messages(page: 1, per_page: 1000)
        return 0 if messages.empty?

        release_locks_for_messages(messages)

        # Group by queue for batch delete — one call per DLQ instead of N calls
        messages.group_by { |m| m[:queue_name] }.sum do |queue_name, msgs|
          ids = msgs.map { |m| m[:msg_id].to_i }
          @client.delete_batch(queue_name, ids, prefixed: false).size
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error batch-discarding DLQ messages from #{queue_name}: #{e.message}" }
          0
        end
      end

      # Processes
      def processes
        rows = connection.select_all(
          "SELECT * FROM pgbus_processes ORDER BY kind, created_at"
        )
        rows.to_a.map { |r| format_process(r) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processes: #{e.message}" }
        []
      end

      # Processed events (audit trail)
      def processed_events(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        rows = connection.select_all(
          "SELECT * FROM pgbus_processed_events ORDER BY processed_at DESC LIMIT $1 OFFSET $2",
          "Pgbus Processed Events",
          [per_page, offset]
        )
        rows.to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processed events: #{e.message}" }
        []
      end

      def processed_event(id)
        connection.select_one(
          "SELECT * FROM pgbus_processed_events WHERE id = $1",
          "Pgbus Processed Event",
          [id.to_i]
        )
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching processed event #{id}: #{e.message}" }
        nil
      end

      def processed_events_count
        result = connection.select_value("SELECT COUNT(*) FROM pgbus_processed_events")
        result.to_i
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting processed events: #{e.message}" }
        0
      end

      def replay_event(event)
        # Re-publish the event payload to all matching subscribers
        routing_key = event["routing_key"] || event["handler_class"]
        return false unless routing_key

        @client.publish_to_topic(routing_key, event["payload"] || "{}")
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error replaying event: #{e.message}" }
        false
      end

      # Recurring tasks
      def recurring_tasks
        records = RecurringTask.order(:key).to_a
        last_runs = RecurringExecution
                    .where(task_key: records.map(&:key))
                    .select("task_key, MAX(run_at) AS run_at")
                    .group(:task_key)
                    .index_by(&:task_key)

        records.map do |record|
          last_exec = last_runs[record.key]
          task = Recurring::Task.from_configuration(record.key,
                                                    class: record.class_name,
                                                    command: record.command,
                                                    schedule: record.schedule,
                                                    queue: record.queue_name,
                                                    args: parse_arguments(record.arguments),
                                                    priority: record.priority,
                                                    description: record.description)

          {
            id: record.id,
            key: record.key,
            class_name: record.class_name,
            command: record.command,
            schedule: record.schedule,
            human_schedule: task.human_schedule,
            queue_name: record.queue_name,
            priority: record.priority,
            description: record.description,
            enabled: record.enabled,
            static: record.static,
            next_run_at: task.next_time,
            last_run_at: last_exec&.run_at,
            created_at: record.created_at,
            updated_at: record.updated_at
          }
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching recurring tasks: #{e.class}: #{e.message}" }
        []
      end

      def recurring_task(id)
        record = RecurringTask.find_by(id: id)
        return nil unless record

        task = Recurring::Task.from_configuration(record.key,
                                                  class: record.class_name,
                                                  command: record.command,
                                                  schedule: record.schedule,
                                                  queue: record.queue_name,
                                                  args: parse_arguments(record.arguments),
                                                  priority: record.priority,
                                                  description: record.description)

        executions = RecurringExecution.for_task(record.key).recent(25).map do |exec|
          { run_at: exec.run_at, created_at: exec.created_at }
        end

        {
          id: record.id,
          key: record.key,
          class_name: record.class_name,
          command: record.command,
          schedule: record.schedule,
          human_schedule: task.human_schedule,
          queue_name: record.queue_name,
          arguments: parse_arguments(record.arguments),
          priority: record.priority,
          description: record.description,
          enabled: record.enabled,
          static: record.static,
          next_run_at: task.next_time,
          executions: executions,
          created_at: record.created_at,
          updated_at: record.updated_at
        }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching recurring task #{id}: #{e.class}: #{e.message}" }
        nil
      end

      def toggle_recurring_task(id)
        record = RecurringTask.find_by(id: id)
        return nil unless record

        record.update!(enabled: !record.enabled)
        record.enabled ? :enabled : :disabled
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error toggling recurring task #{id}: #{e.message}" }
        nil
      end

      def enqueue_recurring_task_now(id)
        record = RecurringTask.find_by(id: id)
        return false unless record

        task = Recurring::Task.from_configuration(record.key,
                                                  class: record.class_name,
                                                  command: record.command,
                                                  schedule: record.schedule,
                                                  queue: record.queue_name,
                                                  args: parse_arguments(record.arguments),
                                                  priority: record.priority)

        schedule = Recurring::Schedule.new(config: Pgbus.configuration)
        schedule.enqueue_task(task, run_at: Time.now.utc)
        true
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error enqueuing recurring task #{id}: #{e.message}" }
        false
      end

      def recurring_tasks_count
        RecurringTask.count
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting recurring tasks: #{e.message}" }
        0
      end

      # Outbox
      def outbox_stats
        {
          unpublished: OutboxEntry.unpublished.count,
          total: OutboxEntry.count,
          oldest_unpublished_age: oldest_unpublished_age
        }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching outbox stats: #{e.message}" }
        { unpublished: 0, total: 0, oldest_unpublished_age: nil }
      end

      def outbox_entries(page: 1, per_page: 25)
        offset = (page - 1) * per_page
        OutboxEntry.order(id: :desc).limit(per_page).offset(offset).to_a
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching outbox entries: #{e.message}" }
        []
      end

      # Lock management
      def discard_lock(lock_key)
        UniquenessKey.where(lock_key: lock_key).delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding lock #{lock_key}: #{e.message}" }
        0
      end

      def discard_locks(lock_keys)
        return 0 if lock_keys.empty?

        UniquenessKey.where(lock_key: lock_keys).delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding locks: #{e.message}" }
        0
      end

      def discard_all_locks
        UniquenessKey.delete_all
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding all locks: #{e.message}" }
        0
      end

      # Job uniqueness keys
      def job_locks
        UniquenessKey.order(created_at: :desc).limit(100).map do |key|
          {
            lock_key: key.lock_key,
            queue_name: key.queue_name,
            msg_id: key.msg_id,
            created_at: key.created_at,
            age_seconds: key.created_at ? (Time.current - key.created_at).to_i : nil
          }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching uniqueness keys: #{e.message}" }
        []
      end

      # Batches
      def batches(limit: 100)
        records = BatchEntry.order(created_at: :desc).limit(limit).to_a
        pending = pending_jobs_by_batch(records.map(&:batch_id))
        records.map { |r| format_batch(r, pending_jobs: pending[r.batch_id]) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching batches: #{e.message}" }
        []
      end

      def batch_detail(batch_id)
        record = BatchEntry.find_by(batch_id: batch_id)
        return nil unless record

        pending = pending_jobs_by_batch([batch_id])
        format_batch(record, pending_jobs: pending[batch_id]).merge(
          properties: record.properties,
          on_finish_class: record.on_finish_class,
          on_success_class: record.on_success_class,
          on_failure_class: failure_callback_class(record),
          on_discard_class: failure_callback_class(record)
        )
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching batch #{batch_id}: #{e.message}" }
        nil
      end

      def batches_count
        BatchEntry.count
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting batches: #{e.message}" }
        0
      end

      def active_batches_count
        BatchEntry.where.not(status: "finished").count
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error counting active batches: #{e.message}" }
        0
      end

      # Job stats
      def job_stats_summary(minutes: 60)
        JobStat.summary(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching job stats summary: #{e.message}" }
        { total: 0, success: 0, failed: 0, dead_lettered: 0, avg_duration_ms: 0, max_duration_ms: 0 }
      end

      def job_throughput(minutes: 60)
        JobStat.throughput(minutes: minutes).map { |time, count| { time: time, count: count } }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching throughput: #{e.message}" }
        []
      end

      def job_status_counts(minutes: 60)
        JobStat.status_counts(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching status counts: #{e.message}" }
        {}
      end

      def slowest_job_classes(limit: 10, minutes: 60)
        JobStat.slowest_classes(limit: limit, minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching slowest classes: #{e.message}" }
        []
      end

      def latency_trend(minutes: 60)
        JobStat.latency_trend(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching latency trend: #{e.message}" }
        []
      end

      def latency_by_queue(minutes: 60)
        JobStat.avg_latency_by_queue(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching latency by queue: #{e.message}" }
        []
      end

      # Queue health — vacuum stats, dead tuples, bloat, MVCC horizon.
      # Returns aggregate health across all queue and archive tables, plus
      # the oldest open transaction age (MVCC horizon pinning risk).
      def queue_health_stats
        tables = fetch_all_table_stats

        total_dead = tables.sum { |t| t[:dead_tuples] }
        total_live = tables.sum { |t| t[:live_tuples] }
        worst_bloat = tables.map { |t| t[:bloat_ratio] }.max || 0.0
        needs_vacuum = tables.count { |t| t[:bloat_ratio] > 0.1 }
        oldest_vacuum = tables.filter_map { |t| t[:last_vacuum_ago_sec] }.max

        {
          total_dead_tuples: total_dead,
          total_live_tuples: total_live,
          worst_bloat_ratio: worst_bloat.round(4),
          tables_needing_vacuum: needs_vacuum,
          oldest_vacuum_ago_sec: oldest_vacuum,
          oldest_transaction_age_sec: oldest_transaction_age,
          tables: tables
        }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching queue health stats: #{e.class}: #{e.message}" }
        {
          total_dead_tuples: 0, total_live_tuples: 0, worst_bloat_ratio: 0.0,
          tables_needing_vacuum: 0, oldest_vacuum_ago_sec: nil,
          oldest_transaction_age_sec: nil, tables: []
        }
      end

      # Per-queue health stats for the queue detail view.
      def queue_health_detail(queue_name)
        sanitized = sanitize_name(queue_name)
        tables = [
          fetch_table_stats("pgmq", "q_#{sanitized}", "queue"),
          fetch_table_stats("pgmq", "a_#{sanitized}", "archive")
        ].compact

        { tables: tables, oldest_transaction_age_sec: oldest_transaction_age }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching health detail for #{queue_name}: #{e.message}" }
        { tables: [], oldest_transaction_age_sec: nil }
      end

      # Stream stats — only populated when streams_stats_enabled is
      # true AND the migration has been run. Controllers should gate
      # rendering on `stream_stats_available?` to avoid showing empty
      # sections.
      def live_stream_metrics
        counter = Pgbus::Web::Streamer.stream_counter
        unless counter
          empty_totals = { broadcasts: 0, active_connections: 0, total_connections: 0, streams: 0 }
          return { streams: {}, totals: empty_totals }
        end

        { streams: counter.snapshot, totals: counter.totals }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching live stream metrics: #{e.message}" }
        { streams: {}, totals: { broadcasts: 0, active_connections: 0, total_connections: 0, streams: 0 } }
      end

      def stream_stats_available?
        Pgbus.configuration.streams_stats_enabled && StreamStat.table_exists?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error checking stream stats availability: #{e.message}" }
        false
      end

      def stream_stats_summary(minutes: 60)
        StreamStat.summary(minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching stream stats summary: #{e.message}" }
        {
          broadcasts: 0, connects: 0, disconnects: 0,
          active_estimate: 0, avg_fanout: 0,
          avg_broadcast_ms: 0, avg_connect_ms: 0
        }
      end

      def top_streams(limit: 10, minutes: 60)
        StreamStat.top_streams(limit: limit, minutes: minutes)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching top streams: #{e.message}" }
        []
      end

      # Pending events — messages sitting in handler queues that haven't been processed.
      # Identifies handler queues via the subscriber registry and queries them
      # for unprocessed messages. Subscriber queue names are logical
      # (e.g. "task_completion_handler"), while `pgmq.meta.queue_name` stores
      # physical names (e.g. "pgbus_task_completion_handler"), so we normalize
      # through `config.queue_name` before intersecting.
      def pending_events(page: 1, per_page: 25)
        handler_queues = handler_queue_physical_names
        return [] if handler_queues.empty?

        existing = connection.select_values(
          "SELECT queue_name FROM pgmq.meta ORDER BY queue_name", "Pgbus Queue Names"
        )
        target_queues = handler_queues & existing
        return [] if target_queues.empty?

        offset = (page - 1) * per_page
        paginated_queue_messages(target_queues, per_page, offset)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching pending events: #{e.message}" }
        []
      end

      # Physical queue names for all registered subscribers. Used for both
      # pending_events lookup and server-side validation of target queues
      # in reroute_event.
      def handler_queue_physical_names
        registered_subscribers.map { |s| s[:physical_queue_name] }.uniq
      end

      # Find the handler class registered for a given physical queue name.
      # Returns nil if no subscriber matches — used to reject forged handler
      # values in mark_event_handled / reroute_event.
      def handler_class_for_queue(physical_queue_name)
        sub = registered_subscribers.find { |s| s[:physical_queue_name] == physical_queue_name }
        sub && sub[:handler_class]
      end

      # Discard (archive) an event message from a handler queue.
      def discard_event(queue_name, msg_id)
        release_lock_for_message(queue_name, msg_id)
        @client.archive_message(queue_name, msg_id.to_i, prefixed: false)
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error discarding event #{msg_id}: #{e.message}" }
        false
      end

      # Mark an event as handled: archive the queue message and insert a
      # ProcessedEvent record so it won't be reprocessed on replay.
      #
      # The insert is performed BEFORE archive. If the archive step fails
      # afterwards the operator can retry — replay protection is already in
      # place and the idempotency dedup will cause the handler to skip the
      # event even if it is eventually re-read from the queue. Doing it the
      # other way around would risk losing the message without recording the
      # marker.
      def mark_event_handled(queue_name, msg_id, handler_class)
        detail = job_detail(queue_name, msg_id)
        return false unless detail

        raw = JSON.parse(detail[:message])
        event_id = raw["event_id"]
        return false unless event_id

        ProcessedEvent.insert(
          { event_id: event_id, handler_class: handler_class, processed_at: Time.now.utc },
          unique_by: %i[event_id handler_class]
        )
        # Release the uniqueness lock while we still hold the payload in
        # memory — otherwise the message is archived but the lock row stays
        # behind, blocking later publishes with the same key. Mirrors
        # discard_event.
        release_lock_for_payload(detail[:message])
        @client.archive_message(queue_name, msg_id.to_i, prefixed: false)
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error marking event #{msg_id} handled: #{e.message}" }
        false
      end

      # Edit the payload of a stuck event: delete old message and re-enqueue
      # with the corrected payload in the same queue. The produce + delete
      # are wrapped in a PGMQ transaction so the message can't be lost if
      # either half fails (same pattern as retry_dlq_message).
      def edit_event_payload(queue_name, msg_id, new_payload_json)
        begin
          parsed = JSON.parse(new_payload_json)
        rescue JSON::ParserError
          return false
        end

        detail = job_detail(queue_name, msg_id)
        return false unless detail

        @client.transaction do |txn|
          txn.produce(queue_name, parsed.to_json, headers: detail[:headers])
          txn.delete(queue_name, msg_id.to_i)
        end
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error editing event #{msg_id}: #{e.message}" }
        false
      end

      # Reroute an event from one handler queue to another. Wrapped in a
      # PGMQ transaction so produce on the target and delete on the source
      # are atomic.
      def reroute_event(source_queue, msg_id, target_queue)
        detail = job_detail(source_queue, msg_id)
        return false unless detail

        @client.transaction do |txn|
          txn.produce(target_queue, detail[:message], headers: detail[:headers])
          txn.delete(source_queue, msg_id.to_i)
        end
        true
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error rerouting event #{msg_id}: #{e.message}" }
        false
      end

      # Bulk discard selected events from handler queues.
      def discard_selected_events(selections)
        return 0 if selections.empty?

        count = 0
        selections.each do |sel|
          discard_event(sel[:queue_name], sel[:msg_id]) && count += 1
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error in bulk discard for #{sel[:msg_id]}: #{e.message}" }
          next
        end
        count
      end

      # Subscriber registry. `queue_name` is the logical name the subscriber
      # registered with; `physical_queue_name` is what the queue is actually
      # called in `pgmq.meta` (e.g. logical "task_completion_handler" ->
      # physical "pgbus_task_completion_handler"). The dashboard needs the
      # physical name to match against pending messages / target queues.
      def registered_subscribers
        EventBus::Registry.instance.subscribers.map do |s|
          {
            pattern: s.pattern,
            handler_class: s.handler_class.name,
            queue_name: s.queue_name,
            physical_queue_name: @client.config.queue_name(s.queue_name)
          }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching subscribers: #{e.message}" }
        []
      end

      # NOTIFY throttle status for all queues with notifications enabled.
      # Returns an array of hashes: { queue_name:, throttle_interval_ms:, last_notified_at: }
      def notify_throttles
        @client.list_notify_insert_throttles.map do |throttle|
          {
            queue_name: throttle.queue_name,
            throttle_interval_ms: throttle.throttle_interval_ms,
            last_notified_at: throttle.last_notified_at
          }
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching notify throttles: #{e.message}" }
        []
      end

      # FIFO group head sampling for a specific queue.
      # Returns the oldest visible message from each distinct group (up to qty).
      # Useful for detecting head-of-line stalls in multi-tenant queues.
      def queue_group_heads(queue_name, qty: 20)
        logical = logical_queue_name(queue_name)
        messages = @client.read_grouped_head(logical, qty: qty) || []
        messages.map { |m| format_pgmq_message(m, queue_name) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching group heads for #{queue_name}: #{e.message}" }
        []
      end

      private

      def connection
        Pgbus::BusRecord.connection
      end

      # Single query to fetch pg_stat_user_tables stats for all queue and
      # archive tables. Avoids 2*N catalog queries on the dashboard.
      def fetch_all_table_stats
        rows = connection.select_all(<<~SQL, "Pgbus All Table Health")
          WITH rels AS (
            SELECT queue_name, 'q_' || queue_name AS relname, 'queue' AS kind FROM pgmq.meta
            UNION ALL
            SELECT queue_name, 'a_' || queue_name AS relname, 'archive' AS kind FROM pgmq.meta
          )
          SELECT
            'pgmq.' || r.relname AS table_name,
            r.kind,
            s.n_live_tup,
            s.n_dead_tup,
            EXTRACT(epoch FROM (NOW() - COALESCE(s.last_vacuum, s.last_autovacuum)))::int AS last_vacuum_ago_sec,
            s.last_vacuum,
            s.last_autovacuum
          FROM rels r
          LEFT JOIN pg_stat_user_tables s
            ON s.schemaname = 'pgmq' AND s.relname = r.relname
          ORDER BY r.queue_name, r.kind
        SQL

        rows.to_a.filter_map { |row| build_table_health_row(row) }
      end

      # Fetch pg_stat_user_tables stats for a single table (used by queue_health_detail).
      def fetch_table_stats(schema, table_name, kind)
        row = connection.select_one(<<~SQL, "Pgbus Table Health", [schema, table_name])
          SELECT
            n_live_tup,
            n_dead_tup,
            EXTRACT(epoch FROM (NOW() - COALESCE(last_vacuum, last_autovacuum)))::int AS last_vacuum_ago_sec,
            last_vacuum,
            last_autovacuum
          FROM pg_stat_user_tables
          WHERE schemaname = $1 AND relname = $2
        SQL

        return nil unless row

        build_table_health_row(row.merge("table_name" => "#{schema}.#{table_name}", "kind" => kind))
      end

      def build_table_health_row(row)
        return nil unless row["n_live_tup"] || row["n_dead_tup"]

        live = row["n_live_tup"].to_i
        dead = row["n_dead_tup"].to_i
        total = live + dead
        bloat = total.positive? ? (dead.to_f / total) : 0.0

        {
          table: row["table_name"],
          kind: row["kind"],
          live_tuples: live,
          dead_tuples: dead,
          bloat_ratio: bloat.round(4),
          last_vacuum_ago_sec: row["last_vacuum_ago_sec"]&.to_i,
          last_vacuum: row["last_vacuum"],
          last_autovacuum: row["last_autovacuum"]
        }
      end

      # Age of the oldest open transaction in seconds — indicates MVCC
      # horizon pinning risk. Returns nil if no active transactions.
      def oldest_transaction_age
        row = connection.select_one(<<~SQL, "Pgbus Oldest Transaction")
          SELECT EXTRACT(epoch FROM (NOW() - xact_start))::int AS age_sec
          FROM pg_stat_activity
          WHERE state != 'idle'
            AND xact_start IS NOT NULL
            AND pid != pg_backend_pid()
          ORDER BY xact_start ASC
          LIMIT 1
        SQL

        row&.dig("age_sec")&.to_i
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error fetching oldest transaction age: #{e.class}: #{e.message}" }
        nil
      end

      # name is the full PGMQ queue name (already prefixed)
      def query_queue_messages(name, limit, offset)
        query_queue_messages_raw(name, limit, offset).map { |m| m.merge(queue: name) }
      end

      def query_queue_messages_raw(full_name, limit, offset)
        rows = connection.select_all(
          "SELECT * FROM pgmq.q_#{sanitize_name(full_name)} ORDER BY msg_id DESC LIMIT $1 OFFSET $2",
          "Pgbus Queue Messages",
          [limit, offset]
        )
        rows.to_a.map { |r| format_message(r, full_name) }
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error querying messages from #{full_name}: #{e.message}" }
        []
      end

      def all_queue_messages(limit, offset)
        dlq_suffix = Pgbus::DEAD_LETTER_SUFFIX
        queues = queues_with_metrics.reject { |q| q[:name].end_with?(dlq_suffix) }
        paginated_queue_messages(queues.map { |q| q[:name] }, limit, offset)
      end

      # Returns messages from multiple PGMQ queues in a single paginated
      # query. Builds a UNION ALL across the target tables and pushes the
      # ORDER BY msg_id DESC + LIMIT + OFFSET down to Postgres so we don't
      # load (limit + offset) rows from every queue into Ruby just to slice
      # out one page. Returns [] if queue_names is empty.
      #
      # Each UNION ALL fragment selects a literal queue name so the outer
      # query can tag every row with its source queue — the pre-SQL
      # implementation tagged it from the iteration variable. The queue
      # name goes through sanitize_name (which calls QueueNameValidator)
      # so it's safe to interpolate into both the schema-qualified table
      # and the literal column. limit/offset are bound parameters.
      def paginated_queue_messages(queue_names, limit, offset)
        return [] if queue_names.empty?

        sanitized = queue_names.map { |name| [name, sanitize_name(name)] }
        fragments = sanitized.map do |(name, qtable)|
          <<~SQL.strip
            SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers,
                   '#{name}' AS queue_name
            FROM pgmq.q_#{qtable}
          SQL
        end

        sql = <<~SQL
          SELECT * FROM (#{fragments.join("\nUNION ALL\n")}) AS combined
          ORDER BY msg_id DESC
          LIMIT $1 OFFSET $2
        SQL

        rows = connection.select_all(sql, "Pgbus Paginated Queue Messages", [limit, offset])
        rows.to_a.map { |r| format_message(r, r["queue_name"]) }
      end

      def batched_queue_metrics(queue_names)
        return [] if queue_names.empty?

        unions = queue_names.filter_map do |name|
          sanitized = sanitize_name(name)
          qtable = "q_#{sanitized}"
          seq_name = "#{qtable}_msg_id_seq"
          <<~SQL
            SELECT
              #{connection.quote(name)} AS queue_name,
              (SELECT count(*) FROM pgmq.#{qtable}) AS queue_length,
              (SELECT count(*) FROM pgmq.#{qtable} WHERE vt <= NOW()) AS queue_visible_length,
              (SELECT EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int FROM pgmq.#{qtable}) AS newest_msg_age_sec,
              (SELECT EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int FROM pgmq.#{qtable}) AS oldest_msg_age_sec,
              (SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM pgmq.#{seq_name}) AS total_messages,
              (SELECT max(read_ct) FROM pgmq.#{qtable}) AS max_read_ct,
              (SELECT count(*) FROM pgmq.#{qtable} WHERE vt <= NOW() AND read_ct = 0) AS visible_unread_length,
              (SELECT EXTRACT(epoch FROM (NOW() - min(vt)))::int FROM pgmq.#{qtable} WHERE vt <= NOW()) AS oldest_claimable_age_sec
          SQL
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Skipping queue metrics for #{name}: #{e.message}" }
          nil
        end

        return [] if unions.empty?

        sql = unions.join(" UNION ALL ")
        rows = connection.select_all(sql, "Pgbus Batched Queue Metrics")
        rows.to_a.map do |row|
          {
            name: row["queue_name"],
            queue_length: row["queue_length"].to_i,
            queue_visible_length: row["queue_visible_length"].to_i,
            parked_length: row["queue_length"].to_i - row["queue_visible_length"].to_i,
            oldest_msg_age_sec: row["oldest_msg_age_sec"]&.to_i,
            oldest_claimable_age_sec: row["oldest_claimable_age_sec"]&.to_i,
            newest_msg_age_sec: row["newest_msg_age_sec"]&.to_i,
            total_messages: row["total_messages"].to_i,
            max_read_ct: row["max_read_ct"]&.to_i,
            visible_unread_length: row["visible_unread_length"].to_i
          }
        end
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching batched queue metrics: #{e.class}: #{e.message}" }
        []
      end

      def queue_metrics_via_sql(queue_name)
        qtable = "q_#{sanitize_name(queue_name)}"
        seq_name = "#{qtable}_msg_id_seq"

        row = connection.select_one(<<~SQL, "Pgbus Queue Metrics")
          WITH q_summary AS (
            SELECT
              count(*) AS queue_length,
              count(CASE WHEN vt <= NOW() THEN 1 END) AS queue_visible_length,
              EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int AS newest_msg_age_sec,
              EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int AS oldest_msg_age_sec,
              EXTRACT(epoch FROM (NOW() - min(vt) FILTER (WHERE vt <= NOW())))::int AS oldest_claimable_age_sec,
              max(read_ct) AS max_read_ct,
              count(CASE WHEN vt <= NOW() AND read_ct = 0 THEN 1 END) AS visible_unread_length
            FROM pgmq.#{qtable}
          ),
          all_metrics AS (
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS total_messages
            FROM pgmq.#{seq_name}
          )
          SELECT
            q_summary.queue_length,
            q_summary.queue_visible_length,
            q_summary.newest_msg_age_sec,
            q_summary.oldest_msg_age_sec,
            q_summary.oldest_claimable_age_sec,
            q_summary.max_read_ct,
            q_summary.visible_unread_length,
            all_metrics.total_messages
          FROM q_summary, all_metrics
        SQL

        return nil unless row

        {
          name: queue_name,
          queue_length: row["queue_length"].to_i,
          queue_visible_length: row["queue_visible_length"].to_i,
          parked_length: row["queue_length"].to_i - row["queue_visible_length"].to_i,
          oldest_msg_age_sec: row["oldest_msg_age_sec"]&.to_i,
          oldest_claimable_age_sec: row["oldest_claimable_age_sec"]&.to_i,
          newest_msg_age_sec: row["newest_msg_age_sec"]&.to_i,
          total_messages: row["total_messages"].to_i,
          max_read_ct: row["max_read_ct"]&.to_i,
          visible_unread_length: row["visible_unread_length"].to_i
        }
      rescue StandardError => e
        Pgbus.logger.error { "[Pgbus::Web] Error fetching metrics for #{queue_name}: #{e.class}: #{e.message}" }
        nil
      end

      def format_message(row, queue_name)
        {
          msg_id: row["msg_id"].to_i,
          read_ct: row["read_ct"].to_i,
          enqueued_at: row["enqueued_at"],
          last_read_at: row["last_read_at"],
          vt: row["vt"],
          message: row["message"],
          headers: row["headers"],
          queue_name: queue_name
        }
      end

      def format_pgmq_message(msg, queue_name)
        {
          msg_id: msg.msg_id.to_i,
          read_ct: msg.read_ct.to_i,
          enqueued_at: msg.enqueued_at,
          last_read_at: msg.respond_to?(:last_read_at) ? msg.last_read_at : nil,
          vt: msg.respond_to?(:vt) ? msg.vt : nil,
          message: msg.message,
          headers: msg.headers,
          queue_name: queue_name
        }
      end

      def format_process(row)
        heartbeat = row["last_heartbeat_at"]
        heartbeat_time = heartbeat.is_a?(String) ? Time.parse(heartbeat) : heartbeat
        stale = heartbeat_time && (Time.now - heartbeat_time) > Process::Heartbeat::ALIVE_THRESHOLD
        metadata = row["metadata"].is_a?(String) ? JSON.parse(row["metadata"]) : row["metadata"]
        status = derive_process_status(stale, metadata, row["kind"])

        {
          id: row["id"].to_i,
          kind: row["kind"],
          hostname: row["hostname"],
          pid: row["pid"].to_i,
          metadata: metadata,
          last_heartbeat_at: heartbeat_time,
          healthy: status == :healthy,
          status: status,
          created_at: row["created_at"]
        }
      end

      def derive_process_status(stale, metadata, kind)
        return :stale if stale
        return :healthy unless metadata.is_a?(Hash)

        threshold = stall_threshold_for(kind)
        return :healthy unless threshold

        loop_tick = metadata["loop_tick_at"]
        return :healthy unless loop_tick

        age = Time.now.to_f - loop_tick.to_f
        age > threshold ? :stalled : :healthy
      end

      # Kind-aware stall threshold. Workers run a tight loop, so the base
      # stall_threshold applies directly. The dispatcher and scheduler sleep
      # between iterations, so their beacon is naturally up to one sleep
      # interval stale even when healthy — widen the window by that interval to
      # avoid false stalls. Consumers idle on an empty-read wait that can reach
      # NOTIFY_FALLBACK_POLL_SECONDS (15s) when a live listener drives wake-up,
      # so widen by that max wait when notify is on, or by the poll interval
      # otherwise. Kinds with no loop beacon (nil) are never stalled.
      def stall_threshold_for(kind)
        config = Pgbus.configuration
        base = config.stall_threshold
        return nil unless base&.positive?

        case kind
        when "worker" then base
        when "dispatcher" then base + config.dispatch_interval
        when "scheduler" then base + config.recurring_schedule_interval
        when "consumer" then base + consumer_stall_slack(config)
        end
      end

      # Widen the consumer's stall window by its maximum empty-read wait. With
      # notify wake-up live, that wait rises to NOTIFY_FALLBACK_POLL_SECONDS as a
      # safety net; without it, the consumer polls at polling_interval.
      def consumer_stall_slack(config)
        if config.worker_notify_wakeup?
          [config.polling_interval, Pgbus::Process::Consumer::NOTIFY_FALLBACK_POLL_SECONDS].max
        else
          config.polling_interval
        end
      end

      def format_batch(record, pending_jobs: nil)
        total = record.total_jobs
        failed = failure_job_count(record)
        done = record.completed_jobs + failed
        pct = total.positive? ? ((done * 100) / total) : 100
        pending = pending_jobs || [total - done, 0].max

        {
          batch_id: record.batch_id,
          description: record.description,
          status: record.status,
          total_jobs: total,
          completed_jobs: record.completed_jobs,
          failed_jobs: failed,
          discarded_jobs: failed,
          pending_jobs: pending,
          progress_pct: pct,
          created_at: record.created_at,
          finished_at: record.finished_at
        }
      end

      def failure_job_count(record)
        record.respond_to?(:discarded_jobs) ? record.discarded_jobs.to_i : record.failed_jobs.to_i
      end

      def failure_callback_class(record)
        if record.respond_to?(:on_failure_class) && record.on_failure_class.present?
          record.on_failure_class
        elsif record.respond_to?(:on_discard_class)
          record.on_discard_class
        end
      end

      def pending_jobs_by_batch(batch_ids)
        return {} unless Batch.executions_migrated?
        return {} if batch_ids.empty?

        BatchExecution.where(batch_id: batch_ids).group(:batch_id).count
      rescue StandardError
        {}
      end

      def sanitize_name(name)
        QueueNameValidator.sanitize!(name)
      end

      def compute_throughput(queues)
        current_totals = queues.sum { |q| q[:total_messages] }
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)

        if @last_throughput_snapshot && @last_throughput_at
          elapsed = now - @last_throughput_at
          delta = current_totals - @last_throughput_snapshot
          rate = elapsed.positive? ? (delta / elapsed).round(1) : 0.0
        end

        @last_throughput_snapshot = current_totals
        @last_throughput_at = now

        rate || 0.0
      rescue StandardError
        0.0
      end

      def logical_queue_name(name)
        name
          .delete_prefix("#{Pgbus.configuration.queue_prefix}_")
          .sub(/_p\d+\z/, "")
      end

      def paused_queue_names
        QueueState.paused.pluck(:queue_name)
      rescue StandardError
        []
      end

      def oldest_unpublished_age
        oldest = OutboxEntry.unpublished.order(:id).pick(:created_at)
        return nil unless oldest

        (Time.now - oldest).to_i
      rescue StandardError
        nil
      end

      def parse_arguments(args)
        case args
        when Array then args
        when String then JSON.parse(args)
        when NilClass then []
        else Array(args)
        end
      rescue JSON::ParserError => e
        Pgbus.logger.debug { "[Pgbus::Web] Invalid recurring task arguments JSON: #{e.message}" }
        []
      end

      # --- Lock cleanup helpers ---

      # Extract uniqueness key from a queue message and release its lock.
      def release_lock_for_message(queue_name, msg_id)
        row = connection.select_one(
          "SELECT * FROM pgmq.q_#{sanitize_name(queue_name)} WHERE msg_id = $1",
          "Pgbus Job Detail",
          [msg_id.to_i]
        )
        return unless row

        release_lock_for_payload(row["message"])
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing lock for message #{msg_id}: #{e.message}" }
      end

      # Extract uniqueness key from a JSON payload string and release its lock.
      def release_lock_for_payload(payload_str)
        key = extract_uniqueness_key_from_payload_str(payload_str)
        UniquenessKey.release!(key) if key
      end

      # Extract uniqueness keys from a collection of formatted messages and
      # release all associated locks in a single query.
      def release_locks_for_messages(messages)
        keys = messages.filter_map { |m| extract_uniqueness_key_from_payload_str(m[:message]) }
        UniquenessKey.where(lock_key: keys).delete_all if keys.any?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing locks for messages: #{e.message}" }
      end

      # Collect uniqueness keys from all failed events and release their locks.
      def release_locks_for_failed_events
        rows = connection.select_all(
          "SELECT payload FROM pgbus_failed_events", "Pgbus Collect Failed Keys"
        )

        keys = rows.to_a.filter_map { |row| extract_uniqueness_key_from_payload_str(row["payload"]) }
        UniquenessKey.where(lock_key: keys).delete_all if keys.any?
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing locks for failed events: #{e.message}" }
      end

      # Single unwrap point for PGMQ message / failed_event payload strings.
      # Accepts a raw JSON string or an already-parsed Hash and returns the
      # uniqueness metadata key, or nil when the payload is blank, unparseable,
      # or carries no uniqueness metadata. Parse errors are swallowed at debug
      # level because callers treat missing keys and malformed payloads
      # identically (no lock to release).
      def extract_uniqueness_key_from_payload_str(payload_str)
        return nil unless payload_str

        payload = payload_str.is_a?(String) ? JSON.parse(payload_str) : payload_str
        payload[Uniqueness::METADATA_KEY]
      rescue JSON::ParserError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error parsing payload for uniqueness key: #{e.message}" }
        nil
      end

      # Archive the queue message a failed_event row points to. Idempotent —
      # silently no-ops if the message no longer exists in the queue.
      def archive_failed_message(event)
        return unless event["queue_name"] && event["msg_id"]

        @client.archive_message(event["queue_name"], event["msg_id"].to_i)
      rescue StandardError => e
        Pgbus.logger.debug do
          "[Pgbus::Web] Error archiving message for failed event #{event["id"]}: #{e.message}"
        end
      end

      # Archive every queue message referenced by a failed_event row.
      # Groups by queue and uses archive_batch so an N-event discard is
      # one SQL statement per queue instead of N per-row roundtrips.
      # Falls back to per-row archive inside a rescue if a batch fails,
      # so one bad queue can't block progress on the others.
      def archive_all_failed_messages
        rows = connection.select_all(
          "SELECT id, queue_name, msg_id FROM pgbus_failed_events WHERE msg_id IS NOT NULL",
          "Pgbus Collect Failed Messages"
        )

        grouped = rows.to_a.group_by { |row| row["queue_name"] }
        grouped.each do |queue_name, events|
          msg_ids = events.filter_map { |row| row["msg_id"]&.to_i }
          next if msg_ids.empty?

          begin
            @client.archive_batch(queue_name, msg_ids)
          rescue StandardError => e
            Pgbus.logger.debug do
              "[Pgbus::Web] archive_batch failed for #{queue_name} (#{e.message}); " \
                "falling back to per-row archive"
            end
            events.each { |row| archive_failed_message(row) }
          end
        end
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error archiving failed messages: #{e.message}" }
      end

      # Release all uniqueness keys associated with a queue before purge/drop.
      # Scans queue messages for uniqueness metadata and deletes matching rows.
      def release_uniqueness_keys_for_queue(queue_name)
        messages = query_queue_messages_raw(queue_name, 10_000, 0)
        release_locks_for_messages(messages)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus::Web] Error releasing uniqueness keys for queue #{queue_name}: #{e.message}" }
      end
    end
  end
end
