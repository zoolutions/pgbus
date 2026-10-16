# frozen_string_literal: true

module Pgbus
  module Outbox
    class Poller
      include Process::SignalHandler

      attr_reader :config

      def initialize(config: Pgbus.configuration)
        @config = config
        @shutting_down = false
      end

      def run
        setup_signals
        start_heartbeat
        Pgbus.logger.info { "[Pgbus] Outbox poller started: interval=#{config.outbox_poll_interval}s" }

        loop do
          break if @shutting_down

          process_signals
          break if @shutting_down

          poll_and_publish
          break if @shutting_down

          interruptible_sleep(config.outbox_poll_interval)
        end

        shutdown
      end

      def graceful_shutdown
        @shutting_down = true
      end

      def immediate_shutdown
        @shutting_down = true
      end

      def poll_and_publish
        published = 0

        loop do
          succeeded = 0

          OutboxEntry.transaction do
            entries = OutboxEntry.unpublished
                                 .order(:id)
                                 .limit(config.outbox_batch_size)
                                 .lock("FOR UPDATE SKIP LOCKED")
                                 .to_a
            break if entries.empty?

            succeeded = publish_entries(entries)
            published += succeeded
            break if succeeded.zero? || entries.size < config.outbox_batch_size
          end

          break if succeeded.zero?
        end

        Pgbus.logger.debug { "[Pgbus] Outbox published #{published} entries" } if published.positive?
        published
      rescue StandardError => e
        ErrorReporter.report(e, { action: "outbox_poll" })
        0
      end

      private

      def publish_entries(entries)
        # Partition: topic-routed entries must be published individually
        # (different routing keys), direct-queue entries can be batched.
        topic_entries, queue_entries = entries.partition { |e| e.routing_key.present? }

        succeeded = 0
        topic_entries.each { |e| succeeded += 1 if publish_single(e) }
        succeeded += publish_queue_batch(queue_entries)
        succeeded
      end

      def publish_single(entry)
        Pgbus.client.publish_to_topic(
          entry.routing_key,
          entry.payload,
          headers: entry.headers,
          delay: entry.delay || 0
        )
        entry.update!(published_at: Time.current)
        true
      rescue StandardError => e
        ErrorReporter.report(e, { action: "outbox_publish_topic", entry_id: entry.id })
        false
      end

      # Group direct-queue entries by (queue_name, priority, delay) and
      # use send_batch for each group to reduce round-trips.
      def publish_queue_batch(entries)
        return 0 if entries.empty?

        succeeded = 0
        entries.group_by { |e| [e.queue_name, e.priority, e.delay || 0] }.each do |(queue, priority, delay), group|
          payloads = group.map(&:payload)
          headers = group.map(&:headers)
          headers = nil if headers.all?(&:blank?)

          Pgbus.client.send_batch(queue, payloads, headers: headers, delay: delay, priority: priority)
          now = Time.current
          group.each { |e| e.update!(published_at: now) }
          succeeded += group.size
        rescue StandardError => e
          ErrorReporter.report(e, { action: "outbox_batch_publish", queue: queue, batch_size: group.size })
          # Fall back to individual publishing for this group
          group.each { |entry| succeeded += 1 if publish_single_queue(entry) }
        end
        succeeded
      end

      # Fallback for individual publishing when a batch fails. Passes the same
      # priority as the batch path so queue placement is identical either way.
      def publish_single_queue(entry)
        Pgbus.client.send_message(
          entry.queue_name,
          entry.payload,
          headers: entry.headers,
          delay: entry.delay || 0,
          priority: entry.priority
        )
        entry.update!(published_at: Time.current)
        true
      rescue StandardError => e
        ErrorReporter.report(e, { action: "outbox_publish_queue", entry_id: entry.id })
        false
      end

      def start_heartbeat
        @heartbeat = Process::Heartbeat.new(
          kind: "outbox_poller",
          config: @config,
          metadata: { pid: ::Process.pid, started_at: Time.now.to_f, supervisor_pid: ::Process.ppid }
        )
        @heartbeat.start
      end

      def shutdown
        @heartbeat&.stop
        restore_signals
        Pgbus.logger.info { "[Pgbus] Outbox poller stopped" }
      end
    end
  end
end
