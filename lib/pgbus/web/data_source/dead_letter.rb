# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The dead-letter queue pages (issue #495): list, count, detail, and the
      # retry/discard actions. Moved out of data_source.rb unchanged.
      module DeadLetter
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
      end
    end
  end
end
