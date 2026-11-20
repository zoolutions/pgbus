# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # The dead-letter queue pages (issue #495): list, count, detail, and the
      # retry/discard actions. Moved out of data_source.rb unchanged.
      module DeadLetter
        # The header path the error-class filter reads (DeadLetterHeader::KEY).
        ERROR_CLASS_SQL = "headers #>> '{#{Pgbus::DeadLetterHeader::KEY},error_class}'".freeze

        # DLQ queue names from queue_names are already fully qualified
        # (e.g., "pgbus_default_dlq"), so we use them directly without re-prefixing.
        # dlq: one DLQ by its full name (unknown names list nothing);
        # error_class: only messages whose dead-letter block names that class.
        def dlq_messages(page: 1, per_page: 25, dlq: nil, error_class: nil)
          offset = (page - 1) * per_page
          names = dlq_queue_names(dlq)
          return [] if names.empty?
          return paginated_queue_messages(names, per_page, offset) unless error_class

          paginated_queue_messages(names, per_page, offset, where: "#{ERROR_CLASS_SQL} = $3", binds: [error_class])
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ messages: #{e.message}" }
          []
        end

        # From the cached metrics, unless a filter needs the rows or a DLQ's
        # metrics are unavailable (issue #677): then a COUNT over the tables,
        # so the pager never reads a DLQ that lists rows as empty.
        def dlq_total_count(dlq: nil, error_class: nil)
          names = dlq_queue_names(dlq)
          return 0 if names.empty?
          return dlq_counts_by_queue.values_at(*names).sum unless error_class || dlq_unmeasured?(names)

          where = "#{ERROR_CLASS_SQL} = $1" if error_class
          connection.select_value(
            "SELECT COUNT(*) FROM (#{queue_message_fragments(names, where)}) AS combined",
            "Pgbus DLQ Count",
            error_class ? [error_class] : []
          ).to_i
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ count: #{e.message}" }
          0
        end

        # { "pgbus_default_dlq" => 12, ... } for the filter chips: every DLQ
        # from queue_names, its count from the cached metrics (no query of its
        # own). A DLQ whose metrics are unavailable (issue #677) stays listed
        # and counts 0.
        def dlq_counts_by_queue
          lengths = queues_with_metrics.to_h { |q| [q[:name], q[:queue_length].to_i] }
          dlq_names.to_h { |name| [name, lengths.fetch(name, 0)] }
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error fetching DLQ counts: #{e.message}" }
          {}
        end

        # msg_ids are unique per DLQ table only: queue_name: picks the table;
        # without it the first DLQ holding the id wins.
        def dlq_message_detail(msg_id, queue_name: nil)
          dlq_queue_names(queue_name).each do |name|
            row = connection.select_one(
              "SELECT * FROM pgmq.q_#{sanitize_name(name)} WHERE msg_id = $1",
              "Pgbus DLQ Detail",
              [msg_id.to_i]
            )
            return format_message(row, name) if row
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

          payload = row["message"].is_a?(String) ? JSON.parse(row["message"]) : row["message"]
          # The row Discard would release is the one the copy takes over.
          holder = lock_holder(queue_name, row["msg_id"], row["headers"], row["enqueued_at"])
          # A live message must not claim to be dead: strip the block, count the trip.
          headers = DeadLetterHeader.strip_for_retry(row["headers"])
          drop_source = -> { @client.delete_message(queue_name, msg_id.to_i, prefixed: false) }
          outcome = resend_job(payload, lock_queue: lock_queue_for_table(original_queue), holder: holder,
                                        drop_source: drop_source, headers: JSON.parse(headers)) do
            @client.transaction do |txn|
              sent = txn.produce(original_queue, row["message"], headers: headers)
              txn.delete(queue_name, msg_id.to_i)
              sent
            end
          end
          outcome != false
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

        private

        # Only names that are real DLQs ever reach sanitize_name and SQL. Names
        # only: listing or opening a DLQ message runs no metrics statement.
        def dlq_queue_names(dlq)
          names = dlq_names
          dlq.nil? ? names : names & [dlq]
        end

        def dlq_names
          queue_names.select { |name| name.end_with?(Pgbus::DEAD_LETTER_SUFFIX) }
        end

        def dlq_unmeasured?(names)
          queues_with_metrics.any? { |q| q[:metrics_unavailable] && names.include?(q[:name]) }
        end
      end
    end
  end
end
