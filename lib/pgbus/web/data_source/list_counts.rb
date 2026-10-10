# frozen_string_literal: true

module Pgbus
  module Web
    class DataSource
      # Row totals for the dashboard's paged lists (issue #496). Each count
      # stops at COUNT_CAP + 1 rows, so a table with millions of rows costs no
      # more than one with ten thousand; past the cap the pager shows "10,000+".
      # The same bound JobList uses for its state tabs.
      module ListCounts
        COUNT_CAP = 10_000

        Count = Data.define(:total, :capped) do
          def capped? = capped
        end

        EMPTY_COUNT = Count.new(total: 0, capped: false)

        # Semaphores and parked jobs, FULL OUTER JOINed: a key can have parked
        # rows and no semaphore (its holder died and the sweep removed the row)
        # or a semaphore and nothing parked. Shared with concurrency_keys.
        CONCURRENCY_KEYS_FROM = <<~SQL
          FROM pgbus_semaphores s
          FULL OUTER JOIN (
            SELECT concurrency_key, COUNT(*) AS parked_count, MIN(created_at) AS oldest_parked_at
            FROM pgbus_blocked_executions
            GROUP BY concurrency_key
          ) b ON s.key = b.concurrency_key
        SQL

        LIST_MODELS = {
          batches: -> { BatchEntry },
          job_locks: -> { UniquenessKey },
          outbox: -> { OutboxEntry },
          recurring_tasks: -> { RecurringTask }
        }.freeze

        # list – :batches, :job_locks, :concurrency_keys, :outbox or :recurring_tasks.
        def list_count(list)
          raise ArgumentError, "unknown list: #{list.inspect}" unless list == :concurrency_keys || LIST_MODELS.key?(list)

          bounded_count(list)
        end

        def batches_count
          BatchEntry.count
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error counting batches: #{e.message}" }
          0
        end

        def recurring_tasks_count
          RecurringTask.count
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error counting recurring tasks: #{e.message}" }
          0
        end

        private

        def bounded_count(list)
          rows = list == :concurrency_keys ? concurrency_keys_probe : LIST_MODELS.fetch(list).call.limit(COUNT_CAP + 1).count
          Count.new(total: [rows.to_i, COUNT_CAP].min, capped: rows.to_i > COUNT_CAP)
        rescue StandardError => e
          Pgbus.logger.debug { "[Pgbus::Web] Error counting #{list}: #{e.message}" }
          EMPTY_COUNT
        end

        def concurrency_keys_probe
          connection.select_value(<<~SQL, "Pgbus Concurrency Keys Count")
            SELECT COUNT(*) FROM (SELECT 1 #{CONCURRENCY_KEYS_FROM} LIMIT #{COUNT_CAP + 1}) c
          SQL
        end
      end
    end
  end
end
