# frozen_string_literal: true

module Pgbus
  # Proactive table maintenance to reduce bloat on PGMQ queue tables.
  #
  # PGMQ's read operation UPDATEs three columns (vt, read_ct, last_read_at)
  # on every message read. With the default fillfactor of 100, every UPDATE
  # creates a new heap tuple AND a new index entry — the dead tuple and its
  # old index pointer remain until VACUUM. Under sustained load, autovacuum
  # can't keep up and B-tree indexes bloat.
  #
  # Setting fillfactor=70 on queue tables reserves 30% of each page for
  # update churn. Because `vt` is indexed and changes on every read, these
  # writes are not HOT updates, but leaving headroom on heap pages still
  # reduces page density for a table that is updated heavily between vacuum
  # passes.
  #
  # More importantly, this module provides targeted VACUUM: instead of
  # relying solely on autovacuum's global heuristics, the dispatcher
  # periodically checks pg_stat_user_tables for tables with high dead tuple
  # ratios and vacuums them explicitly. This is inspired by pgque's
  # philosophy of measuring bloat before acting.
  module TableMaintenance
    FILLFACTOR = 70
    BLOAT_THRESHOLD = 0.1
    MAINTENANCE_INTERVAL = 6 * 3600 # 6 hours

    class << self
      def fillfactor_sql_for_queue(queue_name)
        "ALTER TABLE pgmq.q_#{queue_name} SET (fillfactor = #{FILLFACTOR});"
      end

      def fillfactor_sql_for_all_queues
        <<~SQL
          DO $$
          DECLARE
            q RECORD;
          BEGIN
            FOR q IN SELECT queue_name FROM pgmq.meta LOOP
              EXECUTE format('ALTER TABLE pgmq.%I SET (fillfactor = #{FILLFACTOR})', 'q_' || lower(q.queue_name));
            END LOOP;
          END $$;
        SQL
      end

      def vacuum_candidates(conn, threshold: BLOAT_THRESHOLD)
        rows = conn.exec(<<~SQL)
          SELECT schemaname, relname, n_dead_tup, n_live_tup
          FROM pg_stat_user_tables
          WHERE schemaname = 'pgmq'
            AND relname LIKE 'q_%'
          ORDER BY n_dead_tup DESC
        SQL

        rows.each_with_object([]) do |row, candidates|
          dead = row["n_dead_tup"].to_i
          live = row["n_live_tup"].to_i
          total = dead + live
          next if total.zero?

          ratio = dead.to_f / total
          next unless ratio > threshold

          candidates << {
            table: "#{row["schemaname"]}.#{row["relname"]}",
            dead_tuples: dead,
            live_tuples: live,
            dead_ratio: ratio.round(4)
          }
        end
      end

      def vacuum_sql(table)
        schema, relname = table.split(".", 2)
        "VACUUM \"#{schema}\".\"#{relname}\""
      end

      def reindex_sql(table)
        schema, relname = table.split(".", 2)
        "REINDEX TABLE CONCURRENTLY \"#{schema}\".\"#{relname}\""
      end

      # stop_check is polled before each candidate; when it returns true the
      # loop stops before touching the next table so a mid-pass shutdown does
      # not block on VACUUM/REINDEX of the remaining tables. The default never
      # stops, so callers that don't pass it are unaffected.
      def run_maintenance(conn, threshold: BLOAT_THRESHOLD, reindex: true, stop_check: -> { false })
        candidates = vacuum_candidates(conn, threshold: threshold)
        return 0 if candidates.empty?

        maintained = 0
        candidates.each_with_index do |candidate, index|
          if stop_check.call
            Pgbus.logger.info do
              "[Pgbus::TableMaintenance] Maintenance interrupted by shutdown after #{index} of #{candidates.size}"
            end
            break
          end

          table = candidate[:table]
          Pgbus.logger.info do
            "[Pgbus::TableMaintenance] Vacuuming #{table} " \
              "(dead_ratio=#{candidate[:dead_ratio]}, dead=#{candidate[:dead_tuples]})"
          end
          conn.exec(vacuum_sql(table))

          if reindex
            Pgbus.logger.info { "[Pgbus::TableMaintenance] Reindexing #{table}" }
            conn.exec(reindex_sql(table))
          end

          maintained += 1
        rescue StandardError => e
          Pgbus.logger.error { "[Pgbus::TableMaintenance] Failed to maintain #{table}: #{e.message}" }
        end

        maintained
      end
    end
  end
end
