# frozen_string_literal: true

module Pgbus
  # Shared autovacuum storage parameters for tables with high write churn.
  #
  # Queue tables (q_*) have high insert/delete churn: every read + archive
  # cycle deletes from q_ and inserts into a_. Default autovacuum settings
  # (vacuum at 20% dead tuples) are far too conservative — dead tuples
  # accumulate, bloat B-tree indexes, and eventually degrade lock acquisition
  # times. See: https://planetscale.com/blog/keeping-a-postgres-queue-healthy
  #
  # Several pgbus-owned tables share similar churn patterns:
  # - pgbus_semaphores: rapid upsert+increment per job, periodic expiry
  # - pgbus_uniqueness_keys: INSERT on enqueue, DELETE on completion
  # - pgbus_processed_events: INSERT per event, bulk DELETE on TTL expiry
  #
  # Used by:
  # - Client#ensure_single_queue (runtime, on queue creation)
  # - CreatePgbusTables migration (fresh install)
  # - TunePgbusAutovacuum migration (upgrade for existing installations)
  module AutovacuumTuning
    # Queue tables: very aggressive — high delete rate from read+archive.
    QUEUE_SETTINGS = {
      "autovacuum_vacuum_scale_factor" => "0.01",
      "autovacuum_vacuum_cost_delay" => "2",
      "autovacuum_analyze_scale_factor" => "0.05"
    }.freeze

    # Archive tables: moderately aggressive — append-heavy with periodic purge.
    ARCHIVE_SETTINGS = {
      "autovacuum_vacuum_scale_factor" => "0.05",
      "autovacuum_vacuum_cost_delay" => "5",
      "autovacuum_analyze_scale_factor" => "0.05"
    }.freeze

    # High-churn pgbus tables: rapid INSERT/DELETE or upsert cycles.
    # - semaphores: upsert + increment per job acquire, decrement on release, periodic expiry
    # - uniqueness_keys: INSERT on enqueue, DELETE on job completion (fast lifecycle)
    # - processed_events: INSERT per event handler, bulk DELETE on idempotency TTL expiry
    HIGH_CHURN_SETTINGS = {
      "autovacuum_vacuum_scale_factor" => "0.02",
      "autovacuum_vacuum_cost_delay" => "2",
      "autovacuum_analyze_scale_factor" => "0.05"
    }.freeze

    HIGH_CHURN_TABLES = %w[
      pgbus_semaphores
      pgbus_uniqueness_keys
      pgbus_processed_events
    ].freeze

    class << self
      # Generate ALTER TABLE SQL for a single queue's tables.
      def sql_for_queue(queue_name)
        [
          alter_table_sql("pgmq.q_#{queue_name}", QUEUE_SETTINGS),
          alter_table_sql("pgmq.a_#{queue_name}", ARCHIVE_SETTINGS)
        ].join("\n")
      end

      # Generate ALTER TABLE SQL for all queues discovered via pgmq.meta.
      def sql_for_all_queues
        <<~SQL
          DO $$
          DECLARE
            q RECORD;
          BEGIN
            FOR q IN SELECT queue_name FROM pgmq.meta LOOP
              EXECUTE format('ALTER TABLE pgmq.%I SET (#{settings_clause(QUEUE_SETTINGS)})', 'q_' || lower(q.queue_name));
              EXECUTE format('ALTER TABLE pgmq.%I SET (#{settings_clause(ARCHIVE_SETTINGS)})', 'a_' || lower(q.queue_name));
            END LOOP;
          END $$;
        SQL
      end

      # Generate ALTER TABLE SQL for pgbus-owned high-churn tables.
      def sql_for_high_churn_tables
        HIGH_CHURN_TABLES.map { |table| alter_table_sql(table, HIGH_CHURN_SETTINGS, if_exists: true) }.join("\n")
      end

      private

      def alter_table_sql(table, settings, if_exists: false)
        prefix = if_exists ? "ALTER TABLE IF EXISTS" : "ALTER TABLE"
        "#{prefix} #{table} SET (#{settings_clause(settings)});"
      end

      def settings_clause(settings)
        settings.map { |k, v| "#{k} = #{v}" }.join(", ")
      end
    end
  end
end
