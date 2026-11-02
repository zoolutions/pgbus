# frozen_string_literal: true

module Pgbus
  # Issue #633 upgrade path. PGMQ lower-cases its table names
  # (pgmq.format_table_name), so a queue an older pgbus created as
  # "pgbus_Orders" lives in pgmq.q_pgbus_orders while pgmq.meta records
  # "pgbus_Orders". pgbus now spells every queue in lower case; this renames
  # the old rows to match their tables.
  #
  # Only rows change. The queue tables are shared by both spellings, so nothing
  # here drops, renames or copies a table: dropping either spelling's queue
  # through PGMQ would remove the shared table.
  #
  # Run by the pgbus:fold_queue_name_case migration and by
  # `rake pgbus:queues:fold_case` (for a row an older process recreated after
  # the migration ran). Idempotent. Takes an ActiveRecord connection and
  # folds pgbus's queues only (those named "<prefix>_…" in any case), so
  # another PGMQ user of the same schema keeps its spelling and its NOTIFY
  # channel.
  module QueueNameFold
    module_function

    def run!(connection, prefix: Pgbus.configuration.queue_prefix)
      fold_pgmq_meta(connection, "#{prefix.to_s.downcase}_")
      fold_unique_column(connection, :pgbus_stream_queues, prefer: "")
      fold_unique_column(connection, :pgbus_queue_states, prefer: "paused DESC,")
      fold_column(connection, :pgbus_uniqueness_keys, :queue_name)
      fold_failed_event_source_queues(connection)
    end

    # One mixed-case pgmq.meta row at a time: add the lower-case row, move its
    # topic bindings, delete the old row (and its throttle and binding rows),
    # then re-arm the insert-NOTIFY trigger under the new name. PGMQ 1.13.1's
    # trigger NOTIFYs on the channel of the name it was armed with, which
    # listeners now spell in lower case. Where both spellings exist, the
    # lower-case row's throttle wins.
    def fold_pgmq_meta(connection, own_prefix)
      prefix = connection.quote(own_prefix)
      connection.execute(<<~SQL)
        DO $$
        DECLARE
          q RECORD;
          folded TEXT;
          throttle INTEGER;
          has_throttle BOOLEAN := to_regclass('pgmq.notify_insert_throttle') IS NOT NULL;
          has_bindings BOOLEAN := to_regclass('pgmq.topic_bindings') IS NOT NULL;
        BEGIN
          FOR q IN
            SELECT queue_name, is_partitioned, is_unlogged, created_at
            FROM pgmq.meta
            WHERE queue_name <> lower(queue_name)
              AND left(lower(queue_name), length(#{prefix})) = #{prefix}
            ORDER BY queue_name
          LOOP
            folded := lower(q.queue_name);

            INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged, created_at)
            VALUES (folded, q.is_partitioned, q.is_unlogged, q.created_at)
            ON CONFLICT DO NOTHING;

            throttle := NULL;
            IF has_throttle THEN
              SELECT COALESCE(
                (SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = folded),
                (SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = q.queue_name)
              ) INTO throttle;
              DELETE FROM pgmq.notify_insert_throttle WHERE queue_name = q.queue_name;
            END IF;

            IF has_bindings THEN
              INSERT INTO pgmq.topic_bindings (pattern, queue_name, bound_at)
              SELECT pattern, folded, bound_at FROM pgmq.topic_bindings WHERE queue_name = q.queue_name
              ON CONFLICT DO NOTHING;
              DELETE FROM pgmq.topic_bindings WHERE queue_name = q.queue_name;
            END IF;

            DELETE FROM pgmq.meta WHERE queue_name = q.queue_name;

            IF throttle IS NOT NULL THEN
              PERFORM pgmq.enable_notify_insert(folded, throttle);
            END IF;
          END LOOP;
        END $$;
      SQL
    end

    # A column with a unique index: keep one row per lower-case name (the
    # `prefer` order first, then the row already in lower case, then the
    # oldest), delete the rest, then lower-case what is left.
    def fold_unique_column(connection, table, prefer:)
      return unless connection.table_exists?(table)

      connection.execute(<<~SQL)
        DELETE FROM #{table} WHERE id IN (
          SELECT id FROM (
            SELECT id, row_number() OVER (
              PARTITION BY lower(queue_name)
              ORDER BY #{prefer} (queue_name = lower(queue_name)) DESC, id
            ) AS rank
            FROM #{table}
            WHERE lower(queue_name) IN (SELECT lower(queue_name) FROM #{table} WHERE queue_name <> lower(queue_name))
          ) ranked
          WHERE rank > 1
        )
      SQL
      fold_column(connection, table, :queue_name)
    end

    def fold_column(connection, table, column)
      return unless connection.table_exists?(table) && connection.column_exists?(table, column)

      connection.execute("UPDATE #{table} SET #{column} = lower(#{column}) WHERE #{column} <> lower(#{column})")
    end

    # source_queue is the physical queue a failure was read from, now spelled in
    # lower case. queue_name is the executor's logical name, as configured, and
    # stays. A message that failed under both spellings (before and after the
    # deploy) has two rows: one per (queue_name, msg_id, folded source) stays,
    # the lower-case one if there is one (written after the deploy), else the
    # newest, so the (queue_name, msg_id, source_queue) unique index holds.
    def fold_failed_event_source_queues(connection)
      return unless connection.table_exists?(:pgbus_failed_events) && connection.column_exists?(:pgbus_failed_events, :source_queue)

      connection.execute(<<~SQL)
        DELETE FROM pgbus_failed_events WHERE id IN (
          SELECT id FROM (
            SELECT id, row_number() OVER (
              PARTITION BY queue_name, msg_id, lower(source_queue)
              ORDER BY (source_queue = lower(source_queue)) DESC, id DESC
            ) AS rank
            FROM pgbus_failed_events
            WHERE (queue_name, msg_id, lower(source_queue)) IN (
              SELECT queue_name, msg_id, lower(source_queue) FROM pgbus_failed_events
              WHERE source_queue <> lower(source_queue)
            )
          ) ranked
          WHERE rank > 1
        )
      SQL
      fold_column(connection, :pgbus_failed_events, :source_queue)
    end

    private_class_method :fold_pgmq_meta, :fold_unique_column, :fold_column, :fold_failed_event_source_queues
  end
end
