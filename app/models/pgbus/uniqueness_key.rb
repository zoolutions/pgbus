# frozen_string_literal: true

module Pgbus
  class UniquenessKey < BusRecord
    self.table_name = "pgbus_uniqueness_keys"
    self.primary_key = "lock_key"

    # Atomically try to acquire a uniqueness lock via INSERT ... ON CONFLICT.
    # PostgreSQL's unique index on lock_key guarantees at most one caller wins.
    # Returns true if acquired (row inserted), false if already locked.
    def self.acquire!(lock_key, queue_name:, msg_id:) # rubocop:disable Naming/PredicateMethod
      result = connection.exec_query(
        "INSERT INTO #{table_name} (lock_key, queue_name, msg_id) " \
        "VALUES ($1, $2, $3) ON CONFLICT (lock_key) DO NOTHING RETURNING lock_key, created_at",
        "UniquenessKey Acquire", [lock_key, queue_name, msg_id]
      )
      row = result.rows.first
      return false unless row

      # Ownership stamp for bind!: a successor acquire of the same key after
      # this row is released must not inherit this enqueue's msg_id.
      stamps = Thread.current[:pgbus_uniqueness_created_at] ||= {}
      stamps[lock_key] = row[1]
      true
    end

    # Bind a pre-produce lock to the real queue and PGMQ msg_id after send.
    # Does not touch created_at — the reaper's age floor is from acquire time.
    # Restricted to this enqueue's unbound row (msg_id=0, matching created_at
    # when acquire! stamped one) so a completed job's bind cannot retarget a
    # successor that re-acquired the key.
    def self.bind!(lock_key, queue_name:, msg_id:)
      stamps = Thread.current[:pgbus_uniqueness_created_at]
      created_at = stamps&.delete(lock_key)
      sql = "UPDATE #{table_name} SET queue_name = $2, msg_id = $3 " \
            "WHERE lock_key = $1 AND msg_id = 0"
      binds = [lock_key, queue_name, msg_id]
      if created_at
        sql += " AND created_at = $4"
        binds << created_at
      end
      connection.exec_update(sql, "UniquenessKey Bind", binds)
    end

    # Drop the bind ownership stamp without touching the lock row. Used when
    # this thread acquired the key but will not bind (concurrency :block, or
    # enqueue returning after a failed send already rolled the lock back).
    def self.clear_bind_stamp!(lock_key)
      Thread.current[:pgbus_uniqueness_created_at]&.delete(lock_key)
    end

    # Release a uniqueness lock after job completion or DLQ.
    def self.release!(lock_key)
      Thread.current[:pgbus_uniqueness_created_at]&.delete(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1",
        "UniquenessKey Release", [lock_key]
      )
    end

    # Check if a key is currently locked.
    def self.locked?(lock_key)
      result = connection.select_value(
        "SELECT 1 FROM #{table_name} WHERE lock_key = $1 LIMIT 1",
        "UniquenessKey Check", [lock_key]
      )
      !result.nil?
    end
  end
end
