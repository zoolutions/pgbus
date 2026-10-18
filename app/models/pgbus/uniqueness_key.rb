# frozen_string_literal: true

module Pgbus
  class UniquenessKey < BusRecord
    self.table_name = "pgbus_uniqueness_keys"
    self.primary_key = "lock_key"

    # Atomically try to acquire a uniqueness lock via INSERT ... ON CONFLICT.
    # PostgreSQL's unique index on lock_key guarantees at most one caller wins.
    # Returns true if acquired (row inserted), false if already locked.
    #
    # reacquire_same_message: true (the :while_executing executor path) treats
    # a conflict with a row that already points at THIS msg_id as acquired —
    # it is this message's own previous attempt (PGMQ's visibility timeout
    # guarantees nobody else holds the message), left behind by a crash. A
    # conflict with a different msg_id is a genuine concurrent execution.
    def self.acquire!(lock_key, queue_name:, msg_id:, reacquire_same_message: false) # rubocop:disable Naming/PredicateMethod
      on_conflict = if reacquire_same_message && msg_id.to_i.positive?
                      "DO UPDATE SET queue_name = EXCLUDED.queue_name " \
                        "WHERE #{table_name}.msg_id = EXCLUDED.msg_id"
                    else
                      "DO NOTHING"
                    end
      result = connection.exec_query(
        "INSERT INTO #{table_name} (lock_key, queue_name, msg_id) " \
        "VALUES ($1, $2, $3) ON CONFLICT (lock_key) #{on_conflict} RETURNING lock_key, created_at",
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
    # successor that re-acquired the key. A retry passes the stamp unbind!
    # returned as +acquired_at+.
    def self.bind!(lock_key, queue_name:, msg_id:, acquired_at: nil)
      stamps = Thread.current[:pgbus_uniqueness_created_at]
      created_at = acquired_at || stamps&.delete(lock_key)
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

    # Release a lock only while it still points at this message. Used by the
    # executor when it finds its message already archived by another worker:
    # the :while_executing lock it took belongs to this attempt and has to go
    # back, but an unconditional key-only DELETE could drop a successor that
    # has since acquired the same key.
    # PGMQ message ids are per-queue sequences, so a msg_id alone is not an
    # identity: the same number addresses a different message on every other
    # queue. The queue is part of the match.
    def self.release_if_bound!(lock_key, queue_name:, msg_id:)
      Thread.current[:pgbus_uniqueness_created_at]&.delete(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1 AND queue_name = $2 AND msg_id = $3",
        "UniquenessKey Release If Bound", [lock_key, queue_name.to_s, msg_id.to_i]
      )
    end

    # The row a message holds: bound to that message (queue + msg_id, see
    # release_if_bound!), or still unbound — a parked job's row, or one whose
    # bind failed — and acquired no later than the message was sent ($4). A
    # lock is always taken before its message exists, so an unbound row
    # acquired after the send is a successor's. created_at is a timestamp
    # without time zone written in the session's zone, and the cast compares
    # it in that same zone; PGMQ's enqueued_at string carries its offset.
    # Unbound means msg_id <= 0 only: "pending" (the placeholder queue) is a
    # name a real queue can have too, and a lock bound to one of its messages
    # is bound.
    HELD_BY_CONDITION = "lock_key = $1 AND ((queue_name = $2 AND msg_id = $3 AND msg_id > 0) OR " \
                        "(msg_id <= 0 AND created_at <= $4::timestamptz))"
    private_constant :HELD_BY_CONDITION

    # Release the lock a finishing message holds (success or DLQ). Unlike the
    # key-only release!, it leaves a row that has moved on: handed to the
    # message's retry (unbind!), or taken by a successor after this
    # message's own row went. +enqueued_at+ is when that message was sent;
    # without it only the row bound to the message goes (an unbound row could
    # be a retry's it handed over), and the reaper takes an orphan.
    def self.release_held_by!(lock_key, queue_name:, msg_id:, enqueued_at:)
      Thread.current[:pgbus_uniqueness_created_at]&.delete(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE #{HELD_BY_CONDITION}",
        "UniquenessKey Release Held By", held_by_binds(lock_key, queue_name, msg_id, sent_at_literal(enqueued_at))
      )
    end

    # Hand the running attempt's lock to its retry, which goes out through the
    # key the attempt still holds (issue #333) and never acquires one. Called
    # before the retry exists, as a first enqueue acquires before its send:
    # the row is left unbound on the retry's queue and stamped now, after the
    # attempt was sent, so the attempt's release no longer counts it as its
    # own while the retry's (sent later) does, even if the retry finishes
    # before bind! points the row at it. Returns the new stamp for that
    # bind!, or nil when the attempt no longer holds the key. Without the
    # attempt's enqueued_at only a row bound to it is taken.
    def self.unbind!(lock_key, from_queue_name:, from_msg_id:, from_enqueued_at:, queue_name:)
      binds = held_by_binds(lock_key, from_queue_name, from_msg_id, sent_at_literal(from_enqueued_at))
      result = connection.exec_query(
        "UPDATE #{table_name} SET queue_name = $5, msg_id = 0, created_at = CURRENT_TIMESTAMP " \
        "WHERE #{HELD_BY_CONDITION} RETURNING created_at",
        "UniquenessKey Unbind", binds + [queue_name.to_s]
      )
      result.rows.first&.first
    end

    # release_held_by! for many messages at once — the dashboard's bulk
    # discards and queue purges — as [lock_key, queue_name, msg_id,
    # enqueued_at] tuples, one statement per thousand.
    def self.release_all_held_by!(holders)
      holders.each_slice(1000).sum do |slice|
        values = slice.map do |lock_key, queue_name, msg_id, enqueued_at|
          "(#{connection.quote(lock_key.to_s)}, #{connection.quote(queue_name.to_s)}, #{msg_id.to_i}, " \
            "#{connection.quote(sent_at_literal(enqueued_at))}::timestamptz)"
        end
        connection.exec_delete(
          "DELETE FROM #{table_name} AS u USING (VALUES #{values.join(", ")}) AS m(lock_key, queue_name, msg_id, sent_at) " \
          "WHERE u.lock_key = m.lock_key AND ((u.queue_name = m.queue_name AND u.msg_id = m.msg_id AND u.msg_id > 0) OR " \
          "(u.msg_id <= 0 AND u.created_at <= m.sent_at))",
          "UniquenessKey Release All Held By"
        )
      end
    end

    def self.held_by_binds(lock_key, queue_name, msg_id, sent_at)
      [lock_key, queue_name.to_s, msg_id.to_i, sent_at]
    end
    private_class_method :held_by_binds

    # A message's enqueued_at as a timestamptz literal that carries its own
    # offset. PGMQ hands over a String that already does; a Time (the
    # dashboard reads it through ActiveRecord) or epoch seconds would
    # otherwise be bound as zone-less digits and read in the session's zone.
    # An unknown send time is -infinity: no unbound row counts as the
    # message's own.
    def self.sent_at_literal(enqueued_at)
      sent_at = enqueued_at.is_a?(Numeric) ? Time.at(enqueued_at) : enqueued_at
      return "-infinity" if sent_at.nil?

      sent_at.respond_to?(:iso8601) ? sent_at.iso8601(6) : sent_at.to_s
    end
    private_class_method :sent_at_literal

    # Release a lock only while it is still UNBOUND (msg_id = 0) AND was
    # acquired no later than +acquired_before+. Used when discarding a parked
    # job: a parked job never got a msg_id, so its lock was never bound, and
    # there is no message to identify it by the way release_if_bound! does.
    #
    # Two things could otherwise be dropped by mistake once the unbound-lock
    # reaper has removed the parked job's own row and a successor has taken the
    # same key: a successor that was actually sent (its row is bound, excluded
    # by msg_id = 0), and a successor that is itself parked (also unbound, so
    # only the timestamp separates them). The lock is acquired immediately
    # before the row is parked, so a lock created AFTER the parked row cannot
    # belong to it — pass the parked row's created_at as the ceiling.
    def self.release_if_unbound!(lock_key, acquired_before:)
      Thread.current[:pgbus_uniqueness_created_at]&.delete(lock_key)
      connection.exec_delete(
        "DELETE FROM #{table_name} WHERE lock_key = $1 AND msg_id = 0 AND created_at <= $2",
        "UniquenessKey Release If Unbound", [lock_key, acquired_before]
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
