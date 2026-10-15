# frozen_string_literal: true

module Pgbus
  # Registry of physical PGMQ queue names that back streams (as opposed to
  # job queues). Stream and job queues share the same `#{queue_prefix}_`
  # namespace (see Configuration#queue_name), so there is no reliable way to
  # tell them apart by name alone. `ensure_stream_queue` records each stream
  # queue here on first broadcast/subscribe; consumers that iterate
  # `pgmq.meta` (the dispatcher's stream-archive prune and orphan sweep,
  # `compact_archives`, and the worker's wildcard resolution) use this
  # registry to classify a queue as a stream.
  #
  # Degrades safely: on an install that has not run the migration, the table
  # is absent — `record!` no-ops and `all_names` returns an empty set, so the
  # pre-registry behavior (streams treated as job queues by maintenance) is
  # preserved rather than raising.
  #
  # Pre-registry dormant streams (issue #366): queues created before this
  # table existed never re-broadcast, so they never call `record!`. Those
  # queues still carry the archive `a_<name>_msg_id_idx` index that only
  # `ensure_stream_queue` creates — `fingerprint_matched_names` discovers
  # them, `known_names` unions them with the registry for classification,
  # and `backfill!` (via `rake pgbus:streams:backfill_registry`) writes
  # the missing rows so the registry is complete.
  class StreamQueue < BusRecord
    self.table_name = "pgbus_stream_queues"

    # Serializes the WARN-once latch in log_record_failure: record! runs on
    # the coalescer flush thread as well as callers' threads, and an
    # unsynchronized check-and-set could WARN more than once per process.
    @record_failure_mutex = Mutex.new

    # Guards the @all_names memo (issue #527). The memo is a frozen Set,
    # replaced whole (record! publishes a copy with the new name), so a
    # caller iterating the set it got can never see it change underneath:
    # record! runs on broadcast threads while the dispatcher iterates it.
    @names_lock = Mutex.new

    class << self
      # Inserts the physical queue name. Idempotent and cheap to call on
      # every broadcast; the caller (`ensure_stream_queue`) also memoizes
      # per-process, so the DB write happens once per stream per process.
      # Errors are swallowed — a registry hiccup must never abort a broadcast.
      # Returns true on a successful write, false when the table is absent or
      # the insert failed (so callers like `backfill!` can report accurately).
      #
      # Deliberately raw SQL rather than `upsert(unique_by:)` (issue #401):
      # Rails resolves `unique_by:` through the pool's schema cache, which
      # caches a negative `data_source_exists?` probe permanently — one wrong
      # first probe (while the table genuinely exists and the live
      # `table_exists?` guard above passes) poisons every subsequent record!
      # in the process with "No unique index found". The unique index is owned
      # by this gem's own migration, so there is nothing to resolve.
      def record!(queue_name)
        return false unless table_exists?

        conn = connection
        conn.execute(
          "INSERT INTO #{conn.quote_table_name(table_name)} (queue_name) " \
          "VALUES (#{conn.quote(queue_name)}) ON CONFLICT (queue_name) DO NOTHING"
        )
        # Keep the in-process cache consistent with the write so a subsequent
        # stream? check reflects this registration without a re-query. Only
        # update an ALREADY-LOADED cache — if @all_names is still nil (this
        # process hasn't queried the registry yet), seeding it here would
        # fabricate a one-entry set and silently hide every other
        # already-registered stream until the next reset_cache!. Leaving it
        # nil lets the next all_names call do a real load, which already
        # includes this row since the insert above has committed.
        @names_lock.synchronize { @all_names = (@all_names | [queue_name]).freeze if @all_names }
        true
      rescue ActiveRecord::ActiveRecordError => e
        Pgbus.logger.debug { "[Pgbus] Failed to record stream queue #{queue_name}: #{e.message}" }
        false
      rescue StandardError => e
        log_record_failure(queue_name, e)
        false
      end

      # Set of all registered physical stream queue names. Memoized so a
      # dispatcher maintenance pass or a wildcard re-resolve does one query,
      # not one per queue. Callers that need freshness (a long-lived process
      # picking up newly-created streams) call `reset_cache!` at the top of
      # their loop.
      # The load runs outside the lock (a duplicate load on a race is
      # harmless); the first one published wins.
      def all_names
        cached = @names_lock.synchronize { @all_names }
        return cached if cached

        loaded = load_names.freeze
        @names_lock.synchronize { @all_names ||= loaded }
      end

      # Registry ∪ fingerprint-matched names. Use this for classification
      # (health verdict exclusion, orphan sweep, wildcard worker exclusion)
      # so dormant pre-registry streams are not treated as job queues.
      # Does not write — call `backfill!` to persist the fingerprint matches.
      def known_names
        all_names | fingerprint_matched_names
      end

      # Physical queue names that carry the stream-only archive msg_id index
      # (`a_<queue>_msg_id_idx`). Job queues never get this index. Safe to call
      # when pgmq is absent — returns an empty set rather than raising.
      def fingerprint_matched_names
        Set.new(connection.select_values(<<~SQL.squish))
          SELECT m.queue_name
          FROM pgmq.meta m
          INNER JOIN pg_indexes i
            ON i.schemaname = 'pgmq'
           AND i.indexname = 'a_' || m.queue_name || '_msg_id_idx'
        SQL
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to discover stream queue fingerprints: #{e.message}" }
        Set.new
      end

      # Registers fingerprint-matched queues missing from the registry.
      # Idempotent. Returns the number of names successfully persisted
      # (failed upserts are not counted — see `record!`). No-ops (returns 0)
      # when the registry table is absent.
      def backfill!
        return 0 unless table_exists?

        missing = fingerprint_matched_names - all_names
        return 0 if missing.empty?

        registered = missing.count { |name| record!(name) }
        reset_cache! if registered.positive?
        registered
      end

      def stream?(queue_name)
        all_names.include?(queue_name)
      end

      # Drops the memoized set so the next `all_names`/`stream?` re-queries.
      # Called at the start of each dispatcher maintenance pass and each
      # wildcard resolution so freshly-created streams are picked up without
      # a process restart.
      def reset_cache!
        @names_lock.synchronize { @all_names = nil }
      end

      # Memoized like StreamStat: a successful probe sticks; a transient
      # error returns false without caching, so the next call retries.
      # An idempotent memo: concurrent first callers probe the same schema
      # and store the same Boolean, so the unlocked read/write is safe.
      # rubocop:disable-next ThreadSafety/ClassInstanceVariable
      def table_exists?
        return @table_exists if defined?(@table_exists) && @table_exists

        @table_exists = connection.table_exists?(table_name)
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to check stream queue table: #{e.message}" }
        false
      end

      private

      # A non-ActiveRecord error out of a plain INSERT is a bug signal (the
      # old path's ArgumentError from index resolution was one), not a DB
      # hiccup — surface it at WARN once per process instead of drowning a
      # process-lifetime malfunction in per-broadcast DEBUG spam.
      def log_record_failure(queue_name, error)
        message = "[Pgbus] Failed to record stream queue #{queue_name}: #{error.class}: #{error.message}"
        first = @record_failure_mutex.synchronize do
          @record_failure_warned ? false : (@record_failure_warned = true)
        end
        first ? Pgbus.logger.warn { message } : Pgbus.logger.debug { message }
      end

      def load_names
        return Set.new unless table_exists?

        Set.new(all.pluck(:queue_name))
      rescue StandardError => e
        Pgbus.logger.debug { "[Pgbus] Failed to load stream queues: #{e.message}" }
        Set.new
      end
    end
  end
end
