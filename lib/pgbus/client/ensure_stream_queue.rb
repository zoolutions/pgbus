# frozen_string_literal: true

module Pgbus
  class Client
    # Idempotent stream-queue setup. Creates the PGMQ queue (delegating to
    # `ensure_queue` which already handles schema bootstrap and dedup),
    # overrides the NOTIFY throttle to 0 so every broadcast fires its
    # own NOTIFY, and adds an `msg_id` index on the archive table that
    # PGMQ does not ship with.
    #
    # PGMQ's archive tables (`pgmq.a_<name>`) only carry an `archived_at`
    # index by default. `Client#read_after`'s replay query filters by
    # `WHERE msg_id > $1`, which becomes a sequential scan once the archive
    # grows past a few thousand rows. We add the index here, scoped to
    # stream queues only, so users with chat-history-style retention don't
    # hit a performance cliff.
    #
    # Called from `Pgbus.stream(name).broadcast(...)` on first publish per
    # stream and from the streamer on first subscription per stream.
    #
    # Returns true once the setup is settled and memoized, false when part of
    # it rode the caller's open transaction (the index DDL on the shared
    # ActiveRecord connection, or the registry row): a rollback can take that
    # back, so it is not memoized before the commit and the next call runs it
    # again, the rule `@queues_created` follows (issue #695).
    module EnsureStreamQueue
      def ensure_stream_queue(stream_name)
        full_name = config.queue_name(stream_name)

        with_stale_connection_retry do
          # Create the BARE queue directly. ensure_queue would fan out through
          # the priority strategy to _p0.._pN under priority_levels>1, leaving
          # the bare queue — the one the streamer NOTIFYs on and read_after
          # peeks — uncreated, and the enable_notify_if_needed below would then
          # raise on the missing bare table. Streams never use priority
          # sub-queues (issue #310).
          ensure_pgmq_schema
          ensure_stream_queue_tables(full_name, stream_name)
        end

        # CREATE INDEX IF NOT EXISTS is idempotent in Postgres but still
        # requires a roundtrip and a brief ACCESS SHARE lock on the archive
        # table. Broadcast-per-after_commit loops can hit this 1000x/sec on
        # the same stream, so memoize per-process after the first success.
        # The StreamQueue registration shares this memo — both are one-time
        # per stream per process and must both survive a first-broadcast.
        return true if @stream_indexes_created[stream_name]

        rides_caller_transaction = queue_ddl_rides_caller_transaction?
        sanitized = QueueNameValidator.sanitize!(full_name)
        sql = <<~SQL
          CREATE INDEX IF NOT EXISTS a_#{sanitized}_msg_id_idx
          ON pgmq.a_#{sanitized} (msg_id)
        SQL

        begin
          synchronized do
            with_raw_connection do |conn|
              conn.exec(sql)
            end
          end
        rescue StandardError => e
          # CREATE INDEX IF NOT EXISTS shares pgmq.create's catalog race
          # (issue #404): two first-broadcasts both pass the existence
          # check and the loser gets a raw unique_violation on pg_class.
          # The duplicate proves the index exists — carry on to register
          # and memoize.
          raise unless duplicate_relation_error?(e)
        end

        # Record the physical queue name so maintenance (stream-archive prune,
        # orphan sweep, compact_archives) and wildcard workers can tell this
        # queue apart from a job queue. No-ops on unmigrated installs.
        # Memoized once the registry row is durable: now, or at the caller's
        # commit when the row rode its transaction (never on its rollback).
        # Index DDL that rode the caller's transaction (the shared connection)
        # is never memoized here.
        Pgbus::StreamQueue.record!(full_name) do
          @stream_indexes_created[stream_name] = true unless rides_caller_transaction
        end
        @stream_indexes_created.fetch(stream_name, false)
      end

      private

      # Creates the bare stream queue + forces NOTIFY throttle 0. Retries once
      # when a process-local `@queues_created` memo is stale relative to the
      # database: another process can drop the physical queue (orphan stream
      # sweep, dashboard drop, manual `drop_queue`) without invalidating every
      # peer process's memo. The memoized path then skips `pgmq.create` and
      # `enable_notify_insert` raises "Queue does not exist".
      def ensure_stream_queue_tables(full_name, stream_name)
        # Lock-retry wraps one setup attempt (create-path 250ms notify
        # plus the stream 0ms override). Missing-queue recovery stays
        # *outside* that budget so a later lock error cannot re-run
        # forget+recreate up to ATTEMPTS times. The recovery attempt
        # gets its own lock-retry — the DROP TRIGGER race can still
        # happen after we recreate. Sleeps run after synchronized
        # releases @pgmq_mutex. Do not fold these errors into
        # with_stale_connection_retry (idle-socket only).
        setup_stream_queue_tables(full_name, stream_name)
      rescue PGMQ::Errors::ConnectionError => e
        raise unless missing_pgmq_queue_error?(e)

        forget_stream_queue_memo!(full_name, stream_name)
        setup_stream_queue_tables(full_name, stream_name)
      end

      def setup_stream_queue_tables(full_name, stream_name)
        with_notify_lock_retry(stream_name) do
          ensure_single_queue(full_name)

          # PGMQ's default NOTIFY throttle is 250ms — meant to coalesce
          # high-frequency worker queue inserts. Streams are latency-
          # sensitive and need every broadcast to fire a NOTIFY, even
          # when several are batched within a single millisecond.
          # Override the throttle to 0 specifically for stream queues.
          synchronized { enable_notify_if_needed(full_name, 0) }
        end
      end

      def forget_stream_queue_memo!(full_name, stream_name)
        @queues_created.delete(full_name)
        @stream_indexes_created.delete(stream_name)
      end

      def missing_pgmq_queue_error?(error)
        message = error.message.to_s
        message.include?("does not exist") && message.include?("pgmq.create")
      end
    end
  end
end
