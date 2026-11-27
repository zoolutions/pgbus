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
    # Returns true once the setup is settled and memoized, false while part of
    # it waits on the caller's open transaction (issue #695): the index DDL
    # rode it on the shared ActiveRecord connection, or the registry row is
    # written after its commit. A rollback leaves that part to the next call.
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
        # The StreamQueue registration has its own memo (issue #695): its row
        # waits for the caller's commit, so it is memoized only once written.
        ensure_stream_archive_index(full_name, stream_name) unless @stream_indexes_created[stream_name]
        register_stream_queue(full_name, stream_name) unless @streams_registered[stream_name]
        stream_queue_settled?(stream_name)
      end

      private

      def stream_queue_settled?(stream_name)
        @stream_indexes_created.key?(stream_name) && @streams_registered.key?(stream_name)
      end

      # Index DDL that rode the caller's transaction (the shared ActiveRecord
      # connection) is not memoized: a rollback takes it back, the rule
      # `@queues_created` follows.
      def ensure_stream_archive_index(full_name, stream_name)
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

        @stream_indexes_created[stream_name] = true unless rides_caller_transaction
      end

      # Record the physical queue name so maintenance (stream-archive prune,
      # orphan sweep, compact_archives) and wildcard workers can tell this
      # queue apart from a job queue. Memoized once the row is written: now,
      # or after the caller's commit (never on its rollback, so the next
      # broadcast records it again). An unmigrated install or a failed insert
      # (false) is memoized too, so it is not retried on every broadcast.
      def register_stream_queue(full_name, stream_name)
        result = Pgbus::StreamQueue.record!(full_name) { @streams_registered[stream_name] = true }
        @streams_registered[stream_name] = true if result == false
      end

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
        @streams_registered.delete(stream_name)
      end

      def missing_pgmq_queue_error?(error)
        message = error.message.to_s
        message.include?("does not exist") && message.include?("pgmq.create")
      end
    end
  end
end
