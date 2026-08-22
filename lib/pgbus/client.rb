# frozen_string_literal: true

require "json"
require "socket"
require "timeout"
require_relative "client/read_after"
require_relative "client/ensure_stream_queue"
require_relative "client/notify_stream"
require_relative "client/connection_health"
require_relative "client/resizable_pool"

module Pgbus
  class Client
    include ReadAfter
    include EnsureStreamQueue
    include NotifyStream

    attr_reader :pgmq, :config, :connection_health

    PGMQ_REQUIRE_MUTEX = Mutex.new
    private_constant :PGMQ_REQUIRE_MUTEX

    # Fixed advisory-lock key serializing pgmq schema installation across
    # processes (issue #397). "pgmqinst" in ASCII hex — arbitrary but stable;
    # it only has to be identical in every process that can install.
    PGMQ_INSTALL_LOCK_KEY = 0x70676D71_696E7374

    PGMQ_META_CHECK_SQL = "SELECT 1 FROM pg_tables WHERE schemaname = 'pgmq' AND tablename = 'meta' LIMIT 1"
    private_constant :PGMQ_META_CHECK_SQL

    PGMQ_INSTALL_SAVEPOINT = "pgbus_pgmq_install"
    private_constant :PGMQ_INSTALL_SAVEPOINT

    # Install-race losers see the winner's DDL as one of these. Matched by
    # class NAME so the check works whether or not the pg gem's generated
    # error classes are loaded in this process (mirrors the defined?(PG::…)
    # guards used elsewhere in this file).
    DUPLICATE_INSTALL_ERROR_CLASSES = %w[
      PG::UniqueViolation
      PG::DuplicateSchema
      PG::DuplicateTable
      PG::DuplicateObject
      PG::DuplicateFunction
    ].freeze
    private_constant :DUPLICATE_INSTALL_ERROR_CLASSES

    # Process-wide, not per-instance: on the shared-AR Proc path two Client
    # instances share one underlying libpq connection while each holding their
    # own @pgmq_mutex, so a per-instance guard cannot serialize bootstrap DDL —
    # concurrent install traffic desyncs the protocol ("message type 0x…
    # arrived from server while idle") and wedges a thread on a socket read
    # (issue #397, forensics in getzazu/app#3413).
    @pgmq_install_mutex = Mutex.new

    class << self
      attr_reader :pgmq_install_mutex
    end

    # Throttle window for PGMQ's enable_notify_insert trigger. Postgres
    # NOTIFYs are coalesced into one wake-up per window, so a value of 250ms
    # means: at most 4 broadcasts/sec per queue, regardless of insert rate.
    # The trigger is a Postgres-level concern; exposing it as a setting
    # never came up in practice and changing it on the fly would require
    # re-running the trigger DDL on every queue.
    NOTIFY_THROTTLE_MS = 250

    # PGMQ's per-queue NOTIFY trigger name, as created by
    # pgmq.enable_notify_insert — used to recognize the duplicate-trigger
    # race loser (issue #403).
    NOTIFY_TRIGGER_NAME = "trigger_notify_queue_insert_listeners"
    private_constant :NOTIFY_TRIGGER_NAME

    # Load the pgmq-ruby gem, defining the PGMQ module before requiring it so
    # Zeitwerk's eager_load (called inside pgmq.rb) can resolve the constant.
    # Without the pre-definition, Ruby 4.0 + Zeitwerk 2.7.5 raises NameError
    # because eager_load runs const_get(:Client) on PGMQ before the module is
    # defined. Extracted as a class method so unit specs that fake PGMQ::Client
    # can stub *this* (a per-example class-method stub, torn down cleanly)
    # instead of the global Kernel#require, which — if stubbed before pgmq is
    # genuinely loaded — permanently prevents the real gem from ever loading.
    def self.load_pgmq_gem!
      PGMQ_REQUIRE_MUTEX.synchronize do
        Object.const_set(:PGMQ, Module.new) unless defined?(::PGMQ)
        require "pgmq"
      end
    end

    # `schema_ensured:` lets a caller (in practice, tests) skip the one-time
    # PGMQ schema install probe by asserting the schema already exists. Defaults
    # to false so production always runs the check on first queue access.
    def initialize(config = Pgbus.configuration, schema_ensured: false)
      self.class.load_pgmq_gem!
      @config = config
      conn_opts = config.connection_options
      @shared_connection = conn_opts.is_a?(Proc)

      if @shared_connection
        # When using the Rails lambda path (-> { AR::Base.connection.raw_connection }),
        # the Proc returns the same underlying PG::Connection that ActiveRecord uses.
        # PG::Connection (libpq) is not thread-safe — concurrent access causes
        # segfaults and result corruption. Force pool_size=1 and serialize all
        # operations through a mutex.
        @pgmq = PGMQ::Client.new(conn_opts, pool_size: 1, pool_timeout: config.pool_timeout)
        @pgmq_mutex = Mutex.new
        # No dedicated streams pool on this path: a second PGMQ::Client would
        # still funnel through the same non-thread-safe AR raw_connection.
        # Stream publish + replay share the single serialized connection —
        # @streams_pgmq aliases @pgmq so the code paths are uniform, and
        # #with_streams_connection falls back to with_raw_connection.
        @streams_pgmq = @pgmq
      else
        # With a String URL or Hash params, pgmq-ruby creates its own dedicated
        # PG::Connection per pool slot — no shared state with ActiveRecord.
        # Use the resolved pool size (auto-tuned from worker thread counts
        # unless explicitly set) and let pgmq-ruby's connection_pool handle
        # concurrency internally (no mutex needed).
        #
        # Bound reads with libpq-native mechanisms baked into the connection
        # options (issue #198): a server-side statement_timeout for a slow query,
        # plus client-side tcp_user_timeout + keepalives for a dead/hung socket.
        # Both raise clean PG errors — no Ruby Timeout, no Thread#raise. Only
        # safe on this dedicated-connection branch — never on the shared-AR Proc
        # path, where statement_timeout would leak into application queries.
        conn_opts = wrap_session_gucs(apply_connection_bounds(conn_opts))
        @pgmq = PGMQ::Client.new(conn_opts, pool_size: config.resolved_pool_size, pool_timeout: config.pool_timeout)
        @pgmq_mutex = nil
        # Dedicated streams pool (issue #315): isolates the durable-stream
        # publish INSERT (#send_stream_message) and the dispatcher's per-wake
        # replay reads (#read_after) from the job pool, so a saturated worker
        # pool can't delay a broadcast on pool checkout, and each wake reuses a
        # persistent connection instead of a fresh PG.connect per call. Its own
        # PGMQ::Client → its own connection_pool, sized independently of worker
        # thread counts.
        # Build the streams pool from streams_pool_connection_options (defaults
        # to streams_connection_options so a separate streams DB carries the
        # pool with it — issue #315 — but overridable via streams_pool_* so a
        # pooler-bypass install keeps the pool off the direct port — issue
        # #358), bounds-applied, and tagged with a per-process application_name
        # so the autoscaler can count peer processes from pg_stat_activity
        # (issue #323 P1/P2). Snapshot it so a hot-swap rebuilds a
        # byte-identical pool at a new size.
        @streams_conn_opts = wrap_session_gucs(
          tag_application_name(
            apply_connection_bounds(config.streams_pool_connection_options)
          )
        )
        @streams_pgmq = PGMQ::Client.new(@streams_conn_opts, pool_size: config.streams_pool_size,
                                                             pool_timeout: config.streams_pool_timeout)
      end

      # Wrap the streams pool so its live reference can be atomically hot-swapped
      # to a new size under load without losing broadcasts or leaking connections
      # (issue #323 spike; #resize_streams_pool). All streams-pool access goes
      # through this — see #streams_pool. Default behavior with no swap is
      # byte-identical (one AtomicReference read + a counter bump per op).
      @streams_pool = ResizablePool.new(
        @streams_pgmq,
        shared: @shared_connection,
        drain_timeout: config.streams_pool_timeout + 1.0,
        logger: Pgbus.logger
      )

      @queues_created = Concurrent::Map.new
      @stream_indexes_created = Concurrent::Map.new
      # Guards the one-time build of the publisher autoscale trigger (issue #323).
      # NOT @pgmq_mutex — that is nil on the dedicated path (the only path the
      # trigger exists on), so it wouldn't serialize concurrent first-publishers.
      @streams_trigger_mutex = Mutex.new
      @queue_strategy = QueueFactory.for(config)
      @schema_ensured = schema_ensured
      @connection_health = ConnectionHealth.new(
        on_open: method(:log_circuit_open),
        on_close: method(:log_circuit_close)
      )
      # Snapshot whether libpq's baked-in read bounds fully cover a hung socket
      # on this host/connection, so the read path can skip the Ruby Timeout
      # last resort. Computed once: @shared_connection, config.read_timeout
      # (which apply_connection_bounds also snapshots), the platform, and the
      # linked libpq version are all fixed for a Client's lifetime.
      @libpq_read_bounds_effective = libpq_read_bounds_effective?
      warn_shared_connection_read_bounds
    end

    # True when this client shares ActiveRecord's connection (the Proc
    # connection_options path): pool_size is forced to 1 and every operation is
    # serialized through @pgmq_mutex. False on the dedicated-connection path,
    # where pgmq-ruby owns its own pool and no mutex is needed.
    def shared_connection?
      @shared_connection
    end

    # Whether the shared-connection serialization mutex is currently held. False
    # on the dedicated-connection path (no mutex). Lets callers assert that a
    # code path (e.g. a retry backoff sleep) runs OUTSIDE the mutex without
    # reaching into the mutex object itself.
    def synchronizing?
      @pgmq_mutex ? @pgmq_mutex.locked? : false
    end

    # Actively open a database connection and run `SELECT 1` so a bad
    # database_url / connection_params surfaces at boot instead of on the
    # first operation. PGMQ::Client's pool is lazy — nothing touches the
    # database at init — so without this the supervisor forks children that
    # crash-loop against an unreachable DB. Called from Supervisor#run before
    # any queue bootstrap or forking.
    #
    # Raises Pgbus::ConfigurationError (not a transient PGMQ error) because a
    # failure here means the operator's connection config is wrong: the message
    # carries the underlying error plus which config source was in use.
    def verify_connection!
      synchronized do
        @pgmq.with_connection do |conn|
          conn.exec("SELECT 1")
          # When require_primary is set, reject a connection that landed on a
          # read-only replica at boot rather than letting a read/write-splitting
          # pooler silently route pgmq's VOLATILE read/archive to a standby,
          # where workers read nothing and jobs stop with a healthy heartbeat
          # (issue #332). Off by default, so a single-primary deployment is
          # unaffected.
          Process::PrimaryValidator.validate_primary!(conn) if config.require_primary
        end
      end
      true
    rescue Process::ReplicaConnectionError => e
      raise ConfigurationError,
            "Database connection via #{connection_source} landed on a read-only replica " \
            "(require_primary is set): #{e.message}"
    rescue PGMQ::Errors::ConnectionError, PG::Error => e
      raise ConfigurationError, "Database connection failed via #{connection_source}: #{e.message}"
    end

    # Lightweight liveness probe used by the doctor: open a raw connection and
    # run `SELECT 1`. Unlike verify_connection! (which wraps failures as
    # ConfigurationError for the supervisor boot path), ping lets the raw
    # PG/PGMQ error propagate so the caller can render the underlying reason.
    # Returns true on success; a bad connection raises rather than returning
    # false — the caller renders the underlying reason — so this is a probe,
    # not a boolean predicate, hence no `?` suffix.
    def ping # rubocop:disable Naming/PredicateMethod
      with_raw_connection { |conn| conn.exec("SELECT 1") }
      true
    end

    # Whether the job connection currently lands on a read-only replica
    # (pg_is_in_recovery() => t). Used by the doctor to warn about a
    # read/write-splitting pooler that could route pgmq's VOLATILE read/archive
    # to a standby, silently stalling job processing (issue #332). Raw PG error
    # propagates so the caller can render the reason.
    def in_recovery?
      with_raw_connection do |conn|
        conn.exec(Process::PrimaryValidator::RECOVERY_QUERY).getvalue(0, 0) == "t"
      end
    end

    # The logical queue names pgbus expects to exist based on the configuration
    # (default queue + worker capsules + recurring tasks). Public wrapper around
    # collect_configured_queues so the doctor can diff configured-vs-existing
    # queues without reaching into PGMQ or config internals directly.
    def configured_queues
      collect_configured_queues
    end

    # Whether the given logical queue currently has a live PGMQ insert-NOTIFY
    # trigger with pgbus's throttle interval on every physical table it maps to.
    # Uses the same physical-name resolution as bootstrap (@queue_strategy), so
    # a priority queue's _p0.._pN sub-tables — where the trigger actually lives —
    # are all checked, not the bare prefixed name that priority mode never
    # creates. Returns false when any physical table lacks the trigger or the
    # check can't run.
    def notify_enabled?(queue_name)
      names = @queue_strategy.physical_queue_names(queue_name)
      names.all? { |physical| notify_trigger_current?(physical, NOTIFY_THROTTLE_MS) }
    end

    # The physical PGMQ queue table names a logical queue maps to — one for a
    # standard queue, or the _p0.._pN sub-queues when priority is enabled. This
    # is the SAME resolution the bootstrap path uses (@queue_strategy), so a
    # caller diffing configured-vs-existing queues compares the exact names PGMQ
    # actually holds rather than the bare prefixed name.
    def physical_queue_names(logical_name)
      @queue_strategy.physical_queue_names(logical_name)
    end

    # Whether the PGMQ schema itself is present (the pgmq.meta table exists),
    # independent of pgbus's own version-tracking table. Lets a caller tell
    # "PGMQ installed via the extension / before version tracking" (schema
    # present, no tracking row) apart from "PGMQ not installed at all".
    def pgmq_installed?
      with_raw_connection do |conn|
        conn.exec(PGMQ_META_CHECK_SQL).ntuples.positive?
      end
    end

    # The most recently recorded installed PGMQ schema version string (e.g.
    # "1.5.0"), read from the pgbus_pgmq_schema_versions tracking table. Returns
    # nil when nothing is tracked yet or the table does not exist — the same
    # logic the `pgbus:pgmq:status` rake task uses, kept here so the doctor and
    # the rake task share one raw-SQL path (never SQL outside the Client).
    def pgmq_schema_version
      with_raw_connection do |conn|
        result = conn.exec(
          "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
        )
        row = result.first
        row && row["version"]
      end
    rescue ActiveRecord::StatementInvalid => e
      raise unless undefined_table_error?(e)

      nil
    rescue StandardError => e
      raise unless defined?(PG::UndefinedTable) && e.is_a?(PG::UndefinedTable)

      nil
    end

    def ensure_queue(name)
      ensure_pgmq_schema
      @queue_strategy.physical_queue_names(name).each { |pq| ensure_single_queue(pq) }
    end

    def ensure_all_queues
      queue_names = collect_configured_queues
      Pgbus.logger.info { "[Pgbus] Bootstrapping #{queue_names.size} queue(s): #{queue_names.join(", ")}" }
      queue_names.each { |name| ensure_queue(name) }
    end

    def ensure_dead_letter_queue(name)
      dlq_name = config.dead_letter_queue_name(name)
      return if @queues_created[dlq_name]

      if queue_ddl_rides_caller_transaction?
        create_dead_letter_queue_physically(dlq_name)
      else
        @queues_created.compute_if_absent(dlq_name) do
          create_dead_letter_queue_physically(dlq_name)
          true
        end
      end
    end

    def send_message(queue_name, payload, headers: nil, delay: 0, priority: nil)
      target = @queue_strategy.target_queue(queue_name, priority)
      Instrumentation.instrument("pgbus.client.send_message", queue: target) do
        with_stale_connection_retry do
          ensure_queue(queue_name)
          synchronized { @pgmq.produce(target, serialize(payload), headers: headers && serialize(headers), delay: delay) }
        end
      end
    end

    # Durable stream broadcast. Unlike #send_message, this ALWAYS targets the
    # bare queue (config.queue_name) and never the priority strategy's
    # _p0.._pN sub-queues: streams are delivered by a non-consuming peek
    # (read_after) on the bare queue, and the streamer LISTENs on the bare
    # channel, so a broadcast routed to _p1 would never reach the browser
    # (issue #310). ensure_stream_queue creates the bare queue + NOTIFY
    # trigger + archive index, mirroring this bare-name write path.
    def send_stream_message(stream_name, payload, headers: nil, delay: 0)
      target = config.queue_name(stream_name)
      # Capture the produced msg_id — it is this method's return value (callers
      # like Stream#broadcast rely on it), so the autoscale trigger below must NOT
      # become the last expression.
      msg_id = Instrumentation.instrument("pgbus.client.send_message", queue: target) do
        with_stale_connection_retry do
          ensure_stream_queue(stream_name)
          # Publish through the dedicated streams pool (issue #315) so a
          # saturated job pool can't block a broadcast on pool checkout. On the
          # shared-AR path @streams_pgmq aliases @pgmq and synchronized still
          # serializes on the mutex.
          synchronized do
            streams_pool.produce(target, serialize(payload), headers: headers && serialize(headers), delay: delay)
          end
        end
      end
      # Opportunistically autoscale the streams pool from the publish path so a
      # pure-publisher process (no streamer) still grows under a broadcast storm
      # (issue #323 follow-up). Throttled + fail-soft — never delays or breaks the
      # broadcast; nil (a no-op) unless autoscale is on and the pool is dedicated.
      streams_pool_trigger&.maybe_check
      msg_id
    end

    def send_batch(queue_name, payloads, headers: nil, delay: 0)
      full_name = config.queue_name(queue_name)
      serialized, serialized_headers = serialize_batch(payloads, headers)
      Instrumentation.instrument("pgbus.client.send_batch", queue: full_name, size: payloads.size) do
        with_stale_connection_retry do
          ensure_queue(queue_name)
          synchronized { @pgmq.produce_batch(full_name, serialized, headers: serialized_headers, delay: delay) }
        end
      end
    end

    def read_message(queue_name, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_message", queue: full_name) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read(full_name, vt: vt || config.visibility_timeout) } }
          end
        end
      end
    end

    def read_batch(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_batch", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_batch(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    # Read from priority sub-queues, highest priority (p0) first.
    # Returns [priority_queue_name, messages] pairs.
    def read_batch_prioritized(queue_name, qty:, vt: nil)
      # Non-priority fast path delegates to read_batch, which is already gated
      # by the connection-health breaker — no extra guard needed here.
      unless @queue_strategy.priority?
        return (read_batch(queue_name, qty: qty, vt: vt) || []).map do |m|
          [config.queue_name(queue_name), m]
        end
      end

      # The priority loop issues its own reads, so gate the whole loop: an open
      # breaker fails fast before any sub-queue is touched, and the loop as a
      # unit records one success/failure with the latch.
      guarded_read do
        remaining = qty
        results = []

        config.priority_queue_names(queue_name).each do |pq_name|
          break if remaining <= 0

          msgs = Instrumentation.instrument("pgbus.client.read_batch", queue: pq_name, qty: remaining) do
            with_stale_connection_retry do
              synchronized { with_read_timeout { @pgmq.read_batch(pq_name, vt: vt || config.visibility_timeout, qty: remaining) } }
            end
          end || []

          msgs.each { |m| results << [pq_name, m] }
          remaining -= msgs.size
        end

        results
      end
    end

    def read_with_poll(queue_name, qty:, vt: nil, max_poll_seconds: 5, poll_interval_ms: 100)
      full_name = config.queue_name(queue_name)
      guarded_read do
        with_stale_connection_retry do
          synchronized do
            @pgmq.read_with_poll(
              full_name,
              vt: vt || config.visibility_timeout,
              qty: qty,
              max_poll_seconds: max_poll_seconds,
              poll_interval_ms: poll_interval_ms
            )
          end
        end
      end
    end

    # Read from multiple queues in a single SQL query (UNION ALL).
    # Each returned message includes a queue_name field identifying its source.
    # queue_names should be logical names (prefix is added automatically).
    #
    # `qty` is the per-queue cap (pgmq-ruby semantics), so without `limit:` the
    # caller receives up to `queue_count * qty` messages. Pass `limit:` to cap
    # the total across all queues — required when feeding a fixed-size pool,
    # otherwise the pool can overflow on multi-queue reads (issue #123).
    #
    # STRICT-PRIORITY CONTRACT (issue #381): when `limit:` is smaller than the
    # total available, earlier-listed queues win — the capsule DSL's "list
    # order = strict priority" promise rides on this. The mechanism is
    # incidental: pgmq-ruby builds `pgmq.read(q1) UNION ALL pgmq.read(q2) …
    # LIMIT n`, and Postgres's Append node fills the LIMIT from the subqueries
    # in written order. Nothing upstream promises that, so the contract is
    # pinned by spec/integration/multi_queue_priority_spec.rb — if that canary
    # ever breaks, switch callers to ordered per-queue reads (the
    # Worker#fetch_prioritized pattern) instead of relying on this method.
    #
    # vt-claim caveat: each subquery may claim (set vt on) up to `qty` rows
    # even when the outer LIMIT discards them — a discarded row goes invisible
    # for one visibility timeout without being processed. Size `qty`/`limit`
    # accordingly on latency-sensitive queues.
    def read_multi(queue_names, qty:, vt: nil, limit: nil)
      full_names = queue_names.map { |q| config.queue_name(q) }
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_multi", queues: full_names, qty: qty, limit: limit) do
          with_stale_connection_retry do
            synchronized do
              with_read_timeout do
                @pgmq.read_multi(full_names, vt: vt || config.visibility_timeout, qty: qty, limit: limit)
              end
            end
          end
        end
      end
    end

    # Delete a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name (e.g. from priority sub-queues or dashboard).
    def delete_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.delete(name, msg_id) }
      end
    end

    # Archive a message. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def archive_message(queue_name, msg_id, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.archive(name, msg_id) }
      end
    end

    # Batch archive — moves multiple messages to the archive table in one call.
    def archive_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.archive_batch(name, msg_ids) }
      end
    end

    # Batch delete — permanently removes multiple messages in one call.
    def delete_batch(queue_name, msg_ids, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.delete_batch(name, msg_ids) }
      end
    end

    # Set visibility timeout. Pass prefixed: false when queue_name is already
    # the full PGMQ queue name.
    def set_visibility_timeout(queue_name, msg_id, vt:, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.set_vt(name, msg_id, vt: vt) }
      end
    end

    # Open a PGMQ transaction. The caller block may run twice if the first
    # attempt hits a pre-flight stale-connection error — safe because no SQL
    # was sent on the first attempt (the connection was dead before the BEGIN).
    def transaction(&block)
      with_stale_connection_retry do
        synchronized { @pgmq.transaction(&block) }
      end
    end

    def move_to_dead_letter(queue_name, message)
      dlq_name = config.dead_letter_queue_name(queue_name)
      full_queue = config.queue_name(queue_name)

      with_stale_connection_retry do
        ensure_dead_letter_queue(queue_name)
        synchronized do
          @pgmq.transaction do |txn|
            txn.produce(dlq_name, message.message, headers: message.headers)
            txn.delete(full_queue, message.msg_id.to_i)
          end
        end
      end
    end

    def metrics(queue_name = nil)
      with_stale_connection_retry do
        synchronized do
          if queue_name
            @pgmq.metrics(config.queue_name(queue_name))
          else
            @pgmq.metrics_all
          end
        end
      end
    end

    # Age (seconds) of the oldest message actually eligible for pickup, i.e.
    # whose visibility timeout has elapsed. Unlike pgmq's oldest_msg_age_sec
    # (computed from enqueued_at), a scheduled or backoff-parked message —
    # future vt — contributes nothing until it comes due, so a queue holding
    # only parked messages reads nil ("no claimable backlog") instead of an
    # age growing at wall-clock rate (issue #389). pgmq's metrics_result type
    # is frozen upstream, so this lives here rather than in the SQL function.
    #
    # With a queue name: the age for that (prefixed) queue, or nil.
    # Without: a hash of every physical queue in pgmq.meta to its age.
    #
    # Routes through the pooled @pgmq.with_connection (health-checked, bounded
    # by the statement/socket timeouts applied at Client#initialize) rather
    # than a fresh unbounded PG.connect per call — same rationale as
    # notify_trigger_current?. synchronized: on the shared-Proc path @pgmq
    # rides the AR raw connection, so the query must serialize against
    # concurrent PGMQ operations. One checkout spans all per-queue queries;
    # nothing nests inside it, so the shared pool_size=1 path is safe.
    def oldest_claimable_ages(queue_name = nil)
      synchronized do
        @pgmq.with_connection do |conn|
          if queue_name
            claimable_age_for(conn, config.queue_name(queue_name))
          else
            names = conn.exec("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
                        .map { |row| row["queue_name"] }
            names.to_h { |name| [name, claimable_age_for(conn, name)] }
          end
        end
      end
    end

    # Snapshot of the PGMQ connection pool: {size:, available:, pool_timeout:}.
    #
    # Reads pgmq-ruby's own pool counters (@pgmq.stats -> {size:, available:})
    # and adds the configured pool_timeout so alerting has the full picture:
    # how many connections exist, how many are free right now, and how long a
    # checkout waits before raising a pool-timeout error. Works on both the
    # dedicated-pool path and the shared-Proc path (where size is 1).
    #
    # Purely observational — wrapped in a rescue that returns {} so a probe or
    # heartbeat reading the pool can never break job processing. Not routed
    # through with_stale_connection_retry: reading in-memory counters touches no
    # socket, and a failing read must degrade to {} rather than retry.
    def pool_stats
      @pgmq.stats.merge(pool_timeout: config.pool_timeout)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus::Client] pool_stats unavailable: #{e.class}: #{e.message}" }
      {}
    end

    # Same shape as #pool_stats but for the dedicated streams pool (issue #315).
    # On the shared-AR path @streams_pgmq aliases @pgmq, so this reports the job
    # pool's counters — accurate, since streams share that connection there.
    def streams_pool_stats
      streams_pool.stats.merge(pool_timeout: config.streams_pool_timeout)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus::Client] streams_pool_stats unavailable: #{e.class}: #{e.message}" }
      {}
    end

    def list_queues
      with_stale_connection_retry do
        synchronized { @pgmq.list_queues }
      end
    end

    def purge_queue(queue_name, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      with_stale_connection_retry do
        synchronized { @pgmq.purge_queue(name) }
      end
    end

    def drop_queue(queue_name, prefixed: true)
      name = prefixed ? config.queue_name(queue_name) : queue_name
      result = with_stale_connection_retry do
        synchronized { @pgmq.drop_queue(name) }
      end
      @queues_created.delete(name)
      result
    end

    # Check whether a message exists in the given queue.
    #
    # Pass either +msg_id+ for a fast primary-key lookup, or +uniqueness_key+
    # to scan the queue for any message whose payload carries that key in the
    # +pgbus_uniqueness_key+ JSONB field. The latter is used by the dispatcher
    # reaper to determine if a uniqueness lock with msg_id=0 (placeholder)
    # still has a corresponding queue message.
    #
    # +queue_name+ may be either a logical name (e.g. "default") or an already
    # prefixed physical name (e.g. "pgbus_default"). The client normalizes both.
    #
    # Returns:
    #   true  — the message definitely exists in the queue
    #   false — the message definitely does not exist
    #   nil   — could not determine (e.g. queue table missing or unknown error).
    #           Callers MUST treat nil as "exists" for safety.
    def message_exists?(queue_name, msg_id: nil, uniqueness_key: nil)
      has_msg_id = !msg_id.nil?
      has_uniqueness_key = !uniqueness_key.nil?
      raise ArgumentError, "pass msg_id, uniqueness_key, or both" unless has_msg_id || has_uniqueness_key

      tables = lookup_physical_queue_names(queue_name).map { |name| QueueNameValidator.sanitize!(name) }
      determined = false

      synchronized do
        with_raw_connection do |conn|
          tables.each do |sanitized|
            present = probe_queue_presence(
              conn, sanitized,
              msg_id: has_msg_id ? msg_id.to_i : nil,
              uniqueness_key: has_uniqueness_key ? uniqueness_key : nil
            )
            return true if present == true

            determined = true unless present == :missing
          end
        end
      end

      determined ? false : nil # rubocop:disable Style/ReturnNilInPredicateMethodDefinition -- tri-state: nil means unknown
    end

    # Which uniqueness keys currently appear in any live PGMQ queue payload.
    # Used by the dispatcher reaper for unbound locks (pending / msg_id=0)
    # so it never probes the synthetic `pending` queue (issue #418).
    #
    # Per-queue UndefinedTable is skipped (table dropped between listing and
    # select). Failure to list pgmq.meta — or any non-undefined error — raises;
    # callers must treat that as "unknown" and keep the lock.
    def uniqueness_keys_present(lock_keys)
      keys = Array(lock_keys).compact.map(&:to_s).uniq
      return Set.new if keys.empty?

      found = Set.new
      synchronized do
        with_raw_connection do |conn|
          names = conn.exec("SELECT queue_name FROM pgmq.meta ORDER BY queue_name")
                      .map { |row| row["queue_name"] }
          names.each do |name|
            break if found.size == keys.size

            sanitized = begin
              QueueNameValidator.sanitize!(name)
            rescue ArgumentError
              next
            end

            scan_uniqueness_keys(conn, sanitized, keys, found)
          end
        end
      end
      found
    end

    def purge_archive(queue_name, older_than:, batch_size: 1000)
      full_name = config.queue_name(queue_name)
      sanitized = QueueNameValidator.sanitize!(full_name)
      total = 0

      sql = "DELETE FROM pgmq.a_#{sanitized} " \
            "WHERE ctid = ANY(ARRAY(SELECT ctid FROM pgmq.a_#{sanitized} WHERE enqueued_at < $1 LIMIT $2))"

      loop do
        deleted = synchronized do
          with_raw_connection do |conn|
            conn.exec_params(sql, [older_than, batch_size]).cmd_tuples
          end
        end
        total += deleted
        break if deleted < batch_size
      end

      total
    end

    # --- Grouped reads (PGMQ v1.11.0+) ---

    def read_grouped(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_grouped", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_grouped(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    def read_grouped_rr(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        Instrumentation.instrument("pgbus.client.read_grouped_rr", queue: full_name, qty: qty) do
          with_stale_connection_retry do
            synchronized { with_read_timeout { @pgmq.read_grouped_rr(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
          end
        end
      end
    end

    def read_grouped_head(queue_name, qty:, vt: nil)
      full_name = config.queue_name(queue_name)
      guarded_read do
        with_stale_connection_retry do
          synchronized { with_read_timeout { @pgmq.read_grouped_head(full_name, vt: vt || config.visibility_timeout, qty: qty) } }
        end
      end
    end

    # --- FIFO index management (PGMQ v1.11.0+) ---

    def create_fifo_index(queue_name)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.create_fifo_index(full_name) }
      end
    end

    def create_fifo_indexes_all
      with_stale_connection_retry do
        synchronized { @pgmq.create_fifo_indexes_all }
      end
    end

    # --- LISTEN/NOTIFY management (PGMQ v1.11.0+) ---

    def wait_for_notify(queue_name, timeout: nil, &block)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.wait_for_notify(full_name, timeout: timeout, &block) }
      end
    end

    def update_notify_insert(queue_name, throttle_interval_ms:)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized { @pgmq.update_notify_insert(full_name, throttle_interval_ms: throttle_interval_ms) }
      end
    end

    def list_notify_insert_throttles
      with_stale_connection_retry do
        synchronized { @pgmq.list_notify_insert_throttles }
      end
    end

    # --- Archive partitioning (requires pg_partman extension) ---

    def convert_archive_partitioned(queue_name, partition_interval: "10000", retention_interval: "100000",
                                    leading_partition: 10)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        synchronized do
          @pgmq.convert_archive_partitioned(
            full_name,
            partition_interval: partition_interval,
            retention_interval: retention_interval,
            leading_partition: leading_partition
          )
        end
      end
    end

    # Topic routing
    def bind_topic(pattern, queue_name)
      full_name = config.queue_name(queue_name)
      with_stale_connection_retry do
        ensure_queue(queue_name)
        synchronized { @pgmq.bind_topic(pattern, full_name) }
      end
    end

    def publish_to_topic(routing_key, payload, headers: nil, delay: 0)
      with_stale_connection_retry do
        synchronized do
          @pgmq.produce_topic(
            routing_key,
            serialize(payload),
            headers: headers && serialize(headers),
            delay: delay
          )
        end
      end
    end

    def close
      # Stop the publisher autoscale executor (if one was ever built) so its
      # background thread doesn't leak (issue #323). Outside `synchronized` — it
      # takes no pool lock and shutdown waits on a possibly-running check.
      @streams_pool_trigger.shutdown if defined?(@streams_pool_trigger) && @streams_pool_trigger
      synchronized do
        @pgmq.close
        # Close the CURRENT streams pool too (issue #315) so its connections
        # don't leak. close_current reads the live (possibly hot-swapped, #323)
        # pool once under the swap mutex and skips it when it aliases @pgmq (the
        # shared-AR path) so we don't double-close the same pool.
        @streams_pool.close_current(job_pool: @pgmq)
      end
    end

    # Opt-in hot-swap of the dedicated streams pool to a new size (issue #323
    # spike). Builds a fresh PGMQ::Client at new_size with the SAME bounds-applied
    # connection options, atomically swaps the live reference, then drains +
    # closes the old pool (bounded, never Thread#kill). NOT called automatically —
    # there is no control loop here; a caller triggers it explicitly.
    #
    # No-op on the shared-AR (Proc) path (the streams pool aliases the job pool,
    # which is non-thread-safe and forced to pool_size 1 — swapping it would
    # corrupt the job pool), and no-op when the size is unchanged.
    #
    # @return [ResizablePool::SwapStats] on a swap, or {swapped: false, reason:}
    def resize_streams_pool(new_size)
      raise ArgumentError, "new_size must be a positive integer" unless new_size.is_a?(Integer) && new_size.positive?
      return { swapped: false, reason: :shared_connection } if @shared_connection
      return { swapped: false, reason: :unchanged } if streams_pool.stats[:size] == new_size

      from_size = streams_pool.stats[:size]
      new_pgmq = PGMQ::Client.new(
        @streams_conn_opts, pool_size: new_size, pool_timeout: config.streams_pool_timeout
      )
      @streams_pool.swap(new_pgmq, from_size: from_size, to_size: new_size)
    end

    # Accumulated streams-pool swap telemetry (issue #323) — for the bench and a
    # future control loop. Zero-valued before any swap.
    def streams_swap_stats
      @streams_pool.stats_snapshot
    end

    # Operator escape hatch (issue #354): drop every pooled PGMQ connection —
    # job pool AND the live streams pool — and let the pools rebuild lazily on
    # next checkout (pgmq-ruby >= 0.7.1). Use to recover connections libpq
    # still reports as CONNECTION_OK but that are in fact wedged (e.g. after a
    # wall-clock interrupt cut a query mid-flight), which pgmq-ruby's checkout
    # health check cannot detect. Unlike #close, the pools stay usable.
    # Connections checked out by other threads mid-reload are unaffected.
    #
    # No-op (returns false) on the shared-AR Proc path: those pool slots wrap
    # ActiveRecord's own raw connection — reloading would close AR's socket
    # out from under the application. Returns true after a reload.
    def reload # rubocop:disable Naming/PredicateMethod -- command that reports whether it acted, like #ping
      if @shared_connection
        Pgbus.logger.warn do
          "[Pgbus::Client] reload skipped: pgbus is sharing ActiveRecord's connection " \
            "(Proc connection_options) and won't close a socket it doesn't own. " \
            "Manage that connection through ActiveRecord instead."
        end
        return false
      end

      @pgmq.reload
      @streams_pool.reload
      true
    end

    private

    # Human-readable label for which config knob supplied the connection
    # options, mirroring Configuration#connection_options' precedence. Used in
    # verify_connection!'s error so the operator knows which setting to fix.
    def connection_source
      if config.database_url
        "database_url"
      elsif config.connection_params
        "connection_params"
      else
        "ActiveRecord-derived connection"
      end
    end

    # Accept either a logical name ("default") or an already-prefixed
    # physical name ("pgbus_default") and return the physical name.
    # Coerces symbols to strings so callers can pass either form.
    def resolve_full_queue_name(queue_name)
      name = queue_name.to_s
      prefix = "#{config.queue_prefix}_"
      name.start_with?(prefix) ? name : config.queue_name(name)
    end

    # Logical names expand through QueueFactory so a priority queue's _pN
    # tables are included; already-prefixed physical names stay as-is.
    def lookup_physical_queue_names(queue_name)
      name = queue_name.to_s
      prefix = "#{config.queue_prefix}_"
      name.start_with?(prefix) ? [name] : @queue_strategy.physical_queue_names(name)
    end

    def probe_queue_presence(conn, sanitized, msg_id:, uniqueness_key:)
      if msg_id
        msg_id_present?(conn, sanitized, msg_id, uniqueness_key: uniqueness_key)
      else
        uniqueness_key_present?(conn, sanitized, uniqueness_key)
      end
    rescue ActiveRecord::StatementInvalid => e
      raise unless undefined_table_error?(e)

      :missing
    rescue StandardError => e
      raise unless defined?(PG::UndefinedTable) && e.is_a?(PG::UndefinedTable)

      :missing
    end

    def scan_uniqueness_keys(conn, sanitized, keys, found)
      placeholders = keys.each_index.map { |i| "$#{i + 1}" }.join(", ")
      result = conn.exec_params(
        "SELECT DISTINCT message::jsonb ->> 'pgbus_uniqueness_key' AS k " \
        "FROM pgmq.q_#{sanitized} " \
        "WHERE message::jsonb ->> 'pgbus_uniqueness_key' IN (#{placeholders})",
        keys
      )
      result.each { |row| found.add(row["k"]) if row["k"] }
    rescue ActiveRecord::StatementInvalid => e
      raise unless undefined_table_error?(e)
    rescue StandardError => e
      raise unless defined?(PG::UndefinedTable) && e.is_a?(PG::UndefinedTable)
    end

    def msg_id_present?(conn, sanitized, msg_id, uniqueness_key: nil)
      result = if uniqueness_key
                 conn.exec_params(
                   "SELECT 1 FROM pgmq.q_#{sanitized} WHERE msg_id = $1 " \
                   "AND message::jsonb ->> 'pgbus_uniqueness_key' = $2 LIMIT 1",
                   [msg_id, uniqueness_key]
                 )
               else
                 conn.exec_params(
                   "SELECT 1 FROM pgmq.q_#{sanitized} WHERE msg_id = $1 LIMIT 1",
                   [msg_id]
                 )
               end
      result.ntuples.positive?
    end

    def uniqueness_key_present?(conn, sanitized, uniqueness_key)
      result = conn.exec_params(
        "SELECT 1 FROM pgmq.q_#{sanitized} " \
        "WHERE message::jsonb ->> 'pgbus_uniqueness_key' = $1 LIMIT 1",
        [uniqueness_key]
      )
      result.ntuples.positive?
    end

    # Detect "relation does not exist" via the underlying PG error type.
    # Falls back to message matching only if PG::UndefinedTable is undefined
    # (very old pg gem) — never relies on locale-sensitive text.
    def undefined_table_error?(error)
      cause = error.respond_to?(:cause) ? error.cause : nil
      return true if defined?(PG::UndefinedTable) && cause.is_a?(PG::UndefinedTable)

      false
    end

    def collect_configured_queues
      queues = Set.new
      queues << config.default_queue

      # Queues from worker configs
      (config.workers || []).each do |w|
        worker_queues = w[:queues] || [config.default_queue]
        worker_queues.each { |q| queues << q unless q == "*" }
      end

      # Queues from recurring tasks
      (config.recurring_tasks || {}).each_value do |opts|
        opts = opts.transform_keys(&:to_s) if opts.is_a?(Hash)
        queue = opts["queue"] || opts[:queue]
        queues << queue if queue
      end

      queues.to_a
    end

    def ensure_pgmq_schema
      return if @schema_ensured

      self.class.pgmq_install_mutex.synchronize do
        return if @schema_ensured

        # Cache only a durable result: true only when this call owned the
        # COMMIT. A savepoint-path ensure rides the CALLER's transaction — if
        # that later rolls back the schema is gone (and even a schema found
        # already-present there may be the caller's own uncommitted work), so
        # a cached true would skip every future check (#399 review).
        #
        # synchronized (the per-instance connection mutex) nests INSIDE the
        # class-level install mutex — that lock order is safe because no path
        # acquires them the other way round — so the shared Proc connection is
        # never touched while another thread of this instance is mid-operation
        # on it (single-owner invariant; #399 review).
        durable = synchronized do
          with_raw_connection do |raw_conn|
            if inside_caller_transaction?(raw_conn)
              install_pgmq_schema_in_savepoint(raw_conn)
              false
            else
              install_pgmq_schema_in_own_transaction(raw_conn)
              true
            end
          end
        end
        @schema_ensured = true if durable
      end
    rescue StandardError => e
      raise Pgbus::SchemaNotReady,
            "PGMQ schema installation failed (#{e.class}: #{e.message}). " \
            "Ensure the pgbus database exists and migrations have been run."
    end

    # Check-and-install under a fixed advisory lock: pg_advisory_xact_lock
    # serializes installers across processes and releases itself when its
    # transaction ends — safe through transaction-pooling poolers, where a
    # session-level lock could be released on a different server connection
    # than the one that acquired it (issue #397).
    #
    # The transactional framing must respect who owns the transaction. A
    # Proc-supplied shared connection (the Rails-lambda path) can arrive
    # mid-transaction — e.g. perform_later inside an application
    # `transaction do` block. BEGIN there is a warning-level no-op, and the
    # matching COMMIT/ROLLBACK would then commit or destroy the CALLER's
    # transaction (#398 review). So: own the transaction only when the
    # connection is idle; ride the caller's transaction via a savepoint
    # otherwise.
    #
    # respond_to? guard: a Proc can hand back any connection-shaped object;
    # only a real PG::Connection reports transaction_status (and its presence
    # guarantees the PG constants below are loaded).
    def inside_caller_transaction?(conn)
      conn.respond_to?(:transaction_status) && conn.transaction_status != PG::PQTRANS_IDLE
    end

    def install_pgmq_schema_in_own_transaction(conn)
      conn.exec("BEGIN")
      conn.exec("SELECT pg_advisory_xact_lock(#{PGMQ_INSTALL_LOCK_KEY})")
      install_pgmq_schema(conn) if conn.exec(PGMQ_META_CHECK_SQL).ntuples.zero?
      conn.exec("COMMIT")
    rescue StandardError => e
      recover_from_install_failure(conn, e, "ROLLBACK")
    end

    # The advisory lock joins the CALLER's transaction here, so it is held
    # until that transaction ends — longer than the install needs, but xact
    # locks cannot be released early by design, and over-holding only delays
    # a concurrent installer, never corrupts it.
    def install_pgmq_schema_in_savepoint(conn)
      conn.exec("SAVEPOINT #{PGMQ_INSTALL_SAVEPOINT}")
      conn.exec("SELECT pg_advisory_xact_lock(#{PGMQ_INSTALL_LOCK_KEY})")
      install_pgmq_schema(conn) if conn.exec(PGMQ_META_CHECK_SQL).ntuples.zero?
      conn.exec("RELEASE SAVEPOINT #{PGMQ_INSTALL_SAVEPOINT}")
    rescue StandardError => e
      recover_from_install_failure(conn, e, "ROLLBACK TO SAVEPOINT #{PGMQ_INSTALL_SAVEPOINT}")
    end

    def recover_from_install_failure(conn, error, rollback_sql)
      begin
        conn.exec(rollback_sql)
      rescue StandardError
        # A connection broken enough to refuse the rollback also fails the
        # re-check below, which surfaces the state honestly; re-raising the
        # rollback error here would mask the original install failure.
      end
      raise error unless duplicate_install_error?(error)

      # A process without the advisory lock (older pgbus, or the extension
      # path) won the install race — re-check instead of failing on its
      # success.
      raise error if conn.exec(PGMQ_META_CHECK_SQL).ntuples.zero?
    end

    def duplicate_install_error?(error)
      DUPLICATE_INSTALL_ERROR_CLASSES.include?(error.class.name)
    end

    def install_pgmq_schema(conn)
      mode = config.pgmq_schema_mode

      case mode
      when :extension
        Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing via extension" }
        conn.exec("CREATE EXTENSION IF NOT EXISTS pgmq")
      when :embedded
        Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing embedded SQL" }
        conn.exec(PgmqSchema.install_sql)
      else # :auto
        ext = conn.exec("SELECT 1 FROM pg_available_extensions WHERE name = 'pgmq' LIMIT 1")
        if ext.ntuples.positive?
          Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing via extension" }
          conn.exec("CREATE EXTENSION IF NOT EXISTS pgmq")
        else
          Pgbus.logger.info { "[Pgbus] PGMQ schema not found — installing embedded SQL" }
          conn.exec(PgmqSchema.install_sql)
        end
      end
    end

    # queue_name is a physical (already prefixed) queue name; sanitized to a
    # bare identifier before interpolation, same as the dashboard's DataSource.
    def claimable_age_for(conn, queue_name)
      qtable = "q_#{QueueNameValidator.sanitize!(queue_name)}"
      row = conn.exec(<<~SQL).first
        SELECT EXTRACT(epoch FROM (NOW() - min(vt)))::int AS age_sec
        FROM pgmq.#{qtable}
        WHERE vt <= NOW()
      SQL
      row && row["age_sec"]&.to_i
    end

    def with_raw_connection
      opts = config.connection_options
      owned = false
      conn = case opts
             when Proc
               opts.call
             when String
               owned = true
               PG.connect(opts)
             when Hash
               owned = true
               # :variables is a database.yml convention, not a libpq keyword —
               # strip it before PG.connect and apply the GUCs via SET so this
               # raw bootstrap/DDL connection matches the pooled connections
               # (issue #332). Empty/absent variables is a plain connect.
               variables = opts[:variables]
               conn = PG.connect(**opts.except(:variables))
               variables&.each { |name, value| conn.exec("SET #{name} = '#{value}'") }
               conn
             else
               raise ConfigurationError, "Cannot resolve raw PG connection from #{opts.class}"
             end
      yield conn
    ensure
      conn&.close if owned
    end

    # Yields a PG connection from the dedicated streams pool for the streamer's
    # replay reads (read_after / stream_current_msg_id / stream_oldest_msg_id).
    # On the dedicated (String/Hash) path this checks out a persistent pooled
    # connection — no fresh PG.connect per call (issue #315). On the shared-AR
    # (Proc) path there is no separate pool (@streams_pgmq aliases @pgmq and
    # points at the non-thread-safe AR raw_connection), so we fall back to
    # with_raw_connection, which reuses that same shared connection. Callers
    # already wrap this in `synchronized`, so the shared path stays serialized.
    def with_streams_connection(&)
      if @shared_connection
        with_raw_connection(&)
      else
        streams_pool.with_connection(&)
      end
    end

    def ensure_single_queue(full_name)
      return if @queues_created[full_name]

      if queue_ddl_rides_caller_transaction?
        create_queue_physically(full_name)
      else
        @queues_created.compute_if_absent(full_name) do
          create_queue_physically(full_name)
          true
        end
      end
    end

    def create_queue_physically(full_name)
      synchronized do
        create_queue_table(full_name)
        enable_notify_if_needed(full_name, NOTIFY_THROTTLE_MS)
        create_fifo_index_if_needed(full_name)
      end
    end

    def create_dead_letter_queue_physically(dlq_name)
      synchronized { create_queue_table(dlq_name) }
    end

    # Runs inside synchronized — callers own the connection mutex.
    #
    # CREATE TABLE IF NOT EXISTS is not race-safe: two backends creating a
    # not-yet-existing queue both pass the existence check (READ COMMITTED
    # — neither sees the other's uncommitted catalog rows), both insert
    # into pg_class, and the loser raises unique_violation on
    # pg_class_relname_nsp_index instead of the friendly duplicate_table
    # (issue #404 — the sibling of the #403 trigger race, one DDL step
    # earlier; `synchronized` is process-local, so nothing serializes
    # this across processes). pgmq.create is a single statement and
    # therefore atomic: by the time the loser unblocks, the winner has
    # committed the WHOLE queue — tables, indexes, and the pgmq.meta row
    # — so re-check meta and return (the winner also ran autovacuum
    # tuning). The retry covers the can't-confirm case (e.g. a leftover
    # physical table without a meta row); its failure propagates as the
    # retry's own error, with the original duplicate preserved deeper in
    # the cause chain (raised while $! held it, so Ruby chains it).
    def create_queue_table(name)
      @pgmq.create(name)
      tune_autovacuum(name)
    rescue StandardError => e
      raise unless duplicate_relation_error?(e)
      return if queue_registered?(name)

      @pgmq.create(name)
      tune_autovacuum(name)
    end

    # Queue DDL on the shared Proc-supplied connection joins any transaction
    # the caller has open, so a @queues_created cache write there outlives a
    # caller rollback — later ensures would skip recreation and message
    # operations would fail (#399 review; same durability rule as
    # @schema_ensured). Create the queue (idempotent CREATE IF NOT EXISTS)
    # but let the next ensure re-check. Dedicated String/Hash paths run DDL
    # on pgmq-ruby's own pool connections, never inside an application
    # transaction, so they always cache.
    #
    # The probe itself must hold the connection mutex: even the local
    # transaction_status read honors the single-owner invariant on the
    # shared PG::Connection (#399 review). Sequential with — never nested
    # inside — the create's own synchronized block.
    def queue_ddl_rides_caller_transaction?
      return false unless @shared_connection

      synchronized do
        with_raw_connection { |conn| inside_caller_transaction?(conn) }
      end
    end

    # notify_trigger_current? is a plain SELECT and `synchronized` is a
    # process-local mutex, so this check-then-act races across processes:
    # after a deploy every process's @queues_created memo is cold, and two
    # web processes handling the first broadcast for the same lazy stream
    # queue both see "trigger not current" and both run PGMQ's
    # DROP + CREATE CONSTRAINT TRIGGER cycle. The loser's CREATE blocks on
    # the winner's table lock and fails with PG::DuplicateObject once the
    # winner commits (issue #403). The duplicate proves the trigger exists,
    # so treat it as success — same shape as the #397 duplicate-object
    # rescue on schema install. Re-check the throttle first: racing ensures
    # pass the same value, but a job-queue ensure (250ms) can race the
    # stream override (0ms), so a mismatch means the winner installed a
    # different interval — retry once to converge; a second loss propagates.
    def enable_notify_if_needed(full_name, throttle_ms)
      return unless config.listen_notify
      return if notify_trigger_current?(full_name, throttle_ms)

      @pgmq.enable_notify_insert(full_name, throttle_interval_ms: throttle_ms)
    rescue PGMQ::Errors::ConnectionError => e
      raise unless duplicate_notify_trigger_error?(e)
      return if notify_trigger_current?(full_name, throttle_ms)

      @pgmq.enable_notify_insert(full_name, throttle_interval_ms: throttle_ms)
    end

    # Matched on the trigger name (an identifier — survives server-side
    # message localization) plus either the PG::DuplicateObject cause set by
    # pgmq-ruby's `raise … ConnectionError` inside `rescue PG::Error` (the
    # defined? guard mirrors the other PG::… checks in this file: a cause
    # can only be a PG::DuplicateObject when the class is loaded) or the
    # English "already exists" text when a wrapper dropped the cause.
    def duplicate_notify_trigger_error?(error)
      message = error.message.to_s
      return false unless message.include?(NOTIFY_TRIGGER_NAME)

      (defined?(PG::DuplicateObject) && error.cause.is_a?(PG::DuplicateObject)) ||
        message.include?("already exists")
    end

    def create_fifo_index_if_needed(full_name)
      return unless config.group_mode

      @pgmq.create_fifo_index(full_name)
    rescue StandardError => e
      # CREATE INDEX IF NOT EXISTS has the same catalog race as
      # pgmq.create (issue #404): the loser's duplicate proves a
      # concurrent ensure created the index.
      raise unless duplicate_relation_error?(e)
    end

    # A relation-creation race loser's error: one of the duplicate DDL
    # classes directly (raw conn.exec paths), or wrapped — pgmq-ruby
    # raises ConnectionError inside `rescue PG::Error`, so Ruby sets the
    # duplicate as its cause automatically.
    def duplicate_relation_error?(error)
      duplicate_install_error?(error) || duplicate_install_error?(error.cause)
    end

    # Whether pgmq.meta records the queue — the authoritative "create
    # committed" signal (pgmq.create writes it atomically with the
    # tables). The pooled checkout is a sequential sibling of the failed
    # create's (already returned when the exception unwound), so there is
    # no nested checkout — same reasoning as notify_trigger_current?.
    def queue_registered?(full_name)
      @pgmq.with_connection do |conn|
        conn.exec_params("SELECT 1 FROM pgmq.meta WHERE queue_name = $1 LIMIT 1", [full_name]).ntuples.positive?
      end
    rescue StandardError
      # Can't confirm (aborted caller transaction, schema not ready) —
      # fall through to the retry, which surfaces the state honestly.
      false
    end

    # Check whether the NOTIFY trigger already exists on this queue with the
    # expected throttle interval. When it does, we can skip the destructive
    # DROP TRIGGER + CREATE TRIGGER cycle that causes deadlocks when multiple
    # forked processes race during bootstrap.
    #
    # Routes through the pooled @pgmq.with_connection (health-checked, reused)
    # rather than opening a fresh PG.connect per queue: on the String/Hash path
    # with_raw_connection did a full TCP/TLS/auth setup for every queue at every
    # supervisor boot — and again in each forked child — churning short-lived
    # connections through the pooler. The checkout here is a sequential sibling
    # of the @pgmq.create call above it (create's own checkout has already been
    # returned), so there is no nested checkout: safe even on the shared-Proc
    # pool_size=1 path.
    def notify_trigger_current?(full_name, throttle_ms)
      @pgmq.with_connection do |conn|
        result = conn.exec_params(<<~SQL, [full_name, throttle_ms])
          SELECT 1
          FROM pg_trigger t
          JOIN pg_class c ON t.tgrelid = c.oid
          JOIN pg_namespace n ON c.relnamespace = n.oid
          WHERE n.nspname = 'pgmq'
            AND c.relname = pgmq.format_table_name($1, 'q')
            AND t.tgname = '#{NOTIFY_TRIGGER_NAME}'
            AND EXISTS (
              SELECT 1 FROM pgmq.notify_insert_throttle
              WHERE queue_name = $1
                AND throttle_interval_ms = $2
            )
          LIMIT 1
        SQL
        result.ntuples.positive?
      end
    rescue StandardError
      # If we can't check (e.g. pgmq schema not fully ready), fall back to
      # the unconditional path — same behavior as before this fix.
      false
    end

    # Apply PGMQ-tuned autovacuum + storage parameters to a queue's tables.
    #
    # Delegates to pgmq-ruby's tune_autovacuum (v0.7+), which sets the same
    # queue/archive parameters pgbus used to apply by hand — vacuum scale
    # factor 0.01/0.05, cost_delay 2/5, analyze scale factor 0.05, and
    # fillfactor 70 on the queue table — plus a vacuum_threshold floor of 50.
    # It quotes/lowercases the table name and runs both ALTER TABLEs in one
    # pooled checkout. Tuning is best-effort: a failure here never blocks a
    # queue from being usable, so we log and move on.
    #
    # Pgbus::AutovacuumTuning is still the source for the migration generators
    # (sql_for_all_queues, sql_for_high_churn_tables) which tune pgbus-owned
    # metadata tables the gem doesn't know about.
    def tune_autovacuum(queue_name)
      @pgmq.tune_autovacuum(queue_name)
    rescue StandardError => e
      Pgbus.logger.debug { "[Pgbus::Client] Autovacuum tuning failed for #{queue_name}: #{e.message}" }
    end

    # Serialize PGMQ operations through a mutex when sharing a connection
    # with ActiveRecord (Proc path). When pgmq-ruby owns its own connections
    # (String/Hash path), the internal connection_pool handles concurrency.
    def synchronized(&)
      if @pgmq_mutex
        @pgmq_mutex.synchronize(&)
      else
        yield
      end
    end

    # The ResizablePool wrapping the streams pool. All streams-pool reads
    # (produce / with_connection / stats) go through it so the underlying
    # PGMQ::Client can be atomically hot-swapped to a new size (issue #323).
    attr_reader :streams_pool

    # Lazily-built publisher-side autoscale trigger (issue #323). nil (a no-op)
    # unless streams_pool_autoscale is on AND this is the dedicated-connection
    # path (resize is a no-op on the shared-AR path). Built once — on the first
    # publish, then reused. Runs its headroom query through the job pool (@pgmq),
    # so it never competes with the streams pool it resizes.
    #
    # Double-checked under @streams_trigger_mutex so concurrent first-publishers
    # (the integration spec fires 8) can't each build their own trigger — that
    # would give each thread a trigger with its own throttle, defeating the
    # once-per-interval throttle on the first window. Steady-state publishes hit
    # the fast `defined?` path with no lock.
    def streams_pool_trigger
      return @streams_pool_trigger if defined?(@streams_pool_trigger)

      @streams_trigger_mutex.synchronize do
        return @streams_pool_trigger if defined?(@streams_pool_trigger)

        @streams_pool_trigger =
          if config.streams_pool_autoscale && !@shared_connection
            autoscaler = Streams::PoolAutoscaler.new(client: self, config: config)
            Streams::PoolTrigger.new(
              autoscaler: autoscaler, job_pool: @pgmq,
              interval: config.streams_pool_autoscale_interval,
              application_name_prefix: config.streams_application_name
            )
          end
      end
    end

    # Substrings that indicate the pooled PG::Connection was already dead
    # *before* pgmq-ruby tried to use it — typically killed by a connection
    # pooler (PgBouncer server_idle_timeout / client_idle_timeout), an admin
    # disconnect, or a TCP RST while the slot was idle.
    #
    # Only pre-checkout / pre-flight errors belong here. Mid-flight errors
    # like "server closed the connection" or "connection to server was lost"
    # are excluded because PG may have already committed the INSERT before
    # the socket died, and retrying would duplicate the message.
    #
    # See mensfeld/pgmq-ruby#94.
    STALE_CONNECTION_PATTERNS = [
      "pqsocket() can't get socket descriptor",
      "connection is closed",
      "connection has been closed",
      "connection not open",
      "no connection to the server",
      "ssl error: unexpected eof",
      "ssl syscall error"
    ].freeze
    private_constant :STALE_CONNECTION_PATTERNS

    # Compiled once: matches an error message containing any stale-connection
    # pattern as a substring, case-insensitively (replacing the previous
    # `message.downcase` + substring scan). Regexp.union escapes the literal
    # parens in "pqsocket() ...", so they match literally.
    STALE_CONNECTION_PATTERN =
      Regexp.new(Regexp.union(STALE_CONNECTION_PATTERNS).source, Regexp::IGNORECASE).freeze
    private_constant :STALE_CONNECTION_PATTERN

    # How many times a matched stale-connection error is retried before it
    # propagates. Two attempts (not one) so a transient window — a PgBouncer
    # restart or a brief failover — that outlasts the first immediate retry
    # still gets a second, backed-off chance rather than failing an enqueue
    # the caller may never retry.
    STALE_RETRY_ATTEMPTS = 2
    private_constant :STALE_RETRY_ATTEMPTS

    # Backoff before each retry, indexed by (attempt - 1): ~0.1s before the
    # first retry, ~0.5s before the second. Short enough to stay invisible on
    # a healthy path (error-path only — never slept on success) and to not
    # stall a worker loop, long enough to let a pooler/failover window clear.
    STALE_RETRY_DELAYS = [0.1, 0.5].freeze
    private_constant :STALE_RETRY_DELAYS

    # Rescue PGMQ::Errors::ConnectionError if its message matches a known
    # stale-socket pattern, retrying up to STALE_RETRY_ATTEMPTS times with a
    # short backoff (STALE_RETRY_DELAYS) between attempts. pgmq-ruby's
    # auto_reconnect + verify_connection! recovers a single dead pooled socket
    # on the *next* checkout, but a transient window — a PgBouncer restart or a
    # brief failover — can outlast an immediate retry; the backed-off second
    # attempt gives that window time to clear. Other connection errors (pool
    # timeout, misconfiguration, truly unreachable DB) propagate immediately.
    #
    # Wraps every @pgmq.* call site. Pattern matching is intentionally narrow
    # (pre-flight / idle-socket signals only), so retry is safe even for
    # non-idempotent ops like delete/archive — a matched error means the
    # connection was dead *before* pgmq-ruby tried to use it, so no SQL was
    # ever sent. Mid-flight errors like "server closed the connection" are
    # excluded from the pattern list for this reason.

    # Seconds by which the outer bounds (client-side tcp_user_timeout and the
    # Ruby Timeout last resort) exceed the server-side statement_timeout. Sizing
    # the outer bounds a little higher lets a live-but-slow server's clean
    # statement_timeout cancel win the race, so the outer bounds fire only when
    # the peer is genuinely gone. See apply_connection_bounds and with_read_timeout.
    READ_TIMEOUT_SLACK = 5
    private_constant :READ_TIMEOUT_SLACK

    # Raised (instead of plain ReadTimeoutError) when the Ruby Timeout last
    # resort fires — i.e. Thread#raise interrupted a read mid-flight on a
    # socket libpq couldn't bound (issue #354). IS-A Pgbus::ReadTimeoutError,
    # so the public contract is unchanged; the distinct class lets
    # with_read_timeout tell "wedged socket — reload the pool" (this) apart
    # from "clean server-side statement_timeout cancel — connection healthy"
    # (plain ReadTimeoutError), where reloading would churn a healthy pool on
    # every slow query. Internal signal carried on the unwind path itself (no
    # thread-local/ivar state around a Thread#raise interrupt); application
    # code should rescue Pgbus::ReadTimeoutError.
    class WedgedReadTimeout < Pgbus::ReadTimeoutError; end

    # Bound a read and surface a timeout as Pgbus::ReadTimeoutError. Prefer
    # libpq-native bounds baked into the connection; the Ruby Timeout is a
    # narrow, last-resort fallback used only where libpq cannot bound a hung
    # socket. In order, cleanest to last-resort:
    #
    #   1. statement_timeout (server GUC, baked into the connection) — a slow
    #      query is cancelled by Postgres → PG::QueryCanceled, which pgmq-ruby
    #      wraps as PGMQ::Errors::ConnectionError ("canceling statement due to
    #      statement timeout"); mapping_statement_timeout re-raises it as
    #      Pgbus::ReadTimeoutError. The clean path for a live-but-slow server.
    #   2. tcp_user_timeout / keepalives (client-side libpq, baked into the
    #      connection) — a dead/hung socket makes libpq raise PG::ConnectionBad
    #      synchronously, which pgmq-ruby recognises and reconnects. NO
    #      Thread#raise, no buffer corruption. Linux + libpq >= 12 only.
    #
    # When @libpq_read_bounds_effective (the common production case: Linux,
    # dedicated connection, read_timeout set, libpq >= 12) BOTH bounds are in
    # force and Ruby Timeout is never wired in — pure libpq.
    #
    #   3. Ruby Timeout.timeout — the LAST resort, reached ONLY on a *dedicated*
    #      connection where libpq's socket bound is a no-op: non-Linux hosts
    #      (macOS/BSD/Windows) or a libpq < 12. It interrupts via Thread#raise —
    #      the mechanism issue #198 flags as unsafe — so it is slack-delayed and
    #      used only when there is no libpq alternative on that host.
    #
    #      The shared-AR Proc path deliberately gets NEITHER a baked-in bound NOR
    #      this Ruby Timeout: we don't own that socket, and Thread#raise on a
    #      connection ActiveRecord also queries is the most dangerous place to use
    #      it. Instead the operator configures libpq timeouts in database.yml
    #      (statement_timeout via `variables:`, plus tcp_user_timeout/keepalives),
    #      which AR passes straight through to the connection. #initialize logs a
    #      one-time hint when read_timeout is set on a Proc connection.
    #
    #      WEDGED-SOCKET RECOVERY (issue #354): when (3) fires on a genuinely
    #      hung socket, libpq may leave the pooled PG::Connection reporting
    #      CONNECTION_OK while it will in fact re-hang on reuse, and pgmq-ruby's
    #      checkout health check won't discard it (it isn't CONNECTION_BAD). So
    #      the Timeout raises WedgedReadTimeout (a ReadTimeoutError subclass)
    #      and the rescue below drops every pooled connection via @pgmq.reload
    #      (pgmq-ruby >= 0.7.1) — the pool rebuilds lazily on next checkout.
    #      By the time the rescue runs, Thread#raise has unwound the read and
    #      connection_pool's ensure has checked the poisoned connection back in
    #      as idle, so reload does discard it. A small window remains where
    #      another thread checks it out first; that thread's own read bound /
    #      stale-retry covers it — reload narrows the window, it doesn't need
    #      to close it atomically.
    #
    # MUST wrap only the bare `@pgmq.read*` call, inside both `synchronized` and
    # `with_stale_connection_retry`, so the Timeout clock starts only after the
    # mutex is acquired (a thread queued behind another read is not charged for
    # the wait) and each stale-retry attempt gets its own full budget:
    #
    #   with_stale_connection_retry { synchronized { with_read_timeout { @pgmq.read* } } }
    def with_read_timeout(&block)
      # libpq covers everything (Linux, dedicated conn, read_timeout, libpq>=12),
      # OR this is the Proc path where we defer to AR/database.yml — either way,
      # no Ruby Timeout. Only the dedicated-but-libpq-can't-bound-the-socket case
      # (non-Linux / libpq<12) falls through to the Timeout fallback below.
      return mapping_statement_timeout(&block) if @libpq_read_bounds_effective || @shared_connection

      timeout = config.read_timeout
      return mapping_statement_timeout(&block) unless timeout&.positive?

      # rubocop:disable Pgbus/NoRubyTimeout -- deliberate last-resort bound; see above
      Timeout.timeout(timeout + READ_TIMEOUT_SLACK, WedgedReadTimeout) do
        mapping_statement_timeout(&block)
      end
      # rubocop:enable Pgbus/NoRubyTimeout
    rescue WedgedReadTimeout
      reload_pool_after_wedged_timeout
      raise
    end

    # Best-effort job-pool reload after the Ruby Timeout fallback interrupted a
    # read (see with_read_timeout). A reload failure is logged, never raised —
    # the caller is already unwinding with ReadTimeoutError, which is the
    # actionable error; masking it with a secondary pool failure would hide
    # which read timed out.
    def reload_pool_after_wedged_timeout
      @pgmq.reload
    rescue StandardError => e
      Pgbus.logger.warn do
        "[Pgbus::Client] pool reload after wedged read timeout failed: #{e.class}: #{e.message}"
      end
    end

    # True when libpq's connection-baked read bounds (statement_timeout +
    # tcp_user_timeout + keepalives) fully cover both a slow query AND a
    # dead/hung socket, so with_read_timeout can skip the Ruby Timeout entirely.
    # Requires ALL of:
    #   * a dedicated connection — the shared-AR Proc path has no baked-in bounds
    #     (we don't own that socket; statement_timeout would leak into app queries)
    #   * read_timeout set — apply_connection_bounds no-ops on nil, so skipping
    #     Timeout with no bound installed would leave a read unbounded forever
    #   * TCP_USER_TIMEOUT available — macOS/BSD/Windows no-op it, and keepalives
    #     alone can't bound a stall mid-reply (data sent, never ACKed)
    #   * libpq >= 12 — older libpq rejects the tcp_user_timeout conninfo keyword
    #     outright (it fails the whole connection), so we must not have baked it in
    def libpq_read_bounds_effective?
      return false if @shared_connection
      return false unless config.read_timeout&.positive?
      return false unless Socket.const_defined?(:TCP_USER_TIMEOUT)

      PG.library_version >= 120_000
    end

    # On the shared-AR (Proc) path pgbus doesn't own the connection, so it bakes
    # in no read bounds and deliberately does NOT wrap reads in Ruby Timeout
    # (Thread#raise on a socket ActiveRecord also uses is the most dangerous
    # place for it). When read_timeout is set the operator likely expects reads
    # to be bounded, so point them at the libpq timeouts AR passes through from
    # database.yml — the same bounds pgbus's dedicated path installs itself.
    def warn_shared_connection_read_bounds
      return unless @shared_connection && config.read_timeout&.positive?

      Pgbus.logger.warn do
        "[Pgbus::Client] read_timeout is set but pgbus is sharing ActiveRecord's " \
          "connection, so it can't bound reads itself. Configure libpq timeouts on " \
          "the pgbus connection in database.yml instead: `variables: { statement_timeout: <ms> }` " \
          "plus `tcp_user_timeout: <ms>` and `keepalives: 1` (Linux). Use a dedicated " \
          "database_url/connection_params for pgbus to have it apply these automatically."
      end
    end

    # Substring pgmq-ruby surfaces (wrapped as PGMQ::Errors::ConnectionError)
    # when Postgres cancels a query that overran statement_timeout. Detected in
    # the read paths and re-raised as Pgbus::ReadTimeoutError so the server-side
    # bound preserves the same public contract the Ruby Timeout gave callers.
    STATEMENT_TIMEOUT_PATTERN = "canceling statement due to statement timeout"
    private_constant :STATEMENT_TIMEOUT_PATTERN

    def statement_timeout_error?(error)
      error.message.to_s.downcase.include?(STATEMENT_TIMEOUT_PATTERN)
    end

    # Run a read block, re-raising a server-side statement_timeout cancellation
    # as Pgbus::ReadTimeoutError. Wraps the read call sites so the public
    # contract (ReadTimeoutError on a timed-out read) holds whether the bound
    # fired server-side (the normal case) or via the Ruby Timeout fallback.
    def mapping_statement_timeout
      yield
    rescue PGMQ::Errors::ConnectionError => e
      raise Pgbus::ReadTimeoutError, e.message if statement_timeout_error?(e)

      raise
    end

    # Idle seconds before libpq starts probing a quiet connection with TCP
    # keepalives, and the interval/count of those probes. Sized to detect a
    # dead peer (or a NAT/LB that silently dropped an idle flow) well inside a
    # typical cloud idle-drop window (~350–600s). Pool and LISTEN connections
    # sit idle between reads, so keepalives are what catch a peer that vanished
    # while nothing was in flight. Client-side libpq keywords — never sent in
    # the startup packet, so a pooler (PgBouncer) can't reject them.
    KEEPALIVE_IDLE_SECONDS = 30
    KEEPALIVE_INTERVAL_SECONDS = 10
    KEEPALIVE_COUNT = 3
    private_constant :KEEPALIVE_IDLE_SECONDS, :KEEPALIVE_INTERVAL_SECONDS, :KEEPALIVE_COUNT

    # Bake libpq-native read/connection bounds into the connection options of a
    # dedicated pgmq-ruby connection (issue #198). Two independent libpq
    # mechanisms, deliberately NOT Ruby's Timeout — Timeout interrupts via
    # Thread#raise, which can fire mid-libpq call and corrupt the pooled
    # PG::Connection's result buffer:
    #
    #   1. statement_timeout (server GUC, via `options=-c`) — bounds a query the
    #      server is actively running. Postgres cancels it and sends back
    #      PG::QueryCanceled, which the read paths map to Pgbus::ReadTimeoutError.
    #      This is the bound for a live-but-slow server.
    #   2. tcp_user_timeout + keepalives (client-side libpq conninfo keywords) —
    #      bound a dead/hung socket the server never answers on, where
    #      statement_timeout structurally cannot fire (no live server to cancel).
    #      libpq forces the socket closed and raises PG::ConnectionBad /
    #      PG::UnableToSend synchronously on the calling thread — a clean error
    #      through the normal pgmq path, no Thread#raise, no buffer corruption.
    #      tcp_user_timeout catches death mid-read (data sent, never ACKed);
    #      keepalives catch death on an idle connection.
    #
    # tcp_user_timeout is sized at read_timeout + a small slack so statement_timeout
    # (the clean server-side cancel) wins whenever the server is still answering;
    # the socket bound only fires when the peer is genuinely gone.
    #
    # Called only on the dedicated-connection branch (String URL / Hash params).
    # Never on the shared-AR Proc path — statement_timeout is connection-wide and
    # would leak into application queries, and the socket there is AR's to own.
    #
    # NOTE: statement_timeout is connection-wide, so writes on these connections
    # gain the same bound. Acceptable — an enqueue that can't complete within
    # read_timeout is already failing — and keeps a single server-side mechanism.
    #
    # Returns conn_opts unchanged when read_timeout is nil (bounding disabled).
    # Stamp a per-process application_name on the streams-pool connection options
    # so the autoscaler can count peer processes via
    # `pg_stat_activity.application_name LIKE '<prefix>_%'` (issue #323 P1). The
    # suffix is the pid so DISTINCT application_name is an exact process count.
    # application_name is a cosmetic session GUC — appending it can't break the
    # connection. The Proc (shared-AR) path never reaches here.
    def tag_application_name(conn_opts)
      name = "#{config.streams_application_name}_#{::Process.pid}"
      case conn_opts
      when Hash
        conn_opts.merge(application_name: name)
      when String
        # Two libpq string forms (mirrors #append_connection_bounds): URI form
        # carries params as `?key=value&…` query pairs; key=value conninfo form
        # is space-separated. Appending wins over any earlier application_name.
        if conn_opts.start_with?("postgres://", "postgresql://")
          separator = conn_opts.include?("?") ? "&" : "?"
          "#{conn_opts}#{separator}application_name=#{name}"
        else
          "#{conn_opts} application_name=#{name}"
        end
      else
        conn_opts
      end
    end

    # In :session GUC mode, a Hash conn_opts carrying database.yml `:variables`
    # must apply those GUCs via post-connect `SET` rather than the libpq
    # `options` STARTUP param (which a transaction-mode PgBouncer rejects). We
    # can't pass `:variables` to PG.connect (not a libpq keyword), so wrap the
    # opts in a fresh-connect factory Proc: pgmq-ruby natively accepts a callable
    # per pool slot (pgmq connection.rb), and it must return a UNIQUE
    # PG::Connection each call (pgmq guards against a shared object). Applies to
    # a Hash with `:variables` only — a String URL or no variables passes through
    # unchanged, as does :options mode (where forward_connection_variables
    # already baked the GUCs into `options`). See issue #332.
    def wrap_session_gucs(conn_opts)
      return conn_opts unless config.connection_guc_mode == :session
      return conn_opts unless conn_opts.is_a?(Hash)

      variables = conn_opts[:variables]
      return conn_opts if variables.nil? || variables.empty?

      pg_opts = conn_opts.except(:variables)
      lambda do
        conn = PG.connect(pg_opts)
        variables.each { |name, value| conn.exec("SET #{name} = '#{value}'") }
        conn
      end
    end

    def apply_connection_bounds(conn_opts)
      timeout = config.read_timeout
      return conn_opts unless timeout&.positive?

      statement_ms = (timeout * 1000).to_i
      # Socket-death bound sits just above the server-side query bound so a live
      # server's clean statement_timeout cancel always wins the race.
      socket_ms = ((timeout + READ_TIMEOUT_SLACK) * 1000).to_i
      # tcp_user_timeout is a libpq 12+ conninfo keyword; libpq < 12 rejects it
      # outright and fails the whole connection. So only bake in the socket-level
      # keywords when the linked libpq understands them — statement_timeout (a
      # server GUC via `options`) is always safe. Older libpq keeps just the
      # query bound; the Ruby Timeout fallback covers the socket there.
      with_socket = libpq_supports_socket_bounds?

      case conn_opts
      when Hash
        merge_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket: with_socket)
      when String
        append_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket: with_socket)
      else
        conn_opts
      end
    end

    # Whether the linked libpq accepts the tcp_user_timeout / keepalives conninfo
    # keywords. Added in libpq 12; an older libpq raises "invalid connection
    # option" and fails the connection, so we must not emit them there.
    def libpq_supports_socket_bounds?
      defined?(PG) && PG.respond_to?(:library_version) && PG.library_version >= 120_000
    end

    # Hash form maps 1:1 to libpq keywords, so no escaping/encoding is needed.
    # The GUC stays nested in `options`; the socket keywords are top-level.
    # Preserve any caller-supplied `:options` (e.g. `-c search_path=…`) by
    # appending our `-c statement_timeout=…` rather than overwriting it.
    def merge_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket:)
      options = [conn_opts[:options], "-c statement_timeout=#{statement_ms}"].compact.join(" ")
      merged = conn_opts.merge(options: options)
      return merged unless with_socket

      merged.merge(
        keepalives: 1,
        keepalives_idle: KEEPALIVE_IDLE_SECONDS,
        keepalives_interval: KEEPALIVE_INTERVAL_SECONDS,
        keepalives_count: KEEPALIVE_COUNT,
        tcp_user_timeout: socket_ms
      )
    end

    # libpq accepts two connection-string forms. URI form (postgres:// or
    # postgresql://) carries keywords as URL-encoded query params — the GUC in
    # `options` must percent-encode its space (%20) and `=` (%3D). key=value
    # conninfo form carries them space-separated, with the GUC single-quoted so
    # the outer parser keeps `-c statement_timeout=…` as one value.
    def append_connection_bounds(conn_opts, statement_ms, socket_ms, with_socket:)
      if conn_opts.start_with?("postgres://", "postgresql://")
        separator = conn_opts.include?("?") ? "&" : "?"
        socket = if with_socket
                   "keepalives=1&keepalives_idle=#{KEEPALIVE_IDLE_SECONDS}" \
                     "&keepalives_interval=#{KEEPALIVE_INTERVAL_SECONDS}" \
                     "&keepalives_count=#{KEEPALIVE_COUNT}&tcp_user_timeout=#{socket_ms}&"
                 else
                   ""
                 end
        "#{conn_opts}#{separator}#{socket}options=-c%20statement_timeout%3D#{statement_ms}"
      else
        socket = if with_socket
                   "keepalives=1 keepalives_idle=#{KEEPALIVE_IDLE_SECONDS} " \
                     "keepalives_interval=#{KEEPALIVE_INTERVAL_SECONDS} " \
                     "keepalives_count=#{KEEPALIVE_COUNT} tcp_user_timeout=#{socket_ms} "
                 else
                   ""
                 end
        "#{conn_opts} #{socket}options='-c statement_timeout=#{statement_ms}'"
      end
    end

    # Gate a read through the in-memory connection-health circuit breaker.
    # When the breaker is open the block never runs — Pgbus::ConnectionCircuitOpenError
    # is raised before any pool checkout, sparing a dead database from the whole
    # fleet re-polling and the error tracker from per-poll noise. A completed
    # read records success (closing/resetting the breaker); a
    # PGMQ::Errors::ConnectionError records a failure (and still propagates).
    # Writes are intentionally NOT gated — callers must see enqueue failures.
    def guarded_read(&)
      @connection_health.run_guarded(&)
    end

    def log_circuit_open(backoff)
      Pgbus.logger.warn do
        "[Pgbus::Client] Connection circuit opened after #{ConnectionHealth::OPEN_THRESHOLD}+ " \
          "consecutive connection failures — reads fail fast for ~#{backoff}s"
      end
    end

    def log_circuit_close
      Pgbus.logger.info { "[Pgbus::Client] Connection circuit closed — database reachable again" }
    end

    def with_stale_connection_retry
      attempts = 0
      begin
        yield
      rescue PGMQ::Errors::ConnectionError => e
        attempts += 1
        raise enrich_pool_timeout_error(e) unless attempts <= STALE_RETRY_ATTEMPTS && stale_connection_error?(e)

        # Sleep here — in the rescue, *outside* the yielded block — so the
        # backoff never runs while @pgmq_mutex is held: on the shared-connection
        # path the mutex lives inside `synchronized` within the yielded block,
        # and the raise unwinds out of it (releasing the mutex) before we get
        # here. See STALE_RETRY_DELAYS. Clamp the index to the last delay so a
        # future STALE_RETRY_ATTEMPTS > STALE_RETRY_DELAYS.size never sleeps nil.
        sleep STALE_RETRY_DELAYS[[attempts - 1, STALE_RETRY_DELAYS.size - 1].min]

        Pgbus.logger.warn do
          "[Pgbus::Client] Retrying after stale pgmq connection " \
            "(attempt #{attempts}/#{STALE_RETRY_ATTEMPTS}): #{e.message}"
        end
        retry
      end
    end

    def stale_connection_error?(error)
      # Substring match against any stale-connection pattern. Deliberately a
      # single Regexp rather than `STALE_CONNECTION_PATTERNS.any? { |p| msg.include?(p) }`:
      # Style/ArrayIntersect misfires on that shape and "corrects" it to
      # `patterns.intersect?(msg)`, which raises TypeError (String isn't an Array).
      STALE_CONNECTION_PATTERN.match?(error.message.to_s)
    end

    # Substring pgmq-ruby uses when a pool checkout times out — a
    # ConnectionPool::TimeoutError re-raised as PGMQ::Errors::ConnectionError
    # "Connection pool timeout: ..." (see PGMQ::Connection#with_connection).
    # Deliberately NOT in STALE_CONNECTION_PATTERNS: a saturated pool must not
    # be retried (that just piles more waiters onto an already-exhausted pool).
    POOL_TIMEOUT_MARKER = "connection pool timeout"
    private_constant :POOL_TIMEOUT_MARKER

    def pool_timeout_error?(error)
      error.message.to_s.downcase.include?(POOL_TIMEOUT_MARKER)
    end

    # A bare "Connection pool timeout" tells an operator nothing actionable.
    # For that (and only that) error, return a same-class replacement whose
    # message carries the live pool state and a concrete next step, so the
    # first signal of saturation is diagnosable. The class is preserved so
    # callers rescuing PGMQ::Errors::ConnectionError behave identically. Any
    # other ConnectionError is returned untouched. Enrichment never raises:
    # pool_stats already rescues to {}, and a formatting failure falls back to
    # the original error.
    def enrich_pool_timeout_error(error)
      return error unless pool_timeout_error?(error)

      stats = pool_stats
      detail = stats.empty? ? "" : " (pool #{stats})"
      error.class.new(
        "#{error.message}#{detail} — " \
        "raise Pgbus.configuration.pool_size or reduce worker threads"
      )
    rescue StandardError
      error
    end

    def serialize(data)
      case data
      when String
        data
      else
        JSON.generate(data)
      end
    end

    # Single-pass serialization of payloads and optional headers.
    # Avoids two separate .map iterations over the same index range.
    def serialize_batch(payloads, headers)
      serialized = Array.new(payloads.size)
      serialized_headers = headers ? Array.new(headers.size) : nil

      payloads.each_with_index do |p, i|
        serialized[i] = serialize(p)
        if serialized_headers && i < headers.size
          h = headers[i]
          serialized_headers[i] = h.nil? ? nil : serialize(h)
        end
      end

      [serialized, serialized_headers]
    end
  end
end
