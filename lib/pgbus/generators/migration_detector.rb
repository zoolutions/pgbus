# frozen_string_literal: true

module Pgbus
  module Generators
    # Inspects a live ActiveRecord connection and determines which of
    # pgbus's migration generators need to run to bring the schema up
    # to date.
    #
    # Usage:
    #
    #   detector = Pgbus::Generators::MigrationDetector.new(connection)
    #   detector.missing_migrations
    #   # => [:add_uniqueness_keys, :add_job_stats_queue_index, ...]
    #
    # The returned symbols correspond to generator names that the
    # pgbus:update generator invokes via Thor composition. Each symbol
    # maps to exactly one generator (see GENERATOR_MAP).
    #
    # Detection rules:
    #
    # 1. Fresh install (no core tables) → returns [:fresh_install] as a
    #    sentinel. The caller should tell the user to run pgbus:install
    #    instead of stacking 8 migrations.
    #
    # 2. Core tables missing → queued unconditionally. These are features
    #    pgbus assumes are present.
    #
    # 3. Opt-in feature tables missing → queued unconditionally. The user
    #    asked for update-to-latest, so we add the migration files but
    #    don't enable the feature in config. They'll opt in separately.
    #
    # 4. Columns missing on existing tables → queued. These are in-place
    #    schema upgrades (e.g. add_job_stats_latency adds
    #    enqueue_latency_ms + retry_count).
    #
    # 5. Indexes missing on existing tables → queued. Additive, safe.
    #
    # 6. Modern uniqueness table (pgbus_uniqueness_keys) missing → queue
    #    add_uniqueness_keys unconditionally. A legacy pgbus_job_locks
    #    table left over from an older install is not dropped by this
    #    detector — run `pgbus:migrate_job_locks` by hand to retire it.
    class MigrationDetector
      # Sentinel returned when the database looks empty of pgbus tables.
      # The caller (pgbus:update generator) should redirect the user to
      # pgbus:install rather than trying to stack the full schema as
      # individual add_* migrations.
      FRESH_INSTALL = :fresh_install

      # The set of tables that the base pgbus:install migration creates.
      # If NONE of these exist, we treat the DB as a fresh install.
      CORE_INSTALL_TABLES = %w[
        pgbus_processed_events
        pgbus_processes
        pgbus_failed_events
        pgbus_semaphores
        pgbus_blocked_executions
        pgbus_batches
      ].freeze

      # The three tables pgbus:add_workflows creates (issue #540).
      WORKFLOW_TABLES = %w[pgbus_workflows pgbus_workflow_steps pgbus_workflow_events].freeze

      # The waiting-step columns pgbus:add_workflow_signals adds to
      # pgbus_workflow_steps (issue #541), with the pgbus_workflow_signals table.
      WORKFLOW_WAITING_COLUMNS = %w[batch_id wait_for signal_id waiting_since timeout_at].freeze
      WORKFLOW_WAITING_INDEXES = Ractor.make_shareable(
        [
          %w[pgbus_workflow_steps idx_pgbus_workflow_steps_batch],
          %w[pgbus_workflow_steps idx_pgbus_workflow_steps_timeout],
          %w[pgbus_workflow_signals idx_pgbus_workflow_signals_idempotency],
          %w[pgbus_workflow_signals idx_pgbus_workflow_signals_pending],
          %w[pgbus_workflow_signals idx_pgbus_workflow_signals_claimed]
        ]
      )

      # generator_key → Rails generator name. Passed to Thor's invoke.
      #
      # Note: uniqueness_keys invokes add_uniqueness_keys, which only
      # creates the modern pgbus_uniqueness_keys table. If the legacy
      # pgbus_job_locks table is also present (an install mid-migration),
      # run `pgbus:migrate_job_locks` separately to drop it — that
      # generator is not part of this detector's default flow since it
      # requires the legacy table to already exist.
      GENERATOR_MAP = {
        uniqueness_keys: "pgbus:add_uniqueness_keys",
        add_job_stats: "pgbus:add_job_stats",
        add_job_stats_latency: "pgbus:add_job_stats_latency",
        add_job_stats_queue_index: "pgbus:add_job_stats_queue_index",
        add_stream_stats: "pgbus:add_stream_stats",
        add_stream_queues: "pgbus:add_stream_queues",
        add_presence: "pgbus:add_presence",
        add_presence_seats: "pgbus:add_presence_seats",
        add_queue_states: "pgbus:add_queue_states",
        add_outbox: "pgbus:add_outbox",
        add_recurring: "pgbus:add_recurring",
        add_deploy_tasks: "pgbus:add_deploy_tasks",
        add_workflows: "pgbus:add_workflows",
        add_workflow_signals: "pgbus:add_workflow_signals",
        add_failed_events_index: "pgbus:add_failed_events_index",
        add_processed_event_completion: "pgbus:add_processed_event_completion",
        add_batch_executions: "pgbus:add_batch_executions",
        add_batch_callback_jobs: "pgbus:add_batch_callback_jobs",
        tune_autovacuum: "pgbus:tune_autovacuum",
        tune_fillfactor: "pgbus:tune_fillfactor"
      }.freeze

      # Human-friendly description of each migration for the generator
      # output. Keeps the update generator's run log readable.
      DESCRIPTIONS = {
        uniqueness_keys: "uniqueness keys table (job deduplication)",
        add_job_stats: "job stats table (Insights dashboard)",
        add_job_stats_latency: "job stats latency columns (enqueue_latency_ms, retry_count)",
        add_job_stats_queue_index: "job stats (queue_name, created_at) index",
        add_stream_stats: "stream stats table (opt-in real-time Insights)",
        add_stream_queues: "stream queue registry (per-stream retention, orphan sweep, wildcard-worker safety)",
        add_presence: "presence members table (Turbo Streams presence)",
        add_presence_seats: "presence seats (one row per SSE connection, per-seat state)",
        add_queue_states: "queue states table (pause/resume)",
        add_outbox: "outbox entries table (transactional outbox)",
        add_recurring: "recurring tasks + executions tables",
        add_deploy_tasks: "deploy tasks + runs tables (one-time jobs run after a rollover)",
        add_workflows: "workflow tables (durable multi-step workflows: runs, step attempts, audit trail)",
        add_workflow_signals: "waiting workflow steps (fan_out, wait_for: signals, wait timeouts)",
        add_failed_events_index: "unique index on pgbus_failed_events (queue_name, msg_id)",
        add_processed_event_completion: "completed_at on pgbus_processed_events (two-phase idempotency claim)",
        add_batch_executions: "batch execution-row tracking (self-healing completion, on_failure rename)",
        add_batch_callback_jobs: "batch callback-job columns (configured ActiveJob instances as batch callbacks)",
        tune_autovacuum: "autovacuum tuning for PGMQ queue and archive tables",
        tune_fillfactor: "fillfactor=70 on PGMQ queue tables (reduces page density during update churn)"
      }.freeze

      def initialize(connection)
        @connection = connection
      end

      # Returns an Array of generator keys (symbols) in the order they
      # should run. Dependencies are resolved implicitly via order: the
      # base table creation for a feature always comes before the
      # column/index add-ons.
      def missing_migrations
        return [FRESH_INSTALL] if fresh_install?

        [
          *uniqueness_key_migrations,
          *job_stats_migrations,
          *stream_stats_migrations,
          *stream_queues_migrations,
          *presence_migrations,
          *queue_states_migrations,
          *outbox_migrations,
          *recurring_migrations,
          *deploy_tasks_migrations,
          *workflows_migrations,
          *failed_events_index_migrations,
          *processed_event_completion_migrations,
          *batch_executions_migrations,
          *batch_callback_jobs_migrations,
          *autovacuum_migrations,
          *fillfactor_migrations
        ]
      end

      private

      attr_reader :connection

      def fresh_install?
        CORE_INSTALL_TABLES.none? { |t| table_exists?(t) }
      end

      # Queue add_uniqueness_keys whenever the modern table is missing.
      # This does not touch a legacy pgbus_job_locks table — that is
      # handled separately by `pgbus:migrate_job_locks`, see GENERATOR_MAP.
      def uniqueness_key_migrations
        return [] if table_exists?("pgbus_uniqueness_keys")

        [:uniqueness_keys]
      end

      def job_stats_migrations
        migrations = []

        unless table_exists?("pgbus_job_stats")
          migrations << :add_job_stats
          # The latency columns and queue index are add-ons to job_stats.
          # If we're creating the base table now, the add_job_stats
          # template doesn't include them — add the upgrade migrations
          # so a fresh install lands on the current schema.
          migrations << :add_job_stats_latency
          migrations << :add_job_stats_queue_index
          return migrations
        end

        # Base table exists — check each add-on independently.
        cols = column_names("pgbus_job_stats")
        migrations << :add_job_stats_latency unless cols.include?("enqueue_latency_ms") && cols.include?("retry_count")

        migrations << :add_job_stats_queue_index unless index_exists?("pgbus_job_stats", "idx_pgbus_job_stats_queue_time")

        migrations
      end

      def stream_stats_migrations
        table_exists?("pgbus_stream_stats") ? [] : [:add_stream_stats]
      end

      def stream_queues_migrations
        table_exists?("pgbus_stream_queues") ? [] : [:add_stream_queues]
      end

      # The add_presence template already carries the seat columns, so a
      # missing table needs only that; an existing table without
      # connection_id predates seats (issue #515).
      def presence_migrations
        return [:add_presence] unless table_exists?("pgbus_presence_members")
        return [] if column_names("pgbus_presence_members").include?("connection_id")

        [:add_presence_seats]
      end

      def queue_states_migrations
        table_exists?("pgbus_queue_states") ? [] : [:add_queue_states]
      end

      def outbox_migrations
        table_exists?("pgbus_outbox_entries") ? [] : [:add_outbox]
      end

      def recurring_migrations
        # pgbus_recurring_tasks + pgbus_recurring_executions are the two
        # tables the recurring generator creates. If BOTH exist, nothing
        # to do. If either is missing, we queue the generator. The
        # add_recurring template uses plain create_table (no if_not_exists),
        # so idempotency is provided by this detector, not the template:
        # the migration is only queued while a table is absent, and never
        # re-invoked once both tables are present.
        return [] if table_exists?("pgbus_recurring_tasks") && table_exists?("pgbus_recurring_executions")

        [:add_recurring]
      end

      # Same shape as recurring_migrations: plain create_table in the
      # template, so the generator is queued only while a table is absent.
      def deploy_tasks_migrations
        return [] if table_exists?("pgbus_deploy_tasks") && table_exists?("pgbus_deploy_task_runs")

        [:add_deploy_tasks]
      end

      # The add_workflows template uses if_not_exists throughout, so a
      # half-present set of the three tables is safe to queue again.
      #
      # add_workflows carries the waiting-step schema too, so a database with
      # no steps table needs only that one; a steps table without the columns
      # (or a missing signals table) predates waiting steps.
      def workflows_migrations
        migrations = []
        migrations << :add_workflows unless WORKFLOW_TABLES.all? { |table| table_exists?(table) }
        return migrations unless table_exists?("pgbus_workflow_steps")

        columns = column_names("pgbus_workflow_steps")
        waiting = WORKFLOW_WAITING_COLUMNS.all? { |column| columns.include?(column) } &&
                  table_exists?("pgbus_workflow_signals") &&
                  WORKFLOW_WAITING_INDEXES.all? { |table, index| index_exists?(table, index) }
        migrations << :add_workflow_signals unless waiting
        migrations
      end

      def failed_events_index_migrations
        # pgbus_failed_events is created by pgbus:install, but the
        # unique (queue_name, msg_id) index was added later via its own
        # generator. If the table exists without the unique index,
        # FailedEventRecorder's upsert silently swallows ON CONFLICT
        # errors — so this is a real bug waiting to bite.
        return [] unless table_exists?("pgbus_failed_events")
        return [] if index_exists?("pgbus_failed_events", "idx_pgbus_failed_events_queue_msg")

        [:add_failed_events_index]
      end

      # completed_at backs the two-phase idempotency claim (issue #385).
      # Without it, idempotent handlers fall back to single-phase claims and
      # a crash mid-handler silently drops the execution on redelivery.
      def processed_event_completion_migrations
        return [] unless table_exists?("pgbus_processed_events")
        return [] if column_names("pgbus_processed_events").include?("completed_at")

        [:add_processed_event_completion]
      end

      # Execution-row tracking plus the on_discard → on_failure column rename
      # ship as one upgrade. Either a missing executions table or leftover
      # pre-rename columns means this generator still needs to run.
      def batch_executions_migrations
        return [] unless table_exists?("pgbus_batches")

        batches_columns = column_names("pgbus_batches")
        migrated = table_exists?("pgbus_batch_executions") &&
                   batches_columns.include?("on_failure_class") &&
                   !batches_columns.include?("discarded_jobs")
        return [] if migrated

        [:add_batch_executions]
      end

      # Configured callback instances (on_complete: MyJob.new.set(...)) need
      # somewhere to store the serialized job — issue #415.
      def batch_callback_jobs_migrations
        return [] unless table_exists?("pgbus_batches")
        return [] if column_names("pgbus_batches").include?("on_finish_job")

        [:add_batch_callback_jobs]
      end

      # Autovacuum tuning: check if any PGMQ queue table already has
      # custom autovacuum settings applied. If not, queue the migration.
      def autovacuum_migrations
        return [] unless pgmq_schema_exists?
        return [] if autovacuum_already_tuned?

        [:tune_autovacuum]
      end

      # Fillfactor tuning: check if any PGMQ queue table already has
      # fillfactor applied. If not, queue the migration.
      def fillfactor_migrations
        return [] unless pgmq_schema_exists?
        return [] if fillfactor_already_tuned?

        [:tune_fillfactor]
      end

      # --- schema probes -------------------------------------------------

      def table_exists?(name)
        connection.table_exists?(name)
      rescue StandardError
        false
      end

      def column_names(table)
        connection.columns(table).map(&:name)
      rescue StandardError
        []
      end

      def index_exists?(table, index_name)
        connection.indexes(table).any? { |idx| idx.name == index_name }
      rescue StandardError
        false
      end

      def pgmq_schema_exists?
        result = connection.select_value("SELECT 1 FROM information_schema.schemata WHERE schema_name = 'pgmq'")
        result.present?
      rescue StandardError
        false
      end

      def autovacuum_already_tuned?
        queue_name = connection.select_value("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
        return true unless queue_name # no queues = nothing to tune, skip

        result = connection.select_value(<<~SQL)
          SELECT reloptions::text LIKE '%autovacuum_vacuum_scale_factor%'
          FROM pg_class
          WHERE relname = 'q_#{queue_name}'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
        SQL

        [true, "t"].include?(result)
      rescue StandardError
        true # if we can't tell, assume already tuned (safe default)
      end

      def fillfactor_already_tuned?
        queue_name = connection.select_value("SELECT queue_name FROM pgmq.meta ORDER BY queue_name LIMIT 1")
        return true unless queue_name # no queues = nothing to tune, skip

        result = connection.select_value(<<~SQL)
          SELECT reloptions::text LIKE '%fillfactor%'
          FROM pg_class
          WHERE relname = 'q_#{queue_name}'
            AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgmq')
        SQL

        [true, "t"].include?(result)
      rescue StandardError
        true # if we can't tell, assume already tuned (safe default)
      end
    end
  end
end
