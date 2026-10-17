# frozen_string_literal: true

require "active_record"
require "tempfile"
require "uri"
require "pgbus"

# Integration tests require a real PostgreSQL database with PGMQ installed.
# Set PGBUS_DATABASE_URL to connect:
#   export PGBUS_DATABASE_URL=postgres://pgbus:pgbus@localhost:5432/pgbus_test
PGBUS_DATABASE_URL = ENV.fetch("PGBUS_DATABASE_URL", nil)

def bootstrap_integration_tables(conn)
  unless conn.table_exists?("pgbus_uniqueness_keys")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_uniqueness_keys (
        lock_key VARCHAR NOT NULL,
        queue_name VARCHAR NOT NULL,
        msg_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_uniqueness_keys_key ON pgbus_uniqueness_keys (lock_key);
    SQL
  end

  unless conn.table_exists?("pgbus_presence_members")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_presence_members (
        stream_name VARCHAR NOT NULL,
        member_id VARCHAR NOT NULL,
        metadata JSONB NOT NULL DEFAULT '{}',
        joined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_presence_members_pk
        ON pgbus_presence_members (stream_name, member_id);
      CREATE INDEX idx_pgbus_presence_members_sweep
        ON pgbus_presence_members (stream_name, last_seen_at);
    SQL
  end

  # Seats (issue #515), mirroring the add_presence_seats migration. Applied
  # separately from the CREATE so a database created before seats upgrades.
  unless conn.column_exists?("pgbus_presence_members", "connection_id")
    conn.execute(<<~SQL)
      ALTER TABLE pgbus_presence_members ADD COLUMN connection_id VARCHAR;
      ALTER TABLE pgbus_presence_members ADD COLUMN seat VARCHAR;
      ALTER TABLE pgbus_presence_members ADD COLUMN state JSONB NOT NULL DEFAULT '{}';
      DROP INDEX IF EXISTS idx_pgbus_presence_members_pk;
      CREATE UNIQUE INDEX idx_pgbus_presence_members_member_key
        ON pgbus_presence_members (stream_name, member_id) WHERE connection_id IS NULL;
      CREATE UNIQUE INDEX idx_pgbus_presence_members_connection_key
        ON pgbus_presence_members (stream_name, connection_id) WHERE connection_id IS NOT NULL;
      CREATE INDEX idx_pgbus_presence_members_member
        ON pgbus_presence_members (stream_name, member_id);
      CREATE INDEX idx_pgbus_presence_members_seat_expiry
        ON pgbus_presence_members (last_seen_at) WHERE connection_id IS NOT NULL;
    SQL
  end

  unless conn.table_exists?("pgbus_failed_events")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_failed_events (
        id BIGSERIAL PRIMARY KEY,
        queue_name VARCHAR NOT NULL,
        msg_id BIGINT,
        payload JSONB,
        headers JSONB,
        error_class VARCHAR,
        error_message TEXT,
        backtrace TEXT,
        retry_count INTEGER DEFAULT 0,
        failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    SQL
  end

  # Always ensure the unique index exists, even if the table was created
  # by an older bootstrap (e.g. CI workflow's "Set up pgbus tables" step
  # that predates the upsert). FailedEventRecorder.record! does
  # ON CONFLICT (queue_name, msg_id) UPDATE — without this index it raises
  # "no unique or exclusion constraint matching the ON CONFLICT specification"
  # and silently swallows the error, leaving failed_events.first nil in tests.
  conn.execute(<<~SQL)
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pgbus_failed_events_queue_msg
      ON pgbus_failed_events (queue_name, msg_id);
  SQL

  # Transactional outbox — mirrors lib/generators/pgbus/templates/add_outbox.rb.erb.
  # The Poller reads OutboxEntry.unpublished (published_at IS NULL), so the
  # nullable published_at column is load-bearing for the outbox_flow_spec.
  unless conn.table_exists?("pgbus_outbox_entries")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_outbox_entries (
        id BIGSERIAL PRIMARY KEY,
        queue_name VARCHAR,
        routing_key VARCHAR,
        payload JSONB NOT NULL,
        headers JSONB,
        priority INTEGER DEFAULT 0,
        delay INTEGER DEFAULT 0,
        published_at TIMESTAMP,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT chk_pgbus_outbox_destination
          CHECK ((queue_name IS NOT NULL) <> (routing_key IS NOT NULL))
      );
      CREATE INDEX idx_pgbus_outbox_unpublished
        ON pgbus_outbox_entries (published_at) WHERE published_at IS NULL;
    SQL
  end

  # Idempotency ledger — mirrors the processed_events DDL in
  # lib/generators/pgbus/templates/migration.rb.erb. Handler#claim_idempotency
  # does INSERT ... ON CONFLICT (event_id, handler_class) DO NOTHING, so the
  # unique index is what makes the second delivery return :skipped.
  unless conn.table_exists?("pgbus_processed_events")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_processed_events (
        id BIGSERIAL PRIMARY KEY,
        event_id VARCHAR NOT NULL,
        handler_class VARCHAR NOT NULL,
        processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_processed_events_unique
        ON pgbus_processed_events (event_id, handler_class);
    SQL
  end

  # completed_at arrived with the two-phase claim (issue #385) and this DDL
  # was not updated with it, so every integration run silently exercised the
  # legacy single-phase fallback instead. Added separately from the CREATE so
  # a database created before this line picks it up too.
  unless conn.column_exists?("pgbus_processed_events", "completed_at")
    conn.execute("ALTER TABLE pgbus_processed_events ADD COLUMN completed_at TIMESTAMP")
  end
  Pgbus::ProcessedEvent.reset_column_information
  Pgbus::ProcessedEvent.reset_completion_column_check!

  # Stream queue registry — mirrors lib/generators/pgbus/templates/
  # add_stream_queues.rb.erb. StreamQueue.record! does INSERT ... ON CONFLICT
  # (queue_name) DO NOTHING, so the unique index is load-bearing for the
  # registry regression spec (issue #401).
  unless conn.table_exists?("pgbus_stream_queues")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_stream_queues (
        id BIGSERIAL PRIMARY KEY,
        queue_name VARCHAR NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_stream_queues_queue_name
        ON pgbus_stream_queues (queue_name);
    SQL
  end

  # Batch tracking — mirrors the pgbus_batches DDL in
  # lib/generators/pgbus/templates/migration.rb.erb.
  unless conn.table_exists?("pgbus_batches")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_batches (
        id BIGSERIAL PRIMARY KEY,
        batch_id VARCHAR NOT NULL,
        description VARCHAR,
        on_finish_class VARCHAR,
        on_success_class VARCHAR,
        on_failure_class VARCHAR,
        properties JSONB DEFAULT '{}',
        total_jobs INTEGER NOT NULL DEFAULT 0,
        completed_jobs INTEGER NOT NULL DEFAULT 0,
        failed_jobs INTEGER NOT NULL DEFAULT 0,
        status VARCHAR NOT NULL DEFAULT 'pending',
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_batches_batch_id ON pgbus_batches (batch_id);
    SQL
  end

  # Deploy tasks (issue #536) — mirrors lib/generators/pgbus/templates/
  # add_deploy_tasks_tables.rb.erb.
  unless conn.table_exists?("pgbus_deploy_tasks")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_deploy_tasks (
        id BIGSERIAL PRIMARY KEY,
        key VARCHAR NOT NULL,
        class_name VARCHAR,
        command VARCHAR(2048),
        arguments TEXT,
        queue_name VARCHAR,
        priority INTEGER DEFAULT 0,
        version VARCHAR NOT NULL DEFAULT '1',
        every_deploy BOOLEAN NOT NULL DEFAULT FALSE,
        static BOOLEAN NOT NULL DEFAULT TRUE,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        description TEXT,
        armed_at TIMESTAMP,
        armed_for VARCHAR,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
      );
      CREATE UNIQUE INDEX idx_pgbus_deploy_tasks_key ON pgbus_deploy_tasks (key);
      CREATE INDEX idx_pgbus_deploy_tasks_armed ON pgbus_deploy_tasks (armed_at) WHERE armed_at IS NOT NULL;
    SQL
  end

  unless conn.table_exists?("pgbus_deploy_task_runs")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_deploy_task_runs (
        id BIGSERIAL PRIMARY KEY,
        task_key VARCHAR NOT NULL,
        version VARCHAR NOT NULL,
        revision VARCHAR,
        triggered_by VARCHAR NOT NULL,
        queue_name VARCHAR,
        msg_id BIGINT,
        run_at TIMESTAMP NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE INDEX idx_pgbus_deploy_task_runs_task ON pgbus_deploy_task_runs (task_key, run_at);
      CREATE INDEX idx_pgbus_deploy_task_runs_cleanup ON pgbus_deploy_task_runs (run_at);
    SQL
  end

  # Workflows (issue #540) — mirrors lib/generators/pgbus/templates/
  # add_workflows.rb.erb, including the two partial unique indexes
  # (AlreadyRunning, idempotent start) and the cascading foreign keys
  # (retention deletes steps and events with their run).
  unless conn.table_exists?("pgbus_workflows")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_workflows (
        id BIGSERIAL PRIMARY KEY,
        workflow_id VARCHAR NOT NULL,
        workflow_class VARCHAR NOT NULL,
        subject_type VARCHAR,
        subject_id VARCHAR,
        subject_gid VARCHAR,
        arguments JSONB NOT NULL DEFAULT '{}',
        status VARCHAR NOT NULL DEFAULT 'ready',
        current_step VARCHAR,
        step_job_options JSONB NOT NULL DEFAULT '{}',
        error_class VARCHAR,
        error_message TEXT,
        error_backtrace TEXT,
        paused_at TIMESTAMP,
        finished_at TIMESTAMP,
        cancelled_at TIMESTAMP,
        idempotency_key VARCHAR,
        definition_digest VARCHAR NOT NULL,
        rerun_of_workflow_id VARCHAR,
        created_at TIMESTAMP NOT NULL,
        updated_at TIMESTAMP NOT NULL
      );
      CREATE UNIQUE INDEX idx_pgbus_workflows_workflow_id ON pgbus_workflows (workflow_id);
      CREATE UNIQUE INDEX idx_pgbus_workflows_one_ongoing
        ON pgbus_workflows (workflow_class, subject_type, subject_id)
        WHERE status IN ('ready','performing','waiting','paused');
      CREATE UNIQUE INDEX idx_pgbus_workflows_idempotency
        ON pgbus_workflows (workflow_class, idempotency_key) WHERE idempotency_key IS NOT NULL;
      CREATE INDEX idx_pgbus_workflows_status ON pgbus_workflows (status, updated_at);
      CREATE INDEX idx_pgbus_workflows_class_status ON pgbus_workflows (workflow_class, status, created_at);
      CREATE INDEX idx_pgbus_workflows_subject ON pgbus_workflows (subject_type, subject_id);
    SQL
  end

  unless conn.table_exists?("pgbus_workflow_steps")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_workflow_steps (
        id BIGSERIAL PRIMARY KEY,
        workflow_id VARCHAR NOT NULL
          REFERENCES pgbus_workflows (workflow_id) ON DELETE CASCADE,
        workflow_class VARCHAR NOT NULL,
        step_name VARCHAR NOT NULL,
        attempt INTEGER NOT NULL DEFAULT 1,
        status VARCHAR NOT NULL DEFAULT 'scheduled',
        job_id VARCHAR,
        msg_id BIGINT,
        scheduled_at TIMESTAMP NOT NULL,
        enqueued_at TIMESTAMP,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        stall_at TIMESTAMP,
        outcome VARCHAR,
        output JSONB,
        error_class VARCHAR,
        error_message TEXT,
        error_backtrace TEXT,
        error_causes JSONB,
        worker VARCHAR,
        app_revision VARCHAR,
        created_at TIMESTAMP NOT NULL
      );
      CREATE UNIQUE INDEX idx_pgbus_workflow_steps_attempt
        ON pgbus_workflow_steps (workflow_id, step_name, attempt);
      CREATE INDEX idx_pgbus_workflow_steps_scheduled ON pgbus_workflow_steps (status, scheduled_at);
      CREATE INDEX idx_pgbus_workflow_steps_stall ON pgbus_workflow_steps (status, stall_at);
      CREATE INDEX idx_pgbus_workflow_steps_insights
        ON pgbus_workflow_steps (workflow_class, step_name, finished_at);
    SQL
  end

  unless conn.table_exists?("pgbus_workflow_events")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_workflow_events (
        id BIGSERIAL PRIMARY KEY,
        workflow_id VARCHAR NOT NULL
          REFERENCES pgbus_workflows (workflow_id) ON DELETE CASCADE,
        step_id BIGINT,
        step_name VARCHAR,
        attempt INTEGER,
        event VARCHAR NOT NULL,
        actor VARCHAR NOT NULL,
        reason TEXT,
        details JSONB NOT NULL DEFAULT '{}',
        created_at TIMESTAMP NOT NULL
      );
      CREATE INDEX idx_pgbus_workflow_events_workflow ON pgbus_workflow_events (workflow_id, id);
    SQL
  end

  # Waiting steps (issue #541) — mirrors add_workflow_signals.rb.erb. Applied
  # separately so a database bootstrapped for the core tables upgrades.
  unless conn.column_exists?("pgbus_workflow_steps", "timeout_at")
    conn.execute(<<~SQL)
      ALTER TABLE pgbus_workflow_steps ADD COLUMN IF NOT EXISTS batch_id VARCHAR;
      ALTER TABLE pgbus_workflow_steps ADD COLUMN IF NOT EXISTS wait_for VARCHAR;
      ALTER TABLE pgbus_workflow_steps ADD COLUMN IF NOT EXISTS signal_id BIGINT;
      ALTER TABLE pgbus_workflow_steps ADD COLUMN IF NOT EXISTS waiting_since TIMESTAMP;
      ALTER TABLE pgbus_workflow_steps ADD COLUMN IF NOT EXISTS timeout_at TIMESTAMP;
      CREATE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_batch ON pgbus_workflow_steps (batch_id);
      CREATE INDEX IF NOT EXISTS idx_pgbus_workflow_steps_timeout ON pgbus_workflow_steps (status, timeout_at);
    SQL
  end

  unless conn.table_exists?("pgbus_workflow_signals")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_workflow_signals (
        id BIGSERIAL PRIMARY KEY,
        workflow_id VARCHAR NOT NULL
          REFERENCES pgbus_workflows (workflow_id) ON DELETE CASCADE,
        name VARCHAR NOT NULL,
        payload JSONB NOT NULL DEFAULT '{}',
        idempotency_key VARCHAR,
        status VARCHAR NOT NULL DEFAULT 'pending',
        claimed_by_step_id BIGINT,
        created_at TIMESTAMP NOT NULL,
        claimed_at TIMESTAMP,
        consumed_at TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_workflow_signals_idempotency
        ON pgbus_workflow_signals (workflow_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
      CREATE INDEX idx_pgbus_workflow_signals_pending ON pgbus_workflow_signals (workflow_id, name, status);
    SQL
  end

  if conn.table_exists?("pgbus_batches")
    cols = conn.columns("pgbus_batches").map(&:name)
    if cols.include?("on_discard_class") && !cols.include?("on_failure_class")
      conn.execute("ALTER TABLE pgbus_batches RENAME COLUMN on_discard_class TO on_failure_class")
    end
    if cols.include?("discarded_jobs")
      conn.execute("UPDATE pgbus_batches SET failed_jobs = failed_jobs + discarded_jobs")
      conn.execute("ALTER TABLE pgbus_batches DROP COLUMN discarded_jobs")
    end
    # Configured callback instances (issue #415).
    %w[on_finish_job on_success_job on_failure_job].each do |column|
      conn.execute("ALTER TABLE pgbus_batches ADD COLUMN IF NOT EXISTS #{column} JSONB")
    end
  end

  unless conn.table_exists?("pgbus_batch_executions")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_batch_executions (
        id BIGSERIAL PRIMARY KEY,
        batch_id VARCHAR NOT NULL,
        job_id VARCHAR NOT NULL,
        msg_id BIGINT,
        queue_name VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_batch_executions_job_id ON pgbus_batch_executions (job_id);
      CREATE INDEX idx_pgbus_batch_executions_batch_id ON pgbus_batch_executions (batch_id);
    SQL
  end

  if conn.table_exists?("pgbus_batch_executions")
    conn.execute(<<~SQL)
      CREATE INDEX IF NOT EXISTS idx_pgbus_batch_executions_orphans
        ON pgbus_batch_executions (created_at) WHERE msg_id IS NULL;
    SQL
    conn.execute(<<~SQL)
      DO $$ BEGIN
        ALTER TABLE pgbus_batch_executions
          ADD CONSTRAINT fk_pgbus_batch_executions_batch
          FOREIGN KEY (batch_id) REFERENCES pgbus_batches (batch_id) ON DELETE CASCADE;
      EXCEPTION WHEN duplicate_object THEN NULL;
      END $$;
    SQL
  end
rescue StandardError => e
  warn "[pgbus integration] Bootstrap warning: #{e.message}"
end

RSpec.configure do |config|
  config.example_status_persistence_file_path = ".rspec_integration_status"
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.order = :random
  Kernel.srand config.seed

  unless PGBUS_DATABASE_URL
    config.before(:each, :integration) { skip "PGBUS_DATABASE_URL not set" }
    next
  end

  # Connect ActiveRecord with a pool large enough for concurrent tests.
  # Merge pool via URI parsing to avoid breaking URLs with existing query params.
  #
  # gssencmode=disable is the macOS fix: libpq 1.6.x on darwin/arm64 initializes
  # GSSAPI state lazily in a way that segfaults when a child process tries to
  # open a new PG connection after fork. Disabling GSSAPI entirely skips the
  # bad code path. Harmless on Linux (GSSAPI isn't used in pgbus anyway), so
  # we set it unconditionally to keep one canonical URL for both platforms.
  parsed_base = URI.parse(PGBUS_DATABASE_URL)
  base_params = URI.decode_www_form(parsed_base.query || "").to_h
  base_params["gssencmode"] = "disable"

  # Pgbus::Client#connection_options reads config.database_url and
  # passes it straight to PG.connect (lib/pgbus/client.rb:431).
  # libpq's conninfo_parse rejects "pool" (it's an AR-only param),
  # so the pgbus URL must NOT include pool. It MUST include
  # gssencmode=disable so forked children (which call
  # Pgbus.reset_client! and open fresh PG connections) don't
  # re-trigger the libpq GSSAPI post-fork segfault on macOS ARM64.
  parsed_base.query = URI.encode_www_form(base_params)
  pgbus_url = parsed_base.to_s

  # ActiveRecord wants the same URL plus pool=20. Merge on top of
  # the pgbus URL so they share the gssencmode fix.
  parsed_ar = URI.parse(pgbus_url)
  ar_params = URI.decode_www_form(parsed_ar.query || "").to_h
  ar_params["pool"] = "20"
  parsed_ar.query = URI.encode_www_form(ar_params)
  ActiveRecord::Base.establish_connection(parsed_ar.to_s)

  Pgbus.configure do |c|
    c.database_url = pgbus_url
    c.queue_prefix = "pgbus_int"
    c.default_queue = "default"
    c.logger = Logger.new(IO::NULL)
    c.pgmq_schema_mode = :embedded
    c.listen_notify = false
    c.streams_default_broadcast_mode = :durable
    # Keep the per-client connection footprint small: the integration suite
    # spins up many clients + streamers against a shared local Postgres, and
    # the dedicated streams pool (issue #315) adds connections on top of the
    # job pool. Cap both so the suite doesn't exhaust max_connections.
    c.pool_size = 3
    c.streams_pool_size = 2
  end

  # Bootstrap tables that may not exist in the CI database
  bootstrap_integration_tables(ActiveRecord::Base.connection)

  # Pgbus.reset_client! only drops the memoized client (a forked child must
  # not close the parent's sockets), so every reset in the suite left a job
  # pool and a streams pool open until GC. Hundreds of examples pushed
  # pg_stat_activity close to max_connections and starved specs that read
  # live headroom (streams_pool_autoscale_spec). In the rspec process itself
  # the old client is closed; forked children keep the drop-only behaviour.
  Pgbus.singleton_class.prepend(Module.new do
    root_pid = Process.pid
    define_method(:reset_client!) do
      previous = @client
      super()
      return unless previous && Process.pid == root_pid

      begin
        previous.close
      rescue StandardError => e
        warn "[integration_helper] closing the previous Pgbus client failed: #{e.class}: #{e.message}"
      end
    end
  end)

  # Purge all integration queues BEFORE each test so no stale messages leak.
  # Only around :integration examples (every integration spec is tagged), so a
  # run that loads every spec file, like the conformance gate's
  # `rspec --tag ractor`, does not purge around unrelated examples.
  config.before(:each, :integration) do
    Pgbus.reset_client!
    purge_integration_queues
    cleanup_tables
  end

  config.after(:suite) do
    Pgbus.reset!
  end
end

# Purge all pgbus_* queues via raw SQL to avoid prefix double-application.
# Covers both pgbus_int_* (integration) and pgbus_test_* (unit test leakage).
def purge_integration_queues
  conn = ActiveRecord::Base.connection
  queue_names = conn.select_values("SELECT queue_name FROM pgmq.meta WHERE queue_name LIKE 'pgbus_%'")
  queue_names.each do |full_name|
    conn.execute("DELETE FROM pgmq.q_#{full_name}")
  rescue StandardError
    nil
  end
rescue StandardError => e
  warn "[pgbus integration] Queue purge warning: #{e.message}"
end

def cleanup_tables
  conn = ActiveRecord::Base.connection
  tables = %w[
    pgbus_semaphores pgbus_blocked_executions pgbus_uniqueness_keys
    pgbus_processes pgbus_recurring_executions pgbus_failed_events
    pgbus_presence_members pgbus_outbox_entries pgbus_processed_events
    pgbus_batch_executions pgbus_batches pgbus_stream_queues
    pgbus_deploy_tasks pgbus_deploy_task_runs
    pgbus_workflow_signals pgbus_workflow_events pgbus_workflow_steps pgbus_workflows
  ]
  tables.each do |table|
    conn.execute("DELETE FROM #{table}")
  rescue StandardError
    nil
  end
rescue StandardError => e
  warn "[pgbus integration] Table cleanup warning: #{e.message}"
end

# Build a dedicated PG::Connection for tests that need to LISTEN on the
# same database the integration suite is using. Parses every credential
# from PGBUS_DATABASE_URL — including the password, which is required
# in CI where Postgres has authentication enabled. Previously each
# streams integration spec defined its own copy of this helper, and
# every copy forgot the password, so all PG.connect calls in CI failed
# with `fe_sendauth: no password supplied`.
def build_pg_listen_connection
  require "pg"
  uri = URI.parse(PGBUS_DATABASE_URL)
  PG.connect(
    host: uri.host || "localhost",
    port: (uri.port || 5432).to_i,
    dbname: uri.path.delete_prefix("/"),
    user: uri.user || ENV.fetch("USER"),
    password: uri.password
  )
end
