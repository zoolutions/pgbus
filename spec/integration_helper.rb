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
  # lib/generators/pgbus/templates/migration.rb.erb. Handler#claim_idempotency?
  # does INSERT ... ON CONFLICT (event_id, handler_class) DO NOTHING, so the
  # unique index is what makes the second delivery return :skipped.
  unless conn.table_exists?("pgbus_processed_events")
    conn.execute(<<~SQL)
      CREATE TABLE pgbus_processed_events (
        id BIGSERIAL PRIMARY KEY,
        event_id VARCHAR NOT NULL,
        handler_class VARCHAR NOT NULL,
        processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      CREATE UNIQUE INDEX idx_pgbus_processed_events_unique
        ON pgbus_processed_events (event_id, handler_class);
    SQL
  end

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

  if conn.table_exists?("pgbus_batches")
    cols = conn.columns("pgbus_batches").map(&:name)
    if cols.include?("on_discard_class") && !cols.include?("on_failure_class")
      conn.execute("ALTER TABLE pgbus_batches RENAME COLUMN on_discard_class TO on_failure_class")
    end
    if cols.include?("discarded_jobs")
      conn.execute("UPDATE pgbus_batches SET failed_jobs = failed_jobs + discarded_jobs")
      conn.execute("ALTER TABLE pgbus_batches DROP COLUMN discarded_jobs")
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

  # Purge all integration queues BEFORE each test so no stale messages leak
  config.before do
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
