# frozen_string_literal: true

require_relative "../integration_helper"

# Proves the multi-version PGMQ schema upgrade path end to end against a real
# database. The integration suite already has PGMQ installed; we install
# from_version over it, record it in the tracking table, seed a queue with
# messages, then run the exact SQL the generated `upgrade_pgmq` migration emits
# to move to to_version (drop functions -> re-create at target version ->
# fixups -> NOTIFY repair -> record the upgrade).
#
# After the upgrade the tracking table must report the target, existing queues
# and messages must survive, each fixup on the way must have applied, and the
# core pgmq functions must remain callable.
#
# Guarded by :integration — skipped cleanly when PGBUS_DATABASE_URL is unset.
RSpec.describe "PGMQ schema upgrade path (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:from_version) { "1.13.0" }
  let(:to_version)   { "1.13.1" }
  let(:queue_name)   { "pgbus_int_upgrade_probe" }
  # A stand-in for a partitioned queue. pg_partman is not installed in CI, so
  # the fixup is exercised against a plain table registered as partitioned in
  # pgmq.meta — which is exactly what the fixup's loop selects on.
  let(:partitioned_probe) { "pgbus_int_fixup_probe" }

  # Mirrors lib/generators/pgbus/templates/upgrade_pgmq.rb.erb: drop functions,
  # re-create at the target version, re-install the NOTIFY triggers the drop
  # cascaded away, record the upgrade in the tracking table.
  def run_upgrade_migration_sql(version)
    installed = installed_version
    conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
    conn.execute(Pgbus::PgmqSchema.install_sql(version))
    fixups = Pgbus::PgmqSchema.fixups_sql(after: installed, upto: version)
    conn.execute(fixups) unless fixups.empty?
    conn.execute(Pgbus::PgmqSchema.reinstall_notify_triggers_sql)
    conn.execute(<<~SQL)
      CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
        id SERIAL PRIMARY KEY,
        version VARCHAR NOT NULL,
        installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        install_method VARCHAR NOT NULL DEFAULT 'embedded'
      );

      INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
      VALUES ('#{version}', 'upgrade');
    SQL
  end

  def function_exists?(name)
    conn.select_value(<<~SQL).to_i.positive?
      SELECT count(*) FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = 'pgmq' AND p.proname = '#{name}'
    SQL
  end

  def notify_trigger_exists?(queue)
    conn.select_value(<<~SQL).to_i.positive?
      SELECT count(*) FROM pg_trigger t
      JOIN pg_class c ON t.tgrelid = c.oid
      JOIN pg_namespace n ON c.relnamespace = n.oid
      WHERE n.nspname = 'pgmq'
        AND c.relname = 'q_#{queue}'
        AND t.tgname = 'trigger_notify_queue_insert_listeners'
    SQL
  end

  def installed_version
    return nil unless conn.table_exists?("pgbus_pgmq_schema_versions")

    conn.select_value("SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1")
  end

  def msg_id_identity(table)
    conn.select_value(<<~SQL)
      SELECT identity_generation FROM information_schema.columns
      WHERE table_schema = 'pgmq' AND table_name = '#{table}' AND column_name = 'msg_id'
    SQL
  end

  # A partitioned-queue stand-in whose msg_id still carries the pre-1.13.0
  # GENERATED ALWAYS identity, registered in pgmq.meta so the fixup's loop
  # finds it.
  def seed_partitioned_probe
    conn.execute("DROP TABLE IF EXISTS pgmq.q_#{partitioned_probe}")
    conn.execute(<<~SQL)
      CREATE TABLE pgmq.q_#{partitioned_probe} (
        msg_id BIGINT GENERATED ALWAYS AS IDENTITY,
        read_ct INT DEFAULT 0 NOT NULL,
        enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        last_read_at TIMESTAMP WITH TIME ZONE,
        vt TIMESTAMP WITH TIME ZONE NOT NULL,
        message JSONB,
        headers JSONB
      )
    SQL
    conn.execute(<<~SQL)
      INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
      VALUES ('#{partitioned_probe}', true, false)
      ON CONFLICT DO NOTHING
    SQL
  end

  # Install a clean function/type set for a version on top of whatever pgmq
  # objects already exist. Dropping the functions and the composite types first
  # keeps install_sql (which uses bare CREATE TYPE / CREATE FUNCTION) idempotent.
  def install_pgmq(version)
    conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
    %w[message_record queue_record metrics_result].each do |type|
      conn.execute("DROP TYPE IF EXISTS pgmq.#{type} CASCADE")
    end
    conn.execute(Pgbus::PgmqSchema.install_sql(version))
  end

  before do
    # Establish a clean v1.11.1 baseline (functions + tracking record) and seed
    # a queue with messages that must survive the upgrade.
    install_pgmq(from_version)

    conn.execute("DROP TABLE IF EXISTS pgbus_pgmq_schema_versions")
    conn.execute(Pgbus::PgmqSchema.version_tracking_sql(from_version))

    conn.execute("SELECT pgmq.drop_queue('#{queue_name}')") rescue nil # rubocop:disable Style/RescueModifier
    conn.execute("SELECT pgmq.create('#{queue_name}')")
    conn.execute("SELECT pgmq.enable_notify_insert('#{queue_name}', throttle_interval_ms => 350)")
    conn.execute("SELECT pgmq.send('#{queue_name}', '{\"probe\": 1}'::jsonb)")
    conn.execute("SELECT pgmq.send('#{queue_name}', '{\"probe\": 2}'::jsonb)")
    seed_partitioned_probe
  end

  after do
    # Only reachable when the DB is configured (the suite skips :integration
    # examples without PGBUS_DATABASE_URL before this hook can touch a connection).
    next unless PGBUS_DATABASE_URL

    # Clean up the probe queue and tracking table, then leave PGMQ installed at
    # the latest version so subsequent integration specs run against an
    # up-to-date schema.
    conn.execute("SELECT pgmq.drop_queue('#{queue_name}')") rescue nil # rubocop:disable Style/RescueModifier
    conn.execute("DROP TABLE IF EXISTS pgmq.q_#{partitioned_probe}")
    conn.execute("DELETE FROM pgmq.meta WHERE queue_name = '#{partitioned_probe}'")
    conn.execute("DROP TABLE IF EXISTS pgbus_pgmq_schema_versions")
    install_pgmq(Pgbus::PgmqSchema.latest_version)
  end

  it "records the starting version before the upgrade" do
    latest = conn.select_value(
      "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(latest).to eq(from_version)
  end

  it "advances the tracking table to the target version after the upgrade" do
    run_upgrade_migration_sql(to_version)

    latest = conn.select_value(
      "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(latest).to eq(to_version)
  end

  it "records the upgrade install method" do
    run_upgrade_migration_sql(to_version)

    method = conn.select_value(
      "SELECT install_method FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(method).to eq("upgrade")
  end

  it "preserves the existing queue and its messages across the upgrade" do
    run_upgrade_migration_sql(to_version)

    queue_present = conn.select_value(
      "SELECT count(*) FROM pgmq.meta WHERE queue_name = '#{queue_name}'"
    ).to_i
    expect(queue_present).to eq(1)

    message_count = conn.select_value("SELECT count(*) FROM pgmq.q_#{queue_name}").to_i
    expect(message_count).to eq(2)
  end

  it "replaces create_partitioned with the premake-carrying signature" do
    run_upgrade_migration_sql(to_version)

    expect(conn.select_value("SELECT to_regprocedure('pgmq.create_partitioned(text,text,text,integer)')"))
      .not_to be_nil
    expect(conn.select_value("SELECT to_regprocedure('pgmq.create_partitioned(text,text,text)')"))
      .to be_nil
  end

  it "reports the new default_partition_length column from metrics" do
    run_upgrade_migration_sql(to_version)

    row = conn.select_one("SELECT * FROM pgmq.metrics('#{queue_name}')")

    expect(row).to have_key("default_partition_length")
    # A non-partitioned queue has no default partition, so the column is NULL.
    expect(row["default_partition_length"]).to be_nil
  end

  # 1.13.0's fixup, reached from 1.12.0: the hop crosses two fixups, which
  # also proves they run in version order without tripping over each other.
  describe "the partitioned-queue msg_id fixup" do
    let(:from_version) { "1.12.0" }

    it "is not carried by dropping and re-creating the functions alone" do
      expect(msg_id_identity("q_#{partitioned_probe}")).to eq("ALWAYS")

      conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
      conn.execute(Pgbus::PgmqSchema.install_sql(to_version))

      expect(msg_id_identity("q_#{partitioned_probe}")).to eq("ALWAYS")
    end

    it "moves an existing partitioned queue to GENERATED BY DEFAULT" do
      run_upgrade_migration_sql(to_version)

      expect(msg_id_identity("q_#{partitioned_probe}")).to eq("BY DEFAULT")
    end

    # The loop selects on pgmq.meta.is_partitioned, so an ordinary queue —
    # which pgmq.create() still builds with GENERATED ALWAYS — is untouched.
    it "leaves a non-partitioned queue's msg_id alone" do
      run_upgrade_migration_sql(to_version)

      expect(msg_id_identity("q_#{queue_name}")).to eq("ALWAYS")
    end

    it "is safe to replay" do
      run_upgrade_migration_sql(to_version)

      expect { run_upgrade_migration_sql(to_version) }.not_to raise_error
      expect(msg_id_identity("q_#{partitioned_probe}")).to eq("BY DEFAULT")
    end

    # Replaying the migration does NOT re-run the fixup: the recorded version
    # has advanced to the target, so fixups_sql returns "". The idempotence the
    # fixups directory promises is what makes an install with no recorded
    # version safe, and only re-applying the SQL itself exercises it.
    it "is idempotent when the same fixup SQL is applied twice" do
      run_upgrade_migration_sql(to_version)
      fixups = Pgbus::PgmqSchema.fixups_sql(after: nil, upto: to_version)

      expect(fixups).to include("SET GENERATED BY DEFAULT")
      expect { conn.execute(fixups) }.not_to raise_error
      expect(msg_id_identity("q_#{partitioned_probe}")).to eq("BY DEFAULT")
    end
  end

  it "keeps the core pgmq functions callable after the upgrade" do
    run_upgrade_migration_sql(to_version)

    read = conn.select_value(
      "SELECT count(*) FROM pgmq.read('#{queue_name}', 0, 10)"
    ).to_i
    expect(read).to eq(2)
  end

  describe "NOTIFY insert triggers across the upgrade (issue #360)" do
    it "documents the bug: the function drop CASCADE removes the per-queue trigger" do
      expect(notify_trigger_exists?(queue_name)).to be(true)

      conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
      conn.execute(Pgbus::PgmqSchema.install_sql(to_version))

      # Without the repair step, the trigger is gone — NOTIFY wakeups die and
      # workers silently fall back to polling.
      expect(notify_trigger_exists?(queue_name)).to be(false)
    end

    it "re-installs the trigger at its recorded throttle interval" do
      run_upgrade_migration_sql(to_version)

      expect(notify_trigger_exists?(queue_name)).to be(true)
      throttle = conn.select_value(
        "SELECT throttle_interval_ms FROM pgmq.notify_insert_throttle WHERE queue_name = '#{queue_name}'"
      ).to_i
      expect(throttle).to eq(350)
    end

    it "keeps the restored trigger functional (an insert NOTIFYs listeners)" do
      run_upgrade_migration_sql(to_version)

      # A send through the restored trigger must not raise (the trigger calls
      # pgmq.notify_queue_listeners, re-created by install_sql).
      expect do
        conn.execute("SELECT pgmq.send('#{queue_name}', '{\"probe\": 3}'::jsonb)")
      end.not_to raise_error
    end
  end

  # 1.13.1 stops granting pg_monitor SELECT on every pgmq table. The install
  # SQL only changes what new queues get; the fixup takes the broad grants and
  # default privileges off existing ones and leaves the metrics columns.
  describe "pg_monitor access (1.13.1 fixup)" do
    def monitor_can?(privilege_sql)
      conn.select_value("SELECT #{privilege_sql}")
    end

    it "starts from 1.13.0's whole-table grant on the queue" do
      expect(monitor_can?("has_table_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'SELECT')")).to be(true)
    end

    it "can no longer read an existing queue's or archive's payloads" do
      run_upgrade_migration_sql(to_version)

      expect(monitor_can?("has_table_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'SELECT')")).to be(false)
      expect(monitor_can?("has_table_privilege('pg_monitor', 'pgmq.a_#{queue_name}', 'SELECT')")).to be(false)
      expect(monitor_can?("has_column_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'message', 'SELECT')"))
        .to be(false)
      expect(monitor_can?("has_column_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'headers', 'SELECT')"))
        .to be(false)
    end

    it "keeps what pgmq.metrics needs on an existing queue" do
      run_upgrade_migration_sql(to_version)

      expect(monitor_can?("has_column_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'vt', 'SELECT')")).to be(true)
      expect(monitor_can?("has_column_privilege('pg_monitor', 'pgmq.q_#{queue_name}', 'enqueued_at', 'SELECT')"))
        .to be(true)
      expect(monitor_can?("has_sequence_privilege('pg_monitor', 'pgmq.q_#{queue_name}_msg_id_seq', 'SELECT')"))
        .to be(true)
      expect(monitor_can?("has_table_privilege('pg_monitor', 'pgmq.meta', 'SELECT')")).to be(true)
    end

    it "removes the default privileges that granted SELECT on every new pgmq table" do
      run_upgrade_migration_sql(to_version)

      leftover = conn.select_value(<<~SQL).to_i
        SELECT count(*)
        FROM pg_default_acl d
        JOIN pg_namespace n ON n.oid = d.defaclnamespace
        CROSS JOIN LATERAL aclexplode(d.defaclacl) a
        WHERE n.nspname = 'pgmq' AND a.grantee = 'pg_monitor'::regrole
      SQL
      expect(leftover).to eq(0)
    end
  end

  # A queue table whose default resolved to a now() outside pg_catalog keeps
  # it across a function re-create. Upstream's rebind selects extension member
  # tables only; pgbus's fixup selects pgmq's tables by name, so it reaches an
  # embedded install's queues.
  describe "column defaults bound outside pg_catalog (1.13.1 fixup)" do
    let(:shadow_schema) { "pgbus_int_shadow" }

    def enqueued_at_default(table)
      conn.select_value(<<~SQL)
        SELECT pg_get_expr(d.adbin, d.adrelid)
        FROM pg_attrdef d
        JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
        WHERE d.adrelid = 'pgmq.#{table}'::regclass AND a.attname = 'enqueued_at'
      SQL
    end

    before do
      conn.execute("DROP SCHEMA IF EXISTS #{shadow_schema} CASCADE")
      conn.execute("CREATE SCHEMA #{shadow_schema}")
      conn.execute(<<~SQL)
        CREATE FUNCTION #{shadow_schema}.now() RETURNS timestamptz
        LANGUAGE sql AS 'SELECT pg_catalog.now()'
      SQL
      conn.execute("ALTER TABLE pgmq.q_#{queue_name} ALTER COLUMN enqueued_at SET DEFAULT #{shadow_schema}.now()")
    end

    after do
      next unless PGBUS_DATABASE_URL

      conn.execute("DROP SCHEMA IF EXISTS #{shadow_schema} CASCADE")
    end

    it "rebinds the queue's enqueued_at default to pg_catalog.now()" do
      expect(enqueued_at_default("q_#{queue_name}")).to eq("#{shadow_schema}.now()")

      run_upgrade_migration_sql(to_version)

      expect(enqueued_at_default("q_#{queue_name}")).to eq("now()")
    end
  end

  # 1.13.1's trigger function takes the queue name as an argument instead of
  # rebuilding it from the lowercased table name, so a mixed-case queue's
  # throttle row matches and its NOTIFY fires. The upgrade's NOTIFY repair
  # replays the new enable_notify_insert, which attaches the argument.
  describe "NOTIFY for a mixed-case queue (1.13.1)" do
    let(:mixed_queue) { "PgbusIntMixedProbe" }

    def notified?
      conn.select_value(<<~SQL)
        SELECT last_notified_at > to_timestamp(0)
        FROM pgmq.notify_insert_throttle WHERE queue_name = '#{mixed_queue}'
      SQL
    end

    before do
      conn.execute("SELECT pgmq.drop_queue('#{mixed_queue}')") rescue nil # rubocop:disable Style/RescueModifier
      conn.execute("SELECT pgmq.create('#{mixed_queue}')")
      conn.execute("SELECT pgmq.enable_notify_insert('#{mixed_queue}', 0)")
    end

    after do
      next unless PGBUS_DATABASE_URL

      conn.execute("SELECT pgmq.drop_queue('#{mixed_queue}')") rescue nil # rubocop:disable Style/RescueModifier
    end

    it "re-creates the trigger with the queue name as its argument" do
      run_upgrade_migration_sql(to_version)

      args = conn.select_value(<<~SQL)
        SELECT encode(t.tgargs, 'escape') FROM pg_trigger t
        WHERE t.tgrelid = 'pgmq.q_#{mixed_queue.downcase}'::regclass
          AND t.tgname = 'trigger_notify_queue_insert_listeners'
      SQL
      expect(args).to eq("#{mixed_queue}\\000")
    end

    # The throttle UPDATE is what gates PG_NOTIFY; under 1.13.0 it matched no
    # row for a mixed-case queue, so last_notified_at never moved.
    it "matches the queue's throttle row on insert" do
      run_upgrade_migration_sql(to_version)
      conn.execute("SELECT pgmq.send('#{mixed_queue}', '{\"probe\": 1}'::jsonb)")

      expect(notified?).to be(true)
    end
  end

  # Upstream pins search_path = pg_catalog, pg_temp for the install with a
  # transaction-local setting; install_sql restores the caller's path, so a
  # migration's next unqualified CREATE TABLE still lands in its own schema.
  describe "installing inside a transaction" do
    it "leaves the caller's search_path in place for the rest of the transaction" do
      conn.transaction do
        before_path = conn.select_value("SHOW search_path")
        conn.execute(Pgbus::PgmqSchema.drop_pgmq_functions_sql)
        conn.execute(Pgbus::PgmqSchema.install_sql(to_version))

        expect(conn.select_value("SHOW search_path")).to eq(before_path)
        expect { conn.execute("CREATE TABLE pgbus_int_search_path_probe (id int)") }.not_to raise_error
        expect(conn.select_value("SELECT to_regclass('public.pgbus_int_search_path_probe')")).not_to be_nil

        raise ActiveRecord::Rollback
      end
    end
  end
end
