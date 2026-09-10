# frozen_string_literal: true

require_relative "../integration_helper"

# Proves the multi-version PGMQ schema upgrade path end to end against a real
# database. The integration suite already has PGMQ installed; we mark it as
# v1.11.1 in the tracking table, seed a queue with messages, then run the exact
# SQL the generated `upgrade_pgmq` migration emits to move to v1.12.0
# (drop functions -> re-create at target version -> record the upgrade).
#
# After the upgrade the tracking table must report v1.12.0, existing queues and
# messages must survive, the new read_grouped_head_with_poll function must be
# available, and the core pgmq functions must remain callable.
#
# Guarded by :integration — skipped cleanly when PGBUS_DATABASE_URL is unset.
RSpec.describe "PGMQ schema upgrade path (integration)", :integration do
  let(:conn) { ActiveRecord::Base.connection }
  let(:from_version) { "1.12.0" }
  let(:to_version)   { "1.13.0" }
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

  it "records v1.12.0 before the upgrade" do
    latest = conn.select_value(
      "SELECT version FROM pgbus_pgmq_schema_versions ORDER BY installed_at DESC LIMIT 1"
    )
    expect(latest).to eq(from_version)
  end

  it "advances the tracking table to v1.13.0 after the upgrade" do
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

  describe "the partitioned-queue msg_id fixup" do
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
end
