# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::PgmqSchema do
  describe ".latest_version" do
    it "returns the latest vendored PGMQ version" do
      expect(described_class.latest_version).to eq("1.13.1")
    end
  end

  describe ".sql_path" do
    it "returns the path to the vendored SQL file for a version" do
      path = described_class.sql_path("1.11.0")
      expect(path).to end_with("pgmq_schema/pgmq_v1.11.0.sql")
      expect(File.exist?(path)).to be true
    end

    it "returns the path to the vendored SQL file for v1.11.1" do
      path = described_class.sql_path("1.11.1")
      expect(path).to end_with("pgmq_schema/pgmq_v1.11.1.sql")
      expect(File.exist?(path)).to be true
    end

    it "returns the path to the vendored SQL file for v1.12.0" do
      path = described_class.sql_path("1.12.0")
      expect(path).to end_with("pgmq_schema/pgmq_v1.12.0.sql")
      expect(File.exist?(path)).to be true
    end

    it "returns the path to the vendored SQL file for v1.13.0" do
      path = described_class.sql_path("1.13.0")
      expect(path).to end_with("pgmq_schema/pgmq_v1.13.0.sql")
      expect(File.exist?(path)).to be true
    end

    it "raises for unknown versions" do
      expect { described_class.sql_path("0.0.0") }
        .to raise_error(Pgbus::PgmqSchema::VersionNotFoundError, /0\.0\.0/)
    end
  end

  describe ".available_versions" do
    it "returns sorted list of vendored versions" do
      versions = described_class.available_versions
      expect(versions).to include("1.11.0")
      expect(versions).to eq(versions.sort_by { |v| Gem::Version.new(v) })
    end

    it "lists 1.11.0, 1.11.1, 1.12.0, 1.13.0 and 1.13.1" do
      expect(described_class.available_versions).to include("1.11.0", "1.11.1", "1.12.0", "1.13.0", "1.13.1")
    end
  end

  describe ".sql_for_version" do
    it "returns the SQL content for a version" do
      sql = described_class.sql_for_version("1.11.0")
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).to include("CREATE FUNCTION pgmq.create(")
    end

    it "returns the SQL content for v1.11.1" do
      sql = described_class.sql_for_version("1.11.1")
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).to include("CREATE FUNCTION pgmq.create(")
    end

    it "returns the SQL content for v1.12.0" do
      sql = described_class.sql_for_version("1.12.0")
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).to include("CREATE FUNCTION pgmq.create(")
    end

    it "returns the SQL content for v1.13.0" do
      sql = described_class.sql_for_version("1.13.0")
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).to include("CREATE FUNCTION pgmq.create(")
    end
  end

  describe ".install_sql" do
    it "returns SQL that creates pgmq schema without extension dependency" do
      sql = described_class.install_sql
      expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
      expect(sql).not_to match(/^\s*CREATE EXTENSION/i)
    end

    context "when targeting v1.11.1" do
      subject(:sql) { described_class.install_sql("1.11.1") }

      it "is non-empty" do
        expect(sql).not_to be_empty
      end

      it "creates the pgmq schema without an extension dependency" do
        expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
        expect(sql).not_to match(/^\s*CREATE EXTENSION/i)
      end

      it "defines the new read_grouped_head function" do
        expect(sql).to include("CREATE FUNCTION pgmq.read_grouped_head(")
      end

      it "strips the extension-only _belongs_to_pgmq helper" do
        expect(sql).not_to include("_belongs_to_pgmq")
      end
    end

    context "when targeting v1.12.0" do
      subject(:sql) { described_class.install_sql("1.12.0") }

      it "is non-empty" do
        expect(sql).not_to be_empty
      end

      it "creates the pgmq schema without an extension dependency" do
        expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
        expect(sql).not_to match(/^\s*CREATE EXTENSION/i)
      end

      it "defines the new read_grouped_head_with_poll function" do
        expect(sql).to include("CREATE FUNCTION pgmq.read_grouped_head_with_poll(")
      end

      it "strips the extension-only _belongs_to_pgmq helper" do
        expect(sql).not_to include("_belongs_to_pgmq")
      end
    end

    context "when targeting v1.13.0" do
      subject(:sql) { described_class.install_sql("1.13.0") }

      it "is non-empty" do
        expect(sql).not_to be_empty
      end

      it "creates the pgmq schema without an extension dependency" do
        expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
        expect(sql).not_to match(/^\s*CREATE EXTENSION/i)
      end

      # 1.13.0's whole delta is partitioned-queue work plus one metrics
      # attribute: create_partitioned gains `premake` (and loses its
      # three-argument form), new partitioned queues use GENERATED BY DEFAULT,
      # and metrics_result reports default_partition_length.
      it "defines create_partitioned with the new premake parameter" do
        expect(sql).to include("premake INTEGER DEFAULT 4")
        expect(sql).to include("msg_id BIGINT GENERATED BY DEFAULT AS IDENTITY")
      end

      it "reports default_partition_length in metrics_result" do
        expect(sql).to include("default_partition_length bigint")
      end

      it "strips the extension-only _belongs_to_pgmq helper" do
        expect(sql).not_to include("_belongs_to_pgmq")
      end
    end

    context "when targeting v1.13.1" do
      subject(:sql) { described_class.install_sql("1.13.1") }

      it "creates the pgmq schema without an extension dependency" do
        expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
        expect(sql).not_to match(/^\s*CREATE EXTENSION/i)
        expect(sql).not_to include("pg_extension_config_dump")
      end

      # 1.13.1 is a security release: every function pins its search_path,
      # pg_monitor loses SELECT on queue and archive tables, and the NOTIFY
      # trigger is handed the queue name instead of rebuilding it from the
      # lowercased table name.
      it "pins search_path on pgmq's functions" do
        expect(sql).to include("LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp;")
      end

      it "no longer grants pg_monitor SELECT on every pgmq table" do
        expect(sql).not_to include("GRANT SELECT ON ALL TABLES IN SCHEMA pgmq TO pg_monitor")
        expect(sql).to include("GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor")
      end

      it "passes the queue name to the NOTIFY trigger" do
        expect(sql).to include("EXECUTE PROCEDURE pgmq.notify_queue_listeners(%L)")
      end

      it "strips the extension-only _belongs_to_pgmq helper and nothing after it" do
        expect(sql).not_to include("_belongs_to_pgmq")
        expect(sql).to include("CREATE FUNCTION pgmq.create_non_partitioned(")
        expect(sql).to include("CREATE FUNCTION pgmq.create_unlogged(")
      end

      # Upstream pins search_path = pg_catalog, pg_temp with a transaction-local
      # set_config, which outlives the install inside a migration or a caller's
      # transaction: the next unqualified CREATE TABLE would target pg_catalog.
      # install_sql saves the caller's path before upstream's text and restores
      # it after.
      it "saves the caller's search_path before upstream's text and restores it after" do
        save = sql.index("pg_catalog.set_config('pgbus.install_saved_search_path'")
        pin = sql.index("PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true)")
        restore = sql.rindex("pg_catalog.current_setting('pgbus.install_saved_search_path')")

        expect(save).to be < pin
        expect(restore).to be > sql.index("CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(")
      end
    end

    # The extension-only strip is regex-based, so a change in how upstream
    # terminates a function can make it eat its neighbours (1.13.1 did). Every
    # vendored version must keep every function but _belongs_to_pgmq.
    described_class.available_versions.each do |version|
      it "keeps every upstream function but _belongs_to_pgmq in v#{version}" do
        function_names = lambda do |sql|
          sql.scan(/CREATE (?:OR REPLACE )?FUNCTION (pgmq\.["\w]+)\s*\(/).flatten.tally
        end

        upstream = function_names.call(described_class.sql_for_version(version))
        upstream.delete("pgmq._belongs_to_pgmq")

        expect(function_names.call(described_class.install_sql(version))).to eq(upstream)
      end
    end
  end

  # Table-level SQL that drop-and-reapply cannot carry. Dropping every
  # function and composite type and re-running the target version's schema
  # recreates functions and types, but never touches an existing table — so
  # an upstream hop that ALTERs one needs its own step.
  describe ".fixup_versions" do
    it "lists the versions that ship a fixup, in version order" do
      expect(described_class.fixup_versions).to eq(["1.13.0", "1.13.1"])
    end
  end

  describe ".fixups_sql" do
    it "returns the fixup for a hop that crosses a version carrying one" do
      sql = described_class.fixups_sql(after: "1.12.0", upto: "1.13.0")

      expect(sql).to include("is_partitioned")
      expect(sql).to include("SET GENERATED BY DEFAULT")
    end

    it "returns nothing when the installed version is already the target" do
      expect(described_class.fixups_sql(after: "1.13.0", upto: "1.13.0")).to eq("")
    end

    # 1.13.1's fixup narrows pg_monitor's access on existing queues and rebinds
    # defaults that resolved to a function outside pg_catalog; dropping and
    # re-creating the functions does neither.
    it "returns the 1.13.1 fixup alone for the 1.13.0 -> 1.13.1 hop" do
      sql = described_class.fixups_sql(after: "1.13.0", upto: "1.13.1")

      expect(sql).to include("REVOKE SELECT ON %s %s FROM pg_monitor")
      expect(sql).to include("GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor")
      expect(sql).to include("$pgmq_rebind_defaults$")
      expect(sql).not_to include("SET GENERATED BY DEFAULT")
    end

    it "returns both fixups, in version order, for the 1.12.0 -> 1.13.1 hop" do
      sql = described_class.fixups_sql(after: "1.12.0", upto: "1.13.1")

      expect(sql.index("SET GENERATED BY DEFAULT")).to be < sql.index("FROM pg_monitor")
    end

    it "returns nothing for a hop between versions that carry no fixup" do
      expect(described_class.fixups_sql(after: "1.11.1", upto: "1.12.0")).to eq("")
    end

    # An install with no tracking row has an unknown history, so every fixup
    # up to the target applies. They are idempotent, which is what makes that
    # safe.
    it "applies every fixup up to the target when the installed version is unknown" do
      expect(described_class.fixups_sql(after: nil, upto: "1.13.0")).to include("SET GENERATED BY DEFAULT")
    end

    # The nil floor applies every fixup it can find, so the ceiling is the only
    # thing keeping a newer fixup out of an older target's upgrade.
    it "excludes a fixup newer than the target even when the installed version is unknown" do
      expect(described_class.fixups_sql(after: nil, upto: "1.12.0")).to eq("")
    end

    it "defaults the target to the latest vendored version" do
      expect(described_class.fixups_sql(after: "1.12.0")).to include("SET GENERATED BY DEFAULT")
    end
  end

  describe ".drop_pgmq_functions_sql" do
    subject(:sql) { described_class.drop_pgmq_functions_sql }

    it "drops all functions in the pgmq schema" do
      expect(sql).to include("DROP FUNCTION IF EXISTS")
    end

    it "drops standalone composite types (message_record etc.) but not table row-types" do
      # A CREATE TYPE ... AS (...) gets a pg_class shell with relkind = 'c'.
      # The drop loop must only skip row-types backed by a real relation, so it
      # excludes those on relkind <> 'c' and still drops the composite type shells.
      expect(sql).to include("DROP TYPE IF EXISTS")
      expect(sql).to include("c.relkind <> 'c'")
    end
  end

  describe ".reinstall_notify_triggers_sql" do
    subject(:sql) { described_class.reinstall_notify_triggers_sql }

    it "replays pgmq.enable_notify_insert for every recorded throttle row (issue #360)" do
      # drop_pgmq_functions_sql CASCADE-drops the per-queue NOTIFY triggers
      # (they depend on the dropped trigger function); the throttle TABLE
      # survives and records which queues had notify enabled and at what
      # interval, so replaying enable_notify_insert restores every trigger.
      expect(sql).to include("pgmq.notify_insert_throttle")
      expect(sql).to include("pgmq.enable_notify_insert(r.queue_name, r.throttle_interval_ms)")
    end

    it "no-ops when the throttle table is absent (a vendored version without the notify feature)" do
      expect(sql).to include("to_regclass('pgmq.notify_insert_throttle')")
    end

    it "no-ops when the enable function is absent" do
      expect(sql).to include("to_regprocedure('pgmq.enable_notify_insert(text, integer)')")
    end
  end

  describe ".version_tracking_sql" do
    it "returns SQL to create the version tracking table" do
      sql = described_class.version_tracking_sql
      expect(sql).to include("pgbus_pgmq_schema_versions")
      expect(sql).to include("CREATE TABLE")
    end

    it "includes an insert for the installed version" do
      sql = described_class.version_tracking_sql("1.11.0")
      expect(sql).to include("1.11.0")
    end

    it "defaults to the latest version" do
      expect(described_class.version_tracking_sql).to include("1.13.1")
    end
  end
end
