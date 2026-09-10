# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pgbus::PgmqSchema do
  describe ".latest_version" do
    it "returns the latest vendored PGMQ version" do
      expect(described_class.latest_version).to eq("1.13.0")
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

    it "lists 1.11.0, 1.11.1, 1.12.0 and 1.13.0" do
      expect(described_class.available_versions).to include("1.11.0", "1.11.1", "1.12.0", "1.13.0")
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
      expect(sql).not_to include("CREATE EXTENSION")
    end

    context "when targeting v1.11.1" do
      subject(:sql) { described_class.install_sql("1.11.1") }

      it "is non-empty" do
        expect(sql).not_to be_empty
      end

      it "creates the pgmq schema without an extension dependency" do
        expect(sql).to include("CREATE SCHEMA IF NOT EXISTS pgmq")
        expect(sql).not_to include("CREATE EXTENSION")
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
        expect(sql).not_to include("CREATE EXTENSION")
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
        expect(sql).not_to include("CREATE EXTENSION")
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
  end

  # Table-level SQL that drop-and-reapply cannot carry. Dropping every
  # function and composite type and re-running the target version's schema
  # recreates functions and types, but never touches an existing table — so
  # an upstream hop that ALTERs one needs its own step.
  describe ".fixup_versions" do
    it "lists the versions that ship a fixup, in version order" do
      expect(described_class.fixup_versions).to eq(["1.13.0"])
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

    it "returns nothing for a hop between versions that carry no fixup" do
      expect(described_class.fixups_sql(after: "1.11.1", upto: "1.12.0")).to eq("")
    end

    # An install with no tracking row has an unknown history, so every fixup
    # up to the target applies. They are idempotent, which is what makes that
    # safe.
    it "applies every fixup up to the target when the installed version is unknown" do
      expect(described_class.fixups_sql(after: nil, upto: "1.13.0")).to include("SET GENERATED BY DEFAULT")
    end

    it "excludes a fixup newer than the target" do
      expect(described_class.fixups_sql(after: "1.11.1", upto: "1.12.0")).not_to include("SET GENERATED BY DEFAULT")
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
      expect(described_class.version_tracking_sql).to include("1.13.0")
    end
  end
end
