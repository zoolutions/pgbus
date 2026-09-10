# frozen_string_literal: true

module Pgbus
  # Manages embedded PGMQ SQL schema for extension-free installations.
  #
  # Supports three modes:
  #   :auto      - Try extension first, fall back to embedded SQL
  #   :extension - Require the pgmq PostgreSQL extension
  #   :embedded  - Use vendored SQL (no extension needed)
  #
  # Vendored SQL files live in lib/pgbus/pgmq_schema/pgmq_v{VERSION}.sql
  # and are exact copies of the upstream pgmq-extension/sql/pgmq.sql at each release.
  module PgmqSchema
    class VersionNotFoundError < Pgbus::Error; end

    SCHEMA_DIR = File.expand_path("pgmq_schema", __dir__).freeze
    FIXUPS_DIR = File.join(SCHEMA_DIR, "fixups").freeze

    class << self
      # Returns the latest vendored PGMQ version string.
      def latest_version
        available_versions.last
      end

      # Returns sorted list of all vendored PGMQ versions.
      def available_versions
        Dir.glob(File.join(SCHEMA_DIR, "pgmq_v*.sql"))
           .map { |f| File.basename(f).match(/pgmq_v(.+)\.sql/)[1] }
           .sort_by { |v| Gem::Version.new(v) }
      end

      # Versions that ship a table fixup, in version order.
      #
      # An upgrade drops every pgmq function and composite type and re-runs the
      # target version's schema, which recreates both — but never touches an
      # existing table. An upstream hop that ALTERs one (1.13.0 moves
      # partitioned queues' msg_id from GENERATED ALWAYS to BY DEFAULT) needs
      # its own step, and that is what these files are.
      def fixup_versions
        Dir.glob(File.join(FIXUPS_DIR, "pgmq_v*.sql"))
           .map { |f| File.basename(f).match(/pgmq_v(.+)\.sql/)[1] }
           .sort_by { |v| Gem::Version.new(v) }
      end

      # Concatenated fixups for every version in (after, upto], in version
      # order. Returns "" when the hop crosses none.
      #
      # @param after [String, nil] the installed version; nil (no recorded
      #   version) applies every fixup up to the target, which is safe because
      #   each one is idempotent.
      # @param upto [String] the version being upgraded to
      def fixups_sql(after:, upto: latest_version)
        ceiling = Gem::Version.new(upto)
        floor = after && Gem::Version.new(after)

        applicable = fixup_versions.select do |version|
          candidate = Gem::Version.new(version)
          candidate <= ceiling && (floor.nil? || candidate > floor)
        end

        applicable.map { |version| File.read(File.join(FIXUPS_DIR, "pgmq_v#{version}.sql")) }.join("\n")
      end

      # Returns the filesystem path to the vendored SQL file for a given version.
      #
      # @param version [String] e.g. "1.11.0"
      # @return [String] absolute path
      # @raise [VersionNotFoundError] if no SQL file exists for that version
      def sql_path(version)
        path = File.join(SCHEMA_DIR, "pgmq_v#{version}.sql")
        raise VersionNotFoundError, "No vendored PGMQ SQL for version #{version}" unless File.exist?(path)

        path
      end

      # Returns the raw SQL content for a given version.
      def sql_for_version(version)
        File.read(sql_path(version))
      end

      # Returns the SQL to install PGMQ schema without the extension.
      # Strips the extension-only pg_dump config blocks since they're
      # irrelevant when not installed as an extension.
      #
      # @param version [String] defaults to latest
      # @return [String] SQL
      def install_sql(version = latest_version)
        sql = sql_for_version(version)
        strip_extension_only_blocks(sql)
      end

      # Returns SQL to create the version tracking table and record an installation.
      #
      # @param version [String] defaults to latest
      # @return [String] SQL
      def version_tracking_sql(version = latest_version)
        <<~SQL
          CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
            id SERIAL PRIMARY KEY,
            version VARCHAR NOT NULL,
            installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            install_method VARCHAR NOT NULL DEFAULT 'embedded'
          );

          INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
          VALUES ('#{version}', 'embedded');
        SQL
      end

      # Returns SQL to record an extension-based installation in the version tracking table.
      #
      # @param version [String]
      # @return [String] SQL
      def version_tracking_extension_sql(version = latest_version)
        <<~SQL
          CREATE TABLE IF NOT EXISTS pgbus_pgmq_schema_versions (
            id SERIAL PRIMARY KEY,
            version VARCHAR NOT NULL,
            installed_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            install_method VARCHAR NOT NULL DEFAULT 'embedded'
          );

          INSERT INTO pgbus_pgmq_schema_versions (version, install_method)
          VALUES ('#{version}', 'extension');
        SQL
      end

      # SQL to drop all pgmq functions/types (for clean upgrade).
      # Uses CASCADE so dependent objects are also dropped.
      def drop_pgmq_functions_sql
        <<~SQL
          DO $$
          DECLARE
            r RECORD;
          BEGIN
            -- Drop all functions in pgmq schema
            FOR r IN
              SELECT pg_catalog.pg_get_functiondef(p.oid) AS funcdef,
                     n.nspname || '.' || p.proname || '(' ||
                       pg_catalog.pg_get_function_identity_arguments(p.oid) || ')' AS func_sig
              FROM pg_catalog.pg_proc p
              JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = 'pgmq'
            LOOP
              EXECUTE 'DROP FUNCTION IF EXISTS ' || r.func_sig || ' CASCADE';
            END LOOP;

            -- Drop custom composite types in pgmq schema (e.g. message_record,
            -- queue_record, metrics_result). Standalone `CREATE TYPE ... AS (...)`
            -- types get a pg_class shell with relkind = 'c'; only skip row-types
            -- backed by an actual relation (table/view/etc.), which must be
            -- dropped via DROP TABLE rather than DROP TYPE.
            FOR r IN
              SELECT n.nspname || '.' || t.typname AS type_name
              FROM pg_catalog.pg_type t
              JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE n.nspname = 'pgmq'
                AND t.typtype = 'c'
                AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_class c
                  WHERE c.reltype = t.oid
                    AND c.relkind <> 'c'
                )
            LOOP
              EXECUTE 'DROP TYPE IF EXISTS ' || r.type_name || ' CASCADE';
            END LOOP;
          END $$;
        SQL
      end

      # SQL to re-install the per-queue NOTIFY insert triggers that
      # drop_pgmq_functions_sql cascades away (issue #360). Dropping
      # pgmq.notify_queue_listeners() with CASCADE also drops the
      # trigger_notify_queue_insert_listeners trigger from every queue table;
      # install_sql re-creates the function but nothing re-creates the
      # triggers, so NOTIFY-gated wakeups silently die fleet-wide until each
      # queue happens to be re-ensured by a process restart.
      #
      # The dropped state is fully recoverable: pgmq.notify_insert_throttle is
      # a TABLE (preserved by the upgrade — its rows record exactly which
      # queues had notify enabled and at what throttle, FK-bound to pgmq.meta
      # so it can't reference a dropped queue), and pgmq.enable_notify_insert
      # is idempotent. Replaying it per recorded row restores every trigger at
      # its original interval. No-ops when the throttle table or the enable
      # function is absent (a vendored version without the notify feature).
      def reinstall_notify_triggers_sql
        <<~SQL
          DO $$
          DECLARE
            r RECORD;
          BEGIN
            IF to_regclass('pgmq.notify_insert_throttle') IS NULL THEN
              RETURN;
            END IF;
            IF to_regprocedure('pgmq.enable_notify_insert(text, integer)') IS NULL THEN
              RETURN;
            END IF;

            -- The FOR loop iterates a snapshot, so enable_notify_insert's
            -- internal DELETE + re-INSERT of the same throttle row is safe.
            FOR r IN
              SELECT queue_name, throttle_interval_ms
              FROM pgmq.notify_insert_throttle
            LOOP
              PERFORM pgmq.enable_notify_insert(r.queue_name, r.throttle_interval_ms);
            END LOOP;
          END $$;
        SQL
      end

      private

      # Strips extension-specific blocks (pg_extension_config_dump, pg_depend checks)
      # that only work when pgmq is installed as an extension.
      def strip_extension_only_blocks(sql)
        # Remove the DO block that conditionally creates schema only when extension is missing.
        # Replace with unconditional schema creation.
        sql = sql.sub(
          /DO\s*\$\$\s*BEGIN\s*IF\s*\(SELECT\s+NOT\s+EXISTS.*?END\s*\$\$;/m,
          "CREATE SCHEMA IF NOT EXISTS pgmq;"
        )

        # Remove pg_extension_config_dump blocks
        sql = sql.gsub(
          /DO\s*\$\$\s*BEGIN\s*IF\s+EXISTS\(SELECT\s+1\s+FROM\s+pg_extension.*?END\s*\$\$;/m,
          ""
        )

        # Remove _belongs_to_pgmq function (checks pg_depend on extension)
        sql.gsub(
          /CREATE FUNCTION pgmq\._belongs_to_pgmq.*?LANGUAGE plpgsql;/m,
          ""
        )
      end
    end
  end
end
