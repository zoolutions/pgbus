-- PGMQ 1.13.1 table fixup.
--
-- Adapted from the upstream migration
-- pgmq-extension/sql/pgmq--1.13.0--1.13.1.sql at tag v1.13.1
-- (blob 16cbbe2763ec6508570a80e7ef08b7bee3b11ffd). Only the parts pgbus's
-- upgrade cannot already reproduce are here: dropping every function and
-- composite type and re-running the target schema recreates functions and
-- types, but never touches an existing table, its column defaults or its
-- grants.
--
-- Left out on purpose: upstream's loop that re-creates each queue's NOTIFY
-- trigger with the queue name as its argument. The upgrade's
-- reinstall_notify_triggers_sql step replays the new pgmq.enable_notify_insert
-- for every row of pgmq.notify_insert_throttle, which re-creates the trigger
-- the same way.
--
-- One deliberate deviation from upstream: the rebind-defaults block selects
-- pgmq's tables by name in the pgmq schema instead of by extension membership
-- (pg_depend deptype 'e'). An embedded install has no extension, so upstream's
-- selection would find nothing and the block would never run. The pg_monitor
-- blocks are upstream's, unchanged.
--
-- Every file in this directory MUST be idempotent and self-guarded. The
-- upgrade migration applies each fixup between the recorded installed version
-- and the target, and an install whose version was never recorded gets all of
-- them, so a fixup can be replayed against a database that already has it.

-- Queue tables created by the old functions can retain defaults bound to a
-- function in a writable schema. Replacing the functions does not replace
-- those stored expression dependencies. Rebind only PGMQ's own defaults that
-- refer to a non-catalog now() or to_timestamp(). Include partition children,
-- since their defaults can be stored separately from the parent.
DO $pgmq_rebind_defaults$
DECLARE
    def RECORD;
BEGIN
    FOR def IN
        WITH RECURSIVE pgmq_tables AS (
            -- pgbus: pgmq schema tables by name, not extension members (see header).
            SELECT c.oid, c.relname
              FROM pg_catalog.pg_class c
             WHERE c.relnamespace = 'pgmq'::pg_catalog.regnamespace
               AND c.relkind IN ('r', 'p')
        ), roots AS (
            SELECT c.oid AS relid,
                   CASE c.relname
                       WHEN 'meta' THEN 'meta'
                       WHEN 'topic_bindings' THEN 'topic_bindings'
                       ELSE 'notify_insert_throttle'
                   END AS kind
              FROM pgmq_tables c
             WHERE c.relname IN ('meta', 'topic_bindings', 'notify_insert_throttle')
            UNION
            SELECT c.oid, names.kind
              FROM pgmq_tables core
              JOIN pgmq.meta m ON core.relname = 'meta'
              CROSS JOIN LATERAL (
                  VALUES (pgmq.format_table_name(m.queue_name::pg_catalog.text, 'q'::pg_catalog.text), 'queue'),
                         (pgmq.format_table_name(m.queue_name::pg_catalog.text, 'a'::pg_catalog.text), 'archive')
              ) AS names(relname, kind)
              JOIN pg_catalog.pg_class c
                ON c.relnamespace = 'pgmq'::pg_catalog.regnamespace
               AND c.relname = names.relname
               AND c.relkind IN ('r', 'p')
        ), managed AS (
            SELECT relid, kind FROM roots
            UNION
            SELECT i.inhrelid, m.kind
              FROM managed m
              JOIN pg_catalog.pg_inherits i ON i.inhparent = m.relid
        )
        SELECT n.nspname, c.relname, a.attname,
               pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS old_expression,
               CASE WHEN m.kind = 'notify_insert_throttle'
                    THEN 'pg_catalog.to_timestamp(0)'
                    ELSE 'pg_catalog.now()'
               END AS replacement,
               EXISTS (
                   SELECT 1
                     FROM pg_catalog.pg_depend d
                     JOIN pg_catalog.pg_proc p ON p.oid = d.refobjid
                     JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
                    WHERE d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                      AND d.objid = ad.oid
                      AND d.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                      AND pn.nspname <> 'pg_catalog'
                      AND ((m.kind = 'notify_insert_throttle'
                            AND p.proname = 'to_timestamp'
                            AND p.proargtypes[0] = 'pg_catalog.int4'::pg_catalog.regtype
                            AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) =
                                pg_catalog.format('%I.to_timestamp(0)', pn.nspname))
                        OR (m.kind <> 'notify_insert_throttle'
                            AND p.proname = 'now'
                            AND p.pronargs = 0
                            AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) =
                                pg_catalog.format('%I.now()', pn.nspname)))
               ) AS known_shape
          FROM managed m
          JOIN pg_catalog.pg_class c ON c.oid = m.relid
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
          JOIN pg_catalog.pg_attrdef ad
            ON ad.adrelid = c.oid AND ad.adnum = a.attnum
         WHERE ((m.kind = 'meta' AND a.attname = 'created_at')
             OR (m.kind = 'topic_bindings' AND a.attname = 'bound_at')
             OR (m.kind = 'notify_insert_throttle' AND a.attname = 'last_notified_at')
             OR (m.kind = 'queue' AND a.attname = 'enqueued_at')
             OR (m.kind = 'archive' AND a.attname IN ('enqueued_at', 'archived_at')))
           AND EXISTS (
               SELECT 1
                 FROM pg_catalog.pg_depend d
                 JOIN pg_catalog.pg_proc p ON p.oid = d.refobjid
                 JOIN pg_catalog.pg_namespace pn ON pn.oid = p.pronamespace
                WHERE d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                  AND d.objid = ad.oid
                  AND d.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                  AND pn.nspname <> 'pg_catalog'
           )
    LOOP
        IF NOT def.known_shape THEN
            RAISE EXCEPTION 'PGMQ upgrade cannot safely rebind custom default on %.%.%: %; inspect the non-catalog function dependency before retrying',
                def.nspname, def.relname, def.attname, def.old_expression;
        END IF;
        BEGIN
            EXECUTE pg_catalog.format(
                'ALTER TABLE %I.%I ALTER COLUMN %I SET DEFAULT %s',
                def.nspname, def.relname, def.attname, def.replacement
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE EXCEPTION 'PGMQ upgrade cannot rebind default on %.%.%; use a role that can update the extension and alter the table, or a superuser',
                def.nspname, def.relname, def.attname;
        END;
    END LOOP;
END
$pgmq_rebind_defaults$;

-- pg_monitor could read every table and sequence in the pgmq schema, including
-- queue and archive tables. Replace that broad access with the minimum needed
-- by pgmq.metrics(), pgmq.metrics_all(), and pgmq.list_queues().

-- 1. Remove default privileges that granted SELECT on every new table and
-- sequence. The grantor can differ from the role running this upgrade.
DO $$
DECLARE
    acl RECORD;
BEGIN
    FOR acl IN
        SELECT DISTINCT d.defaclrole::pg_catalog.regrole AS role_name
          FROM pg_catalog.pg_default_acl d
          JOIN pg_catalog.pg_namespace n ON n.oid = d.defaclnamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(d.defaclacl) a
         WHERE n.nspname = 'pgmq'
           AND d.defaclobjtype IN ('r', 'S')
           AND a.grantee = 'pg_monitor'::pg_catalog.regrole
    LOOP
        BEGIN
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor',
                acl.role_name
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %s IN SCHEMA pgmq REVOKE SELECT ON SEQUENCES FROM pg_monitor',
                acl.role_name
            );
        EXCEPTION WHEN insufficient_privilege OR undefined_object THEN
            RAISE WARNING 'pgmq: could not remove default privileges of role % granting SELECT to pg_monitor in schema pgmq; run "ALTER DEFAULT PRIVILEGES FOR ROLE % IN SCHEMA pgmq REVOKE SELECT ON TABLES FROM pg_monitor" (and the same ON SEQUENCES) as that role or a superuser', acl.role_name, acl.role_name;
        END;
    END LOOP;
END;
$$;

-- 2. Revoke whole-relation SELECT from pg_monitor on every relation in pgmq
-- except meta. Step 4 reports grants that the upgrade role cannot remove.
DO $$
DECLARE
    rel RECORD;
BEGIN
    FOR rel IN
        SELECT DISTINCT c.oid::pg_catalog.regclass AS rel_name,
               c.relkind = 'S' AS is_sequence
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
         WHERE n.nspname = 'pgmq'
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
           AND c.relname <> 'meta'
           AND a.grantee = 'pg_monitor'::pg_catalog.regrole
           AND a.privilege_type = 'SELECT'
    LOOP
        BEGIN
            EXECUTE pg_catalog.format(
                'REVOKE SELECT ON %s %s FROM pg_monitor',
                CASE WHEN rel.is_sequence THEN 'SEQUENCE' ELSE 'TABLE' END,
                rel.rel_name
            );
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'pgmq: could not revoke SELECT on % from pg_monitor; run "REVOKE SELECT ON % FROM pg_monitor" as its owner or a superuser', rel.rel_name, rel.rel_name;
        END;
    END LOOP;
END;
$$;

-- 3. Grant the minimal metrics access on each existing queue. A grant on a
-- partitioned parent is sufficient because metrics query the parent.
DO $$
DECLARE
    q RECORD;
    qtable TEXT;
    qseq TEXT;
BEGIN
    FOR q IN SELECT queue_name FROM pgmq.meta LOOP
        qtable := pgmq.format_table_name(
            q.queue_name::pg_catalog.text,
            'q'::pg_catalog.text
        );
        qseq := qtable || '_msg_id_seq';
        IF pg_catalog.to_regclass(pg_catalog.format('pgmq.%I', qtable)) IS NOT NULL THEN
            BEGIN
                EXECUTE pg_catalog.format(
                    'GRANT SELECT (vt, enqueued_at) ON pgmq.%I TO pg_monitor',
                    qtable
                );
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT (vt, enqueued_at) on pgmq.% to pg_monitor; run it as the table owner or a superuser so queue metrics work for pg_monitor', pg_catalog.quote_ident(qtable);
            END;
        END IF;
        IF pg_catalog.to_regclass(pg_catalog.format('pgmq.%I', qseq)) IS NOT NULL THEN
            BEGIN
                EXECUTE pg_catalog.format(
                    'GRANT SELECT ON SEQUENCE pgmq.%I TO pg_monitor',
                    qseq
                );
            EXCEPTION WHEN insufficient_privilege THEN
                RAISE WARNING 'pgmq: could not grant SELECT on sequence pgmq.% to pg_monitor; run it as the sequence owner or a superuser so queue metrics work for pg_monitor', pg_catalog.quote_ident(qseq);
            END;
        END IF;
    END LOOP;
END;
$$;

-- 4. Report any heap-like relation that still exposes whole-table SELECT to
-- pg_monitor. Column grants live in pg_attribute and are not reported here.
DO $$
DECLARE
    leftover TEXT;
BEGIN
    SELECT pg_catalog.string_agg(
               c.oid::pg_catalog.regclass::pg_catalog.text,
               ', ' ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text
           )
      INTO leftover
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
     WHERE n.nspname = 'pgmq'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
       AND c.relname <> 'meta'
       AND a.grantee = 'pg_monitor'::pg_catalog.regrole
       AND a.privilege_type = 'SELECT';

    IF leftover IS NOT NULL THEN
        RAISE WARNING 'pgmq: pg_monitor still has whole-table SELECT on: %. These may expose message payloads; revoke them as the object owner or a superuser.', leftover;
    END IF;
END;
$$;
