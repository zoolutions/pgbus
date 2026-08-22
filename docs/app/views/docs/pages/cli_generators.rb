# frozen_string_literal: true

# Reference for the pgbus CLI and the Rails generators — what each command and
# generator does, and when you need it.
class Views::Docs::Pages::CliGenerators < DocsUI::Page
  title "CLI & generators"
  eyebrow "Reference"

  def lead = "The pgbus command-line interface and every Rails generator, at a glance."

  def content
    cli
    cli_flags
    doctor
    dlq
    core_generators
    feature_generators
    tuning_generators
  end

  private

  def cli
    DocsUI::Section("The CLI") do
      DocsUI::Table(
        [ "Command", "Does" ],
        [
          [ [ :code, "pgbus start" ], "Boot the supervisor (workers, dispatcher, scheduler, consumers)." ],
          [ [ :code, "pgbus status" ], "Show running processes." ],
          [ [ :code, "pgbus queues" ], "List queues with depth and metrics." ],
          [ [ :code, "pgbus dlq" ], "Inspect and drain dead-letter queues (see below)." ],
          [ [ :code, "pgbus doctor" ], "Preflight diagnostics — gates a deploy or CI run (see below)." ],
          [ [ :code, "pgbus mcp" ], "Start the read-only MCP diagnostic server over stdio." ],
          [ [ :code, "pgbus version" ], "Print the version." ],
          [ [ :code, "pgbus help" ], "Show help." ]
        ]
      )
    end
  end

  def doctor
    DocsUI::Section("pgbus doctor", description: "One preflight command for deploy/CI gating.") do
      md <<~'MD'
        `pgbus doctor` (and the equivalent `rake pgbus:doctor`) runs six checks and
        prints a report — config validity, database connectivity, PGMQ schema
        version, configured-queue existence, LISTEN/NOTIFY triggers, and process
        liveness (the same `OK` / `DEGRADED` / `STALLED` verdict as the MCP
        `pgbus_health` tool). It never touches PGMQ or PostgreSQL directly outside
        `Pgbus::Client`, and it never raises — a broken environment turns every
        probe into a failed check instead of a crash.
      MD
      DocsUI::Table(
        [ "Check", "Fails when" ],
        [
          [ "Configuration", [ :md, "`Configuration#validate!` raises." ] ],
          [ "Database", [ :md, "The DB is unreachable (a plain `SELECT 1` via `Client#ping`)." ] ],
          [ "PGMQ schema", "The schema is missing, untracked, or behind the vendored version (warn)." ],
          [ "Queues", "A configured queue has no PGMQ table." ],
          [ "LISTEN/NOTIFY", "A configured queue is missing its insert trigger (warn — falls back to polling)." ],
          [ "Process liveness", [ :md, "`STALLED` (silent worker wedge); `DEGRADED` only warns." ] ]
        ]
      )
      md <<~'MD'
        The report ends with a resolved-config summary (queue prefix, pool size,
        roles, capsules) with any password redacted from `database_url` /
        `connection_params`.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        pgbus doctor        # prints the report; exit 1 if any check failed
        rake pgbus:doctor   # same checks, for a Rake-based deploy pipeline
      SHELL
      DocsUI::Callout(:tip) do
        plain "Warnings (an outdated PGMQ schema, a missing NOTIFY trigger) don't fail "
        plain "the exit code — only a failed check does. Wire "
        code { "pgbus doctor" }
        plain " into your deploy pipeline or CI to gate on a broken environment before "
        plain "it reaches production."
      end
    end
  end

  def dlq
    DocsUI::Section("pgbus dlq", description: "Dead-letter management from a headless deployment.") do
      md <<~'MD'
        Every subcommand routes through `Web::DataSource`, so retry/discard
        semantics are identical to the dashboard — origin-queue re-enqueue,
        transactional produce+delete, lock release on discard — with zero raw
        SQL and no direct PGMQ calls.
      MD
      DocsUI::Table(
        [ "Subcommand", "Does" ],
        [
          [ [ :code, "pgbus dlq list [--page N] [--per-page N]" ], "Table of msg_id / DLQ queue / origin queue / read_ct / enqueued_at, plus a total count. Never prints payloads." ],
          [ [ :code, "pgbus dlq show MSG_ID" ], "Message metadata plus the payload, passed through the dashboard's sensitive-data filter." ],
          [ [ :code, "pgbus dlq retry MSG_ID" ], "Re-enqueue one message to its origin queue." ],
          [ [ :code, "pgbus dlq retry-all" ], "Re-enqueue every dead-letter message; prints the count." ],
          [ [ :code, "pgbus dlq purge MSG_ID" ], "Discard a single message." ],
          [ [ :code, "pgbus dlq purge --all --yes" ], "Discard every dead-letter message. Omitting --yes makes no changes and exits 1." ]
        ]
      )
      DocsUI::Callout(:warning) do
        plain "An unknown "
        code { "msg_id" }
        plain " or an unknown subcommand exits 1. "
        code { "purge --all" }
        plain " without "
        code { "--yes" }
        plain " is a deliberate safety rail — it refuses to purge and exits 1 rather than prompting."
      end
    end
  end

  def cli_flags
    DocsUI::Section("start flags", description: "Split roles and capsules across processes.") do
      DocsUI::Table(
        [ "Flag", "Does" ],
        [
          [ [ :code, "--workers-only" ], "Run only worker processes." ],
          [ [ :code, "--scheduler-only" ], "Run only the recurring-task scheduler." ],
          [ [ :code, "--dispatcher-only" ], "Run only the maintenance dispatcher." ],
          [ [ :code, "--capsule NAME" ], "Boot a single named capsule." ],
          [ [ :code, "--execution-mode async" ], "Run jobs as fibers instead of threads." ],
          [ [ :code, "--doctor" ], "Run the doctor preflight in the booting supervisor and log the report — one Rails boot instead of `pgbus doctor` + `pgbus start`." ],
          [ [ :code, "--doctor-strict" ], "Like --doctor, but refuse to boot (exit non-zero, fork nothing) on a fatal check — bad config or an absent PGMQ schema." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "Prefer "
        code { "--doctor" }
        plain " over an entrypoint that runs "
        code { "pgbus doctor || true" }
        plain " before "
        code { "pgbus start" }
        plain ": it boots Rails once, and the process-liveness check is skipped (no "
        plain "workers are forked yet, so it can't false-fail). "
        code { "--doctor-strict" }
        plain " gates the boot on genuinely-fatal findings only; transient DB blips "
        plain "that the queue bootstrap is built to ride out never abort the boot."
      end
      DocsUI::Callout(:note) do
        plain "The role flags are mutually exclusive, and the auto-tuned "
        code { "pool_size" }
        plain " follows the role — a scheduler-only process opens only the connections it needs. See "
        a(href: "/docs/running-workers", class: "link") { "Running workers" }
        plain "."
      end
    end
  end

  def core_generators
    DocsUI::Section("Core generators") do
      md <<~'MD'
        Every generator accepts `--database=pgbus` to route its migration to
        `db/pgbus_migrate/` for a [separate database](/docs/separate-database).
      MD
      DocsUI::Table(
        [ "Generator", "Does" ],
        [
          [ [ :code, "pgbus:install" ], "Full setup — the metadata migrations, the batches table, and a starter initializer." ],
          [ [ :code, "pgbus:update" ], "Convert a legacy YAML config and add any missing migrations, detecting a separate DB automatically." ],
          [ [ :code, "pgbus:upgrade_pgmq" ], "Upgrade the embedded PGMQ schema to the latest vendored version." ]
        ]
      )
    end
  end

  def feature_generators
    DocsUI::Section("Feature generators", description: "Add the table for an optional feature.") do
      DocsUI::Table(
        [ "Generator", "Adds" ],
        [
          [ [ :code, "pgbus:add_recurring" ], [ :md, "Recurring-task tables + a starter `recurring.yml`." ] ],
          [ [ :code, "pgbus:add_outbox" ], "The transactional-outbox table." ],
          [ [ :code, "pgbus:add_presence" ], "The stream-presence table." ],
          [ [ :code, "pgbus:add_queue_states" ], "The queue pause/resume + circuit-breaker state table." ],
          [ [ :code, "pgbus:add_uniqueness_keys" ], "The job-uniqueness lock table." ],
          [ [ :code, "pgbus:add_batch_executions" ], "Batch execution-row tracking (self-healing completion). Included in fresh pgbus:install." ],
          [ [ :code, "pgbus:add_job_stats" ], "The job-stats table for the Insights dashboard." ],
          [ [ :code, "pgbus:add_stream_stats" ], "The stream-stats table (opt-in metrics)." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "A few migration-maintenance generators also ship for existing installs: "
        code { "add_failed_events_index" }
        plain ", "
        code { "add_job_stats_latency" }
        plain ", "
        code { "add_job_stats_queue_index" }
        plain ", "
        code { "add_batch_executions" }
        plain ", and "
        code { "migrate_job_locks" }
        plain ". The "
        code { "pgbus:update" }
        plain " generator adds whichever of these your database is missing."
      end
    end
  end

  def tuning_generators
    DocsUI::Section("Tuning generators", description: "Re-apply table settings a schema load drops.") do
      DocsUI::Table(
        [ "Generator", "Does" ],
        [
          [ [ :code, "pgbus:tune_autovacuum" ], "Apply aggressive autovacuum settings to the high-churn queue tables." ],
          [ [ :code, "pgbus:tune_fillfactor" ], "Tune table fillfactor for existing installations." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "Run these after "
        code { "db:schema:load" }
        plain ", which drops "
        code { "ALTER TABLE" }
        plain " settings. See "
        a(href: "/docs/performance-tuning", class: "link") { "Performance & tuning" }
        plain "."
      end
    end
  end
end
