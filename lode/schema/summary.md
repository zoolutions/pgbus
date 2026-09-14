# Schema — tables, generators and PGMQ install

pgbus owns two kinds of schema: the **PGMQ** schema (`pgmq.q_*`, `pgmq.a_*`,
`pgmq.meta`, its functions and triggers) and its own **`pgbus_*`** tables. The
first is installed by the gem at runtime; the second by Rails migrations the
generators write.

## The 16 `pgbus_*` tables

`lib/generators/pgbus/templates/` holds 22 templates (`migration.rb.erb` for a
fresh install plus one `add_*.rb.erb` per later addition). Between them they
create:

| Table | Model | Used by |
|---|---|---|
| `pgbus_batches` | `Pgbus::BatchEntry` | [batch](../batch/summary.md) |
| `pgbus_batch_executions` | `Pgbus::BatchExecution` | batch |
| `pgbus_blocked_executions` | `Pgbus::BlockedExecution` | [concurrency](../concurrency/summary.md) |
| `pgbus_semaphores` | `Pgbus::Semaphore` | concurrency |
| `pgbus_uniqueness_keys` | `Pgbus::UniquenessKey` | [uniqueness](../uniqueness/summary.md), unique batches |
| `pgbus_processed_events` | `Pgbus::ProcessedEvent` | [event bus](../event-bus/summary.md) idempotency |
| `pgbus_processes` | `Pgbus::ProcessEntry` | heartbeats, the Processes page |
| `pgbus_recurring_tasks` | `Pgbus::RecurringTask` | the scheduler |
| `pgbus_recurring_executions` | `Pgbus::RecurringExecution` | scheduler dedupe |
| `pgbus_outbox_entries` | `Pgbus::OutboxEntry` | the transactional outbox |
| `pgbus_queue_states` | `Pgbus::QueueState` | pause/resume |
| `pgbus_job_stats` | `Pgbus::JobStat` | throughput, insights |
| `pgbus_stream_queues` | `Pgbus::StreamQueue` | the durable-stream registry |
| `pgbus_stream_stats` | `Pgbus::StreamStat` | stream numbers |
| `pgbus_failed_events` | — (raw SQL: `FailedEventRecorder`) | failed jobs, zombie detection |
| `pgbus_presence_members` | — (raw SQL: `Streams::Presence`) | stream presence |

Models live in `app/models/pgbus/` and inherit from `Pgbus::ApplicationRecord`,
a backward-compatible alias of `Pgbus::BusRecord` (which is in `lib/pgbus/` so
the gem's Zeitwerk loader owns it and engine boot order cannot bite). **No model
carries a `Record` suffix** — CLAUDE.md's Model Naming table gives the
resolution for each collision (`BatchEntry` because `Batch` is the API class,
`ProcessEntry` because `Process` is Ruby's module, and so on).

## Generators

Twenty generator classes under `lib/generators/pgbus/` (plus `migration_path.rb`, a shared module, not a generator). `pgbus:install` writes the
initializer, the binstub and the full migration; every `pgbus:add_*` adds one
later table or column set. `Generators::MigrationDetector` reads the live schema
and reports which of them are missing, so `pgbus:update` and the doctor can tell
an operator exactly what to run.

`Generators::MigrationPath` decides where a migration lands. A separate pgbus
database is selected either by `--database=pgbus` **or** by the app already
having `config.connects_to = { database: { writing: :pgbus } }` — the second
case matters because a bare `rails g pgbus:add_*` in such an app used to write
silently to `db/migrate` and run against the wrong database (issue #344).

## PGMQ install

`Pgbus::PgmqSchema` vendors upstream's `sql/pgmq.sql` verbatim, one file per
release, in `lib/pgbus/pgmq_schema/` — currently `pgmq_v1.11.0`, `v1.11.1`,
`v1.12.0`; `latest_version` is the highest by `Gem::Version`.
`config.pgmq_schema_mode` picks `:auto` (try the extension, fall back to
embedded SQL — the default), `:extension` or `:embedded`.
`rails generate pgbus:upgrade_pgmq` writes the upgrade migration;
`rake pgbus:pgmq:status` and `rake pgbus:pgmq:versions` report installed versus
available (both are engine rake tasks, so they exist inside a host app, not in
the gem's own `rake -T`).

The install itself is `Client#ensure_pgmq_schema`, and it is the most
carefully-ordered method in the gem — class mutex, then connection mutex, then
`pg_advisory_xact_lock(PGMQ_INSTALL_LOCK_KEY)`, with a `SAVEPOINT
pgbus_pgmq_install` instead of an owned `BEGIN`/`COMMIT` when the caller already
has a transaction open. See [../client/summary.md](../client/summary.md).

## Table tuning

Queue and archive tables get PGMQ-tuned autovacuum and storage parameters
through `Client#tune_autovacuum`, which delegates to pgmq-ruby 0.7+ (vacuum
scale factor 0.01/0.05, cost delay 2/5, analyze scale factor 0.05, fillfactor 70
on the queue table, vacuum_threshold floor 50). Tuning is best-effort: a failure
logs at debug and never blocks a queue from being usable.
`Pgbus::AutovacuumTuning` remains the source for the migration generators, which
tune the `pgbus_*` metadata tables pgmq-ruby knows nothing about, and
`Pgbus::TableMaintenance` is the dispatcher's periodic `pg_stat_user_tables`
bloat check.

See also: [../configuration/summary.md](../configuration/summary.md),
[../review/batch.md](../review/batch.md).
