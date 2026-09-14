# Processes — supervisor, forks and the maintenance loop

`pgbus start` runs `Process::Supervisor` (983 lines), which forks one child per
role and never does queue work itself. Eighteen files under
`lib/pgbus/process/`; the big three are `supervisor.rb`, `worker.rb` (883) and
`dispatcher.rb` (713).

## Roles

`Configuration::VALID_ROLES` is `%i[workers dispatcher scheduler consumers
outbox]`, and `Supervisor::ROLE_FLAGS` is the same list. `config.role_enabled?`
answers true unless `config.roles` narrows it; the CLI's `--workers-only`,
`--scheduler-only` and `--dispatcher-only` set that. `#boot_processes` forks in
role order, and `--capsule NAME` runs exactly one worker capsule — the
one-capsule-per-container pattern.

| Role | Fork | What it does |
|---|---|---|
| `workers` | one per `config.workers` capsule (`fork_worker`) | reads job queues, runs `ActiveJob::Executor` |
| `consumers` | one per registered subscriber group (`fork_consumer`) | reads event-bus queues, runs `EventBus::Handler` subclasses |
| `dispatcher` | one (`fork_dispatcher`) | sweeps, reaps, compacts, vacuums |
| `scheduler` | one (`fork_scheduler`) | recurring tasks (`Recurring::Scheduler`) |
| `outbox` | one (`fork_outbox_poller`) | drains `pgbus_outbox_entries` |

## Restart policy

A child that crashes within `RESTART_STABLE_UPTIME` (30s) of forking is
crash-looping: it restarts with exponential backoff from
`RESTART_BACKOFF_BASE` (1s), doubling, capped at `RESTART_BACKOFF_MAX` (60s). A
child that ran longer, **or that exited cleanly** — which is what a recycling
worker does — restarts immediately and resets its crash streak.

Recycling itself is `Worker#recycle_needed?`: `config.max_jobs_per_worker`,
`config.max_memory_mb`, `config.max_worker_lifetime`. Each is nil-able, each
logs the reason it fired. This is the answer to the unbounded-memory problem the
CLAUDE.md Key Design Decisions section names.

## Two pipes per fork, in opposite directions

- **liveness pipe** — the child writes a byte per loop; the supervisor's
  watchdog (`WATCHDOG_INTERVAL` = 10s, against `config.stall_threshold`,
  default 90s) reads it. Any readable byte is liveness, so a dropped
  non-blocking write is harmless.
- **wake pipe** — the supervisor's `NotifyHub` writes a one-byte protocol the
  child's `WakePipe` watcher translates: `W` a NOTIFY arrived for a queue this
  fork reads, `H` the shared listener is healthy (the fork may sleep to the
  NOTIFY poll ceiling), `P` it is degraded (fall back to fast polling). EOF
  means the supervisor is gone. Status starts optimistic so a just-forked
  worker is not pinned to fast polling before the hub's first broadcast.

`config.worker_notify_scope` (default `:supervisor`) chooses between that shared
hub and one LISTEN connection per fork (`:fork`). The hub exists because N forks
each holding a LISTEN backend is N idle connections (issue #381).

## Shutdown

`Supervisor#shutdown` waits for `@forks` to empty, bounded by
`config.shutdown_timeout` (default `drain_timeout + SHUTDOWN_TIMEOUT_MARGIN`, so
raising `drain_timeout` — 30s by default — can never mean SIGKILLing workers
mid-drain; an orchestrator's stop grace period should exceed it, issue #386).
Remaining children get `KILL`, liveness readers are closed so the supervisor
leaks no FDs across a restart of itself, then the notify hub, health server and
heartbeat stop.

Threads that own a `PG::Connection` are joined on a budget sized from what they
wait on, never a flat number: `NotifyListener#stop_join_timeout` and
`Web::Streamer::Listener#stop_join_timeout` are both `health_check_ms / 1000 +
STOP_JOIN_GRACE_SECONDS` (5). The stopper never closes the connection — see
[../review/process-and-streams.md](../review/process-and-streams.md).

## The dispatcher

One process, one loop, thirteen maintenance tasks each on its own interval via
`run_if_due`, which only advances the timestamp when the task **succeeds** (a
failure retries on the next tick rather than waiting out the interval).

| Task | Interval |
|---|---|
| `cleanup_processed_events` | `CLEANUP_INTERVAL` 3600 |
| `reap_stale_processes` | `REAP_INTERVAL` 300 |
| `cleanup_concurrency` (expire semaphores, promote parked) | `CONCURRENCY_INTERVAL` 300 |
| `cleanup_batches` | `BATCH_CLEANUP_INTERVAL` 3600 |
| `sweep_stalled_batches` | `config.batch_sweep_interval` |
| `cleanup_recurring_executions` | `RECURRING_CLEANUP_INTERVAL` 3600 |
| `compact_archives`, `prune_stream_archives` | `ARCHIVE_COMPACTION_INTERVAL` 3600 |
| `cleanup_outbox` | `OUTBOX_CLEANUP_INTERVAL` 3600 |
| `cleanup_job_locks` (the uniqueness reaper) | `JOB_LOCK_CLEANUP_INTERVAL` 300 |
| `cleanup_stats` | `STATS_CLEANUP_INTERVAL` 3600 |
| `sweep_orphan_streams` | `config.streams_orphan_sweep_interval` (skipped when nil) |
| `run_table_maintenance` | `TableMaintenance::MAINTENANCE_INTERVAL` |

When every attempted task fails for two consecutive cycles the dispatcher backs
off entirely — `MAINTENANCE_BACKOFF_BASE` 30s doubling to
`MAINTENANCE_BACKOFF_MAX` 600s — instead of retrying a dozen tasks per interval
against a dead database and flooding the error tracker. Any success exits the
backoff. A pooled backend the server terminated while the dispatcher sat idle
surfaces as `TERMINATED_CONNECTION_SIGNAL` and is logged at INFO, not WARN: it
is expected and self-healing.

## Health

`Web::HealthServer` (bound to `config.health_bind`, 127.0.0.1 by default) serves
`/livez` and `/readyz` from one accept-loop thread, off the supervisor's
`readiness_snapshot` (a `Concurrent::AtomicReference` safe to read from any
thread). Each accepted connection carries a 1-second `IO#timeout` — a core Ruby
3.2+ method, deliberately below `HealthProbe::DEFAULT_TIMEOUT` (2s) — so one
silent client cannot wedge every later probe. `exe/pgbus-health` is the
container health command.

See also: [../configuration/summary.md](../configuration/summary.md),
[../event-bus/summary.md](../event-bus/summary.md),
[../review/process-and-streams.md](../review/process-and-streams.md).
