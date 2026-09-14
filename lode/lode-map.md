# Lode map

The index of this repository's durable memory. Read this first; it beats a
directory listing. Every file describes the system as it is now, with the
rationale; `../CHANGELOG.md` records what changed.

- `summary.md` — what pgbus is, its three non-negotiables, the shape of the code
- `terminology.md` — the words this repo uses (logical vs physical queue, VT, read_ct, capsule, parked job, bind stamp, tri-state probe, master hub…)
- `practices.md` — practices learned from review that `../.claude/rules/` does not state
- `workflow.md` — the profile the shared `/lode:*` workflow skills read: commands, branches, layers, shapes, constraints, docs, CI, flake sources, conflicts, verification
- `plans/README.md` — where plans live

## Subsystems

- `client/summary.md` — `Pgbus::Client`, the only door to PGMQ: the two connection shapes, read bounds and the circuit breaker, the narrow stale-connection retry, schema/queue/trigger bootstrap, the presence probes
- `active-job/summary.md` — `Adapter` and `Executor`: the enqueue ordering, the ambiguous-send table, the execute contract and why archive is the exact-once claim
- `concurrency/summary.md` — `limits_concurrency`: the semaphore upsert, leases and the heartbeat floor, parked jobs and promotion, the dashboard escape hatches
- `uniqueness/summary.md` — `ensures_uniqueness`: the two strategies, the lock row's four lifecycle states, bind stamps, and the unbound-lock reaper
- `batch/summary.md` — `Pgbus::Batch`: the execution-row invariant, the migrated and legacy paths, finish and callbacks, the four-phase sweep
- `event-bus/summary.md` — publish → topic routing → consumer → `Handler`: two-phase idempotency claims and the one retry that is safe
- `process/summary.md` — supervisor, worker, consumer, dispatcher, scheduler, outbox poller; roles, recycling, wake pipes, NOTIFY, shutdown budgets
- `streams/summary.md` — SSE: durable vs ephemeral frames, coalescing, cursors, the streams pool and its autoscaler, the Puma master hub
- `web/summary.md` — the dashboard: `DataSource` as the only reader, controllers, turbo frames, i18n across 12 locales, `/api/metrics`, health and MCP
- `configuration/summary.md` — `Pgbus::Configuration`: capsules, roles, queue naming, the validation pass
- `schema/summary.md` — the 16 `pgbus_*` tables, the generators that create them, PGMQ schema install and upgrade, separate-database support
- `testing-and-ci/summary.md` — the spec layout, what needs a database, the CI matrix and its jobs, benchmarks, release
- `docs-site/summary.md` — `docs/`, a self-contained docs-kit Rails app, and which page maps to which behaviour

## Review rules (`review/`)

Accepted review findings rewritten as rules about the system, verified against
the code. `/lode:gate` reads every file here before reviewing a diff;
`/lode:learn` adds to them.

- `review/concurrency.md` — slot leases, the heartbeat floor, promotion isolation, orphan limits, the release/discard escape hatches
- `review/uniqueness.md` — bind stamps, queue-scoped and unbound-scoped releases, the reaper's placeholder rules, DLQ handling
- `review/batch.md` — the execution-row invariant, conditional cleanup, DLQ identity by job_id, migration column states, schema parity
- `review/client.md` — connection ownership and mutex order, savepoint bootstrap caching, duplicate-error detection, DDL centralisation
- `review/process-and-streams.md` — single-owner PG connections, shutdown join budgets, listener lifecycle, notify-lock retries
- `review/web-and-i18n.md` — DataSource memo lifetime, raw key handling, lease freshness, turbo frame targets, locale parity
- `review/event-bus-and-maintenance.md` — the completion-stamp retry, failed-event persistence, autovacuum pool cleanup
- `review/testing.md` — what a unit spec may claim, restoring shared overrides, deterministic waits, honest fakes, plus two *Not a bug* entries

## Not memory

- `tmp/` — git-ignored: gate diffs and reports, handovers, scratch
