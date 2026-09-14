# Practices

What `../.claude/rules/` already covers, and is not repeated here: file size and
Ruby style (`coding-style.md`), conventional commits and the pre-commit
checklist (`git-workflow.md`), RED→GREEN→REFACTOR and coverage floors
(`testing.md`), the measure-before-you-change rule for hot paths
(`performance.md`), agent orchestration (`agents.md`).

These are the practices this codebase has arrived at that those files do not
state.

## Choose the safe failure direction, and write it down

Every piece of admission control here is designed around which way it may be
wrong. The rule is the same each time: prefer the failure a sweep can repair.

- A slot held with no message is repaired by `Dispatcher#cleanup_concurrency`;
  a message with no slot is not. So `Adapter#enqueue_with_concurrency` commits
  the slot before it sends.
- A batch left waiting is repaired by `Batch::Sweep`; a batch that fires its
  `on_finish` callback early is not. So an ambiguous send keeps its batch count
  (`Adapter#ambiguous_delivery?`), and `BlockedExecution#backfill` runs in its
  own savepoint rather than in the caller's transaction.
- A uniqueness lock held too long expires nothing but throughput; a lock
  dropped from under a live message admits a duplicate. So the reaper treats an
  unknown probe result (`nil`) as "still there".

A method that makes such a choice carries a comment naming both outcomes.
`Adapter#ambiguous_delivery?`, `Client#with_stale_connection_retry` and
`EventBus::StaleConnectionRetry` are the models.

## A conditional write beats a read-then-write

Where two processes can race, the code writes a guard into the statement rather
than checking first. `Pgbus::Semaphore.acquire!` is an `INSERT … ON CONFLICT DO
UPDATE … WHERE value < COALESCE($2, max_value)`; `BatchEntry.finish_if_empty!`
is an `UPDATE … WHERE status = 'processing' AND NOT EXISTS (…)`;
`Batch::Sweep#uncount_orphan!` is a CAS on `msg_id IS NULL`;
`UniquenessKey.release_if_bound!` / `.release_if_unbound!` carry the identity in
the `WHERE`. A `find` followed by an `update` is a review finding here, not a
style preference.

## DDL races are won by doing it and reading the duplicate error as success

Schema install, queue creation and the NOTIFY trigger all run concurrently
across processes. Each does the work, rescues the duplicate-object error,
re-checks the catalog, and retries at most once
(`Client#install_pgmq_schema`, `#create_queue_table`, `#enable_notify_if_needed`).
A duplicate is only swallowed on real evidence — `#duplicate_notify_trigger_error?`
requires the `NOTIFY_TRIGGER_NAME` identifier *plus* a `PG::DuplicateObject`
cause or the English "already exists" text, so a localized message with no cause
still propagates.

## Retry lists are per-call-site, and their width is an argument

There are two stale-connection retry lists and they are deliberately different
widths. `Client::STALE_CONNECTION_PATTERNS` (seven substrings) covers only
sockets that were *already dead before any SQL was sent*, because a
half-committed enqueue would duplicate a message on retry.
`EventBus::StaleConnectionRetry::TRANSIENT_DROP` is wider — it includes
mid-flight shapes — because the only statement it ever repeats is an idempotent
`SET completed_at = <now>`. Never widen one by citing the other; state the
safety argument for the call site.

## Bound every wait, and size the bound from the thing being waited on

A flat timeout is a defect when the loop it joins can legitimately take longer.
`Web::Streamer::Listener#stop_join_timeout` and
`Process::NotifyListener#stop_join_timeout` are `health_check_ms / 1000 +
STOP_JOIN_GRACE_SECONDS`, because the thread can be parked in one full
`wait_for_notify` cycle before it re-reads the stop flag. Reads are bounded by
libpq where libpq can do it (`statement_timeout`, `tcp_user_timeout`,
keepalives) and by a Ruby `Timeout` only where it cannot
(`Client#with_read_timeout`).

## Payloads carry metadata under `pgbus_*` keys, and only injectors write them

`Serializer.serialize_job_hash` produces the payload; `Concurrency`,
`Uniqueness`, `FairShare` and `Adapter#inject_batch_metadata` each merge exactly
one `pgbus_*` key into it, in that fixed order, in both `#enqueue` and
`#enqueue_at`. Nothing else writes into the payload hash, and readers go through
the matching `extract_*` method rather than indexing the string literal.

## A comment explains why, and names the incident

The load-bearing comments in this repo cite the issue or the production
incident that produced them (`issue #397`, `rails/solid_queue#712`,
`getzazu/app#3413`, "Zazu fan-out incident, 2026-08-05"). When you change one
of these paths, keep the citation and add yours; it is the only record of why
the obvious simpler shape is wrong.

## Prose is a claim about code

A sentence in `docs/`, `CHANGELOG.md` or a locale file is audited the same way a
test is. A hand-written list (metrics exported, reasons, statuses, options) is a
completeness claim — open the constant and count. A number in prose ("up to 100
busiest keys") must name the limit the code actually uses. A page title must
match that locale's own nav label. See `review/web-and-i18n.md`.
