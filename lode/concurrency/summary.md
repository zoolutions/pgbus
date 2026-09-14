# Concurrency — admission control by key

`limits_concurrency to:, key:, duration:, on_conflict:` lets at most `to:` jobs
with the same key run at once. Three files, 305 lines together:
`lib/pgbus/concurrency.rb` (110 — the DSL and key resolution),
`lib/pgbus/concurrency/semaphore.rb` (72 — the slot),
`lib/pgbus/concurrency/blocked_execution.rb` (123 — the parking lot). The state
lives in two tables, `pgbus_semaphores` and `pgbus_blocked_executions`, with
`Pgbus::Semaphore` and `Pgbus::BlockedExecution` (`app/models/pgbus/`) holding
the SQL.

## The key

`Concurrency.resolve_key` returns the `key:` proc's value, or — with no proc —
`active_job.class.name`, resolved from the **enqueued** job's class so a base
class's declaration keys each subclass separately (issue #357). The same
superclass walk is in `pgbus_concurrency`: the nearest declaration in the
ancestor chain wins. `inject_metadata` merges the result into the payload under
`Concurrency::METADATA_KEY` (`"pgbus_concurrency_key"`); `extract_key` reads it
back.

`limits_concurrency` validates eagerly: `to:` a positive Integer, `on_conflict:`
one of `:block`, `:discard`, `:raise`, `duration:` a positive Numeric, `key:`
callable or nil. Each raises `ArgumentError`.

## The slot

`Pgbus::Semaphore.acquire!(key, max_value, expires_at)` is one statement:

```sql
INSERT INTO pgbus_semaphores (key, value, max_value, expires_at)
VALUES ($1, 1, COALESCE($2, 1), $3)
ON CONFLICT (key) DO UPDATE
  SET value = pgbus_semaphores.value + 1,
      max_value = COALESCE($2, pgbus_semaphores.max_value),
      expires_at = GREATEST(pgbus_semaphores.expires_at, EXCLUDED.expires_at)
  WHERE pgbus_semaphores.value < COALESCE($2, pgbus_semaphores.max_value)
RETURNING value
```

`RETURNING` rows mean `:acquired`, none mean `:blocked`. Two properties the rest
of the subsystem is built on: the upsert holds the row lock until the
surrounding transaction commits **even when it reports `:blocked`** (which is
what makes check-and-park atomic — rails/solid_queue#712), and a `nil`
`max_value` means "keep the limit this row already records", falling back to 1
only on a fresh row.

`Concurrency::Semaphore.release` decrements with `GREATEST(value - 1, 0)`.
`#signal` wraps release + `BlockedExecution.promote_next` in one transaction, so
an enqueue that is parking a job right now commits before the promote looks.
`#expire_stale` is a `DELETE … WHERE expires_at < $1 RETURNING key` — it matches
on `expires_at` **alone**, which is why anything that empties a row also has to
stamp it expired.

## The lease is a silence budget, not a run-time cap

`expires_at` says how long a holder may go without a heartbeat. `VisibilityHeartbeat`
renews it while the job runs (`ActiveJob::Executor#with_visibility_heartbeat`
passes the key and duration in). Three places size it and all three go through
`Concurrency.effective_duration`, which floors the raw `duration` at
`config.effective_visibility_heartbeat_interval * 2` so a lease can never lapse
before the beat that would have renewed it:

| Site | Lease |
|---|---|
| `Adapter#slot_lease` | `effective_duration(duration) + delay` — a scheduled job waits in PGMQ with nothing beating for it |
| `BlockedExecution#slot_taken?` | `effective_duration(config[:duration]) + delay` — same, for a promoted scheduled job |
| `Semaphore.touch` | `GREATEST(expires_at, now + effective_duration(duration))`, with `now` read *inside* `with_connection` so a pool wait is not charged against the renewal |

The one window the lease cannot cover is a queue backed up longer than
`duration` before any worker picks the message up; that is documented in
`docs/app/views/docs/pages/concurrency_uniqueness.rb`, with "size `duration`
above the worst tolerated queue wait" as the mitigation.

## Conflict handling

`Adapter#handle_conflict` runs inside the same `requires_new` transaction as the
failed acquire:

- `:block` — insert a `pgbus_blocked_executions` row (payload as a Hash, so the
  jsonb attribute serializes it once; a pre-serialized String stored a
  double-encoded document that every `payload->>'…'` reader misread) and return
  true. The job will run later, so its uniqueness lock and batch count stand.
- `:discard` — log, roll back this enqueue's `:until_executed` uniqueness lock,
  and uncount it from its batch. Nothing will ever run to release them.
- `:raise` — `Pgbus::ConcurrencyLimitExceeded`.

## Promotion

A parked job is **never deleted for age**. `expires_at` only orders the sweep;
the single way out of `pgbus_blocked_executions` is promotion.

`BlockedExecution.promote_next` opens a `requires_new` transaction,
`release_next!`s the highest-priority oldest row (`DELETE … FOR UPDATE SKIP
LOCKED … RETURNING`), resolves the payload's remaining `scheduled_at` delay,
takes a slot through the same guarded upsert an enqueue uses, and sends. No slot
→ `ActiveRecord::Rollback`, the row stays parked. The send carries the parked
row's `priority` so priority routing picks the right `_pN` sub-queue (issue
#423). The batch-execution backfill afterwards runs in its **own** savepoint
(`#backfill`): a database error there must not poison the transaction
`Semaphore.signal` opened, because that commit failing would un-delete the
parked row and un-take the slot while the message is already live.

`promote_pending` (the dispatcher's `cleanup_concurrency`, every
`Dispatcher::CONCURRENCY_INTERVAL` = 300s) first runs
`BlockedExecution.repair_double_encoded!`, then walks `promotable_keys` — keys
with parked jobs and either no semaphore row or one with room, longest-waiting
first, capped at 1000. Filtering in SQL is what keeps the cap honest: a plain
oldest-first list would fill with keys whose slots are held (their holders
promote for themselves on completion) and starve a key behind them whose holder
died.

## Release and signal

`Executor`'s `ensure` block calls `signal_concurrency` only when
`job_succeeded` is true — set after a successful archive. A transient failure
does **not** release the slot; the slot goes back on success or on
dead-lettering only.

## The dashboard's two escape hatches

`Web::DataSource#release_concurrency_key` zeroes the row rather than deleting it
(the row is where `max_value` lives), promotes up to `PROMOTE_CAP` jobs through
`promote_next`, then — guarded on `value = 0` so a promotion that just filled it
is untouched — stamps `expires_at = now` so `expire_stale` reaps it. It returns
the number promoted. `#discard_parked_jobs` claims the rows under `FOR UPDATE
SKIP LOCKED`, deletes them in the transaction, then resolves each row's
bookkeeping outside it (`#cleanup_discarded_parked_job`): the batch child is
marked failed, an `:until_executed` lock is released through
`UniquenessKey.release_if_unbound!(key, acquired_before: row.created_at)`. It
returns the number of rows it deleted.

See also: [../active-job/summary.md](../active-job/summary.md),
[../uniqueness/summary.md](../uniqueness/summary.md),
[../review/concurrency.md](../review/concurrency.md).
