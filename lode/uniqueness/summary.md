# Uniqueness — at most one of this job

`ensures_uniqueness strategy:, key:, on_conflict:` keeps at most one job with a
given key in the system. Two files: `lib/pgbus/uniqueness.rb` (228 lines — the
DSL, key resolution, the four lifecycle calls) and
`app/models/pgbus/uniqueness_key.rb` (118 — every statement that touches
`pgbus_uniqueness_keys`). The table is keyed on `lock_key` (it is the primary
key) and carries `queue_name`, `msg_id` and `created_at`.

## Two strategies

- `:until_executed` (default) — the lock is taken at **enqueue**, before the
  produce, and held through execution. Released on success or on
  dead-lettering. PGMQ's visibility timeout is the execution lock, so there is
  no separate claim step.
- `:while_executing` — no enqueue-time lock (`acquire_enqueue_lock` returns
  `:no_lock`); the lock is taken at the top of `Executor#execute`, bound to this
  `msg_id`, and released on success **or on failure**, so the retry after VT can
  acquire it.

`VALID_STRATEGIES` is `%i[until_executed while_executing]`, `VALID_CONFLICTS` is
`%i[reject discard log]`. `lock_ttl:` was removed in 1.0.0 and its presence
raises an `ArgumentError` naming the upgrade page.

## The class-name default is guarded

With no `key:` the key is the enqueued job's class name — correct for a
no-argument job that must not overlap itself, a silent-correctness footgun for a
job taking per-record arguments. `guard_class_name_default!` raises an
`ArgumentError` when an `:until_executed` job declared without `key:` is
enqueued **with** arguments (issue #333). `:while_executing` is unaffected, and
a no-argument job keeps the default. As with concurrency, `pgbus_uniqueness`
walks the superclass chain and no proc is stored for the default, so a
base-class declaration keys each subclass separately (issue #357).

A key that responds to `to_global_id` is serialized, so a model instance can be
returned from the proc directly.

## The row's four states

| `queue_name` | `msg_id` | Meaning | Who cleans up |
|---|---|---|---|
| logical queue | positive | **bound** — a real message exists | the executor, on success or DLQ |
| logical queue | 0 | acquired, send has not landed | `bind!` after send, or the reaper |
| `"pending"` (`Uniqueness::PLACEHOLDER_QUEUE`) | 0 | acquired before the queue was known | the reaper |
| `"batch:<batch_id>"` (`Batch::LOCK_QUEUE_PREFIX`) | 0 | a unique batch's run-scoped lock | `Batch.release_lock`, from `finish_if_needed` |

`Uniqueness.placeholder?` is `msg_id.to_i <= 0 || queue_name.to_s == "pending"`.
The dispatcher partitions on it (`Dispatcher#bound_lock?`): bound rows are
checked with `Client#message_exists?`, unbound rows with
`Client#uniqueness_keys_present`, which scans every live queue's payloads for
the key rather than probing a `pgmq.q_<prefix>_pending` table that does not
exist (issue #418).

## The bind stamp

`UniquenessKey.acquire!` stores the inserted row's `created_at` in
`Thread.current[:pgbus_uniqueness_created_at][lock_key]`. `bind!` consumes it
and adds `AND created_at = $4` to its `UPDATE … WHERE lock_key = $1 AND msg_id =
0`, so a completed job's late bind cannot retarget a successor that has since
re-acquired the same key. Every terminal call (`bind!`, `release!`,
`release_if_bound!`, `release_if_unbound!`, `clear_bind_stamp!`) deletes the
stamp; `Adapter#enqueue_with_concurrency` calls `clear_bind_stamp!` on the way
out when it acquired but will not bind (the `:block` path).

`Adapter` also tracks `Thread.current[:pgbus_acquired_uniqueness_key]` so a
failed enqueue can roll the lock back — and clears it without deleting the row
when the send outcome was ambiguous, so a later discard on the same thread
cannot release a live job's lock.

## The three scoped releases

`release!` is the unconditional key-only `DELETE`, used on the normal
success/DLQ path where this execution provably owns the key. The other two
carry identity in the `WHERE`, because a key-only delete can drop a successor's
row:

- `release_if_bound!(key, queue_name:, msg_id:)` — the executor's `:duplicate`
  path, when another worker archived the message first. Both columns are matched:
  PGMQ message ids are per-queue sequences, so a `msg_id` alone is not an identity.
- `release_if_unbound!(key, acquired_before:)` — the dashboard discarding a
  parked job. A parked job never got a `msg_id`, so there is nothing to match on;
  the ceiling is the parked row's `created_at`, since the lock is acquired
  immediately before the row is parked and a lock created *after* it cannot
  belong to it.

There is deliberately no retried or fallback release after an ambiguous one: the
first `DELETE` may have committed and a successor may already hold the key.

## Enqueue-time rejection

`Adapter#uniqueness_rejected?` runs before anything is sent, and short-circuits
on `active_job.executions.to_i.positive?`. A `retry_on` re-enqueue is the *same*
logical job re-acquiring its own still-held key; rejecting it would dead-letter
the original and lose the retry (issue #333). Otherwise `on_conflict:` decides:
`:reject` raises `Pgbus::JobNotUnique`, `:discard` and `:log` return the job
having done nothing — and `Adapter#enqueue` then calls `uncount_batch_job`,
because a job that will never run must not be waited on by its batch.

## The reaper

`Dispatcher#reap_orphaned_uniqueness_keys` runs every
`JOB_LOCK_CLEANUP_INTERVAL` (300s). It considers only rows older than
`config.visibility_timeout * 2` — younger than that and it could be racing an
in-flight enqueue whose send has not committed. Batch lock rows are judged by
their batch (`Batch.lock_row?` / `.lock_orphaned?`), bound rows by
`message_exists?`, unbound rows by `uniqueness_keys_present`. Anything the
probes cannot determine (`nil`) is kept.

See also: [../active-job/summary.md](../active-job/summary.md),
[../review/uniqueness.md](../review/uniqueness.md).
