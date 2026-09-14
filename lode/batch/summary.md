# Batches — a group of jobs with a terminal outcome

`Pgbus::Batch` (`lib/pgbus/batch.rb`, 634 lines) is a handle over a
`pgbus_batches` row (`Pgbus::BatchEntry`). Jobs enqueued inside `Batch#enqueue`'s
block join it; when the last one reaches a terminal state the batch finishes and
its callbacks fire. `Batch::Sweep` (`lib/pgbus/batch/sweep.rb`, 163 lines)
repairs what the completion path cannot finish.

## The invariant

```
total_jobs == outstanding pgbus_batch_executions rows + completed_jobs + failed_jobs
```

It holds at every commit point, and it is what lets a finish never race an add.
`Batch.track_enqueue` increments `total_jobs` **and** inserts the execution rows
in one transaction, **before** any message is sent (issue #423), so
`BatchEntry.increment_total_jobs!` raises `Batch::AlreadyFinished` at
`perform_later` — before the send — when the batch has already finished.

## Enqueue

`Batch#enqueue(&block)` acquires the run lock (below), creates the row as
`pending`, runs the block with `Thread.current[:pgbus_batch_id]` set
(`#count_jobs`), then flips to `processing` and calls
`BatchEntry.check_finished!` — completion signals that arrived while the block
was open saw an incomplete `total_jobs` and could not finish the batch
themselves. Calling `#enqueue` again on an unfinished batch goes to `#reopen`,
which adds to the existing group (open batches, issue #415); a job running
inside the batch reaches its own handle through `ActiveJob::Base#batch`.

`Adapter#inject_batch_metadata` tags the payload with `Batch::METADATA_KEY`
(`"pgbus_batch_id"`) and calls `track_enqueue`. A bulk `enqueue_all` counts the
whole group with **one** guarded increment. After a successful send,
`Batch.backfill_execution` re-points the row at the real `msg_id` and the
**physical** target queue (`Client#target_queue`, `_pN` suffix included).

## Unique batches

`Batch.new(uniqueness_key: …, on_conflict: :reject|:discard|:log)` holds one
`pgbus_uniqueness_keys` row from `#enqueue` until the batch finishes.
`lock_key` is `"batch:<uniqueness_key>"` and `queue_name` is
`"batch:<batch_id>"` (both `Batch::LOCK_KEY_PREFIX` / `LOCK_QUEUE_PREFIX` —
the prefix is what keeps a job's `ensures_uniqueness` key from colliding with
it). `:reject` raises `Batch::AlreadyRunning`; `:discard` and `:log` set
`#discarded?` and return the handle having done nothing.
`finish_if_needed` is the single place the lock goes back, so every finish path
releases it.

## Two accounting paths

`Batch.executions_migrated?` is `BatchExecution.table_exists?`, and it **caches
only a successful true** — a missing table or a connection error is re-probed,
so a long-lived worker notices the migration without a restart.

| | migrated (execution rows) | legacy (counters only) |
|---|---|---|
| completion signal | `resolve_execution`: delete the row **and** increment the counter in one transaction, then `try_finish!` | `update_counter` → `BatchEntry.increment_counter!` under a row lock |
| signal with no `job_id` | `signal_without_row`: increment, then `try_finish!` (which requires terminal counters) | same |
| finish test | `finish_if_empty!`: `status = 'processing'` AND no execution rows AND `completed + failed = total` | `completed + discarded == total` AND `status = 'processing'` |
| failure column | `failed_jobs` | `discarded_jobs` |

`BatchEntry::COUNTER_COLUMNS` accepts all three names
(`completed_jobs discarded_jobs failed_jobs`) so a gem running before or after
the `add_batch_executions` migration can still increment. `#discarded_jobs` on
the model reads `failed_jobs` once the column is gone. A batch that was in
flight when the migration ran has zero execution rows and counters short of
`total_jobs`; `legacy_untracked_batch?` detects it and keeps it on the counter
path.

`try_finish!` re-checks `BatchExecution.exists?` in a fresh statement after a
winning `UPDATE` and rolls back if a row appeared — Postgres READ COMMITTED can
let a blocked CAS win from a stale `NOT EXISTS` snapshot.

## Callbacks

`finish_if_needed` fires `on_finish` always, then `on_success` when
`failure_count(record)` is zero, else `on_failure`, and only then releases the
run lock and emits `pgbus.batch_finished` (payload: `batch_id`, `total_jobs`,
`completed_jobs`, `failed_jobs`). A callback given as a bare **class** lives in
the legacy `on_*_class` column; a configured ActiveJob **instance** is
serialized into the `on_*_job` jsonb column so `.set` options resolve at
creation. Before the `add_batch_callback_jobs` migration there is nowhere to
keep the instance, so it degrades to its class with a one-time warning
(`warn_callback_jobs_unmigrated`) rather than being dropped.
`on_discard:` is a deprecated alias of `on_failure:`; passing both raises.

## Retries stay in the batch

A `retry_on` re-enqueue is the same ActiveJob `job_id` with a new PGMQ message.
`Batch.track_retry` re-inserts the one execution row (`ON CONFLICT (job_id) DO
NOTHING`) without counting again, and `note_retry_reenqueued` /
`retry_reenqueued?` (a fiber-local Set, right for `execution_mode: :async`) tell
`Executor`'s `ensure` to skip the completion signal: this attempt is over, the
job is not.

## The sweep

`Batch::Sweep.run` (dispatcher, every `config.batch_sweep_interval`, which must
be positive) runs four phases and instruments `pgbus.batch_sweep` with a count
per phase:

1. `sweep_stale_executions` — rows **with** a `msg_id` older than
   `stalled_for`. Not in the queue → look for the payload's `job_id` on the
   physical DLQ (`move_to_dead_letter` mints a new id, so the source `msg_id` is
   not a DLQ identity) → else archived → else warn and resolve as completed.
2. `sweep_orphan_rows` — rows with **no** `msg_id` older than `stalled_for`,
   skipping any `job_id` currently parked as a blocked execution. Probes the
   queue and the DLQ by `job_id` first, because a row stays `msg_id`-less both
   when the enqueue died before the send *and* when only the backfill failed.
   Removal is a CAS: `where(id:, msg_id: nil).delete_all`.
3. `start_stalled_pending` — a `pending` batch older than the threshold with no
   execution row inserted inside the threshold either. Only the status moves.
4. `finish_stalled_processing` — a `processing` batch with no execution rows
   **and** terminal counters. That is the finish `UPDATE` that rolled back after
   a callback enqueue failed, or a `total_jobs = 0` batch whose block crashed.

`Batch.cleanup(older_than:)` deletes finished batches past
`config.batch_retention` on the hourly `cleanup_batches` task.

See also: [../active-job/summary.md](../active-job/summary.md),
[../schema/summary.md](../schema/summary.md),
[../review/batch.md](../review/batch.md).
