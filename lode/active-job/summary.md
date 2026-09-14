# ActiveJob — enqueue and execute

Two classes, both small relative to what they coordinate:
`lib/pgbus/active_job/adapter.rb` (375 lines) and
`lib/pgbus/active_job/executor.rb` (440 lines). The adapter is registered as
`:pgbus_adapter` through `lib/active_job/queue_adapters/pgbus_adapter.rb`.

## Enqueue

`Adapter#enqueue` and `#enqueue_at` build the payload the same way, in a fixed
order: `Serializer.serialize_job_hash`, then `Concurrency.inject_metadata`,
`Uniqueness.inject_metadata`, `FairShare.inject_metadata`, and finally
`#inject_batch_metadata`. Each writes one `pgbus_*` key into the payload hash;
nothing else may.

`#uniqueness_rejected?` runs before anything is sent. It short-circuits on
`active_job.executions.to_i.positive?` — a `retry_on` re-enqueue is the *same*
logical job re-acquiring its own still-held key, and rejecting it would
dead-letter the original while losing the retry.

`#enqueue_with_concurrency` is the load-bearing method. When the job has a
concurrency key it opens a `requires_new` transaction and calls
`Concurrency::Semaphore.acquire`. The upsert holds the semaphore row lock until
that transaction commits *even when it reports `:blocked`*, which is what makes
the check-and-park atomic: a holder signalling at the same moment waits on the
row and then sees the parked row instead of stranding it. The PGMQ send happens
**after** the commit, deliberately — PGMQ has its own connection and can never
join the transaction, so sending first would risk a live message with a
rolled-back slot.

Failure handling turns on one flag, `sending`, and one predicate,
`#ambiguous_delivery?` (a `PGMQ::Errors::ConnectionError` or any `PG::Error`).

| Outcome | Uniqueness lock | Batch count | Concurrency slot |
|---|---|---|---|
| `msg_id` returned | bound (`#bind_acquired_uniqueness_lock`), then backfilled | kept | held |
| error before the send (`sending == false`) | rolled back | untracked | released |
| error from inside the send, not ambiguous | rolled back | untracked | released (`#send_holding_slot`) |
| error from inside the send, ambiguous | **kept** (thread-local dropped, row left) | **kept** | **kept** |

The asymmetry is the point: a batch left waiting for a job that never existed is
recovered by the stalled-batch sweep, while a batch that finishes early and fires
its callback is not recoverable.

`#enqueue_all` partitions first: any job with uniqueness or concurrency config
goes through the individual path, because the bulk `send_batch` cannot take
locks. The rest is grouped by `[queue, priority]` — `send_batch` routes through
the queue strategy, so a mixed-priority bulk needs one `produce_batch` per level.
A short `msg_ids` array raises `Pgbus::EnqueueError`, and the rescue untracks
only the payloads whose id is actually `nil`.

`#enqueue_after_transaction_commit?` returns `true`.

## Execute

`Executor#execute` is one method with an `ensure`, and the ordering inside it is
the contract.

1. `read_ct > config.max_retries` → dead-letter, clear the failed-event row,
   signal concurrency, signal batch *discarded*, release uniqueness, record the
   stat, return `:dead_lettered`.
2. `:while_executing` uniqueness acquires its lock now, bound to this `msg_id`;
   losing that acquire returns `:skipped` and lets VT redeliver.
   `:until_executed` does nothing here — VT *is* the execution lock.
3. `perform_now` runs inside `#with_visibility_heartbeat` (which also renews the
   concurrency lease) and inside the Rails executor/reloader
   (`#execute_job`), so leased AR connections come back.
4. **Archive is the exact-once claim.** `#archive_from` retries once on a
   connection error (archiving is idempotent). A `false` on the *first* attempt
   means another worker already archived it → `:duplicate`, no signals, and the
   `:while_executing` lock is released conditionally via
   `UniquenessKey.release_if_bound!` (matching queue *and* msg_id). A `false`
   after our own retry is `:ambiguous` and the signals proceed.
5. Only after a successful archive is `job_succeeded` set; the `ensure` block
   signals concurrency and batch completion only then.

`retried` (from `Batch.retry_reenqueued?`) suppresses the batch completion
signal: `retry_on` re-enqueues from inside `perform_now` and returns normally,
so this attempt is over but the *job* is not.

The rescue is `rescue Exception` (not `StandardError`) after an explicit
`FATAL_EXCEPTIONS` re-raise list, because `Async::Stop` and `Async::Cancel`
descend from `Exception` and were silently losing control flow under
`execution_mode: :async`. A transient failure never signals concurrency — the
slot is released only on success or dead-lettering — and
`#apply_retry_backoff` extends VT by `RetryBackoff` from `read_ct - 1`, skipping
the first read.

See also: [concurrency/summary.md](../concurrency/summary.md),
[batch/summary.md](../batch/summary.md), [review/concurrency.md](../review/concurrency.md).
