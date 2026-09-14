How a uniqueness lock is taken, bound, and — the part that keeps producing
findings — released. See [../uniqueness/summary.md](../uniqueness/summary.md).

### `bind!` may only retarget the row this enqueue acquired
- **Holds because:** `msg_id = 0` is not ownership, it only means "not sent". Without a second predicate a slow bind could land on a *successor* that acquired the same key after this one was released, pointing that successor's row at a dead message. `UniquenessKey.acquire!` stamps the inserted row's `created_at` into `Thread.current[:pgbus_uniqueness_created_at]`, and `bind!` consumes it and adds `AND created_at = $4`. Every terminal call consumes or clears the stamp — `bind!`, `release!`, `release_if_bound!`, `release_if_unbound!`, and `clear_bind_stamp!`, which `Adapter#enqueue_with_concurrency` calls on the way out (including the concurrency `:block` path, which acquires but never binds).
- **Where:** `app/models/pgbus/uniqueness_key.rb#acquire!`, `#bind!`, `#clear_bind_stamp!`; `lib/pgbus/active_job/adapter.rb#enqueue_with_concurrency`
- **Proven by:** `spec/integration/job_uniqueness_spec.rb:"updates queue_name and msg_id without changing lock_key or created_at"`, `:"does not retarget a successor row acquired after this enqueue released"`, `:"clears the bind stamp without releasing the lock"`
- **Origin:** cubic learning 5c0d8821

### A `:while_executing` lock released after a duplicate archive is matched on queue **and** msg_id
- **Holds because:** PGMQ message ids are per-queue sequences, so the same number addresses a different message on every other queue — a `msg_id`-only match can delete a lock belonging to another queue's job. `release_if_bound!` carries both columns and the executor passes the same queue the execution lock was acquired under. An `:until_executed` lock is left alone on this path: it belongs to the worker that archived the message.
- **Where:** `app/models/pgbus/uniqueness_key.rb#release_if_bound!`; `lib/pgbus/active_job/executor.rb#release_duplicate_execution_lock`
- **Proven by:** `spec/pgbus/active_job/executor_spec.rb:"releases its own :while_executing lock when the message was archived elsewhere"`, `:"leaves an :until_executed lock alone when the message was archived elsewhere"`
- **Origin:** cubic learnings 651b2d9a, 56bd5ddd; PR #460

### Discarding a parked job releases only an **unbound** lock acquired no later than the parked row
- **Holds because:** a parked job never got a `msg_id`, so there is nothing to match on the way `release_if_bound!` does. Once the unbound-lock reaper has removed the parked job's own row, a key-only delete could drop two different successors: one that was actually sent (its row is bound — excluded by `msg_id = 0`) and one that is itself parked (also unbound, so only time separates them). The lock is acquired immediately before the row is parked, so a lock created *after* the parked row cannot belong to it: the caller passes the discarded row's `created_at` as `acquired_before:`.
- **Where:** `app/models/pgbus/uniqueness_key.rb#release_if_unbound!`; `lib/pgbus/web/data_source.rb#release_parked_uniqueness_lock`
- **Proven by:** `spec/integration/dashboard_concurrency_spec.rb:"frees the uniqueness key a parked until_executed job still holds"`, `:"leaves a successor's bound lock alone when the reaper already took the parked job's"`, `:"leaves a still-parked successor's lock alone when the reaper took the original's"`
- **Origin:** cubic learning 1c3eb474; PR #463

### The post-archive release happens once, and an ambiguous release is never retried
- **Holds because:** the first `DELETE` may have committed with only the reply lost, and a successor can acquire the same key before a retry lands — a second key-only delete would drop that successor's lock. `Executor#release_uniqueness_lock` rescues and logs; the `ensure` block signals concurrency and batch completion but issues no fallback delete. Any fallback that is ever added has to be conditional on this execution still owning the lock.
- **Where:** `lib/pgbus/active_job/executor.rb#release_uniqueness_lock` and the `ensure` block of `#execute`
- **Proven by:** `spec/pgbus/active_job/executor_spec.rb:"releases the lock after a successful run"`, `:"does not release the lock if archive_from fails (retry path)"`
- **Origin:** cubic learning 4f54ec13

### An enqueue-time rollback happens only when no message exists
- **Holds because:** `rollback_acquired_uniqueness_lock` and `uncount_batch_job` run in `#enqueue_with_concurrency`'s rescue **only** when `msg_id.nil? && !(sending && ambiguous_delivery?(e))`. When a message did come back — or may have — the thread-local is cleared without deleting the row, so a later discard on the same thread cannot release a live job's lock. A post-send backfill failure therefore leaves both the lock and the execution row for the live message.
- **Where:** `lib/pgbus/active_job/adapter.rb#enqueue_with_concurrency` (rescue), `#rollback_acquired_uniqueness_lock`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"clears the uniqueness thread-local after send without releasing the live lock"`, `:"releases the uniqueness lock when send_message raises before a message exists"`, `:"does not bind when send_message fails, and rolls back the lock"`
- **Origin:** cubic learning 3a247ee3; PR #420

### A concurrency `:discard` rolls the lock back; a `:block` conflict keeps it
- **Holds because:** a discarded job will never reach an executor, so its `:until_executed` lock would be orphaned — nobody releases it. A blocked job's tagged payload is stored and runs later, so its lock is still doing its job. A *uniqueness*-duplicate discard (`:locked`) also keeps the lock, because that lock belongs to the in-flight job this enqueue lost to, not to this enqueue.
- **Where:** `lib/pgbus/active_job/adapter.rb#handle_conflict`, `#uniqueness_rejected?`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"releases the :until_executed lock when a concurrency :discard conflict drops the job"`, `:"does not release the lock when the job is blocked — the stored payload runs later"`, `:"does not release a uniqueness lock it did not acquire (duplicate discarded)"`
- **Origin:** cubic learning 64d3a504

### A retry re-enqueue is let through the duplicate check
- **Holds because:** ActiveJob increments `executions` at the start of `perform_now`, and `retry_on` re-enqueues from inside it — while the executor still holds the key, which it releases only on success or DLQ. Rejecting the re-enqueue would raise `JobNotUnique`, dead-letter the original and lose the retry. `uniqueness_rejected?` returns false for `active_job.executions.to_i.positive?`; the existing row correctly stays held.
- **Where:** `lib/pgbus/active_job/adapter.rb#uniqueness_rejected?`
- **Proven by:** `spec/integration/uniqueness_retry_spec.rb:"runs a retried :until_executed job to success without JobNotUnique or DLQ, and cleans up the key"`
- **Origin:** issue #333, carried in the method's comment

### The unbound-lock reaper scans live queue payloads, and never probes the synthetic `pending` queue
- **Holds because:** `pgmq.q_<prefix>_pending` does not exist, so `message_exists?` returns `nil` (unknown) and the reaper — which never deletes in doubt — would keep every placeholder lock forever. `Uniqueness.placeholder?` partitions on `msg_id <= 0 || queue_name == "pending"`, and unbound rows go to `Client#uniqueness_keys_present`, which reads `pgmq.meta` and scans each live queue's payloads for the key. A per-queue `UndefinedTable` is skipped (the table was dropped between listing and select); any other error raises, and the caller treats that as unknown.
- **Where:** `lib/pgbus/uniqueness.rb#placeholder?`; `lib/pgbus/process/dispatcher.rb#bound_lock?`, `#gone_unbound_locks`; `lib/pgbus/client.rb#uniqueness_keys_present`
- **Proven by:** `spec/integration/dispatcher_reaper_spec.rb:"reaps an aged pending lock when no live queue carries the uniqueness key"`, `:"does NOT reap an aged pending lock while a message on a real queue still holds the key"`, `:"does NOT reap msg_id=0 placeholder locks while a queue message exists for the same key"`
- **Origin:** cubic learning bebe8746 (issue #418)

### The unbound-lock scan skips dead-letter queues
- **Holds because:** a dead-lettered copy still carries the payload's uniqueness key, but it is not in flight — `Executor#execute`'s dead-letter branch already called `Uniqueness.release_lock`. Counting a DLQ row as "present" would pin an unbound lock for as long as the DLQ row exists, and an `:until_executed` + `on_conflict: :discard` job would then be discarded on every enqueue until someone purged the DLQ by hand. `uniqueness_keys_present` skips any queue name ending in `Pgbus::DEAD_LETTER_SUFFIX`.
- **Where:** `lib/pgbus/client.rb#uniqueness_keys_present`; `lib/pgbus/active_job/executor.rb#execute` (the `read_ct > max_retries` branch)
- **Proven by:** no dedicated example found for the DLQ skip; the surrounding reaper behaviour is covered by `spec/integration/dispatcher_reaper_spec.rb`
- **Origin:** supersedes cubic learning 38c9758b ("keep DLQ payloads live during reaper scans"), which PR #450 inverted once the executor's DLQ release made the lock provably free

### The reaper ignores anything younger than `visibility_timeout * 2`
- **Holds because:** a freshly acquired lock whose `send_message` has not committed yet looks exactly like an orphan. The age floor is what stops the reaper racing an in-flight enqueue; it is not a TTL, and an old lock whose message is still in the queue is never reaped no matter how old (a recurring job that fails and retries legitimately holds one for hours).
- **Where:** `lib/pgbus/process/dispatcher.rb#reap_orphaned_uniqueness_keys`
- **Proven by:** `spec/integration/dispatcher_reaper_spec.rb:"does not reap locks newer than the threshold even if the message is gone"`, `:"does NOT reap the lock even when older than visibility_timeout * 2"`
- **Origin:** the method's own comment; reinforced by cubic learning 38c9758b's review thread

### The five-minute reaper asks about in-flight keys directly, without slicing
- **Holds because:** `uniqueness_keys_present` takes the candidate keys as one set and scans queues once. Slicing at an arbitrary 10,000 adds passes for no benefit; batching is only needed when the candidate set could exceed PostgreSQL's 65,535 bind-parameter limit, which this table's in-flight population does not reach.
- **Where:** `lib/pgbus/process/dispatcher.rb#gone_unbound_locks` → `lib/pgbus/client.rb#uniqueness_keys_present`
- **Proven by:** no test (a size-threshold claim; the functional path is covered by `spec/integration/dispatcher_reaper_spec.rb`)
- **Origin:** cubic learning 0d5e3d90
