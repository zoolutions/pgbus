How `limits_concurrency` admits work, and why every failure leans toward
under-admission. See [../concurrency/summary.md](../concurrency/summary.md).

### The slot is committed before the message is sent, never after
- **Holds because:** PGMQ has its own connection and can never join the ActiveRecord transaction that takes the slot, so no ordering makes the handoff atomic. Sending first means a failed commit leaves a live message with a rolled-back slot and the next enqueue runs beside it. Committing first means the only crash window leaves a slot held with no message — the key is under-admitted until `Dispatcher#cleanup_concurrency` reclaims the lease. `Adapter#enqueue_with_concurrency` sets `sending = true` and calls `send_holding_slot` after the `Pgbus::Semaphore.transaction(requires_new: true)` block returns.
- **Safe direction:** under-admission, which a sweep repairs; over-admission is unrecoverable.
- **Where:** `lib/pgbus/active_job/adapter.rb#enqueue_with_concurrency`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"sends the message only after the slot transaction has committed"`, `:"releases the acquired slot when the send fails before reaching the database"`
- **Origin:** PR #460

### An ambiguous send keeps its slot, its uniqueness lock and its batch count
- **Holds because:** a failure raised from inside the produce may have committed with only the reply lost. Undoing the bookkeeping for a message that is in fact live is the worse error — that job runs with no uniqueness lock, uncounted by its batch. `ambiguous_delivery?` is true for `PGMQ::Errors::ConnectionError` and any `PG::Error`, and it is deliberately an over-approximation: `Client#send_message` runs `ensure_queue` inside the same call, so the pre-produce setup's connection errors count as ambiguous too. The `sending` flag keeps the reprieve to the produce itself — a `PG::Error` from the slot upsert or from parking the job is *not* ambiguous and rolls back as always. `sending` is never reset, because once the send returns `msg_id` is set and the rescue routes on that.
- **Where:** `lib/pgbus/active_job/adapter.rb#ambiguous_delivery?`, `#send_holding_slot`, the `rescue StandardError` in `#enqueue_with_concurrency`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"keeps the slot and the uniqueness lock when the send outcome is ambiguous"`, `:"still rolls back the uniqueness lock when the failure came before any send"`
- **Origin:** cubic learnings dc96658d, c706d375, 3a247ee3; PR #460

### Recovery from an ambiguous send is three separate paths, not one
- **Holds because:** the lease expiry (plus `Dispatcher#cleanup_concurrency`) reclaims only the *slot*. The uniqueness row is reclaimed by the dispatcher's unbound-lock reaper, and the batch count by `Batch::Sweep`'s orphan pass. Any prose that says "the lease expiry reclaims them" is wrong.
- **Where:** `lib/pgbus/process/dispatcher.rb#cleanup_concurrency`, `#reap_orphaned_uniqueness_keys`; `lib/pgbus/batch/sweep.rb#sweep_orphan_rows`
- **Proven by:** no single test; each path has its own (`spec/integration/dispatcher_reaper_spec.rb`, `spec/pgbus/batch_sweep_spec.rb:"un-counts a row only when the message exists nowhere"`)
- **Origin:** cubic learning 48bb90f0; PR #460

### Every lease goes through `Concurrency.effective_duration`, which floors it at two heartbeat intervals
- **Holds because:** a slot's lease is renewed only by the visibility heartbeat, which first beats one interval after the job starts. A raw `duration` shorter than that expires before its first renewal and the sweep promotes a second job beside a running one. The floor is applied at acquire (`Adapter#slot_lease`), at promotion (`BlockedExecution#slot_taken?`) **and** inside `Semaphore.touch` itself, so no caller can renew for the raw value.
- **Where:** `lib/pgbus/concurrency.rb#effective_duration`; `lib/pgbus/active_job/adapter.rb#slot_lease`; `lib/pgbus/concurrency/semaphore.rb#touch`; `lib/pgbus/concurrency/blocked_execution.rb#slot_taken?`
- **Proven by:** `spec/pgbus/concurrency_spec.rb:"floors a duration shorter than two heartbeat intervals"`, `:"leaves a duration longer than the floor alone"`; `spec/pgbus/concurrency/semaphore_spec.rb:"renews for at least the floored duration, never the raw one"`
- **Origin:** cubic learnings 353169ab and the `touch` follow-up; PR #460

### A scheduled job's lease covers its delay
- **Holds because:** nothing beats for a message that is sitting invisible in PGMQ, so a `perform_later(wait: 1.hour)` job under a 15-minute `duration` would lose its slot mid-wait and let a second job start. `slot_lease` adds `delay.to_i`, and `promote_next` resolves the parked payload's remaining `scheduled_at` before `slot_taken?` so a promoted scheduled job gets the same treatment. The one window this cannot cover is a *queue backlog* longer than `duration` before any worker picks the message up; that is documented, not fixed — size `duration` above the worst tolerated queue wait.
- **Where:** `lib/pgbus/active_job/adapter.rb#slot_lease`; `lib/pgbus/concurrency/blocked_execution.rb#promote_next`, `#resolve_delay`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"covers the scheduled delay in the slot's lease"`; `spec/pgbus/concurrency/blocked_execution_spec.rb:"adds a promoted scheduled job's remaining delay to its lease"`
- **Origin:** cubic learnings 6ca8afa0, eb7c135a; PR #460

### `Semaphore.touch` reads `Time.current` after the connection is in hand
- **Holds because:** the lease measures silence from the moment of renewal. Computing the expiry before `with_connection` charges the pool wait against the renewal, so a busy pool shortens exactly the lease that is under pressure.
- **Where:** `lib/pgbus/concurrency/semaphore.rb#touch`
- **Proven by:** `spec/pgbus/concurrency/semaphore_spec.rb:"starts the lease when the connection is in hand, not when the wait began"`
- **Origin:** PR #460

### The post-promotion batch backfill runs in its own savepoint
- **Holds because:** `Semaphore.signal` calls `promote_next` inside a transaction, and the message is already live by the time the backfill runs. A database error there would poison the enclosing transaction; its commit then fails, un-deleting the parked row and un-taking the slot while the message is live — so the same job is promoted and executed twice. `BlockedExecution#backfill` opens `transaction(requires_new: true)` and rescues, and `promote_next` still returns true so the caller does not over-release the semaphore. Deferring past the outermost commit was rejected: nested callers cannot know whether they are already inside a transaction.
- **Where:** `lib/pgbus/concurrency/blocked_execution.rb#backfill`, `#promote_next`
- **Proven by:** `spec/pgbus/concurrency/blocked_execution_spec.rb:"isolates a failing batch backfill in its own savepoint"`, `:"returns true when post-commit backfill raises"`
- **Origin:** cubic learning 8b52096a; PR #460

### A parked job whose class no longer resolves is promoted against the semaphore row's stored limit, not a guessed 1
- **Holds because:** `Concurrency.config_for` returns `limit: nil` for an unresolved class, and `Pgbus::Semaphore.acquire!` COALESCEs a nil `max_value` against the row's own. Forcing 1 would refuse every promotion for a `to: 3` key that still holds two slots, so its parked jobs could never reach the executor — which is what dead-letters a missing class in the first place. `nil` falls back to 1 only on a *fresh* row, where there is no recorded limit.
- **Where:** `lib/pgbus/concurrency.rb#config_for`, `#config_for_payload`; `app/models/pgbus/semaphore.rb#acquire!`
- **Proven by:** `spec/pgbus/concurrency_spec.rb:"leaves the limit to the semaphore row when the class does not resolve"`; `spec/integration/concurrency_block_durability_spec.rb:"is promoted against the limit the semaphore row already records"`
- **Origin:** cubic learning 8f14f335; PR #460

### `promotable_keys` filters out keys whose slots are all held, in SQL
- **Holds because:** the scan is capped (1000 keys), and a plain oldest-first list would fill with keys whose holders are still running — those promote for themselves on completion — starving a key behind them whose holder died. The LEFT JOIN's `pgbus_semaphores.key IS NULL OR value < max_value` is what keeps the cap honest. Filtering in Ruby after the fetch would not.
- **Where:** `app/models/pgbus/blocked_execution.rb#promotable_keys`, called from `Concurrency::BlockedExecution#promote_pending`
- **Proven by:** `spec/integration/concurrency_block_durability_spec.rb:"skips keys whose slots are all held, so a promotable key behind them is still serviced"` (the unit example `spec/pgbus/concurrency/blocked_execution_spec.rb:"asks the model for promotable keys, not for every parked key"` is a lookup test only)
- **Origin:** cubic learning 15336e8c; PR #460

### A parked job is never deleted for age — the only way out of the table is promotion
- **Holds because:** `:block` exists so that work is not lost. `expires_at` on a `pgbus_blocked_executions` row is a re-check hint for the sweep's ordering, nothing more; `cleanup_concurrency` calls only `expire_stale` (semaphores) and `promote_pending`. The one deliberate deletion is the dashboard's `discard_parked_jobs`, which an operator asked for and which resolves the bookkeeping the jobs will never resolve.
- **Where:** `lib/pgbus/process/dispatcher.rb#cleanup_concurrency`; `lib/pgbus/concurrency/blocked_execution.rb#insert` (the comment states the contract)
- **Proven by:** `spec/pgbus/process/dispatcher_spec.rb` stubs `delete_all`, `destroy_all`, `delete_by` and `destroy_by` on the model and asserts none is called; `spec/integration/concurrency_block_durability_spec.rb:"is promoted when the holder signals, not dropped"`, `:"survives the dispatcher sweep while its slot is still held"`
- **Origin:** cubic learning 972fe95b; PR #460

### The payload of a parked job is stored as a Hash, not a pre-serialized String
- **Holds because:** the column is `jsonb` and the attribute serializes once. Handing it a String stored a JSON *string* holding the document, which every SQL reader of the column misread — `payload->>'job_id'` in the batch sweep, the job-class lookup, `scheduled_at`. `BlockedExecution.repair_double_encoded!` heals rows written before the fix and runs before every promotion pass; `release_next!` additionally parses up to twice on the way out.
- **Where:** `lib/pgbus/concurrency/blocked_execution.rb#insert`; `app/models/pgbus/blocked_execution.rb#repair_double_encoded!`, `#release_next!`
- **Proven by:** `spec/integration/concurrency_block_durability_spec.rb:"is stored as a JSON object, not a JSON string"`, `:"heals rows a previous release double-encoded and promotes them"`
- **Origin:** PR #460

### Releasing a concurrency key zeroes the row and stamps it expired; it never deletes it
- **Holds because:** the row is the only record of the key's limit, and `Semaphore.acquire!` falls back to 1 on a fresh row — so deleting it silently demotes a `to: 3` key whenever the parked payload's class no longer resolves. But `Concurrency::Semaphore.expire_stale` matches on `expires_at` **alone** (the `Pgbus::Semaphore.expired` scope that also matches `value <= 0` is not what the dispatcher calls), so a zeroed row would sit on the Locks page as a phantom 0/N key for the rest of its lease. The release therefore does both: `update_all(value: 0)`, promote, then `where(key:, value: 0).update_all(expires_at: Time.current)` — guarded on `value = 0` so a row a promotion just filled is never expired out from under its holder.
- **Where:** `lib/pgbus/web/data_source.rb#release_concurrency_key`; `lib/pgbus/concurrency/semaphore.rb#expire_stale`; `app/models/pgbus/semaphore.rb` (`scope :expired`)
- **Proven by:** `spec/integration/dashboard_concurrency_spec.rb:"empties a stranded semaphore and leaves it for the sweep"`, `:"keeps the key's recorded limit so a payload with no resolvable class is judged against it"`
- **Origin:** cubic learnings 81cea35e, 911b0b34; PR #463

### `release_concurrency_key` returns the number of jobs promoted; `discard_parked_jobs` returns the number of rows it removed
- **Holds because:** they answer different questions. A release frees slots and promotes what fits, so the honest number is what it managed to promote; a discard removes every parked row it claimed, so the honest number is that count. Aligning them — in production or in a test double — makes one of the two notices lie.
- **Where:** `lib/pgbus/web/data_source.rb#release_concurrency_key`, `#discard_parked_jobs`; `spec/support/pgbus/stub_data_source.rb` carries a settable `promoted_count`
- **Proven by:** `spec/requests/pgbus/locks_controller_spec.rb:"releases the key and redirects with a notice"`, `:"discards the parked jobs and redirects with a notice"`; `spec/pgbus/web/data_source_spec.rb:"returns 0 when nothing was parked"`
- **Origin:** cubic learning 50d7b14f; PR #463
