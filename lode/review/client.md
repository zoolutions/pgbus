Connection ownership, bootstrap DDL races and retry policy inside
`Pgbus::Client`. See [../client/summary.md](../client/summary.md).

### On the shared-AR path the class install mutex is taken before the connection mutex, and never the other way round
- **Holds because:** two `Client` instances share one underlying libpq connection on the Proc path while each holds its own `@pgmq_mutex`, so a per-instance guard cannot serialize bootstrap DDL — concurrent install traffic desyncs the protocol ("message type 0x… arrived from server while idle") and wedges a thread on a socket read (issue #397, forensics in getzazu/app#3413). `Client.pgmq_install_mutex` is process-wide and is taken first; `synchronized` nests inside it. That order is safe only because no path acquires them the other way. Even the local `transaction_status` probe runs inside `synchronized` — reading the shared `PG::Connection` is still touching it.
- **Where:** `lib/pgbus/client.rb#ensure_pgmq_schema`, `#queue_ddl_rides_caller_transaction?`, `#synchronized`
- **Proven by:** `spec/pgbus/client_spec.rb:"serializes installs process-wide across client instances (#397)"`, `:"holds the connection mutex while installing on a shared Proc connection"`, `:"probes the shared connection only while holding the connection mutex"`
- **Origin:** cubic learnings 5ec66f53 (single-owner connections) and 80135dde (lock order)

### Bootstrap DDL owns `BEGIN`/`COMMIT` only when the connection is idle; otherwise it rides a savepoint
- **Holds because:** a Proc-supplied shared connection can arrive mid-transaction — `perform_later` inside an application `transaction do` block. `BEGIN` there is a warning-level no-op and the matching `COMMIT`/`ROLLBACK` would commit or destroy the *caller's* transaction. `inside_caller_transaction?` decides, and the savepoint path rolls back only to `SAVEPOINT pgbus_pgmq_install`. Across processes the serializer is `pg_advisory_xact_lock(PGMQ_INSTALL_LOCK_KEY)` — a *transaction*-scoped lock, because a session-level one can be released on a different server connection through a transaction-pooling pooler.
- **Where:** `lib/pgbus/client.rb#ensure_pgmq_schema`, `#inside_caller_transaction?`, `#install_pgmq_schema_in_savepoint`, `#install_pgmq_schema_in_own_transaction`
- **Proven by:** `spec/pgbus/client_spec.rb:"frames check+install in a savepoint and never issues BEGIN/COMMIT"`, `:"rolls back to the savepoint — never the whole transaction — on a duplicate install"`
- **Origin:** cubic learning d157142c (issues #397/#398)

### A savepoint install is not cached, and neither is queue DDL that rides a caller transaction
- **Holds because:** the caller may still roll back, taking the schema (or the queue) with it — and a schema found "already present" inside the caller's transaction may be the caller's own uncommitted work. A cached true would skip every future check and message operations would then fail against tables that never committed. `@schema_ensured` is set only on the owned-COMMIT path; `ensure_single_queue` skips the `@queues_created` memo entirely when `queue_ddl_rides_caller_transaction?`. Dedicated String/Hash paths run DDL on pgmq-ruby's own pool connections, outside any application transaction, so they always cache.
- **Where:** `lib/pgbus/client.rb#ensure_pgmq_schema`, `#ensure_single_queue`, `#queue_ddl_rides_caller_transaction?`
- **Proven by:** `spec/pgbus/client_spec.rb:"does not cache schema_ensured — the install is only durable once the caller commits"`, `:"creates the queue but does not cache it — the DDL is only durable once the caller commits"`
- **Origin:** cubic learnings 75d0b1f5, e18ddcbc

### A duplicate-object error is treated as success only after re-checking the catalog
- **Holds because:** `CREATE … IF NOT EXISTS` is not race-safe under READ COMMITTED — two backends both pass the existence check and the loser raises a unique violation on `pg_class_relname_nsp_index` rather than the friendly `duplicate_table` (issue #404). The duplicate proves someone committed the object, so `create_queue_table` re-checks `pgmq.meta` (`queue_registered?`) and returns, retrying `pgmq.create` once only when it cannot confirm. `DUPLICATE_INSTALL_ERROR_CLASSES` is matched by class **name**, so it works whether or not the pg gem's generated classes are loaded, and `duplicate_relation_error?` also inspects `error.cause` because pgmq-ruby raises `ConnectionError` inside `rescue PG::Error`. When the retry fails, its own error is what propagates — the original duplicate is preserved deeper in Ruby's implicit `$!` cause chain, which is what the docs must say.
- **Where:** `lib/pgbus/client.rb#create_queue_table`, `#queue_registered?`, `#duplicate_relation_error?`, `#duplicate_install_error?`, `#create_fifo_index_if_needed`
- **Proven by:** `spec/pgbus/client_spec.rb:"treats the loser's duplicate as success when the winner's queue is registered"`, `:"retries pgmq.create once when the recheck cannot confirm the queue"`, `:"propagates when the retry also fails, carrying the original duplicate as cause"`, `:"recognizes an unwrapped duplicate error class directly"`
- **Origin:** cubic learning d30e1310 (issue #404)

### A duplicate NOTIFY-trigger error needs the trigger name **plus** real duplicate evidence
- **Holds because:** swallowing on the trigger name alone hides a genuine failure whose message merely mentions it. `duplicate_notify_trigger_error?` requires `NOTIFY_TRIGGER_NAME` (an identifier, so it survives server-side message localization) *and* either a `PG::DuplicateObject` cause or the English "already exists" text — so a localized message with no cause propagates. `NOTIFY_TRIGGER_NAME` is reused, never re-typed as a literal, by both this check and `notify_trigger_current?`. The throttle is re-checked before retrying, because a job-queue ensure (250 ms) can race a stream override (0 ms) and a mismatch means the winner installed a different interval; one retry converges, a second loss propagates.
- **Where:** `lib/pgbus/client.rb#enable_notify_if_needed`, `#duplicate_notify_trigger_error?`, `#notify_trigger_current?`
- **Proven by:** `spec/pgbus/client_spec.rb:"treats the loser's duplicate-trigger error as success when the winner installed the same throttle"`, `:"retries enable_notify_insert once when the winner installed a different throttle"`, `:"recognizes the duplicate via the PG::DuplicateObject cause when the message is localized"`, `:"propagates a localized duplicate message when the PG::DuplicateObject cause was dropped"`, `:"propagates duplicate errors about other objects"`
- **Origin:** cubic learnings a188836f, b84ddd67 (issue #403)

### Queue and DLQ table creation plus autovacuum tuning live in one helper, called under `synchronized`
- **Holds because:** both physical creation paths need the same four steps in the same order, and the mutex must be held for all of them on the shared connection. `create_queue_physically` is that helper (`create_queue_table` → `enable_notify_if_needed` → `create_fifo_index_if_needed` → `create_fair_index_if_needed`); `create_dead_letter_queue_physically` reuses `create_queue_table` the same way. Tuning rides inside `create_queue_table`, so no caller can create a queue and forget it.
- **Where:** `lib/pgbus/client.rb#create_queue_physically`, `#create_dead_letter_queue_physically`, `#create_queue_table`, `#tune_autovacuum`
- **Proven by:** `spec/pgbus/client_spec.rb:"tunes autovacuum when creating a queue"`, `:"creates the queue with the prefixed name"`, `:"is idempotent — only creates the queue once"`
- **Origin:** cubic learning 11e2e3ec

### `with_stale_connection_retry` covers only sockets that were dead before any SQL was sent
- **Holds because:** it wraps enqueues, and a half-committed produce would duplicate a message on retry. `STALE_CONNECTION_PATTERNS` is seven substrings that all mean the socket was already gone (`pqsocket() can't get socket descriptor`, `connection is closed`, `connection has been closed`, `connection not open`, `no connection to the server`, `ssl error: unexpected eof`, `ssl syscall error`); mid-flight shapes such as `server closed the connection unexpectedly` are excluded **on purpose**. `STALE_RETRY_ATTEMPTS` is 2 with `STALE_RETRY_DELAYS = [0.1, 0.5]`, the index clamped so a future attempt count cannot sleep nil, and the `sleep` is in the `rescue` — outside the yielded block — so it never runs while `@pgmq_mutex` is held.
- **Safe direction:** raising is harmless; a duplicated message is not.
- **Where:** `lib/pgbus/client.rb#with_stale_connection_retry`, `#stale_connection_error?`, `STALE_CONNECTION_PATTERNS`
- **Proven by:** `spec/pgbus/client_spec.rb`'s "stale pgmq connection recovery" describe block; `Client#synchronizing?` exists so a test can assert the backoff runs outside the mutex
- **Origin:** cubic learning f8cba24b's exclusion clause; the constant's own comment

### A pool-checkout timeout is never retried; it is enriched
- **Holds because:** retrying a saturated pool only adds waiters. `POOL_TIMEOUT_MARKER` ("connection pool timeout") is deliberately absent from `STALE_CONNECTION_PATTERNS`, and `enrich_pool_timeout_error` returns a **same-class** replacement whose message carries live `pool_stats` and a concrete next step, so callers rescuing `PGMQ::Errors::ConnectionError` behave identically. Enrichment never raises: `pool_stats` already rescues to `{}` and a formatting failure falls back to the original error.
- **Where:** `lib/pgbus/client.rb#pool_timeout_error?`, `#enrich_pool_timeout_error`, `#with_stale_connection_retry`
- **Proven by:** `spec/pgbus/client_spec.rb:"returns pgmq pool stats merged with the configured pool_timeout"` and the enrichment examples in the same file
- **Origin:** the constants' own comments; reinforced by the #460 review's safe-direction discussion

### Notify-setup lock failures are retried, but never on a shared connection inside a caller transaction
- **Holds because:** a lock error inside the caller's transaction leaves the connection in `PQTRANS_INERROR`; re-entering the yielded block on that socket cannot recover and would replace the deadlock with "current transaction is aborted", hiding the real error. `NotifyLockRetry` re-raises the original when `queue_ddl_rides_caller_transaction?`. Elsewhere it retries `ATTEMPTS` = 3 with `DELAYS = [0.05, 0.15, 0.35]`, sleeping outside `@pgmq_mutex`, and it wraps the **whole** stream-queue tables attempt — the create path's 250 ms `enable_notify_if_needed` as well as the 0 ms stream override, since the first `DROP TRIGGER` can deadlock the same way. A bare statement timeout (no `while locking` context) and permission errors fail fast; a missing-queue `ConnectionError` uses the existing one-time queue-recreation path in `ensure_stream_queue`, outside the lock-retry block.
- **Where:** `lib/pgbus/client/notify_lock_retry.rb` (`LOCK_FAILURE_PATTERN`, `STATEMENT_TIMEOUT`, `LOCK_WAIT_CONTEXT`, `.retryable?`); `lib/pgbus/client/ensure_stream_queue.rb`
- **Proven by:** `spec/pgbus/client/notify_lock_retry_spec.rb` (the retryable/not-retryable table and the retry-budget examples); `spec/pgbus/client/ensure_stream_queue_spec.rb:"re-raises the original lock error when the caller transaction is aborted"`, and the same file asserts exactly one `enable_notify_insert` call `.with("pgbus_test_chat", throttle_interval_ms: 0).once`
- **Origin:** PR #457

### Per-row presence probes keep their own connection model
- **Holds because:** `message_exists?`, `message_archived?` and `message_with_job_id?` follow the established per-call checkout the uniqueness reaper already uses. Sharing one checkout across a sweep page is a real optimisation but it introduces a second access path for one caller, and the probes are not the sweep's cost centre.
- **Where:** `lib/pgbus/client.rb#message_exists?`, `#message_archived?`, `#message_with_job_id?`
- **Proven by:** no test (a non-change; the probes' behaviour is covered by `spec/pgbus/batch_sweep_spec.rb` and `spec/integration/dispatcher_reaper_spec.rb`)
- **Origin:** cubic learning 1c628345; PR #420 — recorded so the suggestion is not re-implemented
