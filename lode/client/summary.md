# Client — the only door to PGMQ

`lib/pgbus/client.rb` (2134 lines; `Pgbus::Client` spans lines 14-2133) wraps
`pgmq-ruby`. No PGMQ *operation* is issued from anywhere else — outside
`lib/pgbus/client/` the only `PGMQ::` references in the tree are
`PGMQ::Errors::ConnectionError` in a rescue or a `defined?` guard
(`active_job/adapter.rb`, `active_job/executor.rb`, `event_bus/registry.rb`)
and four comments naming a PGMQ class (`process/worker.rb`, `streams.rb`,
`queue_name_validator.rb`, `streams/key.rb`). Building a physical
name is likewise the Client's and `Configuration`'s job; other subsystems only
ever *strip* the prefix off a name PGMQ handed back
(`delete_prefix("#{config.queue_prefix}_")` in `process/worker.rb`,
`process/consumer.rb`, `process/dispatcher.rb`, `process/wildcard_queue_resolver.rb`
and `web/data_source.rb`). Seven collaborators live under `lib/pgbus/client/`;
five of them are mixed in (`client.rb:15-19`: `ReadAfter`, `FairRead`,
`NotifyLockRetry`, `EnsureStreamQueue`, `NotifyStream`) and two are
collaborating objects (`ConnectionHealth`, `ResizablePool`).

## Two connection shapes, decided once in `#initialize`

`Client#initialize` (lines 94-185) branches on `config.connection_options`:

| `connection_options` | `shared_connection?` | Pool | Serialization | Streams pool |
|---|---|---|---|---|
| `Proc` (Rails `-> { AR…raw_connection }`) | true | forced `pool_size: 1` | every call through `#synchronized` (a `Mutex`) | none — `@streams_pgmq` aliases `@pgmq` |
| `String` URL or `Hash` params | false | `config.resolved_pool_size` | none — pgmq-ruby's pool | its own `PGMQ::Client` sized by `streams_pool_size` |

The Proc path returns the *same* `PG::Connection` ActiveRecord is using. libpq
is not thread-safe, so concurrency there is a mutex, not a pool. The dedicated
path additionally bakes read bounds into the connection options
(`#apply_connection_bounds`, `#wrap_session_gucs`): a server-side
`statement_timeout` for a slow query plus client-side `tcp_user_timeout` and
keepalives for a dead socket. Those GUCs are applied *only* on this branch —
on the shared path a `statement_timeout` would leak into application queries.

## Reads are bounded and gated; writes are not

`#guarded_read` runs every read through `Client::ConnectionHealth`, an in-memory
circuit breaker: after enough consecutive connection failures the breaker opens
and reads raise `Pgbus::ConnectionCircuitOpenError` *before* a pool checkout, so
a dead database is not re-polled by the whole fleet. Writes are deliberately not
gated — an enqueue failure must reach the caller.

`#with_read_timeout` layers three bounds, cleanest first: `statement_timeout`
(server cancel → `Pgbus::ReadTimeoutError`), `tcp_user_timeout`/keepalives
(libpq raises `PG::ConnectionBad` synchronously, no `Thread#raise`), and, only
where libpq cannot bound a hung socket, a Ruby `Timeout` last resort that raises
`Client::WedgedReadTimeout` — a `ReadTimeoutError` subclass whose distinct class
tells `#reload_pool_after_wedged_timeout` that the pool needs reloading, where a
clean server-side cancel does not. `@libpq_read_bounds_effective` is computed
once in `#initialize` because every input to it is fixed for a Client's life.

## Retry is narrow on purpose

`#with_stale_connection_retry` wraps every `@pgmq.*` call site. It retries only
errors matching `STALE_CONNECTION_PATTERN` — a compiled union of
substrings (`STALE_CONNECTION_PATTERNS`, seven entries) that all mean *the socket was already
dead before pgmq-ruby used it*. Mid-flight shapes such as "server closed the
connection unexpectedly" are excluded, because a half-committed enqueue would
duplicate a message on retry. `STALE_RETRY_ATTEMPTS` is 2 with
`STALE_RETRY_DELAYS = [0.1, 0.5]`, and the `sleep` happens in the `rescue`,
outside the yielded block, so it never runs while `@pgmq_mutex` is held.

A pool-checkout timeout is explicitly *not* in that list
(`POOL_TIMEOUT_MARKER`): retrying a saturated pool only adds waiters.
`#enrich_pool_timeout_error` instead returns a same-class error whose message
carries live pool stats and a next step.

## Bootstrap: schema, queues, triggers

Three pieces of DDL each have a cross-process race and each resolves it the same
way — do it, and treat the duplicate error as proof someone else won.

- **PGMQ schema** — `#ensure_pgmq_schema` takes the *class-level*
  `pgmq_install_mutex` (process-wide, because two Client instances share one
  libpq connection on the Proc path) and then `#synchronized`; that lock order
  is fixed and never reversed. Inside, `pg_advisory_xact_lock(PGMQ_INSTALL_LOCK_KEY)`
  serializes across processes. If the connection is already inside a caller
  transaction (`#inside_caller_transaction?`) the install rides a
  `SAVEPOINT pgbus_pgmq_install` instead of owning `BEGIN`/`COMMIT` — and then
  `@schema_ensured` stays false, because a caller rollback would take the schema
  with it. Only the owned-COMMIT path caches.
- **Queue tables** — `#create_queue_table` calls `pgmq.create`, and on a
  duplicate-relation error re-checks `pgmq.meta` (`#queue_registered?`) before
  retrying once. `#ensure_single_queue` skips the `@queues_created` memo entirely
  when `#queue_ddl_rides_caller_transaction?`, for the same durability reason.
- **NOTIFY trigger** — `#enable_notify_if_needed` checks
  `#notify_trigger_current?` first (name *and* throttle interval), and on a
  duplicate re-checks then retries once, so a job-queue ensure (250 ms) racing a
  stream override (0 ms) converges. `#duplicate_notify_trigger_error?` requires
  the `NOTIFY_TRIGGER_NAME` identifier *plus* either a `PG::DuplicateObject`
  cause or the English "already exists" text — a localized message with no cause
  propagates rather than being swallowed.

## Presence probes

`#message_exists?`, `#message_archived?`, `#message_with_job_id?` and
`#uniqueness_keys_present` are the reapers' eyes. They expand a logical name to
every physical table the queue strategy owns, run per-table probes, and return
the tri-state described in [terminology](../terminology.md). `#uniqueness_keys_present`
skips any queue whose name ends in `_dlq`: a dead-lettered copy still carries the
uniqueness key, but the executor already released the lock when it moved the
message.

See also: [review/client.md](../review/client.md), [schema/summary.md](../schema/summary.md).
