Rules about the event bus's completion stamp, failure persistence, and the
dispatcher's maintenance rake tasks. See
[../event-bus/summary.md](../event-bus/summary.md).

### Only the completion stamp is retried on a stale ActiveRecord socket
- **Holds because:** `complete_claim!` is the one AR write that happens **after** `handle` has already succeeded, and `Relation#update_all` is marked `allow_retry: false`, so a pooler restart or admin disconnect there raised `ActiveRecord::ConnectionFailed` for work that was in fact done — a false page, plus a PGMQ redelivery and a re-run. The statement is an idempotent `SET completed_at = <now>`, so repeating one that may already have committed is safe. Phase 1 (`claim_idempotency?`) is deliberately **not** wrapped: its INSERT may have committed before the socket died, and on a legacy schema the retry's empty `result.rows` would read as "another consumer owns this claim", turning a recoverable drop into a silently skipped event — VT redelivery is the correct recovery there.
- **Safe direction:** repeating an idempotent stamp is free; repeating a claim silently drops an event.
- **Where:** `lib/pgbus/event_bus/handler.rb#complete_claim!`, `#claim_idempotency?`; `lib/pgbus/event_bus/stale_connection_retry.rb#call`
- **Proven by:** `spec/pgbus/event_bus/stale_connection_retry_spec.rb` (table-driven over all four `TRANSIENT_DROP` shapes) and `spec/pgbus/event_bus/handler_spec.rb`
- **Origin:** cubic learning f8cba24b; PR #467

### `TRANSIENT_DROP` is wider than the client's list, and includes `ssl syscall error`
- **Holds because:** libpq reports the same peer disappearance two ways — `unexpected eof while reading` from the TLS layer and `SSL SYSCALL error: EOF detected` from the syscall layer — so omitting the second re-raises on a drop the first would have retried. The list is `PQconsumeInput`, `server closed the connection unexpectedly`, `unexpected eof while reading`, `ssl syscall error`, matched against the error **and its cause**. It is wider than `Client::STALE_CONNECTION_PATTERNS` for the opposite reason that list is narrow: the client omits mid-flight shapes because a half-committed *enqueue* would duplicate a message. Same vocabulary, different safety argument — never widen one by citing the other. A refused or timed-out *connection* is an outage, not a drop, and is not retried.
- **Where:** `lib/pgbus/event_bus/stale_connection_retry.rb` (`TRANSIENT_DROP`, `.transient_drop?`); `lib/pgbus/client.rb` (`STALE_CONNECTION_PATTERNS`)
- **Proven by:** `spec/pgbus/event_bus/stale_connection_retry_spec.rb`
- **Origin:** cubic learning f8cba24b (including its "do not apply when" clause); PR #467

### The reconnect touches only this thread's leases
- **Holds because:** `clear_all_connections!` would yank sockets out from under sibling consumers sharing the process, turning one recoverable drop into many. `reconnect_leased!` walks `each_connection_pool(:all)` and reconnects only where `pool.active_connection?` returns a lease. It must be `active_connection?`, not `active_connection`: the latter is a `:nodoc:` alias that does not exist at all before Rails 7.2, which is below the gemspec's `railties >= 7.1` floor — so on the 7.1 CI leg the retry would have raised `NoMethodError` on the one path whose purpose is recovering from an error.
- **Where:** `lib/pgbus/event_bus/stale_connection_retry.rb#reconnect_leased!`; the same accessor choice in `lib/pgbus/streams.rb#current_open_transaction`
- **Proven by:** `spec/pgbus/event_bus/stale_connection_retry_spec.rb` runs `reconnect_leased!` for real against `instance_double`s of `ConnectionPool` and `AbstractAdapter`, which reject a method the real classes do not define — restoring the bad accessor fails them
- **Origin:** PR #467 (the review that found it, and the fact that every earlier example had stubbed the method so CI was green on a broken one)

### A failed job execution is persisted in `pgbus_failed_events`; a failed event delivery is not
- **Holds because:** `FailedEventRecorder.record!` is called only by `ActiveJob::Executor#handle_failure`, and the row is what makes a failed job inspectable on the dashboard, what `Executor` clears on a later success or dead-letter, and what `Worker#detect_zombie` reads to tell "the previous read recorded a failure" from "the worker crashed mid-execute". The event-bus consumer takes a different route: `Consumer#handle_message` rescues, records a `Pgbus::JobStat` with `job_class: "EventConsumer"` and `status: "failed"`, trips the per-queue circuit breaker, and lets `read_ct` carry the message to the DLQ past `config.max_retries`. Do not describe the two paths as one.
- **Where:** `lib/pgbus/failed_event_recorder.rb`; `lib/pgbus/active_job/executor.rb#handle_failure`, `#execute`; `lib/pgbus/process/worker.rb#detect_zombie`; `lib/pgbus/process/consumer.rb#handle_message`, `#record_stat`
- **Proven by:** `spec/pgbus/active_job/executor_spec.rb` (the failed-event recording and clearing examples); `spec/integration/dashboard_failed_event_handlers_spec.rb`
- **Origin:** cubic learnings 2cb6a24e and d797d796 (duplicates of one rule), narrowed to what the code does

### `pgbus:tune_autovacuum` on a dedicated database disconnects its pool in an `ensure`
- **Holds because:** the task checks out a `BusRecord` connection for a database the rest of the process does not use; leaving that pool connected after the task raises keeps a backend open for the life of the rake process. The `with_connection` checkout is wrapped in `begin`/`ensure` and the pool is disconnected either way. On a dedicated database the task must never reach `ActiveRecord::Base.connection` — that is the primary, and tuning would run against the wrong database.
- **Where:** `lib/tasks/pgbus_autovacuum.rake`; `lib/pgbus/bus_record.rb` (the pool-disconnect helper)
- **Proven by:** `spec/pgbus/autovacuum_rake_spec.rb` — the dedicated-database examples stub `ActiveRecord::Base.connection` and assert it is never received when `Pgbus.configuration.connects_to` is set
- **Origin:** cubic learnings f8ae4bc0, 276a8f94

### `DatabaseTasksGuard` disconnects every pool before delegating a purge or drop
- **Holds because:** an open pgbus connection to the database being dropped blocks `DROP DATABASE` — the symptom `db:test:purge` produced (issue #409). The ordering is the contract: `disconnect_all_pools!` runs *before* the delegated operation, so a test asserts the interleaving, not just that both happened.
- **Where:** `lib/pgbus/database_tasks_guard.rb`; `lib/pgbus.rb` (`DATABASE_TASK_PREFIXES`, `Pgbus.database_task?`)
- **Proven by:** `spec/pgbus/database_tasks_guard_spec.rb` — the purge/drop examples record call interleaving
- **Origin:** cubic learning 437ad3fd
