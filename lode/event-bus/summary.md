# Event bus — publish, route, handle once

AMQP-style topic routing on PGMQ. Five files under `lib/pgbus/event_bus/`:
`publisher.rb` (96), `registry.rb` (123), `subscriber.rb` (34), `handler.rb`
(189), `stale_connection_retry.rb` (85). A consumer process
(`lib/pgbus/process/consumer.rb`, 550) reads the subscriber queues and calls the
handlers.

## Publish

`Pgbus::EventBus::Publisher.publish(routing_key, payload, headers:, delay:)`
builds the envelope (`event_id`, `payload`, `published_at`), tags it with fair
share when `config.event_fair_share` is set, tags it with the publisher's
`CurrentAttributes` (issue #431), and hands it to
`Client#publish_to_topic` → `Client#bind_topic`. Under `Pgbus::Testing` the
event is pushed into the test store instead, and in `inline?` mode with no delay
the matching handlers run synchronously in the publisher's `Current` context.

`Outbox.publish_event` shares `tag_fair_share`, so the fair-share key is
resolved where the publisher's `Current.*` still exists and rides the outbox row
to the bus.

## Route

`Registry#subscribe(pattern, handler_class, queue_name:)` appends a
`Subscriber` under a mutex; `#handlers_for(routing_key)` selects by AMQP
wildcard match. `#setup_all!` creates each subscriber's queue and binds its
topic — `safe: true` is the boot-time form: it skips entirely under a
`db:`/`assets:` rake task (opening a PGMQ connection there would block
`DROP DATABASE`) and downgrades a genuine *connection* failure to a warning,
while a `PG::Error` subclass such as a permission or missing-table error still
surfaces, because that is a real setup bug.

`#event_queue_names` exists so a wildcard worker (`queues: ['*']`) can exclude
event queues: an event payload is not an ActiveJob job, and a job worker that
adopted one would fail to deserialize and dead-letter it (issue #333).
`#queue_names_for_topics` gives the supervisor's `NotifyHub` the same LISTEN set
the consumer forks actually read; the overlap check is deliberately coarse (any
topic filter ending in `#` claims every subscriber) so the hub listens to too
much rather than too little.

## Handle, exactly once

`Handler#process` wraps `#process!` in `Rails.application.executor` (the
reloader in development), the same way `ActiveJob::Executor#execute_job` does,
so an AR connection leased by the claim or by `handle` goes back to the pool.
Without the wrap every consumed event leaks one connection on the consumer
thread.

Idempotency is a **two-phase claim** against `pgbus_processed_events`
(unique on `(event_id, handler_class)`):

1. `claim_idempotency?` — an in-memory dedup cache short-circuit, then
   `INSERT … unique_by: [event_id, handler_class]`. An inserted row means the
   claim is ours. On a conflict it reads `completed_at`: `nil` means a *pending*
   claim (a previous attempt died, or the row was purged) and the event re-runs;
   a timestamp means a completed execution, so the event is cached and skipped.
2. `complete_claim!` — `UPDATE … SET completed_at = now` after `handle`
   returns, and only then is the key admitted to the dedup cache.

Only completed executions enter the cache. On a schema without the
`completed_at` column (`ProcessedEvent.completion_column?`, detected once under
a mutex) this degrades to the old single-phase claim, cached at claim time.

`handle` raising leaves the message for VT redelivery: at-least-once, never a
silent drop. `pgbus.event_processed` and `pgbus.event_failed` are instrumented
either way. `Consumer#handle_message` then records a `Pgbus::JobStat` row with
`job_class: "EventConsumer"` and `status: "failed"`, trips the per-queue circuit
breaker, and lets `read_ct` carry the message to the DLQ once it passes
`config.max_retries`. Note the asymmetry with jobs: `pgbus_failed_events` rows
are written only by `ActiveJob::Executor#handle_failure` — the consumer path has
no such row.

## The one retry that is safe

`complete_claim!` — and only it — is wrapped in
`EventBus::StaleConnectionRetry`. It is the single AR write that happens **after**
`handle` has already succeeded, and `Relation#update_all` is marked
`allow_retry: false`, so a pooler restart or an admin disconnect there paged the
host app for work that was in fact done. The retry reconnects *this thread's*
leases via `ConnectionPool#active_connection?` (never `clear_all_connections!`,
which would yank sockets out from under sibling consumers) and repeats the
statement once; a second drop raises.

`TRANSIENT_DROP` matches `PQconsumeInput`, `server closed the connection
unexpectedly`, `unexpected eof while reading`, `ssl syscall error`. That is
deliberately **wider** than `Client::STALE_CONNECTION_PATTERNS`, because the
only statement repeated here is an idempotent `SET completed_at = <now>`.
Phase 1 is deliberately **not** wrapped: its INSERT may have committed before
the socket died, and on a legacy schema the retry's empty `result.rows` would
read as "another consumer owns this claim" — turning a recoverable drop into a
silently skipped event.

See also: [../process/summary.md](../process/summary.md),
[../review/event-bus-and-maintenance.md](../review/event-bus-and-maintenance.md).
