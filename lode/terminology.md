# Terminology

The words this repository uses, and what they mean here. Several of them are
overloaded across the industry; the local meaning is the one that matters.

## Queues and messages

- **logical queue** — the name an app writes: `"default"`, `"critical"`. Never
  the name in PostgreSQL.
- **physical queue** — `"#{queue_prefix}_#{logical}"` (`Configuration#queue_name`),
  the name PGMQ knows and `pgmq.q_<name>` / `pgmq.a_<name>` are built from.
  Under priority routing it grows a `_pN` suffix (`Configuration#priority_queue_name`).
- **DLQ** — `"#{physical}#{Pgbus::DEAD_LETTER_SUFFIX}"`, i.e. `_dlq`. The suffix
  is a hard-coded constant, not a setting (`lib/pgbus.rb:11`). Priority
  sub-queues share their logical queue's one DLQ
  (`Client#dead_letter_physical_name`).
- **VT / visibility timeout** — PGMQ's invisibility window on a read message.
  pgbus treats it as the execution lock: while VT holds, no other worker can
  take the message.
- **read_ct** — PGMQ's per-message delivery counter. `read_ct > max_retries` is
  the dead-letter trigger (`ActiveJob::Executor#execute`).
- **archive** — PGMQ's move from `q_` to `a_`. In pgbus it is the *exact-once
  claim* on an execution: whoever archives owns the completion signals.
- **tri-state probe** — `Client#message_exists?`, `#message_in_queue?` (a
  delegate), `#message_archived?` and `#message_with_job_id?` return
  `true` / `false` / `nil`, where `nil` means "could not determine". Every
  caller must read `nil` as "still there" (`Batch::Sweep#classify_stale` and
  `#classify_orphan` test `!= false`; `Dispatcher#message_gone?` tests
  `== false`); the reapers never delete in doubt.

## Processes

- **supervisor** — the parent process (`pgbus start`). Forks children, reaps
  them, restarts with backoff, runs a watchdog.
- **role** — one of `:workers`, `:dispatcher`, `:scheduler`, `:consumers`,
  `:outbox`; gated by `Configuration#role_enabled?` and narrowed by the
  `--workers-only` / `--scheduler-only` / `--dispatcher-only` CLI flags.
- **capsule** — one entry in `config.workers`: a set of queues, a thread count
  and an execution mode, forked as one worker process. Named via the capsule
  DSL (`lib/pgbus/configuration/capsule_dsl.rb`).
- **worker** — a forked process that reads job queues and runs ActiveJob jobs.
- **consumer** — a forked process that reads event-bus queues and runs
  `EventBus::Handler` subclasses.
- **dispatcher** — the single maintenance process: sweeps, reaps, compacts,
  vacuums (`lib/pgbus/process/dispatcher.rb`).
- **recycling** — a worker exiting on purpose after `max_jobs_per_worker`,
  `max_memory_mb` or `max_worker_lifetime`; the supervisor restarts it
  immediately (a clean exit resets the crash streak).
- **liveness pipe / wake pipe** — two opposite-direction `IO.pipe`s per fork.
  The child writes a byte per loop on the liveness pipe (parent watchdog reads
  it); the `NotifyHub` writes W/H/P bytes on the wake pipe (child reads them).

## Admission control

- **concurrency key** — the string a `limits_concurrency key:` proc returns;
  defaults to the *enqueued* job's class name. At most `to:` jobs run per key.
- **semaphore** — a `pgbus_semaphores` row holding `value`, `max_value` and
  `expires_at` for one key. `expires_at` is a *silence* lease, not a run-time
  cap: the visibility heartbeat renews it while the job runs.
- **parked job / blocked execution** — a `pgbus_blocked_executions` row: a job
  that hit its limit under `on_conflict: :block`. A parked job is never deleted
  for age; the only way out is promotion.
- **promotion** — moving a parked job back into PGMQ after taking a slot
  (`Concurrency::BlockedExecution.promote_next`).
- **uniqueness lock** — a `pgbus_uniqueness_keys` row keyed on `lock_key`.
  `:until_executed` takes it at enqueue and holds it through execution;
  `:while_executing` takes it at execution start.
- **bound / unbound lock** — bound means the row carries the real
  `queue_name` + `msg_id` written by `UniquenessKey.bind!` after the send.
  Unbound means `msg_id = 0` and/or the synthetic `pending` queue
  (`Uniqueness::PLACEHOLDER_QUEUE`) — a row whose send has not landed yet.
- **bind stamp** — a thread-local `created_at` recorded by
  `UniquenessKey.acquire!` so `bind!` can only retarget *this* enqueue's row and
  never a successor's.

## Batches

- **batch** — a `Pgbus::Batch` handle over a `pgbus_batches` row
  (`Pgbus::BatchEntry`). Jobs enqueued inside `Batch#enqueue`'s block join it.
- **execution row** — a `pgbus_batch_executions` row, one per outstanding job,
  inserted *before* the send. The invariant is
  `total_jobs == outstanding rows + completed_jobs + failed_jobs`.
- **open batch** — an unfinished batch that `#enqueue` may be called on again.
- **migrated** — shorthand for "the `pgbus_batch_executions` table exists"
  (`Batch.executions_migrated?`). Installs before that migration run a legacy
  counter-only path.

## Streams

- **stream** — a logical SSE channel (`Pgbus.stream(name)`), backed by its own
  PGMQ queue when durable.
- **durable vs ephemeral** — durable stores the frame in PGMQ and NOTIFYs a bare
  wake; ephemeral rides the NOTIFY payload itself, so it is capped at
  `Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES` (7999 — PostgreSQL rejects
  a NOTIFY payload of 8000 bytes or more). An oversized *ephemeral broadcast*
  does not raise: `Stream#broadcast_ephemeral` logs a warning and publishes
  durably instead (`#durable_fallback`). `Streams::PayloadTooLarge` is raised
  only by `Client#notify_stream` when something calls it directly.
- **master hub** — one LISTEN connection per web host, in the Puma master,
  fanning wakes to workers over a Unix socket (`Web::Streamer::MasterHub`).
  Its absence is not an error: each worker falls back to its own listener.
- **cursor** — the highest `msg_id` a connection has been sent; replay is
  `read_after(cursor)`.
