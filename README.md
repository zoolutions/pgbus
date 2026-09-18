# Pgbus

PostgreSQL-native job processing and event bus for Rails, built on [PGMQ](https://github.com/tembo-io/pgmq).

**Why Pgbus?** If you already run PostgreSQL, you don't need Redis for background jobs. Pgbus gives you ActiveJob integration, AMQP-style topic routing, dead letter queues, worker memory management, and a live dashboard -- all backed by your existing database.

📖 **Documentation:** [pgbus.zoolutions.llc](https://pgbus.zoolutions.llc) — guides, flow diagrams, and a full configuration reference. (This README stays the canonical GitHub reference.)

[![Ruby](https://github.com/zoolutions/pgbus/actions/workflows/main.yml/badge.svg)](https://github.com/zoolutions/pgbus/actions/workflows/main.yml)

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
  - [1. Configure (optional)](#1-configure-optional)
  - [2. Use as ActiveJob backend](#2-use-as-activejob-backend)
  - [3. Event bus (optional)](#3-event-bus-optional)
  - [4. Start workers](#4-start-workers)
  - [5. Mount the dashboard](#5-mount-the-dashboard)
- [Reliability](#reliability)
  - [Job uniqueness](#job-uniqueness)
  - [Concurrency controls](#concurrency-controls)
  - [Circuit breaker and queue pause/resume](#circuit-breaker-and-queue-pauseresume)
  - [Client-level circuit breaker (database-down)](#client-level-circuit-breaker-database-down)
  - [Read timeouts (libpq-native)](#read-timeouts-libpq-native)
  - [Prefetch flow control](#prefetch-flow-control)
  - [Worker recycling](#worker-recycling)
  - [Retry backoff](#retry-backoff)
- [Routing and ordering](#routing-and-ordering)
  - [Priority queues](#priority-queues)
  - [Consumer priority](#consumer-priority)
  - [Single active consumer](#single-active-consumer)
- [Persistence and batching](#persistence-and-batching)
  - [Batches](#batches)
  - [Transactional outbox](#transactional-outbox)
  - [Archive compaction](#archive-compaction)
- [Observability](#observability)
  - [Error reporting](#error-reporting)
  - [Connection pool metrics](#connection-pool-metrics)
  - [Structured logging](#structured-logging)
  - [Queue health monitoring](#queue-health-monitoring)
  - [Health endpoints (liveness / readiness)](#health-endpoints-liveness--readiness)
  - [Boot diagnostics banner](#boot-diagnostics-banner)
- [Real-time broadcasts](#real-time-broadcasts-turbo-streams-replacement)
- [Testing](#testing)
  - [RSpec setup](#rspec-setup)
  - [Minitest / TestUnit setup](#minitest--testunit-setup)
  - [Event bus assertions](#event-bus-assertions)
  - [RSpec matchers](#rspec-matchers)
  - [Testing modes](#testing-modes)
  - [SSE streams in tests](#sse-streams-in-tests)
- [Operations](#operations)
  - [CLI](#cli)
  - [Dashboard](#dashboard)
  - [Database tables](#database-tables)
  - [Switching from another backend](#switching-from-another-backend)
- [Reference](#reference)
  - [Architecture](#architecture)
  - [Configuration reference](#configuration-reference)
- [Development](#development)
- [License](#license)

## Features

- **ActiveJob adapter** -- drop-in replacement, zero config migration from other backends
- **Turbo Streams replacement** -- `pgbus_stream_from` drops into turbo-rails apps with no ActionCable, no Redis, no lost messages on reconnect. Includes transactional broadcasts (deferred until commit), backlog replay on connect, server-side audience filtering, and presence tracking. Fixes rails/rails#52420, hotwired/turbo#1261, and hotwired/turbo-rails#674.
- **Event bus** -- publish/subscribe with AMQP-style topic routing (`orders.#`, `payments.*`)
- **Dead letter queues** -- automatic DLQ routing after configurable retries
- **Worker recycling** -- memory, job count, and lifetime limits prevent runaway processes
- **LISTEN/NOTIFY** -- instant wake-up, polling as fallback only
- **Idempotent events** -- deduplication via `(event_id, handler_class)` unique index with in-memory cache
- **Live dashboard** -- Turbo Frames auto-refresh with throughput rate, no ActionCable required
- **Supervisor/worker model** -- forked processes with heartbeat monitoring and lifecycle state machine
- **Priority queues** -- route jobs to priority sub-queues, highest-priority-first processing
- **Circuit breaker** -- auto-pause queues after consecutive failures, exponential backoff
- **Queue pause/resume** -- manual or automatic via dashboard
- **Prefetch flow control** -- cap in-flight messages per worker to prevent overload
- **Archive compaction** -- automatic purge of old archived messages
- **Transactional outbox** -- publish events atomically inside database transactions
- **Single active consumer** -- advisory-lock-based exclusive queue processing for strict ordering
- **Consumer priority** -- higher-priority workers get first dibs, lower-priority workers back off
- **Job uniqueness** -- prevent duplicate jobs with reaper-based crash recovery, no TTL-driven expiry
- **Retry backoff** -- exponential backoff with jitter for VT-based retries, per-job overrides
- **Error reporting** -- pluggable error reporters for APM integration (Appsignal, Sentry, etc.)
- **Structured logging** -- JSON log formatter with component extraction and thread-local context
- **Queue health** -- dead tuple monitoring, autovacuum tuning, Prometheus metrics

## Requirements

- Ruby >= 3.3
- Rails >= 7.1
- PostgreSQL with the [PGMQ extension](https://github.com/tembo-io/pgmq)

## Installation

Add to your Gemfile:

```ruby
gem "pgbus"
```

Then install the PGMQ extension in your database:

```sql
CREATE EXTENSION IF NOT EXISTS pgmq;
```

## Quick start

### 1. Configure (optional)

Pgbus works with zero config in Rails -- it uses your existing `ActiveRecord` connection. For custom setups, drop a Ruby initializer:

```ruby
# config/initializers/pgbus.rb
Pgbus.configure do |c|
  c.queue_prefix      = "myapp"
  c.max_retries       = 5
  c.visibility_timeout = 30.seconds   # ActiveSupport::Duration accepted
  c.idempotency_ttl   = 7.days

  # Worker recycling — prevents long-lived processes from leaking memory
  c.max_jobs_per_worker = 10_000
  c.max_memory_mb       = 512
  c.max_worker_lifetime = 1.hour

  # Capsule string DSL — Sidekiq-style "queues: threads; queues: threads"
  c.workers = "default, mailers: 10; critical: 5"

  # Or use named capsules with advanced options
  c.capsule :ordered, queues: %w[ordered_events], threads: 1, single_active_consumer: true
end
```

The capsule string DSL is the shortest form for the common case. Use `c.capsule` when you need named capsules with advanced options like `single_active_consumer` or `consumer_priority`. See [Routing and ordering](#routing-and-ordering) for the full set.

Configuration is validated eagerly: `Pgbus.configure` runs `Configuration#validate!` right after your block yields, so an invalid value (`visibility_timeout = 0`, for example) raises `Pgbus::ConfigurationError` at boot instead of failing later inside a worker. Set `c.eager_validation = false` for the rare setup that intentionally holds a transiently-invalid config across sequential `configure` calls.

> **Upgrading from an older pgbus?** Run `rails generate pgbus:update`. It inspects your live database and adds any missing pgbus migrations to `db/migrate` (or `db/pgbus_migrate` if you use `connects_to`). The generator detects your separate-database config automatically from `Pgbus.configuration.connects_to` or by scanning the initializer / `config/application.rb`, so you don't have to re-specify `--database=pgbus` every time.
>
> Useful flags: `--dry-run` (print the plan without creating files), `--skip-migrations`, `--quiet`. Running it on a database with no pgbus tables at all will redirect you to `pgbus:install` instead of stacking individual add_* migrations.
>
> **YAML config was removed in 1.0.** `config/pgbus.yml` is no longer loaded at boot; if one is present, pgbus warns once at boot that it's inert. Port its settings into `config/initializers/pgbus.rb` as a `Pgbus.configure` block (see the example above) and delete the YAML — the last release to auto-convert it was 0.9.x via `pgbus:update`.
>
> For the full step-by-step procedure (including the vendored PGMQ schema check and post-upgrade verification with `pgbus doctor`) plus per-version breaking changes, see [Upgrading pgbus](https://pgbus.zoolutions.llc/docs/upgrading-pgbus).

### 2. Use as ActiveJob backend

```ruby
# config/application.rb
config.active_job.queue_adapter = :pgbus
```

That's it. Your existing jobs work unchanged:

```ruby
class OrderConfirmationJob < ApplicationJob
  queue_as :mailers

  def perform(order)
    OrderMailer.confirmation(order).deliver_now
  end
end

# Enqueue
OrderConfirmationJob.perform_later(order)

# Schedule
OrderConfirmationJob.set(wait: 5.minutes).perform_later(order)
```

### 3. Event bus (optional)

Publish events with AMQP-style topic routing:

```ruby
# Publish an event
Pgbus.publish(
  "orders.created",
  { order_id: order.id, total: order.total }
)

# Publish with delay
Pgbus.publish_later(
  "invoices.due",
  { invoice_id: invoice.id },
  delay: 30.days
)
```

`Pgbus.publish` / `Pgbus.publish_later` are top-level shortcuts for
`Pgbus::EventBus::Publisher.publish` / `.publish_later` (symmetric with
`Pgbus.stream`). The long form still works.

Subscribe with handlers:

```ruby
# app/handlers/order_created_handler.rb
class OrderCreatedHandler < Pgbus::EventBus::Handler
  idempotent!  # Deduplicate by (event_id, handler_class)

  def handle(event)
    order_id = event.payload["order_id"]
    Analytics.track_order(order_id)
    InventoryService.reserve(order_id)
  end
end

# Register in an initializer
Pgbus::EventBus::Registry.instance.subscribe(
  "orders.created",
  OrderCreatedHandler
)

# Wildcard patterns
Pgbus::EventBus::Registry.instance.subscribe(
  "orders.#",           # matches orders.created, orders.updated, orders.shipped.confirmed
  OrderAuditHandler
)
```

Each subscriber gets its own queue, and a queue is consumed only by the
handler(s) registered against it: one published event matching N subscribers
becomes N deliveries, one per handler, so every handler runs exactly once per
event. (Two handlers sharing an explicit `queue_name:` both run on that queue's
delivery.) A message on a queue no subscriber in this process owns — a stale
topic binding left by a renamed or removed handler — is archived, logged once
per queue, and reported as `pgbus.event_unrouted`.

`idempotent!` uses a **two-phase claim**: a *pending* row in
`pgbus_processed_events` is inserted before `handle` runs, and only stamped
`completed_at` after `handle` returns. Deduplication applies to **completed**
executions only — if the consumer process is killed mid-handler (deploy,
OOM, supervisor watchdog), the redelivered message finds the pending claim
and **re-runs the handler** instead of silently skipping it. The semantics
are at-least-once with dedup of completed executions: the only
double-execution window is a handler still running past its visibility
timeout — the same window every non-idempotent handler already has.
Installs created before this feature need the upgrade migration:
`rails generate pgbus:add_processed_event_completion` (supports
`--database`); until it runs, idempotent handlers fall back to the old
single-phase claim and log a warning.

### 4. Start workers

```bash
bundle exec pgbus start
```

This boots a supervisor that manages:
- **Workers** -- process ActiveJob queues
- **Dispatcher** -- runs maintenance tasks (idempotency cleanup, stale process reaping)
- **Consumers** -- process event bus messages

### 5. Mount the dashboard

```ruby
# config/routes.rb
mount Pgbus::Engine => "/pgbus"
```

The dashboard shows queues, jobs, processes, failures, dead letter messages, and event subscribers. It auto-refreshes via Turbo Frames with no WebSocket dependency.

Protect it in production with a simple auth lambda:

```ruby
Pgbus.configure do |config|
  config.web_auth = ->(request) {
    request.env["warden"].user&.admin?
  }
end
```

Or inherit from your own authenticated controller (like mission_control-jobs):

```ruby
Pgbus.configure do |config|
  config.base_controller_class = "Admin::BaseController"
end
```

When `base_controller_class` is set, all dashboard controllers inherit from that class instead of `ActionController::Base`. This is the recommended approach when mounting the dashboard inside an authenticated namespace -- your base controller's `before_action` filters, helper methods, and authentication logic apply automatically without monkey-patching.

Add a "back to app" button in the dashboard nav to return to your main application:

```ruby
Pgbus.configure do |config|
  config.return_to_app_url = "/admin"
end
```

## Reliability

These features stop bad jobs from cascading into outages: deduplication, concurrency caps, automatic queue pausing on repeated failures, in-flight backpressure, and worker recycling.

### Job uniqueness

Prevent duplicate jobs from running. Unlike `limits_concurrency` (which controls *how many* jobs with the same key run), uniqueness guarantees *at most one* job with a given key exists in the system at any time.

```ruby
class ImportOrderJob < ApplicationJob
  ensures_uniqueness strategy: :until_executed,
                     key: ->(order_id) { "import-order-#{order_id}" },
                     on_conflict: :reject

  def perform(order_id)
    # Only ONE instance per order_id can exist — from enqueue through completion.
    # If another ImportOrderJob for this order_id is already enqueued or running,
    # the duplicate is rejected immediately.
  end
end
```

#### Strategies

| Strategy | Lock acquired | Lock released | Prevents |
|----------|--------------|---------------|----------|
| `:until_executed` | At enqueue | On completion or DLQ | Duplicate enqueue AND execution |
| `:while_executing` | At execution start | On completion or DLQ | Duplicate execution only |

#### Conflict policies

| Policy | Behavior |
|--------|----------|
| `:reject` | Raise `Pgbus::JobNotUnique` (default) |
| `:discard` | Silently drop the duplicate |
| `:log` | Log a warning and drop |

#### Lock lifecycle

The lock is **never released by a timer**. It is held as long as the job's message exists in the queue:

```text
Enqueue ──→ pgbus_uniqueness_keys (lock_key, queue_name, msg_id)
                  │
  Worker picks up and runs the job
                  │
          ┌───────┴───────┐
          ▼               ▼
      Success           Crash
      release!        (lock orphaned —
      (row deleted)    message still tracked
                        by lock_key/queue_name/msg_id)
                          │
                          ▼
                    Reaper checks:
                    Does the referenced message
                    still exist in the PGMQ queue?
                          │
                    ┌─────┴─────┐
                   Gone       Present
                    ▼            ▼
                release!      (keep lock,
                (orphaned)     job still in flight)
```

**Crash recovery** works through the reaper (runs periodically in the dispatcher's `cleanup_job_locks`). It checks whether the message referenced by each lock (`queue_name` + `msg_id`) still exists in the PGMQ queue via `Client#message_exists?`. If the message is gone (delivered, expired, or the queue was truncated) and the lock is older than `2 * visibility_timeout` — to avoid racing a lock whose `send_message` hasn't committed yet — the lock is released. A lock backed by a message still in the queue is never touched, even if it looks old, so a recurring job that fails and retries can hold its lock for hours.

#### Uniqueness vs concurrency controls

| | `ensures_uniqueness` | `limits_concurrency` |
|---|---|---|
| **Purpose** | Prevent duplicate jobs | Limit concurrent execution slots |
| **Lock type** | Binary lock (one or none) | Counting semaphore (up to N) |
| **At enqueue** | `:until_executed` blocks duplicates | Checks semaphore, blocks/discards/raises |
| **At execution** | `:while_executing` blocks duplicate runs | Not checked (semaphore acquired at enqueue) |
| **Duplicate in queue** | `:until_executed`: impossible. `:while_executing`: allowed, only one runs | Allowed up to N, rest blocked |
| **Crash recovery** | Reaper checks heartbeats | Semaphore `expires_at` + dispatcher cleanup |
| **Use when** | "This exact job must not run twice" | "At most N of these can run at once" |

**When to use which:**
- Payment processing, order import, unique email sends → `ensures_uniqueness`
- Rate-limited API calls, resource-constrained tasks → `limits_concurrency`
- Both at once → combine them (they use separate tables, no conflicts)

#### Setup

```bash
rails generate pgbus:add_uniqueness_keys                   # Add the migration
rails generate pgbus:add_uniqueness_keys --database=pgbus   # For separate database
```

### Concurrency controls

Limit how many jobs with the same key can run concurrently:

```ruby
class ProcessOrderJob < ApplicationJob
  limits_concurrency to: 1,
                     key: ->(order_id) { "ProcessOrder-#{order_id}" },
                     duration: 15.minutes,
                     on_conflict: :block

  def perform(order_id)
    # Only one job per order_id runs at a time
  end
end
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `to:` | (required) | Maximum concurrent jobs for the same key |
| `key:` | Job class name | Proc receiving job arguments, returns a string key |
| `duration:` | `15.minutes` | Safety expiry for the semaphore (crashed worker recovery) |
| `on_conflict:` | `:block` | What to do when the limit is reached |

#### Conflict strategies

| Strategy | Behavior |
|----------|----------|
| `:block` | Hold the job in a blocked queue. It is automatically released when a slot opens or the semaphore expires. |
| `:discard` | Silently drop the job. |
| `:raise` | Raise `Pgbus::ConcurrencyLimitExceeded` so the caller can handle it. |

#### How concurrency works

1. **Enqueue**: The adapter checks a semaphore table for the concurrency key. If under the limit, it increments the counter and sends the job to PGMQ. If at the limit, it applies the `on_conflict` strategy.
2. **Complete**: After a job succeeds or is dead-lettered, the executor signals the concurrency system via an `ensure` block (guaranteeing the signal fires even if the archive step fails). It first tries to promote a blocked job (atomic delete + enqueue in a single transaction). If nothing to promote, it releases the semaphore slot.
3. **Safety net**: The dispatcher periodically cleans up expired semaphores and orphaned blocked executions to recover from crashed workers.

#### Concurrency compared to other backends

Pgbus, SolidQueue, GoodJob, and Sidekiq all offer concurrency controls, but with fundamentally different locking strategies and trade-offs.

##### Architecture comparison

| | **Pgbus** | **SolidQueue** | **GoodJob** | **Sidekiq Enterprise** |
|---|---|---|---|---|
| **Lock backend** | PostgreSQL rows (`pgbus_semaphores` table) | PostgreSQL rows (`solid_queue_semaphores`) | PostgreSQL advisory locks (`pg_advisory_xact_lock`) | Redis sorted sets (lease-based) |
| **Lock granularity** | Counting semaphore (allows N concurrent) | Counting semaphore (allows N concurrent) | Count query under advisory lock | Sorted set entries with TTL |
| **Acquire mechanism** | Atomic `INSERT ... ON CONFLICT DO UPDATE WHERE value < max` (single SQL) | `UPDATE ... SET value = value + 1 WHERE value < limit` | `pg_advisory_xact_lock` then `SELECT COUNT(*)` in rolled-back txn | Redis Lua script (atomic check-and-add) |
| **At-limit behavior** | `:block` (hold in queue), `:discard`, or `:raise` | Blocks in `solid_queue_blocked_executions` | Enqueue: silently dropped. Perform: retry with backoff (forever) | Reschedule with backoff (raises `OverLimit`, middleware re-enqueues) |
| **Blocked job storage** | `pgbus_blocked_executions` table with priority ordering | `solid_queue_blocked_executions` table | No blocked queue — retries via ActiveJob retry mechanism | No blocked queue — job returns to Redis queue with delay |
| **Release on completion** | `ensure` block: promote next blocked job or decrement semaphore | Inline after `finished`/`failed_with` (inside same transaction as of PR #689) | Release advisory lock via `pg_advisory_unlock` | Lease auto-expires from sorted set |
| **Crash recovery** | Semaphore `expires_at` + dispatcher `expire_stale` cleanup | Semaphore `expires_at` + concurrency maintenance task | Advisory locks auto-release on session disconnect | TTL-based lease expiry (default 5 min) |
| **Message lifecycle** | PGMQ visibility timeout (`FOR UPDATE SKIP LOCKED`) — message stays in queue until archived | AR-backed `claimed_executions` table | AR-backed `good_jobs` table with advisory lock per row | Redis list + sorted set |

##### Key design differences

**Pgbus** uses PGMQ's native `FOR UPDATE SKIP LOCKED` for message claiming and a separate semaphore table for concurrency control. This two-layer approach means the message queue and concurrency system are independent — PGMQ handles exactly-once delivery, the semaphore handles admission control. The semaphore acquire is a single atomic SQL (`INSERT ... ON CONFLICT DO UPDATE WHERE value < max`), avoiding the need for explicit row locks.

**SolidQueue** uses AR models for everything — jobs, claimed executions, and semaphores all live in PostgreSQL tables. This means the entire lifecycle can be wrapped in AR transactions. However, as documented in [rails/solid_queue#689](https://github.com/rails/solid_queue/pull/689), this model is vulnerable to race conditions when semaphore expiry, job completion, and blocked-job release interleave across transactions. Pgbus avoids several of these by design: PGMQ's visibility timeout handles message recovery without a `claimed_executions` table, and there is no "release during shutdown" codepath.

**GoodJob** takes a different approach entirely: advisory locks. Each job dequeue acquires a session-level advisory lock on the job row, and concurrency checks use transaction-scoped advisory locks on the concurrency key. This means the check and the perform are serialized at the database level. The downside is that advisory locks are session-scoped — if a connection is returned to the pool without unlocking, the lock persists. GoodJob handles this by auto-releasing on session disconnect, but connection pool sharing between web and worker can cause surprising behavior.

**Sidekiq Enterprise** uses Redis sorted sets with TTL-based leases. Each concurrent slot is a sorted set entry with an expiry timestamp. This is fast and simple but has no durability guarantee — Redis failover can lose leases, temporarily allowing over-limit execution. The `sidekiq-unique-jobs` gem (open-source) uses a similar Lua-script approach but with more lock strategies (`:until_executing`, `:while_executing`, `:until_and_while_executing`) and configurable conflict handlers (`:reject`, `:reschedule`, `:replace`, `:raise`).

##### Race condition resilience

| Scenario | Pgbus | SolidQueue | GoodJob | Sidekiq |
|---|---|---|---|---|
| **Worker crash mid-execution** | PGMQ visibility timeout expires → message re-read. Semaphore expires via `expire_stale`. | `claimed_execution` survives → supervisor's process pruning calls `fail_all_with`. | Advisory lock released on session disconnect. | Lease TTL expires in Redis. |
| **Blocked job released while original still executing** | Not possible — promote only happens in `signal_concurrency`, which only runs after job success/DLQ. | Fixed in PR #689 — now checks for claimed executions before releasing. | N/A — no blocked queue; retries independently. | N/A — no blocked queue. |
| **Archive succeeds but signal fails** | `ensure` block guarantees signal fires even if archive raises. For SIGKILL: semaphore expires via dispatcher. | Fixed in PR #689 — `unblock_next_job` moved inside same transaction as `finished`. | Advisory lock released by session disconnect. | Lease auto-expires. |
| **Concurrent enqueue and signal race** | Semaphore acquire is a single atomic SQL — no read-then-write gap. | Fixed in PR #689 — `FOR UPDATE` lock on semaphore row serializes enqueue with signal. | `pg_advisory_xact_lock` serializes the concurrency check. | Redis Lua script is atomic. |

### Circuit breaker and queue pause/resume

Pgbus automatically pauses queues that fail repeatedly, preventing cascading failures.

```ruby
Pgbus.configure do |config|
  config.circuit_breaker_enabled = true   # default
end
```

The trip threshold (`5` consecutive failures), base backoff (`30s`), and
max backoff (`600s`) are tuned via constants on `Pgbus::CircuitBreaker`.
Override the constants in an initializer if you need different values —
they are not exposed as configuration because tweaking them at runtime
has never proved useful in practice.

When a queue hits the failure threshold:
1. The circuit breaker **auto-pauses** the queue with exponential backoff
2. After the backoff expires, the queue **auto-resumes** and the trip counter resets
3. If failures continue, each trip doubles the backoff (capped at `MAX_BACKOFF`)

You can also **manually pause/resume** queues from the dashboard. The pause state is stored in the `pgbus_queue_states` table and survives restarts.

```bash
rails generate pgbus:add_queue_states           # Add the queue_states migration
rails generate pgbus:add_queue_states --database=pgbus  # For separate database
```

### Client-level circuit breaker (database-down)

The circuit breaker above (`Pgbus::CircuitBreaker`) is **per-queue** and persists its pause state in the database — useless when the database itself is down, since its `check_paused` rescues and returns `false`, tripping nothing. `Pgbus::Client::ConnectionHealth` is a **separate, in-memory, process-local** latch owned by `Pgbus::Client` for exactly that failure mode. Do not confuse the two:

| | `Pgbus::CircuitBreaker` | `Client::ConnectionHealth` |
|---|---|---|
| Scope | Per queue | Per client (whole process) |
| Trips on | Job execution failures | Consecutive `PGMQ::Errors::ConnectionError` |
| State lives in | `pgbus_queue_states` (DB) | In-process memory (Mutex-guarded) |
| Survives restart | Yes | No — resets on process start |
| Purpose | Isolate a queue whose job code keeps failing | Stop hammering a database that is down |
| Configuration | `circuit_breaker_enabled` + constants | None — constants only |

`ConnectionHealth` trips open after 5 consecutive connection errors across *any* operation. Once open, the read paths (`read_message`, `read_batch`, `read_multi`, `read_batch_prioritized`, `read_grouped*`, `read_with_poll`) fail fast with a new `Pgbus::ConnectionCircuitOpenError` **without checking out a pool connection**. A single half-open probe is admitted after a monotonic backoff (1s base, doubling per re-open, capped at 60s); its success closes and resets the breaker, its failure re-opens it with a doubled window. Enqueues (`send_message`/`send_batch`) are **never** short-circuited — callers must see enqueue failures. `Worker#fetch_messages` rescues `ConnectionCircuitOpenError` before its generic rescue and idles the poll with no `ErrorReporter` call, so a whole-fleet outage produces two log lines total (a `warn` on open, an `info` on close) instead of one per worker per poll.

There is no configuration for this breaker — like `Pgbus::CircuitBreaker`'s thresholds, the values rarely need tuning.

### Read timeouts (libpq-native)

`config.read_timeout` (default `30` seconds) caps how long a single PGMQ read can block. Reads used to be bounded with Ruby `Timeout.timeout`, which interrupts via `Thread#raise` — that can fire mid-libpq-call and leave a pooled connection corrupted for the next checkout. On a **dedicated connection** (`database_url` or `connection_params`), pgbus now bakes two libpq-native bounds into the connection at boot instead:

| Bound | How | Effect |
|---|---|---|
| Server-side | `statement_timeout` via `options=-c statement_timeout=<read_timeout>ms` | Postgres cleanly cancels an overrunning query, surfaced as `Pgbus::ReadTimeoutError` |
| Client-side | `tcp_user_timeout` + `keepalives` (sized `read_timeout + 5s`) | A dead/hung socket makes libpq raise `PG::ConnectionBad` synchronously — no `Thread#raise`, no buffer corruption |

The client-side bound only applies on Linux with libpq ≥ 12 (older libpq rejects the `tcp_user_timeout` conninfo keyword; non-Linux hosts no-op it) — detected automatically via `Socket.const_defined?(:TCP_USER_TIMEOUT)` and `PG.library_version`, no configuration needed. Ruby `Timeout` remains only as a narrow last-resort fallback on a dedicated connection where libpq can't bound the socket (non-Linux hosts, or libpq < 12).

**The shared-AR `Proc` connection path** (`-> { ActiveRecord::Base.connection.raw_connection }`) gets neither bound automatically — pgbus doesn't own that socket. Configure the same libpq timeouts yourself in `database.yml`; ActiveRecord passes them straight through:

```yaml
# config/database.yml
production:
  primary:
    <<: *default
    variables:
      statement_timeout: 30000   # ms — match config.read_timeout
    # tcp_user_timeout / keepalives: set via the connection URL or driver/OS
    # defaults; ActiveRecord passes libpq conninfo options straight through.
```

A custom RuboCop cop, `Pgbus/NoRubyTimeout`, bans `Timeout.timeout` project-wide to prevent this class of bug from regressing.

### Prefetch flow control

Cap the number of in-flight (claimed but unfinished) messages per worker:

```ruby
Pgbus.configure do |config|
  config.prefetch_limit = 20  # nil = unlimited (default)
end
```

The worker tracks in-flight messages with an atomic counter and only fetches `min(idle_threads, prefetch_available)` messages per cycle. The counter is decremented in an `ensure` block so it never gets stuck.

### Worker recycling

Pgbus workers recycle themselves to prevent memory bloat. This is the main reliability difference vs. solid_queue, which leaves workers alive forever.

```ruby
Pgbus.configure do |config|
  config.max_jobs_per_worker = 10_000  # Restart after 10k jobs
  config.max_memory_mb = 512           # Restart if memory exceeds 512MB
  config.max_worker_lifetime = 1.hour  # Restart after 1 hour
end
```

When a limit is hit, the worker drains its thread pool, exits, and the supervisor forks a fresh process. RSS memory is sampled from `/proc/self/statm` (Linux) or `ps -o rss` (macOS).

### Retry backoff

When a job fails, Pgbus extends the PGMQ visibility timeout with exponential backoff so retries are spread out instead of bunched at fixed intervals:

```ruby
Pgbus.configure do |config|
  config.retry_backoff     = 5       # base delay (seconds)
  config.retry_backoff_max = 300     # cap at 5 minutes
  config.retry_backoff_jitter = 0.15 # +-15% randomization
end
```

The delay formula is `base * 2^(attempt-1) * (1 + random_jitter)`. For a job that fails 4 times with defaults: ~5s, ~10s, ~20s, ~40s before hitting DLQ on the 5th read.

Jobs can override the global settings per-class:

```ruby
class FragileApiJob < ApplicationJob
  include Pgbus::RetryBackoff::JobMixin

  pgbus_retry_backoff base: 10, max: 600, jitter: 0.2

  def perform(...)
    # ...
  end
end
```

### Long-running jobs: visibility heartbeat

PGMQ hands a message to one reader for `visibility_timeout` seconds. Without help, a job that runs longer is redelivered *while it is still running*: a second copy starts, `read_ct` climbs on every redelivery, and after `max_retries` the message is dead-lettered — all without the job ever raising.

Pgbus keeps that from happening. While `perform` runs, a per-process heartbeat thread re-arms the message's visibility timeout every `visibility_heartbeat_interval` seconds (default: a third of `visibility_timeout`), so the timeout only fires for a process that is actually gone — a crash or a `SIGKILL` — which is what it exists for. The heartbeat is dropped before the message is archived or the retry backoff adjusts its VT, so neither path changes.

```ruby
Pgbus.configure do |config|
  config.visibility_timeout = 10.minutes
  config.visibility_heartbeat = true            # default
  config.visibility_heartbeat_interval = 2.minutes  # default: visibility_timeout / 3
end
```

Opt a job class out (a job that must be redelivered if it stalls, for example):

```ruby
class WatchdogJob < ApplicationJob
  pgbus_visibility_heartbeat false
end
```

Each extension emits `pgbus.job_visibility_extended` and increments the `pgbus_visibility_extended` counter (tags: `queue`, `job_class`) — a job class that shows up there is one that outlives `visibility_timeout`, so you can size the timeout from data instead of guessing. `drain_timeout` still bounds what a graceful shutdown waits for; a job still running past it is redelivered after the timeout, as before. Event-bus handlers are not covered yet.

### Async execution mode (fibers)

Workers can optionally execute jobs as fibers instead of threads. This is ideal for I/O-bound workloads (HTTP calls, email delivery, LLM API calls) where jobs spend most of their time waiting on network I/O.

```ruby
Pgbus.configure do |config|
  # Global: all workers use async mode
  config.execution_mode = :async

  # Or per-worker: mix thread and async workers
  config.workers = [
    { queues: %w[webhooks emails], threads: 100, execution_mode: :async },
    { queues: %w[default], threads: 5 }  # stays thread-based
  ]
end
```

**Prerequisites:**

1. Add `gem "async"` to your Gemfile
2. Set `config.active_support.isolation_level = :fiber` in your Rails app

**Why it reduces connections:** In thread mode, each thread holds a database connection while waiting on I/O. With 50 threads, that's 50 connections. In async mode, 50 fibers share 3-5 connections because fibers yield during I/O and only one runs at a time.

**CLI flag:** `pgbus start --execution-mode async`

**Safety:** Messages stay in PGMQ with visibility timeout protection regardless of execution mode. If a fiber or worker crashes, the visibility timeout expires and messages become available for re-read. No data loss risk.

**Not recommended for:** CPU-bound jobs (image processing, heavy computation). These block the single reactor thread and should use thread mode.

## Routing and ordering

How messages flow between producers and the workers that handle them: priority sub-queues, consumer priority for active/standby workers, and single-active-consumer for strict ordering.

### Priority queues

Route jobs to priority sub-queues so high-priority work is processed first:

```ruby
Pgbus.configure do |config|
  config.priority_levels = 3    # Creates _p0, _p1, _p2 sub-queues per logical queue
  config.default_priority = 1   # Jobs without explicit priority go to _p1
end
```

Workers read from `_p0` (highest) first, then `_p1`, then `_p2`. Only when higher-priority sub-queues are empty does the worker read from lower ones.

Use ActiveJob's built-in `priority` attribute:

```ruby
class CriticalAlertJob < ApplicationJob
  queue_as :default
  queue_with_priority 0  # Highest priority

  def perform(alert_id)
    # ...
  end
end

class ReportJob < ApplicationJob
  queue_as :default
  queue_with_priority 2  # Lowest priority

  def perform(report_id)
    # ...
  end
end
```

When `priority_levels` is `nil` (default), priority queues are disabled and all jobs go to a single queue per logical name.

### Fair share across tenants

One tenant enqueuing 100 000 jobs must not put every other tenant's work behind them. `config.fair_share` tags each job with a key (and optional weight) at enqueue, and the worker's read interleaves across keys inside each queue — a weighted, work-conserving round-robin. No per-tenant queues.

```ruby
Pgbus.configure do |config|
  # key (String/Symbol/Integer), [key, weight], or nil to leave a job unkeyed
  config.fair_share = ->(job) { [Current.tenant&.id, Current.tenant&.plan_weight || 1] }
end
```

Weight 3 vs 1 yields a 3:1 split while both have work; a lone tenant still gets the whole worker. Composes with `priority_levels` (strict between levels, fair within) and `limits_concurrency`; mutually exclusive with `group_mode`. Workers build the supporting index `CONCURRENTLY` on queues they already serve. Details: [Routing & ordering](https://pgbus.zoolutions.llc/docs/routing-ordering).

The event bus has the same knob: `config.event_fair_share = ->(event) { key | [key, weight] | nil }` receives the `Pgbus::Event` at publish (`Pgbus.publish`, `publish_later`, `Outbox.publish_event`), tags the event envelope — never your payload — and consumers interleave reads across keys inside each subscriber queue. Details: [Event bus](https://pgbus.zoolutions.llc/docs/event-bus).

### Current attributes

`ActiveSupport::CurrentAttributes` is reset around every job. Ask pgbus to carry it and `Current.tenant` / `Current.user` / `Current.request_id` are there inside `perform` — and inside `retry_on` / `discard_on` blocks, under the pgbus worker and Rails' `:test` / `:inline` adapters alike:

```ruby
Pgbus.configure do |config|
  config.current_attributes = :auto   # or [Current, "Admin::Current"], or { Current => { except: [:request] } }
  config.fair_share = ->(job) { Current.tenant&.id }   # pairs naturally with fair share
end
```

Events get the same hop: `Pgbus.publish` captures `Current` into the event envelope and the consumer restores it around every `handle` — including across the transactional outbox (captured at `Outbox.publish_event`, inside your transaction). Handlers can also read the raw form via `event.context`.

Captured at enqueue via `ActiveJob::Arguments` (records become GlobalIDs, gated by `allowed_global_id_models`), preserved across retries, concurrency-blocked promotion, dead-letter retry and `perform_all_later`; an unserializable attribute raises at `perform_later` with the `except:` fix, while an **unpersisted** record (no id — it could never be restored) is skipped with a debug log instead of aborting the enqueue. The dashboard shows the context on failed-job and dead-letter pages. Details: [Active Job → Current attributes](https://pgbus.zoolutions.llc/docs/active-job).

### Consumer priority

When multiple workers subscribe to the same queues, higher-priority workers process messages first. Lower-priority workers back off (3x polling interval) when a higher-priority worker is active.

```ruby
Pgbus.configure do |c|
  c.capsule :primary,  queues: %w[default], threads: 10, consumer_priority: 10
  c.capsule :fallback, queues: %w[default], threads: 5,  consumer_priority: 0
end
```

Priority is stored in heartbeat metadata. Workers check the `pgbus_processes` table to discover higher-priority peers. When a high-priority worker goes stale (no heartbeat for 5 minutes), lower-priority workers automatically resume normal polling.

### Single active consumer

For queues that require strict ordering, enable single active consumer mode. Only one worker process can read from a queue at a time — others skip it and process other queues.

```ruby
Pgbus.configure do |c|
  c.capsule :ordered_primary,  queues: %w[ordered_events], threads: 1, single_active_consumer: true
  c.capsule :ordered_standby,  queues: %w[ordered_events], threads: 1, single_active_consumer: true
end
```

Uses PostgreSQL session-level advisory locks (`pg_try_advisory_lock`). The lock is non-blocking — workers that can't acquire it simply skip the queue. Locks auto-release on connection close (including crashes), so failover is automatic. The standby capsule takes over within one polling tick if the primary dies.

## Persistence and batching

How Pgbus integrates with your application's transactions and tracks groups of related work: outbox for atomic publish, batches for fan-out coordination, archive compaction for keeping the queue tables small.

### Batches

Coordinate groups of jobs with callbacks when all complete:

```ruby
batch = Pgbus::Batch.new(
  on_finish: BatchFinishedJob,
  on_success: BatchSucceededJob,
  on_discard: BatchFailedJob,
  description: "Import users",
  properties: { initiated_by: current_user.id }
)

batch.enqueue do
  users.each { |user| ImportUserJob.perform_later(user.id) }
end
```

#### Callbacks

| Callback | Fired when |
|----------|------------|
| `on_finish` | All jobs completed (success or discard) |
| `on_success` | All jobs completed successfully (zero discarded) |
| `on_discard` | At least one job was dead-lettered |

Callback jobs receive the batch `properties` hash as their argument:

```ruby
class BatchFinishedJob < ApplicationJob
  def perform(properties)
    user = User.find(properties["initiated_by"])
    ImportMailer.complete(user).deliver_later
  end
end
```

#### Unique batches (run-scoped locks)

`ensures_uniqueness` protects one job. A multi-job run — a nightly import that fans out into hundreds of chunk jobs, a pipeline whose jobs enqueue the next stage — has no single job that lives for the whole run, so a lock on the entry job releases the moment that job finishes and the next scheduler tick starts a second run on top of the first. Give the batch the lock instead:

```ruby
batch = Pgbus::Batch.new(
  uniqueness_key: "perfecta-daily",
  on_conflict: :discard,           # :reject (raise AlreadyRunning), :discard, :log
  on_success: ExportPricesJob
)

batch.enqueue do
  ImportChunkJob.perform_later("products", 0)
end
```

The lock is taken at `enqueue` and held until the batch finishes — through every open-batch `batch.enqueue` add from inside a running job — then released in the same single-winner finish that fires the callbacks, so completion, the stalled-batch sweep and cleanup all release it. While it is held, another `Pgbus::Batch.new(uniqueness_key: "perfecta-daily").enqueue { ... }` raises `Pgbus::Batch::AlreadyRunning` (`:reject`), or skips its block and returns a batch that answers `discarded?` (`:discard`, `:log`). The row lives in `pgbus_uniqueness_keys` as `batch:<key>`, so it never collides with a job's key and shows up in the dashboard like any other lock; the reaper judges it by the batch's status, never by queue contents.

#### How batches work

1. `Batch.new(...)` creates a tracking row in `pgbus_batches` with `status: "pending"`
2. `batch.enqueue { ... }` tags each enqueued job with the `pgbus_batch_id` in its payload
3. After each job completes or is dead-lettered, the executor atomically updates the batch counters
4. When `completed_jobs + discarded_jobs == total_jobs`, the batch status flips to `"finished"` and callback jobs are enqueued
5. The dispatcher cleans up finished batches older than 7 days

### Transactional outbox

Publish events atomically inside your database transactions. A background poller moves outbox entries to PGMQ.

```bash
rails generate pgbus:add_outbox                  # Add the outbox migration
rails generate pgbus:add_outbox --database=pgbus # For separate database
```

```ruby
Pgbus.configure do |config|
  config.outbox_enabled = true
  config.outbox_poll_interval = 1.0  # seconds
  config.outbox_batch_size = 100
  config.outbox_retention = 1.day    # ActiveSupport::Duration also accepted
end
```

Usage:

```ruby
ActiveRecord::Base.transaction do
  order = Order.create!(params)

  # Published atomically with the order — if the transaction rolls back,
  # the outbox entry is also rolled back. No lost or phantom events.
  Pgbus::Outbox.publish("default", { order_id: order.id })

  # For topic-based event bus:
  Pgbus::Outbox.publish_event("orders.created", { order_id: order.id })
end
```

The outbox poller uses `FOR UPDATE SKIP LOCKED` inside a transaction to claim entries, publishes them to PGMQ, and marks them as published. Failed entries are skipped and retried next cycle.

### Archive compaction

PGMQ archive tables grow unbounded. Pgbus automatically purges old entries:

```ruby
Pgbus.configure do |config|
  config.archive_retention = 7.days               # ActiveSupport::Duration (default 7 days)
end
```

The compaction loop runs every hour and deletes up to 1000 rows per
queue per cycle. Both knobs live as constants on
`Pgbus::Process::Dispatcher` (`ARCHIVE_COMPACTION_INTERVAL`,
`ARCHIVE_COMPACTION_BATCH_SIZE`) — they have never been worth surfacing
as configuration. The dispatcher runs archive compaction as part of its
maintenance loop, deleting archived messages older than `archive_retention`
in batches to avoid long-running transactions.

## Observability

Error reporting, structured logging, and queue health monitoring.

### Error reporting

By default, Pgbus logs caught exceptions and continues. To route them to your APM service (Appsignal, Sentry, Honeybadger, etc.), push callable reporters onto `config.error_reporters`:

```ruby
Pgbus.configure do |c|
  c.error_reporters << ->(ex, ctx) {
    Appsignal.set_error(ex) { |t| t.set_tags(ctx) }
  }
end
```

Each reporter receives `(exception, context_hash)`. The context hash includes keys like `action`, `queue`, `job_class`, and `msg_id` depending on the call site. Reporters that accept a third argument also receive the Pgbus configuration object.

Reporters are wired into all critical rescue paths: job execution failures, worker fetch/process errors, dispatcher maintenance, supervisor fork failures, circuit breaker trips, outbox publish errors, and failed event recording. Non-critical paths (dashboard queries, stat recording) remain log-only.

`ErrorReporter.report` is guaranteed to never raise — if a reporter or the logger itself throws, the error is swallowed silently. This preserves fault-tolerance invariants at every rescue site.

### Error hierarchy

Every operational error Pgbus raises descends from `Pgbus::Error`, so a single rescue catches them all:

```ruby
begin
  Pgbus.configure { |c| c.visibility_timeout = 0 }
rescue Pgbus::Error => e
  # ConfigurationError, EnqueueError, ExecutionPoolError, SerializationError,
  # QueueNotFoundError, SchemaNotReady, ... all land here
end
```

The named subclasses (a partial list): `Pgbus::ConfigurationError` (`validate!` and config setters), `Pgbus::SerializationError` (payload / GlobalID rejection), `Pgbus::EnqueueError` (batch enqueue integrity), `Pgbus::ExecutionPoolError` (pool shutting down / at capacity), `Pgbus::QueueNotFoundError`, `Pgbus::DeadLetterError`, `Pgbus::JobNotUnique`, `Pgbus::SchemaNotReady`, `Pgbus::ReadTimeoutError`, and `Pgbus::ConnectionCircuitOpenError`.

**One deliberate exception to the rule:** errors that reject a malformed *argument shape* stay `ArgumentError` subclasses, because that's what `ArgumentError` means — `Pgbus::Streams::StreamNameTooLong`, `Pgbus::Streams::Cursor::InvalidCursor`, and `Pgbus::Configuration::CapsuleDSL::ParseError`. Rescue those with `ArgumentError` (or the specific class), not `Pgbus::Error`.

If you upgraded from 0.9.x and previously did `rescue ArgumentError` around `Pgbus.configure`, switch it to `rescue Pgbus::Error` — config validation now raises `Pgbus::ConfigurationError`, which is not an `ArgumentError`.

### AppSignal integration

When the `appsignal` gem is loaded in your app, Pgbus auto-installs a subscriber and a minutely probe that report into AppSignal:

- **Background-job transactions** for every ActiveJob run and every event-bus handler invocation. Action names follow the AppSignal convention: `MyJob#perform`, `MyHandler#handle`. Tags include `queue`, `job_class`/`handler`, `routing_key`, `attempts`, and the `active_job_id` / `provider_job_id`. `enqueued_at` becomes the AppSignal `queue_start` timestamp so "time on queue" shows up correctly in the timeline.
- **Custom counters and distributions** for sends, reads, broadcasts, outbox publishes, recurring scheduling, and worker recycles. All metric names are prefixed `pgbus_`.
- **A minutely probe** that gauges queue depth (visible vs total), oldest message age per queue, DLQ depth, failed events count, dead-tuple totals, MVCC horizon age, active processes, and stream connection estimates.

There is nothing to wire up — load the appsignal gem and the integration installs itself in a Rails initializer. To opt out:

```ruby
Pgbus.configure do |c|
  c.appsignal_enabled = false        # disable subscriber + probe entirely
  c.appsignal_probe_enabled = false  # keep transactions, drop the gauge probe
end
```

#### Dashboards

Four importable AppSignal dashboards ship with the gem:

| Name | File | Purpose |
|------|------|---------|
| `main` | `lib/pgbus/integrations/appsignal/dashboard.json` | All-in-one overview (the automated dashboard submitted to appsignal/public_config) |
| `throughput` | `lib/pgbus/integrations/appsignal/dashboards/pgbus_throughput.json` | Jobs/sec, perform-duration percentiles, send/read counts |
| `health` | `lib/pgbus/integrations/appsignal/dashboards/pgbus_health.json` | Queue depth, oldest message age, DLQ, dead tuples, MVCC horizon, worker recycles |
| `streams` | `lib/pgbus/integrations/appsignal/dashboards/pgbus_streams.json` | Broadcasts, fanout, active SSE connections, outbox, recurring tasks |

The files are stored in AppSignal's automated-dashboard format (a `metric_keys` trigger wrapping
the dashboard object); `pgbus dashboard <name>` unwraps one into the JSON that AppSignal's
"New dashboard" → "Import dashboard" dialog accepts:

```bash
pgbus dashboard --list            # available names and titles
pgbus dashboard health | pbcopy   # paste into the import dialog
```

### Metrics adapter (Prometheus / StatsD)

AppSignal is one consumer of pgbus's `ActiveSupport::Notifications` events. For teams on Prometheus, Datadog, or plain StatsD, the built-in **metrics adapter** consumes the same events and forwards them to a backend — no hand-written subscribers. It is **off by default** (`metrics_backend = nil` installs nothing, zero overhead) and runs independently of AppSignal, so both can be active at once.

```ruby
Pgbus.configure do |c|
  c.metrics_backend = :prometheus   # in-process registry, scraped via the exporter
  # or
  c.metrics_backend = :statsd       # UDP datagrams (DogStatsD dialect)
  c.statsd_host = "127.0.0.1"       # default
  c.statsd_port = 8125              # default
end
```

You can also assign a custom backend instance — any object subclassing `Pgbus::Metrics::Backend` (implementing `increment`, `gauge`, `histogram`):

```ruby
c.metrics_backend = MyOpenTelemetryBackend.new
```

**Metrics emitted** (all `pgbus_`-prefixed, low-cardinality tags only):

| Metric | Type | Tags |
|--------|------|------|
| `pgbus_queue_job_count` | counter | `queue`, `job_class`, `status` (`processed`/`failed`/`dead_lettered`) |
| `pgbus_visibility_extended` | counter | `queue`, `job_class` — one per re-armed visibility timeout; a class listed here outlives `visibility_timeout` |
| `pgbus_job_duration_ms` | histogram | `queue`, `job_class` |
| `pgbus_event_count` | counter | `handler`, `routing_key`, `status` |
| `pgbus_event_duration_ms` | histogram | `handler`, `routing_key` |
| `pgbus_messages_sent` / `pgbus_messages_read` | counter | `queue` |
| `pgbus_stream_broadcast_count` | counter | `stream`, `deferred` |
| `pgbus_outbox_published` | counter | `kind` |
| `pgbus_recurring_enqueued` | counter | `task`, `class_name` |
| `pgbus_worker_recycled` | counter | `reason`, `kind` (`worker`/`consumer`) |
| `pgbus_pool_size` / `pgbus_pool_available` | gauge | `hostname` |

A backend that raises (registry bug, StatsD socket down) is logged and swallowed — a metrics failure never propagates into the thread that emitted the event.

#### Mounting the Prometheus exporter

The `:prometheus` backend is an in-process registry; expose it for scraping by mounting `Pgbus::Metrics::PrometheusExporter` — a self-contained Rack app that renders text exposition format (v0.0.4):

```ruby
# config/routes.rb
mount Pgbus::Metrics::PrometheusExporter.new => "/metrics"
```

With no argument the exporter reads `config.metrics_backend`, so a single `config.metrics_backend = :prometheus` wires both the subscriber and the exporter to the same registry. The app is plain Rack, so it also runs under any standalone Rack server (e.g. a one-line `config.ru`) — point Prometheus at `GET /metrics`.

> The exporter returns **503** if `metrics_backend` is not a Prometheus backend (e.g. it's `:statsd` or `nil`), since there is no in-process registry to render.

#### Custom subscriptions

The metrics adapter above covers Prometheus and StatsD. For anything else (New Relic, OpenTelemetry, a bespoke sink), the events are built on `ActiveSupport::Notifications` — subscribe directly:

```ruby
ActiveSupport::Notifications.subscribe(/^pgbus\./) do |name, start, finish, _id, payload|
  duration_ms = (finish - start) * 1_000
  YourApm.record(name, duration_ms, payload)
end
```

Metrics-relevant events emitted (the subset the metrics subscriber maps; see `lib/pgbus/instrumentation.rb` for the full `pgbus.*` catalog): `pgbus.executor.execute`, `pgbus.job_completed`, `pgbus.job_failed`, `pgbus.job_dead_lettered`, `pgbus.event_processed`, `pgbus.event_failed`, `pgbus.client.send_message`, `pgbus.client.send_batch`, `pgbus.client.read_batch`, `pgbus.client.pool`, `pgbus.stream.broadcast`, `pgbus.outbox.publish`, `pgbus.recurring.enqueue`, `pgbus.worker.recycle`, `pgbus.consumer.recycle`. Payload keys are documented in `lib/pgbus/instrumentation.rb`.

### Connection pool metrics

The PGMQ connection pool was previously invisible — pgmq-ruby exposes `size`/`available` counters, but nothing read them, so the first sign of an undersized or leaking pool was an opaque `PGMQ::Errors::ConnectionError: Connection pool timeout`. `Pgbus::Client#pool_stats` surfaces the live counters:

```ruby
Pgbus.client.pool_stats
# => { size: 10, available: 3, pool_timeout: 5 }
```

It's purely observational (rescues internally to `{}`), so reading the pool can never break job processing, and it works on both the dedicated-pool path and the shared-`Proc` connection path (`size` reports `1` there). The worker heartbeat emits a `pgbus.client.pool` instrumentation event once per beat (never on a per-job hot path) carrying that payload, and the AppSignal minutely probe reports `pgbus_pool_size` / `pgbus_pool_available` gauges tagged by hostname (the pool is per-process, unlike the cluster-wide queue/summary gauges).

A pool-timeout error is re-raised as the same `PGMQ::Errors::ConnectionError` class (so `with_stale_connection_retry` semantics and non-retryability are unchanged) with the live pool state and an actionable hint appended:

```
Connection pool timeout (pool {size: 10, available: 0, pool_timeout: 5}) — raise Pgbus.configuration.pool_size or reduce worker threads
```

### Structured logging

Pgbus ships two log formatters inspired by Sidekiq's `Logger::Formatters`:

```ruby
Pgbus.configure do |c|
  c.log_format = :json   # or :text (default)
end
```

**Text format** (default):

```text
INFO 2025-01-15T10:30:00.000Z pid=1234 tid=abc queue=default: Starting job
```

**JSON format**:

```json
{"ts":"2025-01-15T10:30:00.000Z","pid":1234,"tid":"abc","lvl":"INFO","component":"Pgbus","msg":"Starting job","ctx":{"queue":"default"}}
```

The JSON formatter extracts `[Pgbus]` and `[Pgbus::Web]` prefixes from log messages into a separate `component` field so the `msg` field stays clean for log aggregators. Thread-local context can be added via `Pgbus::LogFormatter.with_context(queue: "default") { ... }` and appears under the `ctx` key.

You can also set a formatter directly on the logger:

```ruby
Pgbus.configure do |c|
  c.logger.formatter = Pgbus::LogFormatter::JSON.new
end
```

### Queue health monitoring

The dashboard includes a **Queue Health** panel showing PostgreSQL vacuum stats per PGMQ table: dead tuple counts, live tuple counts, bloat ratio (dead / total), last vacuum age, and MVCC horizon age. The same stats appear on individual queue detail pages.

#### Autovacuum tuning

PGMQ queue tables have high insert/delete churn that overwhelms PostgreSQL's default autovacuum settings. Pgbus applies aggressive per-table tuning automatically:

- **New queues at runtime**: `Client#ensure_single_queue` applies tuning after `pgmq.create()`
- **Existing installations**: `rails generate pgbus:update` detects untuned tables
- **Fresh installs**: The install migration includes tuning for the default queue

To apply tuning manually or after `db:schema:load` (which loses `ALTER TABLE` settings):

```bash
rails generate pgbus:tune_autovacuum                  # Generate migration
rails generate pgbus:tune_autovacuum --database=pgbus # For separate database
```

The `pgbus:tune_autovacuum` rake task also hooks into `db:schema:load` automatically.

#### Prometheus metrics

When `config.metrics_enabled = true` (default), the dashboard exposes Prometheus-compatible gauges:

| Metric | Description |
|--------|-------------|
| `pgbus_queue_oldest_claimable_age_seconds` | Age of the oldest message eligible for pickup (visibility timeout elapsed) — safe to alert on: scheduled/backoff-parked messages don't count until due; the series is omitted entirely when no claimable backlog exists (while the raw `pgbus_queue_oldest_message_age_seconds` gauge may still report a parked message's age) |
| `pgbus_table_dead_tuples` | Dead tuple count per PGMQ table |
| `pgbus_table_live_tuples` | Live tuple count per PGMQ table |
| `pgbus_table_bloat_ratio` | Dead / (dead + live) per table |
| `pgbus_table_last_vacuum_age_seconds` | Seconds since last vacuum per table |
| `pgbus_oldest_transaction_age_seconds` | MVCC horizon pin risk |
| `pgbus_worker_pool_capacity` | Total worker thread slots |
| `pgbus_worker_pool_busy` | Currently busy worker threads |
| `pgbus_worker_pool_utilization` | Busy / capacity ratio |

### MCP diagnostic server (read-only)

Pgbus ships an optional, **read-only** [MCP](https://modelcontextprotocol.io) server so an AI agent (or any MCP client) can diagnose pgbus directly — "are queues backed up?", "is `read_ct` advancing?", "are workers heart-beating but not claiming?" — instead of hand-writing `pgmq` / `pg_stat_activity` SQL against production. It is a thin adapter over the same read layer the dashboard uses, so it adds no new database access path.

Add the optional `mcp` gem to your `Gemfile` first (`gem "mcp"`, 0.23 or newer — 1.x is fully supported); both entry points below tell you if it's missing.

#### Choosing a deployment

There are two ways to run it, depending on **who connects and from where**:

| | **stdio** (`pgbus mcp`) | **HTTP** (mount in Rails) |
|---|---|---|
| Who connects | A local operator (Claude Desktop / Claude Code on your machine) | A remote agent or alerting system |
| Process model | The MCP client **spawns** a short-lived process on demand | Runs **inside your existing Rails server** — no second process |
| Reaches production? | Only if run where prod DB creds are available | Yes — co-located with the app, same credentials |
| Use it for | Hands-on, interactive debugging | Automated/remote diagnostics & alerting |

> **Don't start a second `bin/pgbus` instance for HTTP.** The HTTP transport is a Rack app — mount it in the Rails app you already deploy.

##### stdio (local operator)

```bash
bundle exec pgbus mcp        # speaks MCP over stdio
```

##### HTTP (mount in Rails)

```ruby
# config/routes.rb
Rails.application.routes.draw do
  # ... your routes ...
  mount Pgbus::MCP.rack_app(token: ENV["PGBUS_MCP_TOKEN"]) => "/pgbus/mcp"
end
```

`Pgbus::MCP.rack_app` returns a gated Rack app. It runs the transport in **stateless + JSON-response mode**, so every request is a self-contained POST with no in-memory session — safe behind multiple Puma/Falcon workers (any worker can answer any request). Keep the endpoint on an internal network / behind your VPN; it is not meant to be internet-exposed.

Options:

| Option | Default | Meaning |
|--------|---------|---------|
| `token:` | `nil` | Shared secret. When set, requests must send `Authorization: Bearer <token>` (constant-time compared). |
| `auth:` | `nil` | A callable `->(rack_request) { ... }` returning truthy to allow — mirrors `config.web_auth`. Wins over `token:`. |
| `allow_payloads:` | `false` | When true, tools honor a per-call `include_payloads` flag (see Security). |
| `dns_rebinding_protection:` | `nil` | The `mcp` gem's Host/Origin validation (on by default since mcp 0.23, loopback hosts only). `nil` follows the gate: **off when `token:`/`auth:` is set, on when unauthenticated.** `true`/`false` forces it. |
| `allowed_hosts:` | `nil` | Extra `Host` values accepted when the check is on (`"app.example.com"` matches any port, `"app.example.com:8443"` exactly). |
| `allowed_origins:` | `nil` | Extra `Origin` values accepted beyond same-origin when the check is on. |

If you set neither `token:` nor `auth:`, pgbus logs a warning — an unauthenticated diagnostic endpoint exposes operational metadata to anyone who can reach it.

> **Why the Host check follows the gate.** DNS-rebinding protection defends a server bound to `localhost` against a browser page whose DNS name was re-pointed at `127.0.0.1`. Such a page can never carry your bearer token (the secret doesn't exist at the attacker's origin), so on a gated mount the check is redundant — and left on, it rejects every request to a real hostname (`https://app.example.com/pgbus/mcp` → `403 Invalid Host header`). Pgbus therefore turns it off when a gate is configured and keeps it on for the (warned-about) unauthenticated mount. If your `auth:` callable trusts something a rebound page *would* have — a source-IP allowlist, say — pass `dns_rebinding_protection: true` plus `allowed_hosts:` for your hostname. Requires `mcp >= 0.23`; older gems raise `Pgbus::Error` naming the floor.

> Clients must send `Accept: application/json` and `Content-Type: application/json` on every POST, or the transport replies `406 Not Acceptable`. MCP clients do this automatically.

Need a **standalone HTTP pod** instead of mounting in your main app? The same Rack app works under any Rack server, e.g. a one-line `config.ru`:

```ruby
require "pgbus"
Pgbus::MCP.load!
run Pgbus::MCP.rack_app(token: ENV["PGBUS_MCP_TOKEN"])
# rackup -p 9293   (run it in a container that shares the app's DB config)
```

#### Tools

All tools are read-only — no tool mutates state, and there is no raw-SQL passthrough.

| Tool | Purpose |
|------|---------|
| `pgbus_health` | One-call verdict: `OK` / `DEGRADED` / `STALLED`. STALLED is the silent-worker-wedge signal (visible backlog while workers heart-beat but don't claim). Suitable for automated alerting. |
| `pgbus_queues` | All queues: depth, visible count, oldest-message age, paused state. |
| `pgbus_queue_detail` | Per-queue metrics + paused state + table health (dead tuples, bloat, vacuum age). |
| `pgbus_processes` | Every process with kind, pid, heartbeat age, and `healthy`/`stale`/`stalled` status. |
| `pgbus_jobs` / `pgbus_job_detail` | Inspect enqueued messages (`read_ct`, `vt`, `enqueued_at`). Paginated. |
| `pgbus_dlq` / `pgbus_dlq_detail` | Dead-letter inspection. Paginated. |
| `pgbus_locks` | Active uniqueness locks (the leaked-lock diagnostic). |
| `pgbus_throughput` / `pgbus_stats` | Recent throughput time series and status counts. |
| `pgbus_recurring` | Recurring task schedule + last/next run times. |

#### Security

The server is built to be safe against a production datastore:

- **Read-only by default.** No tool mutates state and no arbitrary-query tool exists.
- **Payloads redacted.** Message bodies, headers, and job arguments are replaced with `[redacted]` unless payloads are explicitly allowed **and** `include_payloads: true` is passed on the call. Both gates must be open. Allow payloads with `PGBUS_MCP_ALLOW_PAYLOADS=1` (stdio) or `Pgbus::MCP.rack_app(allow_payloads: true)` (HTTP).
- **Bounded queries.** Every list tool paginates with a row cap (`pgbus_jobs` / `pgbus_dlq` cap at 100 rows/page; time windows cap at 1440 minutes).
- **Reuses your DB credentials.** No new privileged path — it reads through the app's existing connection config.
- **Authentication.**
  - *HTTP:* set `token:` (clients send `Authorization: Bearer <token>`) or a custom `auth:` callable; unauthenticated requests get `401`.
  - *stdio:* the channel is local (the client spawns the process), so the gate is a boot-time precondition — set `PGBUS_MCP_TOKEN` and the server refuses to start unless `PGBUS_MCP_AUTH_TOKEN` matches (constant-time compare).

#### Client configuration

For the **stdio** transport, a minimal MCP client config (Claude Code / Claude Desktop style):

```json
{
  "mcpServers": {
    "pgbus": {
      "command": "bundle",
      "args": ["exec", "pgbus", "mcp"],
      "env": { "RAILS_ENV": "production" }
    }
  }
}
```

For the **HTTP** transport, point the client at the mounted URL with a streamable-HTTP server config, sending the bearer token, e.g.:

```json
{
  "mcpServers": {
    "pgbus": {
      "url": "https://your-app.internal/pgbus/mcp",
      "headers": { "Authorization": "Bearer ${PGBUS_MCP_TOKEN}" }
    }
  }
}
```

### Health endpoints (liveness / readiness)

For orchestrators like Kubernetes, Pgbus exposes two HTTP probes: `/livez` (is the serving process up?) and `/readyz`. Readiness means different things in the two places the probes are served:

- **Mounted in Rails** (`Pgbus::Web::HealthApp`): `/readyz` runs the cluster-wide `OK` / `DEGRADED` / `STALLED` verdict, same as the MCP `pgbus_health` tool — `STALLED` (visible backlog while workers heart-beat but don't claim) fails readiness.
- **Standalone from the supervisor** (`health_port`): `/readyz` is **container-local** — did *this* supervisor finish booting, and are all the children *it* forked alive? That is the signal a rolling deploy's health gate needs; the cluster verdict would let a brand-new container pass on the strength of the *old* container's workers.

| Path | Method | 200 | 503 | Touches DB |
|---|---|---|---|---|
| `/livez` | GET | always (`ok`) | never | no |
| `/readyz` (mounted) | GET | verdict `OK` or `DEGRADED` | verdict `STALLED`, or DB unreachable (`{"status":"ERROR"}`) | yes |
| `/readyz` (supervisor) | GET | `OK` — booted, all children live | `BOOTING`, `DEGRADED` (child down), `DRAINING` (stopping) | no |

Unknown paths return `404`; non-`GET` methods return `405`. The `/readyz` body is JSON, so a probe failure is self-describing in the pod's event log.

#### Mount in your Rails app

`Pgbus::Web::HealthApp` is a plain Rack app — mount it wherever your web pods already serve HTTP. It needs no auth (it exposes only aggregate health, never payloads) but keep it on an internal network:

```ruby
# config/routes.rb
mount Pgbus::Web::HealthApp.new => "/pgbus/health"
```

```yaml
# kubelet probes (Deployment spec)
livenessProbe:
  httpGet: { path: /pgbus/health/livez, port: 3000 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /pgbus/health/readyz, port: 3000 }
  periodSeconds: 10
  failureThreshold: 3
```

#### Standalone from the supervisor

Worker pods run `bin/pgbus` (the supervisor), not Puma — so there is no Rails server to mount into. Set `health_port` and the supervisor serves both paths itself over a tiny TCP server (no Rails, no dashboard), letting a kubelet probe the process that actually forks and watches workers:

```ruby
Pgbus.configure do |c|
  c.health_port = 9394        # nil (default) = disabled
  c.health_bind = "0.0.0.0"   # default "127.0.0.1"
end
```

```yaml
# probe the supervisor pod directly
livenessProbe:
  httpGet: { path: /livez, port: 9394 }
readinessProbe:
  httpGet: { path: /readyz, port: 9394 }
```

The supervisor's `/readyz` answers from its own state, never the database:

```json
{ "status": "OK", "expected": 3, "live": 3 }
```

- `BOOTING` (503) until the connection is verified, queues are bootstrapped, and every configured child has been forked. `expected` is stamped at that instant.
- `OK` (200) while all expected children are in the fork table. A clean worker recycle never dips the count — the snapshot refreshes after reap-and-restart each monitor pass.
- `DEGRADED` (503) when a child died and is waiting out crash-restart backoff. During a rolling deploy this is the desired failure mode: a crash-looping replacement never goes ready, so the old container keeps running.
- `DRAINING` (503) the moment a stop signal arrives.

#### `pgbus-health`: container HEALTHCHECK probe

`pgbus-health` ships with the gem: a dependency-free probe (plain Ruby + stdlib sockets — no Bundler, no Rails, nothing else loaded) that GETs `127.0.0.1:<port>/readyz` and exits `0` on 200, `1` on anything else, `2` on usage errors. Cheap enough for a 1–5s `HEALTHCHECK` interval, and it works in images without curl:

```bash
pgbus-health --port 9394              # or PGBUS_HEALTH_PORT=9394 pgbus-health
pgbus-health --port 9394 --path /livez --timeout 2
```

### Rolling restarts (dash, docker)

[dash](https://github.com/zoolutions/dash) (per-role health checks) can rolling-restart a non-proxied job role: start the new container, poll its docker `HEALTHCHECK` until healthy, and only then `docker stop` the old one. Wire the pgbus container into that gate:

```yaml
# config/deploy.yml
servers:
  job:
    hosts: [...]
    cmd: bin/pgbus start
    healthcheck:
      cmd: bin/pgbus-health --port 9394
      interval: 5s
      start_period: 30s     # cover Rails boot + queue bootstrap
    stop_timeout: 45        # must exceed pgbus shutdown_timeout (see below)
env:
  clear:
    PGBUS_HEALTH_PORT: 9394
```

(`bundle binstubs pgbus` generates `bin/pgbus-health`; adjust the path if your image invokes gem executables differently.)

**The shutdown timeline.** On `docker stop`, SIGTERM reaches the supervisor and readiness flips to `DRAINING`; children stop claiming work and drain in-flight jobs for up to `drain_timeout` (default 30s); the supervisor waits `shutdown_timeout` (default `drain_timeout + 5`) before SIGKILLing stragglers. Align the three knobs outside-in:

```text
orchestrator stop_timeout  >  pgbus shutdown_timeout  >  pgbus drain_timeout
        45s                        35s (derived)                30s
```

If the orchestrator's stop grace period is *shorter* than `shutdown_timeout`, docker SIGKILLs the whole tree mid-drain and the graceful path never gets to finish. Raising `drain_timeout` raises the derived `shutdown_timeout` automatically; raise `stop_timeout` to match.

**The overlap window is safe by construction.** Between "new container healthy" and "old container stopped", two supervisors run against the same database. Nothing double-fires: queue claims use `FOR UPDATE SKIP LOCKED`, `single_active_consumer` queues arbitrate via session-level advisory locks (released the instant a killed process's connection dies), two live recurring schedulers dedup on the `(task_key, run_at)` unique record, and dispatcher maintenance is idempotent. "One scheduler per deployment" is a steady-state rule; a deploy window may briefly violate it without consequence.

**What a hard kill still costs.** Jobs killed past the drain window are redelivered after their visibility timeout (at-least-once holds) — but PGMQ's `read_ct` increments exactly like a logical failure, so a long-running job that straddles *repeated* deploy kills can be pushed to the DLQ without its code ever raising. `zombie_detection` logs exactly this pattern (`read_ct > 1` with no recorded failure). Keep jobs shorter than `drain_timeout`, or raise it (and `stop_timeout`) for queues that can't be. For `idempotent!` event handlers there is a separate crash-window caveat tracked in [#385](https://github.com/zoolutions/pgbus/issues/385).

### Boot diagnostics banner

`Supervisor#run` logs a one-block banner right after the heartbeat starts and before queues bootstrap, so a misconfigured deployment states its actual settings instead of forcing an operator to attach a console. Every line is `"[Pgbus] boot:"`-prefixed and renders cleanly under both the `:text` and `:json` log formatters:

```
[Pgbus] boot: pgbus 0.9.8 pid=42317
[Pgbus] boot: connection=host/dbname pool=12
[Pgbus] boot: pgmq_schema_mode=auto pgmq_version=1.4.0
[Pgbus] boot: listen_notify=true worker_notify_wakeup=true
[Pgbus] boot: roles=workers,dispatcher,scheduler
[Pgbus] boot: capsule=critical queues=critical threads=5 mode=threads
[Pgbus] boot: capsule=default queues=default,mailers threads=10 mode=threads
```

It states: the pgbus version; the connection target reduced to `host/dbname` (never the password) across all three `connection_options` forms (`database_url` string, `connection_params` hash, AR-derived hash); the resolved pool size; `pgmq_schema_mode` and the installed PGMQ version (best-effort — `unknown` on any error); `listen_notify` and `worker_notify_wakeup?`; the roles that will actually boot (honoring `config.roles`); and one line per worker capsule (name, queues, threads, execution mode) and per event consumer (topics, threads).

Every DB-dependent field is wrapped so a transient failure degrades that field to `unknown` — the banner can never abort boot.

## Real-time broadcasts (turbo-streams replacement)

Pgbus ships a drop-in replacement for turbo-rails' `turbo_stream_from` helper that fixes several well-known ActionCable correctness bugs by using PGMQ message IDs as a replay cursor. Same API as turbo-rails. No Redis. No ActionCable. No lost messages on reconnect.

**Bugs fixed:**

- [**rails/rails#52420**](https://github.com/rails/rails/issues/52420) -- "page born stale": a broadcast that fires between controller render and WebSocket subscribe is silently lost with ActionCable. Pgbus captures a PGMQ `msg_id` watermark at render time and replays any messages published in the gap via the SSE `Last-Event-ID` mechanism.
- [**hotwired/turbo#1261**](https://github.com/hotwired/turbo/issues/1261) -- missed messages on reconnect. Pgbus persists the cursor on the client (EventSource's built-in `Last-Event-ID`) and replays from the PGMQ archive on every reconnect.
- [**hotwired/turbo-rails#674**](https://github.com/hotwired/turbo-rails/issues/674) -- no way to detect disconnect. Pgbus dispatches `pgbus:open`, `pgbus:gap-detected`, and `pgbus:close` DOM events on the stream element.

### Usage

Swap `turbo_stream_from` for `pgbus_stream_from` in your view:

```erb
<%# Before %>
<%= turbo_stream_from @order %>

<%# After %>
<%= pgbus_stream_from @order %>
```

Everything else stays the same. The model concern keeps working unchanged:

```ruby
class Order < ApplicationRecord
  broadcasts_to ->(order) { [order.account, :orders] }
end
```

`broadcasts_to`, `broadcast_replace_to`, `broadcasts_refreshes`, `broadcast_append_later_to`, and every other `Turbo::Broadcastable` helper funnels through a single `Turbo::StreamsChannel.broadcast_stream_to` method that pgbus monkey-patches at engine boot. The signed-stream-name verification reuses `Turbo.signed_stream_verifier_key` so existing signed tokens Just Work.

Add the Puma plugin to `config/puma.rb` so SSE connections drain cleanly on deploy:

```ruby
# config/puma.rb
plugin :pgbus_streams
```

Without the plugin, Puma closes hijacked SSE sockets abruptly during graceful restart, which looks to browsers like a network error and triggers an immediate reconnect. With the plugin, the streamer writes a `pgbus:shutdown` sentinel before the socket closes; browsers reconnect to the new worker and replay missed messages via `Last-Event-ID`.

### Requirements

- **Puma 6.1+ or Falcon.** Streams use `rack.hijack` by default. Puma 6.1+ supports it via partial hijack (thread-releasing, see [puma/puma#1009](https://github.com/puma/puma/issues/1009)). Falcon supports both the hijack path (via [protocol-rack](https://github.com/socketry/protocol-rack)'s emulation) and a native streaming body path (`streams_falcon_streaming_body = true`) that integrates with Falcon's fiber scheduler for better backpressure and connection lifecycle management. Unicorn, Pitchfork, and Passenger return HTTP 501 from the streams endpoint.
- **PostgreSQL LISTEN/NOTIFY.** `config.listen_notify = true` (the default). Stream queues override PGMQ's 250ms NOTIFY throttle to 0 so every broadcast fires individually.
- **HTTP/2 or HTTP/3 in production.** SSE has a 6-connection-per-origin limit on HTTP/1.1; HTTP/2 lifts it. Falcon supports HTTP/2 natively without a reverse proxy.

### Configuration

```ruby
Pgbus.configure do |c|
  c.streams_enabled                = true          # default
  c.streams_default_retention      = 5 * 60        # 5 minutes
  c.streams_retention              = {             # per-stream overrides
    /^chat_/        => 7 * 24 * 3600,              # 7 days for chat history
    "presence_room" => 30                          # 30 seconds for presence
  }
  c.streams_heartbeat_interval     = 15            # seconds
  c.streams_max_connections        = 2_000         # per web-server process (Puma worker or Falcon process)
  c.streams_idle_timeout           = 3_600         # close idle connections after 1h
  c.streams_listen_health_check_ms = 250           # PG LISTEN keepalive + ensure_listening ack budget
  c.streams_write_deadline_ms      = 5_000         # write_nonblock deadline
  c.streams_falcon_streaming_body  = false         # opt-in: Falcon-native streaming body
  c.streams_broadcast_queue        = nil           # dedicated queue for turbo-rails broadcast jobs (see below)
end
```

#### Realtime broadcast isolation

The default `broadcasts_to` / `broadcasts_refreshes` model macros use turbo-rails' `broadcast_*_later_to` helpers, which enqueue the render+broadcast as a **background job** on the default queue. Delivery is isolated (the streamer runs in the web process with its own LISTEN connection), but the *enqueue-render hop* is not: under worker saturation, a broadcast job waits behind long-running jobs, so the browser sees the update only after a worker thread frees up.

**New installs get this out of the box:** `rails generate pgbus:install` ships `config/initializers/pgbus.rb` with `c.streams_broadcast_queue = "realtime"` and a dedicated `realtime` worker capsule. The code default is `nil` (so a programmatic `Pgbus.configure` and existing installs are unchanged) — the generated initializer is where the recommended setup lives.

Two ways to keep broadcasts off the critical path:

1. **Dedicated broadcast queue (recommended for `broadcasts_to`).** Route turbo-rails' broadcast jobs to their own queue and back it with a dedicated worker capsule:

   ```ruby
   c.streams_broadcast_queue = "realtime"
   c.workers = [
     { queues: ["realtime"], threads: 3 },   # broadcasts get their own pool
     { queues: ["*"],        threads: 10 }    # everything else
   ]
   ```

   pgbus applies the queue to `Turbo::Streams::ActionBroadcastJob`, `BroadcastJob`, and `BroadcastStreamJob` at boot. **The queue is only useful if a worker drains it** — `pgbus doctor` warns if `streams_broadcast_queue` is set but no capsule reads it (broadcasts would pile up unread), and warns in production if you use streams with turbo-rails but leave it unset.

2. **Synchronous broadcast (`durable:`).** Passing any non-nil `durable:` to the macros renders and broadcasts in the request thread — no queue hop at all — at the cost of the render happening on the web request:

   ```ruby
   class Message < ApplicationRecord
     broadcasts_to :room, durable: true   # sync: renders + broadcasts inline
   end
   ```

#### Falcon-native streaming body (opt-in)

When running on Falcon, enable native streaming body support for better integration with Falcon's fiber scheduler:

```ruby
c.streams_falcon_streaming_body = true
```

With this flag, `StreamApp` returns `[200, headers, Writable]` instead of hijacking the socket. Falcon drives the response lifecycle with proper backpressure, connection cleanup, and fiber-scheduled IO. SSE writes go through `Protocol::HTTP::Body::Writable` which is fiber-safe and yields to other fibers when blocked.

Without the flag (default), Falcon uses the same `rack.hijack` path as Puma via protocol-rack's emulation. Both paths are tested and work correctly — the streaming body path is an optimization for Falcon deployments that want tighter scheduler integration.

### How it works

Stream broadcasts are stored in PGMQ queues named `#{queue_prefix}_<stream>` (e.g. `pgbus_chat_42`), the same namespace as job queues; the `pgbus_stream_queues` registry records which of those queues back streams so maintenance and wildcard workers can tell them apart. Each broadcast is assigned a monotonic `msg_id` by PGMQ. The `pgbus_stream_from` helper captures the current `MAX(msg_id)` at render time and embeds it in the HTML as `since-id`. When the SSE client connects, it sends that cursor as `?since=` on the first request and as `Last-Event-ID` on reconnects. The streamer replays from `pgmq.q_*` (live) UNION `pgmq.a_*` (archive) for any `msg_id > cursor`, then switches to LISTEN/NOTIFY for the live path. There is no message identity gap between the render and the subscribe — the cursor model guarantees every broadcast is delivered exactly once, in order, even across reconnects.

One Puma worker (or Falcon reactor) hosts one `Pgbus::Web::Streamer::Instance` singleton with three threads (Listener / Dispatcher / Heartbeat) and one dedicated PG connection for LISTEN. Hijacked SSE sockets are held outside the web server's thread pool on Puma (confirmed by an integration test that fires 20 concurrent hijacked connections and observes them complete in parallel on an 8-thread Puma server, [puma/puma#1009](https://github.com/puma/puma/issues/1009)) and inside a fiber on Falcon (one fiber per hijacked connection, scheduler-backed non-blocking IO).

> **Don't use `ActionController::Live` for pgbus streams.** It's the conventional Rails answer for SSE and it's the wrong one. `Live` blocks inside `@app.call(env)` for the lifetime of the connection, which ties up a Puma thread *per subscriber* — the exact problem the `rack.hijack`-based architecture above exists to avoid. Pgbus's streams endpoint is a mounted Rack app (not a Rails controller) so the temptation isn't even available, and a `rake pgbus:streams:lint_no_live` task fails CI if any pgbus controller `include`s it. If you're tempted to wire SSE through a Rails controller, use `pgbus_stream_from` in your view instead — the helper handles the cursor, replay, reconnect, and Puma-thread-release concerns for you.

Per-stream retention is handled by the main pgbus dispatcher process on the same interval as the dispatcher's `ARCHIVE_COMPACTION_INTERVAL` constant. Streams default to a 5-minute retention because SSE clients reconnect within seconds; chat-style applications override the retention to days via `streams_retention`.

### Stream name helpers

Apps using UUID primary keys with turbo-rails-style dom IDs can hit PGMQ's 47-character queue-name ceiling (`"gid://app/Ai::Chat/9c14e8b2-...:messages"` exceeds the limit before the `pgbus_` prefix is even added). Pgbus provides helpers to generate short, collision-safe stream names:

```ruby
# In your ApplicationRecord
class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class
  include Pgbus::Streams::Streamable
end
```

This gives every model `short_id` (16-hex SHA-256 prefix of the GlobalID) and `to_stream_key`:

```ruby
chat = Ai::Chat.find("9c14e8b2-...")
chat.short_id        # => "ai_chat_a3f8c1e9d2b47610"
chat.to_stream_key   # => "ai_chat_a3f8c1e9d2b47610"

# Compose multi-part stream names
Pgbus.stream_key(chat, :messages)  # => "ai_chat_a3f8c1e9d2b47610_messages"

# Use in views
<%= pgbus_stream_from Pgbus.stream_key(@chat, :messages) %>
```

The budget is computed from `config.queue_prefix` at call time so prefix overrides adjust automatically. If a stream name exceeds the budget, `Pgbus::Streams::StreamNameTooLong` is raised immediately with the offending name, computed budget, and a pointer to `Pgbus.stream_key` — before PGMQ is ever touched.

#### `stream_key` idempotency

A single `String` argument to `Pgbus.stream_key` is treated as an already-built pgbus stream key and returned unchanged (after the queue-name budget check), instead of tripping the colon-separator guard. This lets a consumer hold one `stream_key` value and pass it to both `pgbus_stream_from` and the broadcaster without a second call raising `ArgumentError`:

```ruby
key = Pgbus.stream_key(chat, :messages)  # => "ai_chat_a3f8c1e9d2b47610:messages"

# Both calls accept the same pre-built key without raising:
pgbus_stream_from(key)
Pgbus.stream(key).broadcast(html)
```

`Pgbus.stream_key!(key)` accepts a pre-built key explicitly (`String` required, budget still enforced) for call sites that want to be explicit that no re-keying should happen. The guard still fires for the genuinely ambiguous multi-fragment join (`stream_key("a:b", :c)` — colliding with `stream_key("a", "b:c")`), and a `Symbol`/record fragment containing a colon is still rejected (a colon there never came from `stream_key`).

### Transactional broadcasts

**This is the feature no other Rails real-time stack can offer.** A broadcast issued inside an open ActiveRecord transaction is deferred until the transaction commits. If it rolls back, the broadcast silently drops — clients never see the change that the database never persisted.

```ruby
ActiveRecord::Base.transaction do
  @order.update!(status: "shipped")
  @order.broadcast_replace_to :account           # ← deferred until commit
  RelatedService.update_counters!(@order)        # ← might raise, rolling back the update
end
# If RelatedService raised, the database state is unchanged AND no SSE client
# ever saw a "shipped" broadcast. The broadcast and the data mutation are
# atomic with respect to each other.
```

ActionCable can't do this because its broadcast path goes through Redis pub/sub, which has no concept of your application's transaction boundary. Pgbus detects the open AR transaction via `ActiveRecord::Base.connection.current_transaction.after_commit`, which is a first-class Rails API — no outbox table, no background worker, no extra storage.

Outside an open transaction, broadcasts are synchronous and return the assigned `msg_id` as before. Inside a transaction, they return `nil` (the id isn't known until commit time).

### Replaying history on connect (`replay:`)

By default `pgbus_stream_from @room` captures `MAX(msg_id)` at render time and replays only broadcasts published after that — the page-born-stale fix. For chat-history-style applications where the page should show backlog on load, pass `replay:`:

```erb
<%# Show the last 50 messages on load, then stream live %>
<%= pgbus_stream_from @room, replay: 50 %>

<%# Show everything in PGMQ retention on load %>
<%= pgbus_stream_from @room, replay: :all %>

<%# Default behavior (post-render only) — same as omitting the option %>
<%= pgbus_stream_from @room, replay: :watermark %>
```

The replay cap is applied server-side: the helper computes `since_id = max(0, current_msg_id - N)` for integer `N` and writes that into the HTML attribute. The client just reads the attribute and sends it as `?since=` on connect. Nothing else changes about the transport.

How much history is actually available depends on the stream's retention setting (`streams_retention` or `streams_default_retention`, both in seconds). A chat stream configured with `streams_retention = { /^chat_/ => 7.days }` will replay up to seven days of history with `replay: :all`; a notification stream with the 5-minute default will only go back five minutes.

### `msg_id` reconciliation for optimistic UI

Every delivered frame carries its monotonic PGMQ `msg_id` as the SSE `id:` line — the same watermark that powers reconnect replay. `<pgbus-stream-source>` surfaces it to the client two ways: the standard `message` `MessageEvent` sets `lastEventId` to the msg_id (Turbo ignores it; a reactive runtime listening for `message` reads the revision with no pgbus-specific API), and a `pgbus:message` `CustomEvent` carries `{ msgId, data }` (`msgId` is a `Number` when numeric; a negative value marks an ephemeral frame that bypassed PGMQ rather than a durable, archived one).

**The reconciliation recipe:** track the highest applied `msgId` per render target; when a frame arrives, skip the morph if you've already applied a newer revision for that target. This stops a late echo — a broadcast that was in flight when a newer one landed — from clobbering a newer optimistic edit:

```js
const appliedRevision = new Map() // target -> highest applied msgId

document.addEventListener("pgbus:message", (event) => {
  const { msgId, data } = event.detail
  const target = extractTargetFrom(data) // however your markup encodes it

  const highest = appliedRevision.get(target) ?? -Infinity
  if (msgId != null && msgId < highest) return // stale — skip the morph

  appliedRevision.set(target, msgId)
  applyMorph(target, data)
})
```

This complements `exclude:` (below): `exclude:` handles the actor (never receives its own echo at all); msg_id reconciliation handles out-of-order delivery for everyone else.

### Server-side audience filtering

Some broadcasts shouldn't reach every subscriber on a stream. Pgbus supports per-connection filtering via a registry of named predicates evaluated against each connection's authorize-hook context:

```ruby
# config/initializers/pgbus_streams.rb
Pgbus::Streams.filters.register(:admin_only) { |user| user&.admin? }
Pgbus::Streams.filters.register(:workspace_member) do |user, stream_name|
  user&.workspace_ids&.include?(stream_name.split(":").last.to_i)
end
```

The authorize hook on `Pgbus::Web::StreamApp` doubles as a context provider — return any non-boolean value (typically a `User` model) and pgbus will pass it to the filter predicate when evaluating broadcasts:

```ruby
Pgbus::Web::StreamApp.new(authorize: ->(env, _stream_name) {
  user = User.find_by(id: env["rack.session"][:user_id])
  return false unless user
  user  # ← context attached to the connection
})
```

Then label broadcasts with the filter you want to apply:

```ruby
@order.broadcast_replace_to :account                                # delivered to everyone
Pgbus.stream("ops").broadcast(html, visible_to: :admin_only)        # admins only
```

Failure semantics:

- **Unknown filter label** → fail-CLOSED with a warning log. Audience filtering is a data-isolation feature; failing open on a typo would turn a restricted broadcast into a public one. The warning log is loud enough that typos still get noticed in dev ("why are no subscribers receiving my broadcast?" → check the log).
- **Filter predicate raises** → fail-CLOSED. A buggy predicate that crashes is treated as "deny" so private data doesn't leak on an exception path.
- **No `visible_to` on the broadcast** → no filter applied; everyone sees it.

The filter registry is process-local. Each Puma worker (or Falcon reactor) has its own copy populated at boot. Filter predicates run **on the subscriber side** — the predicate itself can't be serialized through PGMQ, so the broadcast carries only the label name.

### Actor-echo suppression (`exclude:`)

An actor who just triggered a change already applied it via the HTTP response of their own action. If the resulting broadcast reaches their own SSE connection too, it double-applies — re-running animations or clobbering an optimistic edit. Pass `exclude:` with a connection id to skip delivery to that one connection; everyone else still gets the broadcast:

```ruby
Pgbus.stream(room).broadcast(html, exclude: connection_id)
```

Every server-minted SSE connection exposes its id to the page: right after the open handshake, pgbus sends a `pgbus:connected` frame carrying the connection id, and `<pgbus-stream-source>` captures it onto its `connection-id` attribute and re-dispatches it as a `pgbus:connected` event (also present in `pgbus:open`'s detail). The page reads that id and sends it back as the `X-Pgbus-Connection` header on the action request that triggers the broadcast:

```js
document.addEventListener("pgbus:connected", (event) => {
  document.querySelector("meta[name='pgbus-connection-id']")
    ?.setAttribute("content", event.detail.connectionId)
})

// On the next fetch/form submit:
fetch(url, {
  method: "POST",
  headers: { "X-Pgbus-Connection": connectionIdMetaTag() }
})
```

```ruby
def create
  @message = @room.messages.create!(message_params)
  @room.broadcast_append_to(:messages, exclude: request.headers["X-Pgbus-Connection"])
end
```

A nil or blank `exclude:` is a no-op — the common path for background jobs and other server-initiated broadcasts with no originating connection. Reuses the existing per-connection delivery (Filters) path.

### `broadcast_render` — render and broadcast in one call

`Stream#broadcast_render` renders a Phlex component, a ViewComponent, or a pre-rendered HTML string into a complete `<turbo-stream>` action tag and broadcasts it atomically — removing the off-request render + tag-building boilerplate (and the easy-to-get-wrong view context) from every call site:

```ruby
Pgbus.stream("chat", room).broadcast_render(
  renderable: Chat::Message.new(chat_message: msg),
  action: :append,
  target: "chat-messages-#{room}",
  exclude: connection_id   # composes with #exclude
)
```

`action` defaults to `:replace`; `target:` is required. The renderable is resolved via `String` → `#call` (Phlex) → `#render_in` (ViewComponent; called with a `nil` view context since there's no controller off-request) → `#to_s`. Content-less actions (`remove`) emit no `<template>` wrapper. `exclude:`, `visible_to:`, `durable:`, `event:`, and `coalesce:` all forward to `#broadcast` unchanged, so actor-echo suppression and audience filtering compose. A component that needs URL helpers or a full view context should be rendered by the app and the resulting string passed as `renderable:`.

### Typed SSE event names (`event:`)

A broadcast can set the SSE `event:` field while keeping the payload a Turbo Stream, so clients route on a typed name instead of sniffing the HTML:

```ruby
Pgbus.stream(name).broadcast(html, event: "presence")
Pgbus.stream(name).broadcast_render(renderable: component, target: "cursor", event: "reactive")
```

The default (`nil` or `"turbo-stream"`) is omitted from the JSONB payload to avoid redundancy, but is still set on the SSE frame's `event:` line (falling back to `turbo-stream`), so default consumers still get the standard `message`/turbo-stream path.

On the client, `<pgbus-stream-source>` dispatches a typed broadcast two ways: a generic `pgbus:event` (`{ event, data, msgId }`) for one listener that handles every typed event, and a named `pgbus:<event>` (`{ data, msgId }`) for `addEventListener("pgbus:presence", …)` ergonomics:

```js
document.addEventListener("pgbus:presence", (event) => {
  const { data, msgId } = event.detail
  // ...
})
```

Native `EventSource` (the reconnect path) only invokes listeners registered by name, so declare every typed event name you use on the element's `listen-events` attribute (comma- or space-separated) — otherwise a typed broadcast is silently dropped after a reconnect, even though it worked on the first connection (which uses `fetch()` and routes any event generically):

```erb
<%= pgbus_stream_from @room, "listen-events": "presence reactive" %>
```

### `coalesce:` — publish-side debounce

A chatty component — a live cursor, a typing indicator, a progress bar — can fan out many small broadcasts per second. Pass `coalesce:` (a window in milliseconds, or `true` for the 50ms default) together with `target:` to batch broadcasts per `(stream, target)` and publish only the *latest* frame within the window:

```ruby
Pgbus.stream(name).broadcast_render(
  renderable: CursorPosition.new(x:, y:),
  target: "cursor-#{user_id}",
  coalesce: true   # or coalesce: 100 for a 100ms window
)
```

Superseded frames never hit the bus at all — no PGMQ insert, no NOTIFY, no fan-out. This is last-write-wins, so it's only safe for **idempotent replace/update of a stable target** (exactly the high-frequency case above) — never for actions where every intermediate frame matters (an `append` to a running log, for instance).

Semantics: the first submit for a `(stream, target)` schedules the flush one window later; every subsequent submit within that window only overwrites the buffered payload. Latency is bounded to one window (trailing-edge-with-max-wait, not a resettable debounce). The flush re-enters the normal broadcast path, so a coalesced frame still composes with `visible_to:`, `exclude:`, `event:`, and `durable:`. Coalescing is process-wide and in-memory — behind multiple Puma workers or Falcon processes, each process debounces its own submissions independently.

### Presence

Pgbus tracks who is currently subscribed to a stream via a `pgbus_presence_members` table. This is the standard "X people are in this room" feature that chat apps and collaboration tools need:

```ruby
rails generate pgbus:add_presence
rails db:migrate
```

```ruby
class RoomsController < ApplicationController
  def show
    @room = Room.find(params[:id])
    Pgbus.stream(@room).presence.join(
      member_id: current_user.id.to_s,
      metadata: { name: current_user.name, avatar: current_user.avatar_url }
    ) do |member|
      render_to_string(partial: "presence/joined", locals: { member: member })
    end
  end

  def destroy
    Pgbus.stream(@room).presence.leave(member_id: current_user.id.to_s) do |member|
      "<turbo-stream action=\"remove\" target=\"presence-#{member['id']}\"></turbo-stream>"
    end
  end
end
```

The block passed to `join`/`leave` is rendered into HTML and broadcast through the regular pgbus stream pipeline — so it shows up in every connected client's DOM in real time, alongside the normal `broadcasts_to` output. Reading the current member list:

```ruby
Pgbus.stream(@room).presence.members
# => [{ "id" => "7", "metadata" => {...}, "joined_at" => "...", "last_seen_at" => "..." }]

Pgbus.stream(@room).presence.count
# => 5
```

**Heartbeat and expiry.** Members that don't ping `presence.touch(member_id: ...)` periodically can be expired by a sweeper:

```ruby
# Run from a cron, ActiveJob, or after each subscriber heartbeat
Pgbus.stream(@room).presence.sweep!(older_than: 60.seconds.ago)
```

The sweep uses `DELETE ... RETURNING` so multiple workers running it concurrently won't double-emit leave events.

**Deliberately left to the application (manual API)**:

- Join/leave is explicit, not connection-driven, unless you opt into connection-driven presence below. The controller decides who is "present" — a connected SSE client is not always a present user (think tab-in-background, multi-tab dedup).
- The stale-member sweep is manual. Run it from a cron, an ActiveJob, or your existing heartbeat — pgbus does not assume one over the others.
- The DOM markup for join/leave is whatever your `join`/`leave` block returns. Pgbus does not impose a fixed presence schema on `<pgbus-stream-source>`.

#### Connection-driven presence (opt-in)

Streams matching `config.streams_presence_patterns` (an exact string or a `Regexp`, mirroring `streams_durable_patterns`) automatically join a member when an SSE connection opens, leave when it closes, and refresh `last_seen_at` on every keepalive heartbeat tick — no explicit `join`/`leave`/sweeper calls required:

```ruby
Pgbus.configure do |c|
  c.streams_presence_patterns = [/^room:/, "lobby"]
end
```

Identity comes from the connection's authorize-hook context (the value your `StreamApp` `authorize:` callable returns). The built-in extractor handles the common shapes without any configuration: a `Hash` with `:member_id` (or `:id`) and optional `:metadata`, or any object responding to `#id` (e.g. a `User` model). For anything else, provide a custom extractor — a `->(context) { { id:, metadata: } }` callable returning `nil` for a context with no derivable identity (anonymous connections are simply skipped, not an error):

```ruby
Pgbus.configure do |c|
  c.streams_presence_patterns = [/^room:/]
  c.streams_presence_member = ->(user) {
    { id: user.id, metadata: { name: user.name, avatar: user.avatar_url } } if user
  }
end
```

Membership work runs on the dispatcher thread (which already releases AR connections each pass); the heartbeat posts a batched touch per tick. Presence failures are logged and swallowed so a presence-table hiccup can't knock a live SSE connection out of the registry.

### Stream stats (opt-in)

Pgbus can record one row in `pgbus_stream_stats` per broadcast, connect, and disconnect so the `/pgbus/insights` dashboard shows stream throughput alongside job throughput. This is **disabled by default** because stream event volume can dwarf job volume in chat-style apps — enable it deliberately when you want the observability.

```ruby
# config/initializers/pgbus.rb
Pgbus.configure do |c|
  c.streams_stats_enabled = true
end
```

Then run the migration generator once:

```bash
rails generate pgbus:add_stream_stats                  # Add the migration
rails generate pgbus:add_stream_stats --database=pgbus # For separate database
rails db:migrate
```

The Insights tab gains a "Real-time Streams" section with counts of broadcasts / connects / disconnects, an "active" estimate (connects − disconnects in the selected window), average fanout per broadcast, and a "Top Streams by Broadcast Volume" table. The existing `stats_retention` config covers cleanup, so there is no separate retention knob.

Overhead on a real Puma + PGMQ setup (`bundle exec rake bench:streams`): the most visible cost is an INSERT per connect/disconnect pair, which shows up under thundering-herd connect scenarios (K=50 concurrent connects: ~+20% per-connect latency). Steady-state broadcast and fanout numbers stay in the run-to-run noise band. Enable it if Insights is useful; leave it off if the write traffic worries you.

## Testing

Pgbus ships opt-in test helpers for both RSpec and Minitest. The testing module is **never autoloaded** by Zeitwerk -- you must `require` it explicitly, so it cannot leak into production.

### RSpec setup

Add one line to your `spec/rails_helper.rb` (or `spec/spec_helper.rb`):

```ruby
# spec/rails_helper.rb
require "pgbus/testing/rspec"
```

This does three things:

1. Loads `Pgbus::Testing` with the in-memory `EventStore` and mode management
2. Registers the `have_published_event` matcher
3. Includes `Pgbus::Testing::Assertions` into all example groups

You still need to activate a testing mode and clear the store per test. Add a `before`/`after` block:

```ruby
# spec/rails_helper.rb
require "pgbus/testing/rspec"

RSpec.configure do |config|
  config.before { Pgbus::Testing.fake! }
  # append_after, not after: config-level `after` hooks run in reverse
  # registration order, so one registered after `capybara/rspec` runs BEFORE
  # Capybara.reset_sessions! — while the browser page is still open.
  # append_after runs once the page is closed and its pending SSE requests
  # are drained (see "SSE streams in tests" below).
  config.append_after do
    Pgbus::Testing.disabled!
    Pgbus::Testing.store.clear!
  end
end
```

Or scope it to specific groups:

```ruby
RSpec.configure do |config|
  config.before(:each, :pgbus) { Pgbus::Testing.fake! }
  config.append_after(:each, :pgbus) do
    Pgbus::Testing.disabled!
    Pgbus::Testing.store.clear!
  end
end

# Usage:
RSpec.describe OrderService, :pgbus do
  it "publishes an event" do
    expect { described_class.create!(attrs) }
      .to have_published_event("orders.created")
  end
end
```

### Minitest / TestUnit setup

Add the require and include to your `test/test_helper.rb`:

```ruby
# test/test_helper.rb
require "pgbus/testing/minitest"

class ActiveSupport::TestCase
  include Pgbus::Testing::MinitestHelpers
end
```

`MinitestHelpers` hooks into Minitest's lifecycle automatically:

- **`before_setup`** -- activates `:fake` mode and clears the event store before each test
- Includes all assertion helpers (`assert_pgbus_published`, `assert_no_pgbus_published`, `pgbus_published_events`, `perform_published_events`)

No additional `setup`/`teardown` blocks are needed -- the module handles it.

### Event bus assertions

Both RSpec and Minitest share the same assertion helpers via `Pgbus::Testing::Assertions`:

```ruby
# Assert that a block publishes exactly N events
assert_pgbus_published(count: 1, routing_key: "orders.created") do
  OrderService.create!(attrs)
end

# Assert that a block publishes zero events
assert_no_pgbus_published(routing_key: "orders.created") do
  OrderService.preview(attrs)
end

# Inspect captured events directly
events = pgbus_published_events(routing_key: "orders.created")
assert_equal 1, events.size
assert_equal({ "id" => 42 }, events.first.payload)

# Capture events, then dispatch them to registered handlers
perform_published_events do
  OrderService.create!(attrs)
end
# After the block, all captured events have been dispatched to their
# matching handlers synchronously -- useful for testing side effects
```

### RSpec matchers

The `have_published_event` matcher supports chainable constraints:

```ruby
# Basic: assert any event was published with the given routing key
expect { publish_order(order) }
  .to have_published_event("orders.created")

# With payload matching (uses RSpec's values_match?, so hash_including works)
expect { publish_order(order) }
  .to have_published_event("orders.created")
  .with_payload(hash_including("id" => order.id))

# With header matching
expect { publish_order(order) }
  .to have_published_event("orders.created")
  .with_headers(hash_including("x-tenant" => "acme"))

# Exact count
expect { publish_order(order) }
  .to have_published_event("orders.created")
  .exactly(1)

# Combine all constraints
expect { publish_order(order) }
  .to have_published_event("orders.created")
  .with_payload(hash_including("id" => order.id))
  .with_headers(hash_including("x-tenant" => "acme"))
  .exactly(1)

# Negated
expect { publish_order(order) }
  .not_to have_published_event("orders.cancelled")
```

### Testing modes

Three modes control how `Pgbus::EventBus::Publisher.publish` behaves:

| Mode | Behavior | Use case |
|------|----------|----------|
| `:fake` | Captures events in-memory, no PGMQ calls, no handler dispatch | Most unit/integration tests |
| `:inline` | Captures events AND immediately dispatches to matching handlers | Testing side effects (handler logic) |
| `:disabled` | Pass-through to real publisher (production behavior) | Default; integration tests with real PGMQ |

Switch modes globally or scoped to a block:

```ruby
# Global (persists until changed)
Pgbus::Testing.fake!
Pgbus::Testing.inline!
Pgbus::Testing.disabled!

# Scoped (restores previous mode after block)
Pgbus::Testing.inline! do
  OrderService.create!(attrs)  # handlers fire synchronously
end
# mode is restored to whatever it was before

# Query current mode
Pgbus::Testing.fake?     # => true/false
Pgbus::Testing.inline?   # => true/false
Pgbus::Testing.disabled? # => true/false
```

The `:inline` mode skips delayed publishes (`delay: > 0`) -- those are captured in the store but not dispatched. Use `Pgbus::Testing.store.drain!` to manually dispatch all captured events including delayed ones.

### SSE streams in tests

When using `use_transactional_fixtures = true` (the default in Rails), pgbus SSE streams are incompatible with transactional test isolation. The `rack.hijack` mechanism spawns background threads that acquire their own database connections outside the test transaction, which causes:

- Connection pool exhaustion after enough system tests
- CI hangs (tests freeze waiting for a connection)
- `Errno::EPIPE` errors when the browser navigates away

**Automatic fix with `Pgbus::Testing`:** When you activate `:fake` or `:inline` mode (as shown above), pgbus automatically enables `streams_test_mode`. The SSE endpoint returns a stub response (valid SSE headers + a comment + immediate close) without hijacking, without spawning background threads, and without acquiring any database connections. The `<pgbus-stream-source>` custom element still renders and connects, but no PGMQ polling occurs.

If you're using the RSpec or Minitest setup shown above, **you don't need to do anything extra** -- streams are safe automatically.

**Manual configuration** (if you don't use `Pgbus::Testing`):

```ruby
# config/initializers/pgbus.rb
Pgbus.configure do |c|
  c.streams_test_mode = true if Rails.env.test?
end
```

Or toggle it per test:

```ruby
# RSpec
before { Pgbus.configuration.streams_test_mode = true }
after  { Pgbus.configuration.streams_test_mode = false }

# Minitest
setup    { Pgbus.configuration.streams_test_mode = true }
teardown { Pgbus.configuration.streams_test_mode = false }
```

**What `streams_test_mode` does:** The `StreamApp` short-circuits after signature verification and authorization checks but before any hijack, streaming body, or capacity logic. It returns:

```
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache, no-transform

retry: 86400000

: pgbus test mode — connection accepted, no polling
```

This is a valid SSE response that the browser's EventSource will accept. No `Streamer` singleton is created, no PG LISTEN connection is opened, and no dispatcher/heartbeat/listener threads are spawned. The `retry:` directive tells `EventSource` to wait 24 hours before reconnecting: without it a page left open re-requests the closed stub every ~3 seconds for the whole example, and that reconnect storm is what turns a teardown race into a real streamer running inside the test process.

**Hook ordering with Capybara:** `Pgbus::Testing.disabled!` turns `streams_test_mode` back off. If it runs while the browser page is still open, a reconnect landing in that window takes the real stream path and starts a live `Streamer` — with listener/dispatcher/heartbeat threads and a LISTEN connection — inside your RSpec process. RSpec runs config-level `after` hooks in reverse registration order, so a plain `config.after { Pgbus::Testing.disabled! }` registered after `capybara/rspec` fires *before* `Capybara.reset_sessions!`. Register it with `config.append_after` (as in the snippet above) so the page is closed first. If a live streamer does get started and its threads outlive the bounded shutdown, `disabled!` raises `Pgbus::Testing::StreamerLeakError` with this diagnosis rather than letting the suite hang on a corrupted shared connection; a streamer that shut down cleanly is only logged as a warning, because that is expected inside `Pgbus::Testing.disabled! do ... end` real-stream tests.

**Testing actual stream delivery:** If you need to verify end-to-end SSE message delivery in integration tests, disable `streams_test_mode` and use the `PumaTestHarness` from the pgbus test support:

```ruby
require "pgbus/testing"

Pgbus::Testing.disabled! do
  # streams_test_mode is automatically disabled
  # Use real Puma + real PGMQ for end-to-end stream tests
end
```

See `spec/integration/streams/` in the pgbus source for examples of integration tests that exercise the full SSE pipeline with a real Puma server.

## Operations

Day-to-day running of Pgbus: starting and stopping processes, observing what is happening on the dashboard, the database tables Pgbus relies on, and how to migrate from an existing job backend.

### CLI

```bash
pgbus start     # Start supervisor with workers + dispatcher + scheduler
pgbus status    # Show running processes
pgbus queues    # List queues with depth/metrics
pgbus dlq       # Inspect and drain dead-letter queues (see below)
pgbus doctor    # Preflight diagnostics; exits 1 on any failed check (see below)
pgbus mcp       # Start the read-only MCP diagnostic server over stdio
pgbus version   # Print version
pgbus help      # Show help
```

#### pgbus doctor

A single preflight command that answers "is this environment healthy enough to run?" — useful as a deploy or CI gate. It runs 11 checks and never raises; a broken environment turns every probe into a failed/warned check instead of a crash:

| Check | Fails (`:fail`) when | Warns (`:warn`) when |
|---|---|---|
| Configuration | `Configuration#validate!` raises | — |
| Database | Unreachable (`SELECT 1` via `Client#ping`) | — |
| PGMQ schema | Schema not installed | Installed but untracked, or behind the vendored version |
| Queues | A configured queue has no PGMQ table | — |
| LISTEN/NOTIFY | — | A configured queue is missing its insert trigger (falls back to polling) |
| Process liveness | Verdict is `STALLED` | Verdict is `DEGRADED` |
| GlobalID allowlist | — | `allowed_global_id_models` is `nil` (allow-all) in production |
| Broadcast queue | — | Turbo broadcasts share the default queue in production, or `streams_broadcast_queue` is set but no worker capsule drains it |
| Primary affinity | — | Job connection is on a read-only replica (`pg_is_in_recovery`) — a read/write-splitting pooler may be stalling jobs |
| Dedicated connections | Streamer LISTEN and/or worker notify dedicated path cannot connect | — |
| Connection budget | — (informational: prints how many direct LISTEN connections the current config pins — 1 per host under `worker_notify_scope: :supervisor`, one per fork under `:fork`; streams add 1 per web host under `streams_listen_scope: :master` or 1 per web process under `:process`) | — |

```bash
pgbus doctor       # prints the report; exit 1 unless every check passed
rake pgbus:doctor  # identical checks, for a Rake-based deploy pipeline
```

The report ends with a resolved-config summary (queue prefix, pool size, roles, capsules) with any password redacted from `database_url` / `connection_params`. Warnings do not fail the exit code — only a `:fail` result does.

##### Doctor preflight at boot (single Rails boot)

Running `pgbus doctor || true` in a container entrypoint before `pgbus start` boots the full Rails app **twice** on every deploy, and the pre-supervisor `Process liveness` check false-fails (no workers exist yet) — which is why the `|| true` is needed, swallowing any genuinely-fatal finding too. Instead, run the preflight *inside* the booting supervisor:

```bash
pgbus start --doctor          # run the preflight, log the report, always boot
pgbus start --doctor-strict   # refuse to boot on a fatal check (exit non-zero, fork nothing)
```

or set it in the initializer:

```ruby
Pgbus.configure do |c|
  c.doctor_on_boot = :report   # or :strict; nil/false (default) is off
end
```

The boot preflight runs after the DB is verified reachable and queues are bootstrapped, but **before any worker is forked** — one Rails boot, and the `Process liveness` check is skipped (nothing to observe yet, so it can't false-fail). `:strict` aborts the boot only on a genuinely-fatal, non-transient check — a `Configuration` failure or an **absent** PGMQ schema. A transient DB blip that the lenient queue bootstrap is designed to ride out (surfacing as a `Queues` or `Database` failure) is reported but never aborts, so a fleet-wide cold boot against a momentarily-saturated primary doesn't fail every pod in lockstep.

#### pgbus dlq

Dead-letter inspect/drain operations from a headless deployment or incident runbook, routed through `Web::DataSource` so retry/discard semantics are identical to the dashboard (origin-queue re-enqueue, transactional produce+delete, lock release on discard) with zero raw SQL and no direct PGMQ calls:

| Subcommand | Does |
|---|---|
| `pgbus dlq list [--page N] [--per-page N]` | Table of msg_id / DLQ queue / origin queue / read_ct / enqueued_at, plus a total count. Never prints payloads. |
| `pgbus dlq show MSG_ID` | Message metadata plus the payload, filtered through the dashboard's sensitive-data filter. |
| `pgbus dlq retry MSG_ID` | Re-enqueue one message to its origin queue. |
| `pgbus dlq retry-all` | Re-enqueue every dead-letter message; prints the count. |
| `pgbus dlq purge MSG_ID` | Discard a single message. |
| `pgbus dlq purge --all --yes` | Discard every dead-letter message. Omitting `--yes` makes no changes and exits 1. |

An unknown `msg_id` or an unknown subcommand exits 1.

#### Role flags (split deployments)

By default, `pgbus start` boots every role in one supervisor (workers, dispatcher, scheduler, event consumers, outbox poller). For containerized deployments where each role lives in a separate process, use the role flags:

```bash
pgbus start --workers-only      # Only worker processes
pgbus start --scheduler-only    # Only the recurring-task scheduler
pgbus start --dispatcher-only   # Only the maintenance dispatcher
```

These flags are mutually exclusive. The auto-tuned `pool_size` adjusts to the role: a `--scheduler-only` deployment with 50 worker threads configured only opens the connections it actually needs (1 for the scheduler), not 51.

#### Capsule selection

`--capsule NAME` boots a single named capsule. Combine with `--workers-only` to run one capsule per container:

```bash
pgbus start --workers-only --capsule critical
pgbus start --workers-only --capsule default
```

The capsule name is the `:name` you passed to `c.capsule` in your initializer (or the first queue token when using the string DSL).

### Dashboard

The dashboard is a mountable Rails engine at `/pgbus` with:

- **Overview** — queue depths, enqueued count, active processes, failure count, throughput rate
- **Queues** — per-queue metrics, purge/pause/resume/delete actions
- **Jobs** — enqueued and failed jobs, retry/discard actions
- **Dead letter** — DLQ messages with retry/discard, bulk actions
- **Processes** — active workers/dispatcher/consumers with heartbeat status **and per-worker throughput** (e.g. `12.4/s processed · 0.2/s failed`)
- **Events** — registered subscribers and processed events
- **Outbox** — transactional outbox entries pending publication
- **Locks** — active job uniqueness locks with state (queued/executing), owner PID@hostname, age
- **Insights** — throughput chart (jobs/min), status distribution donut, slowest job classes table

All tables use Turbo Frames for periodic auto-refresh without page reloads. Destructive actions use styled confirmation dialogs (not browser `confirm()`), and flash messages appear as auto-dismissing toast notifications.

Each worker snapshots its live in-process rate counter (dequeued/processed/failed) into its heartbeat metadata on every beat, so the Processes panel shows a cluster-wide, near-real-time throughput view with no extra query. Zero rates are omitted from the rendered string. Non-worker processes (dispatcher, scheduler, consumers) carry no rate data and render only their static boot metadata, unchanged.

#### Queue management

The queues page lets you manage PGMQ queues directly:

- **Purge** — removes all messages from the queue (the queue itself remains)
- **Delete** — permanently drops the queue from PGMQ (removes the queue table and metadata)
- **Pause / Resume** — pauses or resumes job processing for a queue

All destructive actions require confirmation. Pause/resume and delete are available on both the queue index and detail pages.

#### Dark mode

The dashboard supports dark mode via Tailwind CSS `dark:` classes. It respects your system preference on first visit and persists your choice via localStorage. Toggle with the sun/moon button in the nav bar.

#### Job stats and insights

The executor records every job completion to `pgbus_job_stats` (job class, queue, status, duration). The insights page visualizes this data with ApexCharts (loaded via CDN, zero npm dependencies).

```bash
rails generate pgbus:add_job_stats           # Add the stats migration
rails generate pgbus:add_job_stats --database=pgbus
```

Stats collection is enabled by default (`config.stats_enabled = true`). Old stats are cleaned up by the dispatcher based on `config.stats_retention` (default: 30 days). If the migration hasn't been run yet, stat recording is silently skipped.

Stats are buffered in memory and bulk-inserted rather than written one row per job. Each worker flushes its buffer when it fills to `config.stats_flush_size` entries (default: `100`), when `config.stats_flush_interval` seconds have elapsed (default: `5`), and immediately when the worker begins draining (graceful `TERM` shutdown or a recycle threshold). Tune the thresholds down for high-throughput deployments that want a tighter loss window, or up to reduce insert frequency:

```ruby
Pgbus.configure do |c|
  c.stats_flush_size = 500     # flush after 500 buffered stats
  c.stats_flush_interval = 2   # ...or every 2 seconds, whichever comes first
end
```

**Loss window:** stats are advisory (dashboard/insights only), never job payloads. Buffered entries are flushed on graceful shutdown and drain entry, so a clean stop loses nothing. If the supervisor watchdog `SIGKILL`s a stalled worker — which cannot be trapped — up to `stats_flush_interval` seconds / `stats_flush_size` entries accumulated since the last flush are lost. This affects insights accuracy only; no job is dropped.

### Database tables

Pgbus uses these tables (created via PGMQ and migrations):

| Table | Purpose |
|-------|---------|
| `q_pgbus_*` | PGMQ job queues (managed by PGMQ) |
| `a_pgbus_*` | PGMQ archive tables (managed by PGMQ, compacted by dispatcher) |
| `pgbus_processes` | Heartbeat tracking for workers/dispatcher/consumers |
| `pgbus_failed_events` | Failed event dispatch records |
| `pgbus_processed_events` | Idempotency deduplication (event_id, handler_class) |
| `pgbus_semaphores` | Concurrency control counting semaphores |
| `pgbus_blocked_executions` | Jobs waiting for a concurrency semaphore slot |
| `pgbus_batches` | Batch tracking with job counters and callback config |
| `pgbus_uniqueness_keys` | Job uniqueness locks (lock_key, queue_name, msg_id) |
| `pgbus_job_stats` | Job execution metrics (class, queue, status, duration) |
| `pgbus_queue_states` | Queue pause/resume and circuit breaker state |
| `pgbus_outbox_entries` | Transactional outbox entries pending publication |
| `pgbus_recurring_tasks` | Recurring job definitions |
| `pgbus_recurring_executions` | Recurring job execution history |
| `pgbus_presence_members` | Stream presence tracking (who is subscribed) |
| `pgbus_stream_stats` | Stream broadcast/connect/disconnect metrics (opt-in) |

### Switching from another backend

Already using a different job processor? These guides walk you through the migration:

- **[Switch from Sidekiq](docs/switch_from_sidekiq.md)** — remove Redis, convert native workers, replace middleware with callbacks
- **[Switch from SolidQueue](docs/switch_from_solid_queue.md)** — similar architecture, swap config format, gain LISTEN/NOTIFY + worker recycling
- **[Switch from GoodJob](docs/switch_from_good_job.md)** — both PostgreSQL-native, swap advisory locks for PGMQ visibility timeouts

See [docs/README.md](docs/README.md) for a full feature comparison table.

## Reference

Architectural overview and the full list of configuration settings.

### Architecture

```text
Supervisor (fork manager)
  ├── Worker 1        (queues: [default, mailers], threads: 10, priority: 10)
  ├── Worker 2        (queues: [critical], threads: 5, single_active_consumer: true)
  ├── Dispatcher      (maintenance: cleanup, compaction, reaping, circuit breaker)
  ├── Scheduler       (recurring tasks via cron)
  ├── Consumer        (event bus topics)
  └── Outbox Poller   (transactional outbox → PGMQ, when enabled)

PostgreSQL + PGMQ
  ├── pgbus_default          (job queue)
  ├── pgbus_default_dlq      (dead letter queue)
  ├── pgbus_critical         (job queue)
  ├── pgbus_critical_dlq     (dead letter queue)
  ├── pgbus_mailers          (job queue)
  └── pgbus_queue_states     (pause/resume + circuit breaker state)
```

#### How a job flows through the system

1. **Enqueue**: ActiveJob serializes the job to JSON, Pgbus sends it to the appropriate PGMQ queue
2. **Read**: Workers poll queues (or wake instantly via LISTEN/NOTIFY) and claim messages with a visibility timeout
3. **Execute**: The job is deserialized and executed within the Rails executor
4. **Archive/Retry**: On success, the message is archived. On failure, the visibility timeout expires and the message becomes available again. PGMQ's `read_ct` tracks delivery attempts
5. **Dead letter**: When `read_ct` exceeds `max_retries`, the message is moved to the `_dlq` queue for manual inspection

### Configuration reference

Curated headline options for the README. The full operator reference (with types, groups, and drift-checked accessors) lives on the [Configuration](https://pgbus.zoolutions.llc/docs/configuration) docs page.

| Option | Default | Description |
|--------|---------|-------------|
| `database_url` | `nil` | PostgreSQL connection URL (auto-detected in Rails) |
| `connection_params` | `nil` | Extra connection parameters merged into the pool |
| `connects_to` | `nil` | Rails multi-database config for a dedicated pgbus database (`{ database: { writing: :pgbus } }`) |
| `require_primary` | `false` | Reject a job connection that lands on a read-only replica (`pg_is_in_recovery`) at boot — pooler safety against a read/write splitter |
| `connection_guc_mode` | `:options` | How database.yml GUCs (`variables:`) reach pgbus connections — `:options` (libpq startup) or `:session` (post-connect `SET`, for transaction-mode PgBouncer) |
| `queue_prefix` | `"pgbus"` | Prefix for all PGMQ queue names |
| `default_queue` | `"default"` | Default queue for jobs without explicit queue |
| `pool_size` | `nil` (auto) | Connection pool size. Auto-tuned from worker thread counts: `sum(workers.threads) + sum(event_consumers.threads) + 2`. Set explicitly to override. |
| `pool_timeout` | `5` | Seconds to wait for a pooled connection |
| `workers` | `[{queues: ["default"], threads: 5}]` | Worker capsule definitions. String DSL (`"default: 5; critical: 10"`), Array, or `nil`. |
| `event_consumers` | `nil` | Event consumer process definitions (same format as workers) |
| `roles` | `nil` (all) | Supervisor role filter — usually set via CLI flags (`--workers-only` etc.) |
| `polling_interval` | `0.1` | Seconds between polls (LISTEN/NOTIFY is primary) |
| `visibility_timeout` | `30` | Time before unacked message becomes visible again. Accepts seconds or `ActiveSupport::Duration` (e.g. `10.minutes`) |
| `visibility_heartbeat` | `true` | Re-arm a running job's visibility timeout while `perform` runs, so long jobs are not redelivered or dead-lettered mid-run. `false` restores plain PGMQ semantics |
| `visibility_heartbeat_interval` | `nil` (`visibility_timeout / 3`) | Seconds (or Duration) between two extensions; must stay below `visibility_timeout` |
| `max_retries` | `5` | Failed reads before routing to dead letter queue |
| `retry_backoff` | `5` | Base delay in seconds for VT-based retry backoff (exponential: `base * 2^(attempt-1)`) |
| `retry_backoff_max` | `300` | Maximum retry delay in seconds (caps the exponential curve) |
| `retry_backoff_jitter` | `0.15` | Jitter factor (0-1) added to retry delays to spread retries |
| `max_jobs_per_worker` | `nil` | Recycle worker after N jobs (nil = unlimited) |
| `max_memory_mb` | `nil` | Recycle worker when memory exceeds N MB |
| `max_worker_lifetime` | `nil` | Recycle worker after N seconds. Accepts seconds or Duration. |
| `listen_notify` | `true` | Use PGMQ's LISTEN/NOTIFY for instant wake-up |
| `worker_notify_wakeup` | `nil` (follows `listen_notify`) | Dedicated LISTEN connection for worker wake-up. `nil` follows `listen_notify`; set `false` to force polling. |
| `worker_notify_host` / `worker_notify_port` / `worker_notify_database_url` | `nil` | Override host/port/URL for the worker notify LISTEN connection (e.g. direct primary port when jobs go through PgBouncer) |
| `prefetch_limit` | `nil` | Max in-flight messages per worker (nil = unlimited) |
| `dispatch_interval` | `1.0` | Seconds between dispatcher maintenance ticks |
| `circuit_breaker_enabled` | `true` | Enable auto-pause on consecutive failures (threshold and backoff are tuned via `Pgbus::CircuitBreaker` constants) |
| `zombie_detection` | `true` | Detect and reclaim work from crashed workers |
| `read_timeout` | `30` | Seconds before a single PGMQ read is bounded (libpq `statement_timeout` + `tcp_user_timeout` on a dedicated connection; nil disables) |
| `drain_timeout` | `30` | Seconds to wait for in-flight jobs during graceful shutdown before abandoning them |
| `shutdown_timeout` | `drain_timeout + 5` | Seconds the supervisor waits for children after TERM before SIGKILL; an orchestrator's stop grace period must exceed it |
| `stall_threshold` | `300` | Seconds without progress before a worker is considered stalled |
| `priority_levels` | `nil` | Number of priority sub-queues (nil = disabled, 2-10) |
| `default_priority` | `1` | Default priority for jobs without explicit priority |
| `group_mode` | `nil` | Grouped-read ordering mode for a queue. Experimental — exempt from the 1.0 stability promise. |
| `fair_share` | `nil` | Callable `->(job) { key \| [key, weight] \| nil }` evaluated at enqueue; workers interleave reads across keys (weighted, work-conserving). Mutually exclusive with `group_mode` |
| `event_fair_share` | `nil` | Event-bus twin of `fair_share`: callable `->(event) { key \| [key, weight] \| nil }` receiving the `Pgbus::Event` at publish; consumers interleave reads across keys within each subscriber queue. Independent of `fair_share` |
| `current_attributes` | `nil` | Persist `ActiveSupport::CurrentAttributes` across enqueue → perform: `:auto`, an Array of classes/names, or `{ Current => { except: [...] } }`. Restored around the whole `perform_now` |
| `archive_retention` | `7.days` | How long to keep archived messages. Accepts seconds, Duration, or `nil` to disable cleanup |
| `outbox_enabled` | `false` | Enable transactional outbox poller process |
| `outbox_poll_interval` | `1.0` | Seconds between outbox poll cycles |
| `outbox_batch_size` | `100` | Max entries per outbox poll cycle |
| `outbox_retention` | `1.day` | How long to keep published outbox entries. Accepts seconds, Duration, or `nil` to disable cleanup |
| `idempotency_ttl` | `7.days` | How long to keep processed event records. Accepts seconds, Duration, or `nil` to disable cleanup |
| `allowed_global_id_models` | `nil` | Allowlist of Class/Module models permitted as GlobalID **job arguments** and EventBus payloads. `nil` = allow-all (default). `[]` = deny-all. Apps with ActiveStorage should include `ActiveStorage::Blob` (and related models). |
| `base_controller_class` | `"::ActionController::Base"` | Base class for dashboard controllers (string, constantized at load time) |
| `return_to_app_url` | `nil` | URL for "back to app" button in dashboard nav (nil hides the button) |
| `web_auth` | `nil` | Lambda for dashboard authentication |
| `web_refresh_interval` | `5000` | Dashboard auto-refresh interval in milliseconds |
| `web_live_updates` | `true` | Enable Turbo Frames auto-refresh on dashboard |
| `stats_enabled` | `true` | Record job execution stats for insights dashboard |
| `stats_retention` | `30.days` | How long to keep job stats. Accepts seconds, Duration, or `nil` to disable cleanup |
| `stats_flush_size` | `100` | Buffered stat entries per worker before a bulk insert flush. Positive integer. Lower = tighter SIGKILL loss window. |
| `stats_flush_interval` | `5` | Seconds between periodic stat buffer flushes. Positive number. |
| `streams_enabled` | `true` | Enable the SSE streams transport |
| `streams_default_retention` | `300` | Default stream archive retention in **seconds** (5 minutes). Chat-style apps raise this. |
| `streams_retention` | `{}` | Per-stream retention overrides (exact string or `Regexp` keys → seconds/Duration) |
| `streams_orphan_threshold` | `86400` | Age (seconds) after which a durable stream queue is eligible for the orphan sweep (default **24h**). `nil` disables age-based drop. Empty queues are dropped regardless. |
| `streams_orphan_sweep_interval` | `3600` | Seconds between orphan stream sweeps (default **hourly**). `nil` disables the sweep. |
| `streams_durable_patterns` | `[]` | Streams (exact string or `Regexp`) that default to durable broadcast mode |
| `streams_default_broadcast_mode` | `:ephemeral` | Default broadcast mode when no pattern matches (`:ephemeral` or `:durable`) |
| `streams_presence_patterns` | `[]` | Streams (exact string or `Regexp`) that get connection-driven presence. Experimental. |
| `streams_presence_member` | `nil` | Custom `->(context) { { id:, metadata: } }` extractor for presence. Experimental. |
| `streams_broadcast_queue` | `nil` | Dedicated queue for turbo-rails async broadcast jobs. `nil` leaves them on the default queue (can wait behind long jobs). Set a name and back it with a worker capsule. |
| `streams_pool_size` | `5` | Dedicated DB pool size for durable-stream publish + replay (isolated from the job pool) |
| `streams_pool_timeout` | `5` | Seconds to wait for a streams-pool connection |
| `streams_pool_autoscale` | `false` | Grow/shrink the streams pool from live Postgres headroom under saturation (opt-in) |
| `streams_pool_max` | `nil` | Optional hard ceiling for streams-pool autoscaling |
| `streams_database_url` / `streams_host` / `streams_port` | `nil` | Override the streamer's LISTEN connection target (e.g. direct primary port when workers use a pooler) |
| `streams_pool_database_url` / `streams_pool_host` / `streams_pool_port` | `nil` | Override the streams **pool** target independently of LISTEN (so pool traffic can stay on the pooler while LISTEN stays direct) |
| `streams_test_mode` | `false` | Return a stub SSE response without hijack or background threads. Auto-enabled by `Pgbus::Testing.fake!`/`.inline!`. See [SSE streams in tests](#sse-streams-in-tests). |
| `streams_stats_enabled` | `false` | Record stream broadcast/connect/disconnect stats (opt-in, can be high volume) |
| `streams_path` | `nil` | Custom URL path for the SSE endpoint (nil = auto-detected from engine mount) |
| `execution_mode` | `:threads` | Global execution mode (`:threads` or `:async`). Per-worker override via capsule config. |
| `error_reporters` | `[]` | Array of callables invoked on caught exceptions. Each receives `(exception, context_hash)`. |
| `log_format` | `:text` | Log formatter (`:text` or `:json`). Sets `logger.formatter` automatically. |
| `metrics_enabled` | `true` | Enable Prometheus-compatible metrics on the dashboard |
| `metrics_backend` | `nil` | Generic metrics adapter: `nil` (off), `:prometheus`, `:statsd`, or a `Pgbus::Metrics::Backend` instance |
| `statsd_host` | `"127.0.0.1"` | StatsD UDP host (used when `metrics_backend = :statsd`) |
| `statsd_port` | `8125` | StatsD UDP port (used when `metrics_backend = :statsd`) |
| `health_port` | `nil` | Port for standalone HTTP liveness/readiness probes served by the supervisor; nil disables |
| `health_bind` | `"127.0.0.1"` | Bind address for the standalone health server |
| `pgmq_schema_mode` | `:auto` | PGMQ schema install mode (`:auto`, `:extension`, `:embedded`) |
| `doctor_on_boot` | `nil` | Run doctor inside the booting supervisor: `nil`/`false` = off, `:report` = log and boot, `:strict` = refuse to boot on a fatal check. Also via `--doctor` / `--doctor-strict`. |
| `eager_validation` | `true` | Run `Configuration#validate!` automatically after the `Pgbus.configure` block yields; an invalid value raises `Pgbus::ConfigurationError` at boot. Set `false` to suppress and validate manually. |

## Development

```bash
bundle install
bundle exec rake          # Run tests + rubocop
bundle exec rspec         # Run tests only
bundle exec rubocop       # Run linter only
```

System tests use Playwright via Capybara:

```bash
bun install
bunx --bun playwright install chromium
bundle exec rspec spec/system/
```

End-to-end streams benchmarks (real Puma + real PGMQ + real SSE clients):

```bash
PGBUS_DATABASE_URL=postgres://user@host/db bundle exec rake bench:streams
```

The harness measures single-broadcast roundtrip latency, burst throughput, fanout to many clients, and concurrent connect under thundering herd. See `benchmarks/streams_bench.rb`.

### Dependency watch

A nightly GitHub Action (`.github/workflows/dependency-watch.yml`) compares the
vendored PGMQ SQL (`lib/pgbus/pgmq_schema/pgmq_v*.sql`) and the locked
`pgmq-ruby` gem version against their upstream releases, opening a GitHub issue
on drift (deduplicated by exact title, so repeat runs are a no-op). It never
auto-vendors SQL or auto-bumps the gem -- it only notifies. Set the optional
`SLACK_WEBHOOK_URL` repository secret to also post drift notifications to
Slack; the workflow logs and skips that step gracefully when the secret is
unset.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
