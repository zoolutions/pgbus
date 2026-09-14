# pgbus — what the system is

pgbus is a Ruby gem (`lib/pgbus.rb`, `pgbus.gemspec`) that turns a PostgreSQL
database into a job processor and an event bus for a Rails application. It is a
Rails engine (`lib/pgbus/engine.rb`) plus a forking supervisor
(`lib/pgbus/process/supervisor.rb`) plus a CLI (`exe/pgbus`), and it carries its
own dashboard (`app/`), its own SSE streaming stack (`lib/pgbus/streams.rb`,
`lib/pgbus/web/streamer/`), and a read-only MCP diagnostic server
(`lib/pgbus/mcp/`). Message transport is PGMQ, reached only through the
`pgmq-ruby` gem; nothing outside `Pgbus::Client` calls PGMQ. Required Ruby is
`>= 3.3.0` and railties `>= 7.1, < 9.0` (`pgbus.gemspec`); the other runtime
dependencies are concurrent-ruby, fugit, globalid, pgmq-ruby and zeitwerk.

The audience is a Rails team that already runs PostgreSQL and does not want
Redis: ActiveJob adapter, AMQP-style topic routing, dead-letter queues, batches,
job uniqueness, concurrency limits, recurring tasks, a transactional outbox,
worker recycling, and a live dashboard, all in the database the app already has.

## The non-negotiables

**A PG connection has exactly one owner.** libpq is not thread-safe, and
`PG::Connection#close` is `PQfinish` — it frees the PGconn and its OpenSSL
state. Two shapes follow from that. On the shared-ActiveRecord path
(`connection_options` is a `Proc`) `Client#initialize` forces `pool_size: 1` and
every PGMQ call goes through `Client#synchronized`; on the dedicated path
(String URL or Hash) pgmq-ruby owns its own pool and no mutex exists
(`Client#shared_connection?`). Long-lived LISTEN threads own their connection
and close it from their own teardown, never from the thread that asked them to
stop (`Web::Streamer::Listener`, `Process::NotifyListener`). Break either rule
and you get `PG::ConnectionBad`, corrupted results, or a segfault.

**Never over-admit.** Concurrency slots, uniqueness locks and batch counters are
all written so that the failure mode is under-admission, which a sweep repairs,
never over-admission, which is unrecoverable. `Adapter#enqueue_with_concurrency`
takes the slot and commits *before* the PGMQ send; a crash there leaves a slot
held with no message (the dispatcher's `cleanup_concurrency` reclaims it) rather
than a message with no slot. A send whose outcome is unknown keeps its slot, its
uniqueness lock and its batch count (`Adapter#ambiguous_delivery?`). Promotion of
a parked job goes through the same guarded upsert as an enqueue
(`Concurrency::BlockedExecution#slot_taken?` → `Pgbus::Semaphore.acquire!`), so
even the dashboard's "release key" button cannot push a key past its limit.

**The request path does not block on the queue.** The dashboard reads through
`Web::DataSource` (no raw SQL in controllers), the streams publish path uses a
dedicated pool separate from the job pool (`Client#with_streams_connection`), and
a broadcast inside an open ActiveRecord transaction is deferred to `after_commit`
so clients never see a change the database rolled back
(`Streams::Stream#broadcast`).

## Shape of the code

`lib/` holds 185 Ruby files, 161 of them under `lib/pgbus/`; `app/` holds 34 more
(15 models, 17 controllers — 14 dashboard + 3 under `app/controllers/pgbus/api`,
and 2 helpers) plus 34 ERB views, with 12 locale files in `config/locales/`.
Zeitwerk loads the gem (`Pgbus.loader`) with eight deliberate `ignore` calls
(`lib/pgbus.rb:105-129`) — generators, the ActiveJob adapter shim,
`pgbus/testing`, the optional MCP subsystem (directory and file), the Phlex
stream helper, vendor integrations, and the Puma plugin — each for a reason
recorded inline.

Error policy is written down at `lib/pgbus.rb:20-37`: operational failures
descend from `Pgbus::Error` (12 subclasses declared at `lib/pgbus.rb:39-66`,
plus `Process::ReplicaConnectionError` at line 82, and more on individual
classes such as `Batch::AlreadyFinished`, `Streams::PayloadTooLarge`,
`Process::Lifecycle::InvalidTransition`); malformed-argument failures stay
`ArgumentError` subclasses (`Streams::StreamNameTooLong`,
`Streams::Cursor::InvalidCursor`, `Configuration::CapsuleDSL::ParseError`).

Start at [lode-map.md](lode-map.md).
