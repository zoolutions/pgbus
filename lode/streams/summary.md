# Streams — server-sent events over PGMQ + LISTEN/NOTIFY

`Pgbus.stream(name)` returns a `Streams::Stream` (`lib/pgbus/streams.rb`, 426
lines). A broadcast becomes either a PGMQ row plus a bare NOTIFY wake (durable)
or a NOTIFY whose payload *is* the frame (ephemeral). Browsers hold an SSE
connection served by `Web::StreamApp`; a per-host LISTEN connection fans wakes
out to them. Seventeen files under `lib/pgbus/streams/`, fifteen more under
`lib/pgbus/web/streamer/`.

## Publish

`Stream#broadcast(payload, visible_to:, durable:, exclude:, event:, coalesce:,
target:)` wraps the payload as `{"html" => …}` plus optional `visible_to`,
`exclude` and `event` keys, then takes one of three paths.

- **durable** (`durable: true`, or the stream's default, or a
  `streams_durable_patterns` match) — `ensure_queue!`, then
  `Client#send_stream_message`. If an ActiveRecord transaction is open on this
  thread the send is deferred to `after_commit`, so a client never sees a change
  the database rolled back. `current_open_transaction` reads
  `ActiveRecord::Base.connection_pool.active_connection?` and never checks out a
  connection of its own: the earlier `with_connection` variants leaked the
  caller's lease when its thread died (Zazu fan-out incident, 2026-08-05).
- **ephemeral** — `JSON.generate` then `Client#notify_stream`. Capped at
  `Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES` (7999; PostgreSQL rejects a
  NOTIFY payload of 8000 bytes or more). Over the cap, `#durable_fallback` warns
  and publishes durably instead — it does not raise, and it does not defer to
  `after_commit`, because the ephemeral path it replaces never did.
- **coalesced** (`coalesce:` with a `target:`) — `Streams::Coalescer` holds the
  last frame per `(stream, target)` for `DEFAULT_WINDOW_MS` (50) or the given
  number of ms and flushes last-write-wins. `target:` is required: there is no
  way to dedupe without a key.

`#broadcast_render(target:, action:, renderable:)` renders a Phlex component,
ViewComponent or string into a complete `<turbo-stream>` tag through
`Streams::Renderer` and broadcasts it in one call.
`Streams::TurboBroadcastable` / `BroadcastableOverride` carry the same options
onto `Turbo::StreamsChannel.broadcast_*_to`, extracting the pgbus keys via
`BroadcastOpts.extract!` before calling `super` so nothing leaks into turbo's
rendering kwargs.

## Names and secrets

A stream name is `Streams::Stream.name_from(streamables)`, validated against the
queue-name budget (`validate_name_length!` → `Streams::StreamNameTooLong`, an
`ArgumentError`, naming `QueueNameValidator::MAX_QUEUE_NAME_LENGTH`). A durable
stream's PGMQ queue is `#{queue_prefix}_<name>` like any other — there is no
separate stream prefix (removed in 1.0, issue #335); the registry table
`pgbus_stream_queues` is what identifies one. `Streams::SignedName` mints and
verifies the token the browser presents (`InvalidSignedName`, `MissingSecret`).

## Deliver

`Web::StreamApp#call` authorizes, resolves the signed name, parses the cursor,
and hijacks the socket (falling back to a streaming body where hijack is
unavailable). `Streams::Cursor.parse(query_since:, last_event_id:)` prefers the
`Last-Event-ID` header so a reconnect resumes exactly where it dropped;
`Stream#read_after(after_id:, limit: 500)` replays. An unparseable value raises
`Cursor::InvalidCursor`.

Wakes reach a connection through `Web::Streamer`:
`Streamer::Listener` owns one `PG::Connection` and one thread, with all
LISTEN/UNLISTEN SQL going through a command queue the listener thread drains
between notifies. `Streamer::MasterHub` (started from the host app's `config/puma.rb` via
`plugin :pgbus_streams`, shipped as `lib/puma/plugin/pgbus_streams.rb`) holds one LISTEN connection in the Puma **master** and
fans wakes to workers over a Unix socket. Its absence is not an error: each
worker falls back to its own listener.

## The streams pool

On the dedicated-connection path the Client builds a **second** `PGMQ::Client`
for streams (`streams_pool_size`, `streams_pool_timeout`), so a saturated worker
pool cannot delay a broadcast on pool checkout and each wake reuses a persistent
connection (issue #315). It is wrapped in `Client::ResizablePool` so its live
reference can be hot-swapped to a new size under load without losing broadcasts
or leaking connections; `Streams::PoolAutoscaler` drives that from
`pg_stat_activity`, counting peer processes by the per-process
`application_name` tag `Client#tag_application_name` writes (issue #323). On the
shared-AR path there is no second pool: `@streams_pgmq` aliases `@pgmq`.

`Streams::Presence` tracks who is subscribed in `pgbus_presence_members` (raw
SQL; there is no AR model for that table). `pgbus_stream_stats` and
`Pgbus::StreamStat` carry the dashboard's stream numbers, and
`rake pgbus:streams:backfill_registry` registers pre-registry durable queues
found by the archive-index fingerprint (issue #366).

Stream messages are read by a non-consuming peek, so they sit visible with
`read_ct = 0` forever — health verdicts must not read that as a wedge, which is
why `Web::DataSource#stream_queue_names` exists (issue #359).

See also: [../client/summary.md](../client/summary.md),
[../web/summary.md](../web/summary.md),
[../review/process-and-streams.md](../review/process-and-streams.md).
