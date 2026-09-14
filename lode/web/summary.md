# Web — the dashboard, the API and MCP

A mountable engine surface: 14 dashboard controllers under
`app/controllers/pgbus/` plus 3 under `app/controllers/pgbus/api/`, 34 ERB views,
2 helpers, and `lib/pgbus/web/` (`data_source.rb` 1786 lines,
`metrics_serializer.rb` 301, `stream_app.rb` 239, `health_server.rb` 173,
`health_app.rb` 124, `authentication.rb` 66, `job_context.rb` 80,
`payload_filter.rb` 68, `streamer.rb` 58).

## One reader

`Web::DataSource` is the only thing in the dashboard that touches the database.
Controllers call it; no controller writes SQL (CLAUDE.md rule #3). Every public
read method rescues `StandardError`, logs at debug and returns a neutral value — an
empty array, a zeroed Hash, nil — so one broken query never takes the page down.
The four write methods (`purge_queue`, `drop_queue`, `retry_job`, `discard_job`)
have no rescue and let a `Client` error reach the controller, by design: a write
that failed must not look like one that succeeded.

`config.web_data_source` is a documented extension point, so a caller may hand
in another implementation. That is why the dashboard's own views keep fallbacks
for keys `DataSource` always sets (the batch remaining count is
`@batch[:pending_jobs] || [total - done, 0].max` in both
`DataSource#format_batch` and `app/views/pgbus/batches/_progress.html.erb`) —
this repo's own QA source under `spec/dummy` omits some of them.

**Memo lifetime is per instance, and the instance's lifetime differs by caller.**
`queues_with_metrics` and `concurrency_summary` are memoized because one page
asks for them more than once, and a dashboard request builds a fresh
`DataSource`. The AppSignal probe and the MCP server each hold **one** instance
for the life of the process, so both call `#reset_cache!` per iteration / per
tool call. Without it their numbers freeze at the first read.

## Routes

`config/routes.rb` draws queues, jobs, recurring tasks, batches, processes,
events, `dlq`, outbox, locks and insights, plus `api/{stats,insights,metrics}`
and a versioned `frontend/{modules,static}` asset path. `Web::StreamApp` is
mounted at `/streams` as a bare Rack app — it bypasses the Rails middleware
stack deliberately — and only when `config.streams_enabled`.

Two routes are deliberately **collection**, not member: `locks#release_key` and
`locks#discard_parked`. A concurrency key is a free-form string that may contain
`.`, `/` or `:` and has no business in a path segment, so both take the key as a
form param — and both pass `params[:key].to_s` through verbatim, using
`.strip.empty?` only for the blank check, because surrounding whitespace in a
`key:` proc's output is data.

## Turbo frames

Lists poll on `config.web_refresh_interval` through `data-auto-refresh` on their
`turbo-frame`. Anything that navigates **out** of a frame needs
`data: { turbo_frame: "_top" }` — the dashboard's Parked jobs card sits inside
the `dashboard-stats` frame and `/locks` has no frame of that id, so without it
the click does nothing visible.

## Authentication

`Web::Authentication` is a concern on the engine's base controller. With
`config.web_auth` set it calls the block with the request and answers
`head :unauthorized` on a falsy result. With no block it warns **once** that the
dashboard is unauthenticated — unless `config.base_controller_class` is
something other than `::ActionController::Base`, which signals the host app has
already gated it.

## Metrics

`Web::MetricsSerializer` turns `DataSource` output into Prometheus text
exposition at `/api/metrics`. Each family rescues independently so one failure
does not blank the scrape. `#serialize` reads `summary_stats` **once** and hands
it to both the summary and the concurrency families — that read is not memoized
and it advances the throughput snapshot, so calling it twice halves the next
scrape's window.

Concurrency exports exactly three unlabelled gauges —
`pgbus_concurrency_blocked_executions`,
`pgbus_concurrency_blocked_oldest_age_seconds`, `pgbus_concurrency_slots_held` —
and is omitted entirely when nothing is parked and nothing is held. Keys are
per-record, so a per-key label would be an unbounded series; the per-key detail
is the Locks page (up to 100 rows, busiest first) and the `pgbus_concurrency`
MCP tool. **`keys_at_limit` is a Locks card and an MCP field only — it gates the
metrics block but is not itself exported.**

## Health and MCP

`Web::HealthApp` is the in-app Rack health surface (`/livez`, `/readyz`), where
`/readyz` builds a `DataSource` and runs `MCP::HealthAnalyzer#verdict`.
`Web::HealthServer` is the standalone supervisor-side one. `Pgbus::MCP` is a
read-only diagnostic server — `Pgbus::MCP.load!` is called by `pgbus mcp`
because the subsystem is kept out of Zeitwerk (the `mcp` gem is optional) — with
13 tools under `lib/pgbus/mcp/tools/` and `MCP::Redactor` scrubbing payloads.

## i18n

12 locales in `config/locales/` (`da de en es fi fr it ja nb nl pt sv`). Two
rules the review history keeps producing: a page heading must use that locale's
own nav term (`pgbus.locks.index.title` matches `pgbus.layout.nav.locks` in every
locale), and a new key must be defined in **all twelve**, not just `en`. Plural
forms follow the locale's grammar — Finnish takes the partitive singular after a
numeral, so `discard_parked_confirm.other` reads "pysäköityä %{count} työtä".
`bin/i18n-tasks` is the checker.

See also: [../streams/summary.md](../streams/summary.md),
[../review/web-and-i18n.md](../review/web-and-i18n.md).
