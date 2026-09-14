Rules about the dashboard's read layer, its controllers and views, and the
locale files. See [../web/summary.md](../web/summary.md).

### A `DataSource` memo lives as long as its instance, so long-lived holders call `#reset_cache!`
- **Holds because:** `queues_with_metrics` and `concurrency_summary` are memoized for a *request*, and a dashboard request builds a fresh instance — mutations redirect, so a per-request memo can never serve stale data. But the AppSignal probe holds one `DataSource` on a `Probe::Runner` registered once by `install!`, and `MCP::Server.build` injects a single instance into `server_context`, so without a reset their gauges freeze at the first minute's reading and every MCP tool call after the first replays the first snapshot. Both call `#reset_cache!` per iteration / per tool call. The dashboard deliberately does not.
- **Where:** `lib/pgbus/web/data_source.rb#reset_cache!`, `#queues_with_metrics`, `#concurrency_summary`; `lib/pgbus/integrations/appsignal/probe.rb`; `lib/pgbus/mcp/base_tool.rb#data_source_from`
- **Proven by:** `spec/pgbus/web/data_source_spec.rb:"re-runs the summary query after reset_cache!"`
- **Origin:** cubic learning f7dd4ca5; PR #463

### `MetricsSerializer#serialize` reads `summary_stats` once and shares it
- **Holds because:** `summary_stats` is **not** memoized, so a second call re-runs the queue, health and process reads — and `compute_throughput` advances `@last_throughput_snapshot`, leaving the next scrape to compute its rate over a near-zero window. One read is passed to both `append_summary_metrics` and `append_concurrency_metrics`. The read has its own rescue returning nil, so a failing summary skips exactly those two families instead of blanking the scrape.
- **Where:** `lib/pgbus/web/metrics_serializer.rb#serialize`, `#summary_stats`
- **Proven by:** `spec/pgbus/web/metrics_serializer_spec.rb:"drops only the concurrency and summary families"`
- **Origin:** cubic learning caaaf72e; PR #463

### Three concurrency gauges are exported; `keys_at_limit` is not one of them
- **Holds because:** concurrency keys are per-record (one per order, one per sync group), so a per-key label would be an unbounded series — the gauges are unlabelled aggregates and the per-key detail lives on the Locks page and in the `pgbus_concurrency` MCP tool. `keys_at_limit` is computed by `fetch_concurrency_summary` and rendered as a Locks card, and it participates in the "omit the family entirely" gate, but it has no gauge. Any doc sentence saying the cards and the gauges carry "the same totals" is wrong: three of the four cards are exported.
- **Where:** `lib/pgbus/web/metrics_serializer.rb#append_concurrency_metrics`; `lib/pgbus/web/data_source.rb#fetch_concurrency_summary`; `app/views/pgbus/locks/_concurrency.html.erb`; `docs/app/views/docs/pages/concurrency_uniqueness.rb`
- **Proven by:** `spec/pgbus/web/metrics_serializer_spec.rb:"includes the concurrency gauges, unlabelled"`, `:"omits the concurrency family entirely"`
- **Origin:** cubic learning ea2c6c58; PR #463

### A concurrency key reaches the data source verbatim; `strip` is only a blank test
- **Holds because:** the key comes from a user-supplied `key:` proc and is a free-form string, so surrounding whitespace is data — stripping it addresses a different key, or none. Both `LocksController#release_key` and `#discard_parked` take `params[:key].to_s`, guard on `key.strip.empty?`, and pass the raw value on; `DataSource` does the same. This is also why both are **collection** routes taking a form param: a key may contain `.`, `/` or `:` and has no business in a path segment.
- **Where:** `app/controllers/pgbus/locks_controller.rb#release_key`, `#discard_parked`; `lib/pgbus/web/data_source.rb#release_concurrency_key`, `#discard_parked_jobs`; `config/routes.rb`
- **Proven by:** `spec/requests/pgbus/locks_controller_spec.rb:"redirects with an alert when the key is blank"` (both actions); `spec/pgbus/web/data_source_spec.rb` asserts `" spaced key "` reaches `Pgbus::Semaphore.where` unchanged
- **Origin:** cubic learning 71c7a39c; PR #463

### A key is "live" only when it holds a slot **and** its lease is in the future
- **Holds because:** `lease_fresh` is the one fact an operator needs before releasing a key — a live lease means a holder is probably still running and releasing lets another job start beside it. Since a release now leaves the row at `value = 0` rather than deleting it, an `expires_at`-only test would render an emptied key as Live. The SQL is `(s.value > 0 AND s.expires_at > now())`, and `format_concurrency_key` reads the result as `[true, "t"].include?(…)` because the adapter may hand back either.
- **Where:** `lib/pgbus/web/data_source.rb#concurrency_keys`, `#format_concurrency_key`
- **Proven by:** `spec/pgbus/web/data_source_spec.rb:"carries lease_fresh straight from the query"`, `:"reads a postgres boolean string as lease_fresh"`; `spec/integration/dashboard_concurrency_spec.rb:"marks an expired lease as stale"`
- **Origin:** cubic learning ba8c9806; PR #463

### The concurrency key list is a FULL OUTER JOIN, capped at 100 rows, busiest first
- **Holds because:** a key can have parked jobs and no semaphore (its holder died and the sweep removed the row) or a semaphore and nothing parked, and both shapes matter to an operator — a LEFT join would hide one of them. The cap is real, so the prose must say "up to 100 key rows — busiest first" and add that the cards carry the true totals, or an operator who does not find their key concludes it is not there.
- **Where:** `lib/pgbus/web/data_source.rb#concurrency_keys` (`limit: 100`), `#concurrency_stats`; `docs/app/views/docs/pages/concurrency_uniqueness.rb`
- **Proven by:** `spec/integration/dashboard_concurrency_spec.rb:"still lists a key whose semaphore is gone but whose jobs are parked"`, `:"lists a held key with nothing parked"`, `:"reports a key at its limit with its parked jobs"`
- **Origin:** cubic learning b7b0abab; PR #463

### A link that leaves its turbo-frame carries `turbo_frame: "_top"`
- **Holds because:** the dashboard's Parked jobs card sits inside the `dashboard-stats` frame and `/locks` has no frame of that id, so without `_top` the click does nothing visible. A system spec that asserts the card's text and href cannot catch this — the spec has to click through and assert `have_current_path`.
- **Where:** `app/views/pgbus/dashboard/_stats_cards.html.erb`; the same pattern in `app/views/pgbus/queues/_queues_list.html.erb`, `jobs/_failed_table.html.erb`, `locks/_uniqueness.html.erb`
- **Proven by:** `spec/system/dashboard_spec.rb:"navigates to the locks page when the card is clicked"` — it clicks and asserts `have_current_path("/pgbus/locks")`
- **Origin:** cubic learning 0a79404c; PR #463

### A view keeps a clamped fallback for a key `DataSource` always sets
- **Holds because:** `config.web_data_source` is a documented extension point and this repo's own QA source (`spec/dummy/lib/stub_data_source.rb`, behind `rake dummy:server`) omits `:pending_jobs`, so simplifying to `@batch[:pending_jobs]` renders a blank cell there. And the fallback can legitimately go negative — counters may exceed `total_jobs` while an open batch is still publishing its total (issue #423) — so it clamps exactly as `format_batch` does: `[total - done, 0].max`.
- **Where:** `app/views/pgbus/batches/_progress.html.erb`; `lib/pgbus/web/data_source.rb#format_batch`
- **Proven by:** `spec/system/batches_spec.rb` — the "with a data source that omits pending_jobs" context
- **Origin:** cubic learning 2f07b6f5; PR #458

### A page heading uses that locale's own nav term, and a new key is defined in all twelve locales
- **Holds because:** a heading that reads as a different concept than the tab that led to it makes the page look like the wrong page. `pgbus.locks.index.title` must equal `pgbus.layout.nav.locks` in each locale (`nl` said "Vergrendelingen" in the nav and something else in the heading; `it` said "Blocchi"). The same completeness rule applies to every new key: the batch view uses the canonical `pgbus.batches.show.failed` and `pgbus.batches.show.on_failure` keys, and both are defined in `da de en es fi fr it ja nb nl pt sv`.
- **Where:** `config/locales/*.yml`; `app/views/pgbus/locks/index.html.erb`; `app/views/pgbus/batches/show.html.erb`
- **Proven by:** `spec/i18n_spec.rb` — `"does not have missing keys"`, `"does not have unused keys"`, `"files are normalized"`, `"does not have inconsistent interpolations"`, all via `bin/i18n-tasks`
- **Origin:** cubic learnings f6610e77, 60495b20, 1f6f83c4, 38597a44; PRs #420, #463

### A pluralized string follows the target language's grammar, not English's
- **Holds because:** Finnish takes the partitive singular after a numeral, and the adjective agrees — so `discard_parked_confirm.other` is "pysäköityä %{count} työtä", not the plural adjective form. Translations are reviewed as content, not as string substitution.
- **Where:** `config/locales/fi.yml` (`pgbus.locks.concurrency.discard_parked_confirm`)
- **Proven by:** no test — `spec/i18n_spec.rb` checks key presence, normalization and interpolation consistency, never grammar
- **Origin:** cubic learning 4e987c8a; PR #463

### A docs sentence about a diagnosis names the discriminator
- **Holds because:** "growing backlog" has two causes with opposite fixes: behind a *fresh* lease it is demand above the limit (raise `to:` or add capacity), behind a *stale* lease it is a holder that died without releasing (the sweep will reclaim, or release the key). The alert stays on the oldest wait; the Locks page is where the two are told apart. Likewise the surfaces list is a completeness claim — four places show parked jobs, and the home card is the one most operators hit first.
- **Where:** `docs/app/views/docs/pages/observability.rb`, `docs/app/views/docs/pages/concurrency_uniqueness.rb`
- **Proven by:** `docs/spec/requests/` renders every registered page; content accuracy is not test-enforced
- **Origin:** cubic learnings e50f08fd, and the "Four places" fix in PR #463
