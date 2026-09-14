# Workflow profile

Everything the shared workflow skills (`/lode:lfg`, `/lode:review-pr`,
`/lode:finish-prs`, `/lode:debug-flaky`, `/lode:tdd`, `/lode:plan`) need to know
about this repository that is not already in `../CLAUDE.md`, `../.claude/rules/`
or the rest of `lode/`. Keep every heading, even when its body is one line
saying "none".

## Commands

| Purpose | Command | Notes |
|---|---|---|
| fast loop (one file) | `bundle exec rspec <file>` | no database needed for anything under `spec/pgbus/` |
| full suite | `bundle exec rspec spec/pgbus/ spec/generators/` | no services, no network. Safe in two worktrees at once |
| integration suite | `PGBUS_DATABASE_URL=postgres://… bundle exec rspec spec/integration/` | **needs a real PostgreSQL with PGMQ**, creates and truncates real tables — **not** safe in two worktrees against the same database |
| system suite | `bundle exec rspec spec/system/` | boots `spec/dummy` + Playwright Chromium (`bunx --bun playwright install chromium`); binds a port |
| everything the default task runs | `bundle exec rake` | `spec` (which is `spec/pgbus/**` only) + `rubocop` + `pgbus:streams:lint_no_live` |
| lint | `bundle exec rubocop` (or `rake rubocop`) | **always with explicit paths** — the Rakefile passes `app benchmarks config lib spec Gemfile Rakefile pgbus.gemspec`; a bare run discovers `docs/.rubocop.yml` and crashes |
| ERB lint | `bun run lint:herb` | needs `bun install --frozen-lockfile` first |
| one CI cell locally | `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle install && BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle exec rspec spec/pgbus/ spec/generators/` | the Rails 7.1 leg |
| benchmarks | `bundle exec rake bench:all`, `rake bench:one[client_bench]`, `rake bench:memory` | `bench:integration`, `bench:streams`, `bench:fair_read`, `bench:pool_*`, `bench:notify_*`, `bench:job_burst`, `bench:execution_modes`, `bench:streams_hub` all need `PGBUS_DATABASE_URL` |
| rebuild the CSS artifact | `bundle exec rake frontend:css` | required after adding a Tailwind class to a view, or `spec/pgbus/web/compiled_css_coverage_spec.rb` fails |
| docs build / check | `cd docs && bundle exec rake lint && bundle exec rspec` | separate bundle; run from inside `docs/` |
| run the app | `bundle exec rake dummy:server` (stub data, PORT=3003, no database) or `cd docs && bin/dev` | `exe/pgbus start` needs a real database |
| release | `bin/release list`, `bin/release [patch\|minor\|major\|X.Y.Z] [-n]` | only from `main`; see `../RELEASING.md` |

## Branches and PRs

- Default branch: `main`
- Work branches: `feature/*`, `fix/*`, `refactor/*`, `ci/*`, `chore/*`, `docs/*`, rooted off fresh `origin/main`
- Commits: conventional (`feat(scope):`, `fix:`, `refactor:`, `perf:`, `docs:`, `test:`, `chore:`, `ci:`); the body says **why**, and ends with `Refs #<n>` where an issue exists
- PR body sections, in order: **Summary**, **Test plan**, **Deviations & judgment calls**, **Gate**. The deviations section is read first — it is the audit trail for every decision the plan did not make; write "None — the plan held." when there were none
- Write a PR body with a single-quoted heredoc (`<<'EOF'`) or `--body-file`; backticks and `$` are literal, never escape them
- Merge policy: squash on `main` when green and approved. Never force-push a shared branch. Never commit to `main` directly — **except** `rake release`, which pushes the version bump straight to `main` by design
- Attribution: **never** add `Co-Authored-By: Claude`, "Generated with Claude Code" or any AI attribution to a commit, PR body or issue comment

## Layers

| Layer | Files | Edit rule |
|---|---|---|
| Config | `lib/pgbus/configuration.rb`, `lib/pgbus/configuration/capsule_dsl.rb` | owned here; a new setting needs a default in `#initialize`, a check in `#validate!`, and a row in `docs/app/models/config_reference.rb` |
| Client | `lib/pgbus/client.rb`, `lib/pgbus/client/*` | owned here; **the only place that may call PGMQ**. New behaviour goes in a `client/` mixin, not another 100 lines in `client.rb` (it is already 2134 lines, far past the 800 in `.claude/rules/coding-style.md`) |
| ActiveJob | `lib/pgbus/active_job/{adapter,executor}.rb` | owned here; the ordering inside `#enqueue_with_concurrency` and `#execute` is the contract — read `lode/active-job/summary.md` before reordering anything |
| Admission control | `lib/pgbus/concurrency*`, `lib/pgbus/uniqueness.rb`, `lib/pgbus/batch*`, `app/models/pgbus/{semaphore,blocked_execution,uniqueness_key,batch_*}.rb` | owned here; every write is a conditional statement, never read-then-write |
| Event bus | `lib/pgbus/event_bus/*` | owned here |
| Process model | `lib/pgbus/process/*`, `lib/pgbus/execution_pools/*` | owned here; anything that owns a `PG::Connection` follows the single-owner rule |
| Streams | `lib/pgbus/streams*`, `lib/pgbus/web/streamer/*`, `lib/puma/plugin/pgbus_streams.rb` | owned here |
| Web | `app/controllers/`, `app/views/`, `app/helpers/`, `lib/pgbus/web/*` | owned here; **no raw SQL outside `Web::DataSource`** |
| Models | `app/models/pgbus/*.rb`, `lib/pgbus/bus_record.rb` | owned here; **no `Record` suffix** — see CLAUDE.md's Model Naming table |
| Migrations | `lib/generators/pgbus/templates/*.erb` | owned here; a released migration template is append-only — add a new `add_*` generator rather than editing one apps have run |
| Vendored PGMQ SQL | `lib/pgbus/pgmq_schema/pgmq_v*.sql` | **vendored — never edit**; add a new version file copied verbatim from upstream |
| Built asset | `app/frontend/pgbus/style.css` | **generated** — edit the views, then `rake frontend:css` |
| Vendored JS | `app/frontend/pgbus/vendor/turbo.js`, `vendor/apexcharts.js` | **vendored** — never edit; re-vendor from upstream |
| Lockfiles | `Gemfile.lock`, `gemfiles/rails_7_1.gemfile.lock`, `docs/Gemfile.lock`, `bun.lock`, `docs/bun.lock` | **generated** — never hand-merge; see Conflicts |
| Docs site | `docs/**` | separate app, separate bundle, separate lint; see `lode/docs-site/summary.md` |

## Shapes

Check a change against every one of these before calling it done. A reviewer
will name the one that was forgotten.

- **Both connection shapes.** A `Proc` (shared ActiveRecord connection, `pool_size: 1`, everything through `#synchronized`) and a String URL / Hash (pgmq-ruby's own pool, no mutex, its own streams pool). Most Client bugs are one path only.
- **Inside a caller transaction, and not.** Bootstrap DDL, queue creation and any `after_commit` deferral behave differently when the shared connection arrives mid-transaction.
- **Priority routing on and off.** `priority_levels > 1` turns one logical queue into `_pN` sub-queues that share one `_dlq`.
- **Migrated and unmigrated schema.** `Batch.executions_migrated?`, `Batch.callback_jobs_migrated?` and `ProcessedEvent.completion_column?` each gate a whole alternate path, and a batch that was *in flight* across the migration is a third state.
- **A `nil` probe result.** `message_exists?`, `message_in_queue?`, `message_archived?` and `message_with_job_id?` are tri-state; `nil` means "unknown" and must be read as "still there".
- **An ambiguous send.** The message may be live even though the call raised.
- **A `retry_on` re-enqueue.** Same `job_id`, `executions > 0`, still holding its uniqueness key, still a batch member.
- **A job class that no longer resolves.** A parked payload naming a deleted class still has to promote and dead-letter.
- **All 12 locales.** A new user-facing string is not done in `en` alone.
- **Ruby 3.3 and 4.0, Rails 7.1 and 8.x, PostgreSQL 17 and 18.** The gemspec floor is Ruby 3.3 and `railties >= 7.1`; CI proves both endpoints.
- **`execution_mode: :async`.** `Async::Stop` / `Async::Cancel` descend from `Exception`, and fiber-local state (`Thread.current[…]`) is the right scope there.

## Constraints

Reviewer suggestions that are wrong in this repository, with the reason.
`/lode:review-pr` pushes back on these on sight.

| Suggestion | Why it is wrong here |
|---|---|
| `require "io/timeout"` before `IO#timeout=` | `IO#timeout=` is core Ruby since 3.2 and the floor is 3.3; there is no such file, so the require raises `LoadError` and takes the health server down |
| Widen `Client::STALE_CONNECTION_PATTERNS` to match `EventBus::StaleConnectionRetry` | the client wraps enqueues — a half-committed produce duplicates a message on retry. Different call site, different safety argument |
| Retry a pool-checkout timeout | it only adds waiters to an exhausted pool; `enrich_pool_timeout_error` reports instead |
| Release the concurrency slot / uniqueness lock / batch count when the send raised | only when the failure proves nothing was produced. An ambiguous outcome keeps all three |
| Delete the semaphore row when releasing a key | the row is the only record of `max_value`; zero it and stamp it expired |
| `insert_all(unique_by:)` for `BatchExecution` | Rails resolves `unique_by` through the schema cache (issue #401); raw `ON CONFLICT` instead |
| Close a `PG::Connection` from the stopping thread to unblock a wait | `PQfinish` under a concurrent libpq call is a SEGV, not a rescuable error |
| A flat 5-second join timeout | budget one full `health_check_ms` wait plus the grace |
| `clear_all_connections!` to recover a dropped socket | it yanks sockets from sibling consumers in the same process |
| Run `bundle lock` to fix a lockfile | `docs/Gemfile.lock`'s broad PLATFORMS list makes a full re-resolve fail on platform-only gems; edit the pin line |
| A bare `rubocop` / a directory glob | it loads `docs/.rubocop.yml` mid-scan and crashes; pass explicit paths |
| Simplify the batch `pending_jobs` fallback away | `config.web_data_source` is a public extension point and the QA stub omits the key |
| Strip whitespace from a concurrency key | the key comes from a user proc; whitespace is data |
| Add a hard CI perf gate | the `bench` job is run-and-report; shared runners are too noisy for a threshold |

## Docs

- User-facing docs live in `docs/app/views/docs/pages/` (one `DocsUI::Page` subclass per page, registered in `docs/app/models/doc.rb`). The page-to-behaviour map is in [docs-site/summary.md](docs-site/summary.md)
- Changelog: `CHANGELOG.md`, entries under `## [Unreleased]` → `### Added` / `### Fixed` / `### Changed` / `### Security` / `### Breaking Changes` (the five headings the file actually uses; there is no `### Removed`). Entries are long-form prose naming the symptom, the mechanism and the issue
- A change to **behaviour** updates its docs page **and** `CHANGELOG.md` in the same PR. A change to a **setting** also updates `docs/app/models/config_reference.rb`. A fact that appears on more than one page changes on all of them — `grep` the subject first
- Files that pin a version and drift after a release: `Gemfile.lock`, `gemfiles/rails_7_1.gemfile.lock` and `docs/Gemfile.lock` all carry a `pgbus (X.Y.Z)` pin. `rake release` edits the pin lines directly; `spec/pgbus/frozen_lockfile_sync_spec.rb` fails when they drift

## CI

- Workflows: `.github/workflows/main.yml` (on push to `main` and every PR — jobs `security`, `lint`, `lint_floor`, `bench`, `test`, `integration`, `system_test`), `docs-ci.yml` (only when `docs/**` changes), `deploy-docs.yml`, `release.yml` (on a published GitHub Release), `dependency-watch.yml`
- Matrix: `test` is Ruby 3.3 / 3.4 / 4.0 on the main Gemfile (Rails 8.x), plus two `include` legs pinning Rails 7.1 on Ruby 3.3 and 4.0 — endpoints only. `integration` is PostgreSQL 17 and 18
- Cells that differ from local: the integration job creates the PGMQ schema and the `pgbus_*` tables inline in the workflow (not through the generators), so a new table needs adding there too; `lint_floor` runs RuboCop on Ruby 3.3, where a 3.4-only syntax passes locally and fails there
- Fetch a failure: `gh pr checks <PR>`, then `gh run view <RUN_ID> --job=<JOB_ID> --log-failed`
- "Green" means every job except `Benchmarks (report only)`, which is `continue-on-error` and uploads an artifact — never a merge blocker
- Known not-this-branch failures: a frozen-install failure naming the `pgbus (X.Y.Z)` pin is lockfile drift, not this branch's code — fix the pin
- Shared or rate-limited services: none. The integration job runs its own PostgreSQL service container; nothing reaches the network

## Flake sources

- **The integration suite shares one database.** Two runs against the same `PGBUS_DATABASE_URL` truncate each other's tables. One runner at a time.
- **Threads and forks.** Listener, streamer, supervisor and pool specs start real threads and real processes; a fixed `sleep` instead of waiting for a state is the usual cause of an intermittent failure (see [review/testing.md](review/testing.md)).
- **Leaked configuration.** `Pgbus.configuration` is process-global. A suite-scoped override that is not restored makes later examples order-dependent.
- **Wall clock.** Leases, `expires_at`, stall thresholds and `created_at` ceilings are compared against `Time.current`; an example that backdates a row and then asserts a boundary is sensitive to a slow machine.
- **Playwright.** The system suite depends on a cached Chromium keyed on `bun.lock`; a cache miss plus a slow install is a timeout, not a code failure.
- Never mask one with a `sleep`, a `retry` or a `skip` — `.claude/rules/performance.md` and `.claude/rules/testing.md` both forbid the workaround, and the fix is always a state to wait on.

## Conflicts

| File | Rule |
|---|---|
| `CHANGELOG.md` | union under the same `### <Category>` anchor — keep both sides, this PR's `Refs #<n>` entry exactly once, no duplicated subheadings. Verify with `grep -n '^<<<<<<<\|^=======\|^>>>>>>>\|^|||||||'` afterwards |
| `Gemfile.lock`, `gemfiles/rails_7_1.gemfile.lock`, `docs/Gemfile.lock` | never hand-merge and never `bundle lock`. If only the `pgbus (X.Y.Z)` pin conflicts, take the base's file and edit the pin to match `lib/pgbus/version.rb`. If real dependencies changed, take the base's file and re-resolve (`bundle install`, `cd docs && bundle install`, `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle install`) |
| `bun.lock`, `docs/bun.lock` | take the base's, then `bun install` in that directory |
| `lib/pgbus/version.rb` | an ordinary branch never edits it — releases land directly on `main`. Take the base's version unless the branch's own commits show a deliberate, explained bump |
| `app/frontend/pgbus/style.css` | generated — take either side, then `rake frontend:css` |
| `app/frontend/pgbus/vendor/turbo.js`, `vendor/apexcharts.js` | vendored upstream builds — never hand-merge; take one side whole (normally the newer vendored version) or re-vendor from upstream; `spec/pgbus/web/vendored_assets_spec.rb` checks them |
| `config/locales/*.yml` | union; a key must end up defined in all 12 files |
| `docs/app/models/doc.rb` | append-only registry — keep both `page` lines, base order first |
| `spec/support/*.rb`, fixtures | add a second double/fixture rather than merging two shapes into one |
| anything else | resolve **semantically** — read both sides and produce the version that keeps both intents. Never blanket `--ours`/`--theirs` on a source file; if the correct combination is not decidable from the code, stop and ask |

## Verification

- The manual check a user of this change would do: `bundle exec rake dummy:server` and open `http://localhost:3003/pgbus` for anything dashboard-shaped; `PGBUS_DATABASE_URL=… bundle exec rspec spec/integration/<area>_spec.rb` for anything that touches PGMQ, a lock or a batch; `bundle exec exe/pgbus <command>` for a CLI change
- Before pushing: `bundle exec rubocop <paths>` and the specs for what you touched; `bundle exec rake` before a PR
- Stress iterations for a flake proof: **50** runs of the single example (`--seed` varied), or the repeated start/stop cycle count for a listener-lifecycle claim
- Where evidence goes: `lode/tmp/` (git-ignored) unless the PR needs an auditable trail, in which case it goes in the PR body
