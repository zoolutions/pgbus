# Testing and CI

RSpec, 323 `_spec.rb` files. `bundle exec rake` is `spec` + `rubocop` +
`pgbus:streams:lint_no_live`, and `rake spec`'s pattern is
`spec/pgbus/**/*_spec.rb` **only** — integration and system specs are separate
runs.

## Four suites

| Suite | Files | Entry | Needs |
|---|---|---|---|
| unit | `spec/pgbus/` (211) | `spec_helper.rb` | nothing; PGMQ is doubled |
| generators | `spec/generators/` (21) | `rails_helper.rb` | nothing |
| requests | `spec/requests/` (17) | `rails_helper.rb` | the dummy app |
| integration | `spec/integration/` (60) | `integration_helper.rb` | `PGBUS_DATABASE_URL` — a real PostgreSQL with PGMQ |
| system | `spec/system/` (16) | `system_helper.rb` | the dummy app + Playwright/Chromium |

`spec/support/` (11 files) holds the shared doubles — `PgmqDoubles`,
`StubDataSource`, the fake Turbo stream helpers. `spec/rubocop/` holds the
repo's own cop spec. `spec/dummy/` is the host Rails app the request and system
suites boot.

SimpleCov starts in `spec_helper.rb` **before** `pgbus` is required, so lib files
are instrumented as they load. `integration_helper.rb` and `system_helper.rb`
deliberately do not require it, so those runs are unmeasured. The floors are
`minimum_coverage line: 89, branch: 75`, set just below the measured baseline of
`spec/pgbus/ spec/generators/` (line 89.75%, branch 75.41%) so CI is green while
the repo ratchets toward the 80%/100% targets in `.claude/rules/testing.md`.

## What a spec is allowed to claim

Three rules the review history keeps re-deriving, all in
[../review/testing.md](../review/testing.md):

- A unit spec that only checks *which query was asked for* is a lookup test.
  Name it that way and leave the behavioural claim to the integration spec that
  runs the real scan.
- A fake must reject what production would reject. A fake that swallows an extra
  keyword into `**` cannot catch the leak it exists to catch; a fake
  `target_queue` that hardcodes the suite's prefix cannot catch a prefixing bug.
- Any override of shared configuration is snapshotted and restored at the same
  scope it was set (`after(:all)` for an `before(:all)` override). A leaked
  override makes later suites order-dependent.

Waits are deterministic: wait until the other thread has *settled* (blocked on
the mutex, or terminated) rather than sleeping a fixed window, and fail
explicitly when the budget expires instead of falling through to the assertion.

## `.github/workflows/main.yml`

Runs on push to `main` and on every pull request. Seven jobs:

| Job | What |
|---|---|
| `security` | `bundle exec bundle-audit check --update`, Ruby 3.4 |
| `lint` | `rubocop app benchmarks config gemfiles lib spec Gemfile Rakefile pgbus.gemspec`, `bun run lint:herb`, `rake build` |
| `lint_floor` | the same RuboCop + `rake build` on Ruby **3.3**, the gemspec's floor |
| `bench` | `rake bench:all`, `continue-on-error`, uploads `tmp/benchmarks/*.txt` — never a merge blocker |
| `test` | `rspec spec/pgbus/ spec/generators/` on Ruby 3.3 / 3.4 / 4.0 against the main Gemfile (Rails 8.x), plus two `include` legs pinning Rails 7.1 via `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile` on 3.3 and 4.0 — endpoints only, no full cube |
| `integration` | PostgreSQL 17 and 18 services; installs the PGMQ schema and the `pgbus_*` tables inline, then `rspec spec/integration/` |
| `system_test` | Playwright Chromium (cached on `bun.lock`), `rspec spec/system/`, uploads `tmp/capybara/**/*.png` on failure |

RuboCop is always given **explicit paths**. A bare run discovers
`docs/.rubocop.yml` while scanning — before `AllCops/Exclude` applies — and
crashes on gems that are not in the gem's bundle. `docs/` lints itself in
`docs-ci.yml`.

The other workflows: `docs-ci.yml` (only on `docs/**`), `deploy-docs.yml`,
`dependency-watch.yml`, `release.yml`.

## Benchmarks

22 `*.rb` files in `benchmarks/` — 19 `*_bench.rb`, plus `memory_profile.rb`, `bench_helper.rb` and `bench_support.rb`. `Rakefile`'s `db_benches` list names the ones that
need a real database or boot Puma; `bench:all` is everything else, derived from
the directory so a new unit bench is picked up automatically. Each bench runs
under `RbConfig.ruby` — the same interpreter and gemset as the Rake process —
because a bare `ruby` from `PATH` makes before/after numbers meaningless.
`.claude/rules/performance.md` maps hot path → file → bench and forbids a
performance claim without a same-machine before/after.

## Release

`bin/release [patch|minor|major|X.Y.Z] [-n|--dry-run] [-f|--force]` (and
`bin/release list`) works out the next version and hands off to
`rake release[X.Y.Z]`, which: refuses a dirty tree, bumps
`lib/pgbus/version.rb`, **string-edits the pgbus pin** in the three frozen
lockfiles (`Gemfile.lock`, `gemfiles/rails_7_1.gemfile.lock`,
`docs/Gemfile.lock` — a full `bundle lock` re-resolve trips over platform gems
and aborts the release, see #338/#341), verifies `gem build --strict`, commits,
pushes `main`, and publishes the GitHub Release. `release.yml` then runs the
suite, builds and verifies the gem, signs with Sigstore and publishes to
RubyGems.

`app/frontend/pgbus/style.css` is a committed build artifact; after adding a
Tailwind class to a view run `rake frontend:css`, or
`spec/pgbus/web/compiled_css_coverage_spec.rb` fails the build.

See also: [../workflow.md](../workflow.md),
[../docs-site/summary.md](../docs-site/summary.md).
