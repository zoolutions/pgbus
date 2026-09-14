# The docs site (`docs/`)

`docs/` is a self-contained [docs-kit](https://github.com/zoolutions/docs-kit)
Rails app with its own `Gemfile`, `.rubocop.yml`, `bun.lock` and RSpec suite. It
depends on the gem through `path: ".."`, so `docs/Gemfile.lock` pins
`pgbus (X.Y.Z)` — which is why `rake release` string-edits that pin along with
the two root lockfiles. It is published at https://pgbus.zoolutions.llc.

It is **not** part of the gem: `pgbus.gemspec` only packages `app/`, `config/`,
`exe/`, `lib/`, `CHANGELOG.md`, `LICENSE.txt`, `README.md` and `Rakefile`.

## How a page works

Every page is a `DocsUI::Page` subclass under `docs/app/views/docs/pages/`, and
`docs/app/models/doc.rb` is the registry: 27 `page` lines, one per file, which
also drive the sidebar, search, `/llms.txt` and the MCP surface. A page whose
view class does not resolve yet is silently skipped everywhere, so the list can
be declared ahead of the writing. Scaffold with
`cd docs && rails g docs_kit:page "Title" --group=…`, which appends the registry
line and writes the class. Never hand-write HTML or daisyUI markup — compose the
`DocsUI::` helpers. The full authoring contract is `docs/AGENTS.md`.

## Page → behaviour

| Page | Owns the prose for |
|---|---|
| Overview, Installation, Quick start, Configuration | getting started |
| Architecture | the layer diagram, request vs command path |
| ActiveJob adapter | enqueue/execute, `provider_job_id`, `enqueue_all` |
| Event bus | publish, topics, handlers, idempotency |
| Retries & dead letters | `max_retries`, `read_ct`, backoff, `_dlq` |
| Concurrency & uniqueness | `limits_concurrency`, `ensures_uniqueness`, leases, the Locks page, the parked-jobs surfaces |
| Routing & ordering | priority levels, capsules, fair share |
| Batches | `Batch#enqueue`, open batches, callbacks, the sweep |
| Recurring tasks | the scheduler, `recurring.yml` |
| Transactional outbox | `pgbus_outbox_entries`, the poller |
| Real-time streams | durable vs ephemeral, coalescing, presence, the Puma plugin |
| Running workers | `pgbus start`, roles, recycling, shutdown |
| Dashboard | every page of the engine UI |
| Observability | metrics, gauges, AppSignal, MCP, health probes |
| Performance & tuning | pool sizing, autovacuum, retention |
| Separate database | `connects_to`, `--database=pgbus` |
| Rolling restarts | drain and deploy |
| Testing | `Pgbus::Testing`, inline mode, assertions |
| Upgrading pgbus | breaking changes and required migrations |
| From Sidekiq / SolidQueue / GoodJob | migration guides |
| Configuration reference | every setting, generated from `docs/app/models/config_reference.rb` |
| CLI & generators | `exe/pgbus`, the 20 generators, the rake tasks |

A behaviour change updates its page **and** `CHANGELOG.md` in the same PR. A
fact that appears on more than one page (a flag's scope, a precedence rule, a
limit) has to change on all of them — `grep` the subject before finishing.

## Commands (run from inside `docs/`)

```bash
cd docs && bundle exec rake lint     # RuboCop over an explicit file list
cd docs && bundle exec rspec         # request specs render every registered page
cd docs && bin/dev                   # locally
```

`docs/Rakefile` passes explicit paths to RuboCop for the same reason the gem's
Rakefile does: a bare run or a directory glob silently lints zero files here.
`docs-ci.yml` runs this only when `docs/**` changes; `deploy-docs.yml` ships it.

See also: [../testing-and-ci/summary.md](../testing-and-ci/summary.md).
