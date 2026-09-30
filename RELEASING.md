# Releasing pgbus

pgbus is released with a single command: **`bin/release`**, the front door to
`rake release[X.Y.Z]` (`rakelib/release.rake`). It bumps the version and the
lockfile pins, verifies the gem builds, commits, pushes `main`, and creates the
GitHub Release. Creating that Release triggers `.github/workflows/release.yml`, which
re-runs the full test suite, rebuilds and verifies the gem, and publishes to
RubyGems via OIDC trusted publishing with a Sigstore attestation.

Nothing is published from a branch or a PR — a PR only *stages* the changelog
roll and any release-prep changes. The tag is cut from `main` by `bin/release`.

`bin/release`, `rakelib/release.rake` and everything below the `test` job in
`release.yml` are byte-identical across the zoolutions gems (docs-kit, daisyui,
dash, pgbus, phlex-reactive): the gem name, version file and lockfiles all come
from `pgbus.gemspec`. Change them in every repo or none.

## Pre-release checklist

Run these on `main` (or the branch you're about to merge and release):

- [ ] `bundle exec rake` is green (RuboCop + the full RSpec suite). `release.yml`
      re-runs this on Ruby 3.3 and 3.4 before it will publish, so a red suite
      blocks the release — catch it locally first.
- [ ] `CHANGELOG.md` `[Unreleased]` holds every user-facing change since the last
      release. `rake release` does **not** roll the changelog for you — roll it in
      the release-prep PR (see below) or as a commit on `main` before releasing.
- [ ] `bundle exec rake pgbus:pgmq:status` — confirm the installed vs. vendored
      PGMQ schema version. If a new `lib/pgbus/pgmq_schema/pgmq_v*.sql` was vendored
      this cycle, the release notes should remind operators to run
      `rails generate pgbus:upgrade_pgmq` after upgrading.
- [ ] The three tracked lockfiles that pin the pgbus path gem — `Gemfile.lock`,
      `gemfiles/rails_7_1.gemfile.lock`, `docs/Gemfile.lock` — are current.
      `rake release` bumps their `pgbus (X.Y.Z)` pin in the bump commit (and
      `spec/pgbus/frozen_lockfile_sync_spec.rb` fails the suite on drift), so
      you normally don't touch them; if you changed dependencies, re-lock the
      affected file (`bundle install`, `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile
      bundle install`, `cd docs && bundle install`) and commit it in the
      release-prep PR. All three are installed frozen in CI — a stale one fails
      `release.yml`'s own `bundle install` before anything is published.
- [ ] Working directory is clean. `bin/release` aborts on any uncommitted change.

## Release-prep PR (the changelog roll)

Version bumps land on `main` via `rake release`, but the **changelog roll** is a
reviewable change, so do it in a PR first:

1. In `CHANGELOG.md`, rename the top `## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`
   (today), and add a fresh empty `## [Unreleased]` above it with the standard
   category stubs (`### Added`, `### Changed`, `### Fixed`, `### Security`). Keep
   entries **user-facing only** — pure chore/test/docs-infra commits get no bullet.
2. Commit, open a PR, merge it once green.

Leave `lib/pgbus/version.rb` and the lockfile pins alone in this PR — `rake
release` bumps them together.

## Cutting the release

On a clean, up-to-date `main`:

```bash
bin/release list        # last releases + what each bump would give
bin/release --dry-run   # the version it would cut + the changes since the last tag
bin/release             # patch bump (or: minor, major, an explicit X.Y.Z)
```

`bin/release` refuses unless you're on a clean `main` that matches
`origin/main`, checks no tracked lockfile pins pgbus below the new version, asks
for confirmation, then runs `rake release[X.Y.Z]`, which in order:

1. Aborts unless on `main` with a clean working directory.
2. Reads every tracked lockfile that sources pgbus (`Gemfile.lock`,
   `gemfiles/rails_7_1.gemfile.lock`, `docs/Gemfile.lock`) and aborts, before
   touching anything, if one has no `pgbus (X.Y.Z)` pin to bump.
3. Updates `lib/pgbus/version.rb` to `X.Y.Z` (the single source of truth;
   `release.yml` fails the publish if the tag and the gemspec version disagree).
4. Bumps the `pgbus (X.Y.Z)` pins in those lockfiles in place (no re-resolve).
5. Runs `gem build pgbus.gemspec --strict` as a local sanity check (and removes
   the built `.gem`).
6. Commits `chore: bump version to X.Y.Z`.
7. Pushes to `origin/main`.
8. Creates the GitHub Release `vX.Y.Z` with `gh release create --generate-notes`,
   which is what triggers the publish pipeline below.

You never run `git tag`, `git push --tags`, or `gem push` by hand — `bin/release`
creates the tag+Release and the workflow owns publishing.

### Variants

- **Prerelease:** `bin/release 1.2.0.rc1` — a version matching `alpha|beta|rc|pre`
  is auto-detected and the GitHub Release is marked `--prerelease`.
  `rake release[pre]` cuts a prerelease of the *current* `version.rb` without
  bumping.
- **Re-cut a botched release:** `bin/release X.Y.Z --force` — deletes the existing
  `vX.Y.Z` GitHub Release and tag (remote + local) first, then re-runs. Use only
  when a release failed partway and needs redoing; never to overwrite a release
  that already published to RubyGems.

## What `release.yml` does after `bin/release` creates the Release

Triggered by `release: [published]`, the workflow runs these jobs in order:

1. **`test`** — runs `bundle exec rake` on Ruby 3.3 and 3.4 (`fail-fast`), so a
   broken build blocks the publish.

2. **`build`** (needs `test`) —
   - **Tag/version consistency check:** reads the `pgbus.gemspec` version, strips the `v`
     from the release tag, and fails the job if they differ.
   - Builds the gem with `gem build pgbus.gemspec --strict`.
   - **Gem-contents guard:** unpacks the gem and fails if any `.git*` file,
     `*.gemspec`, or a `spec`/`test` directory leaked into the package.
   - Generates SHA256/SHA512 checksums and uploads the gem + checksums as the
     `gem` artifact.

3. **`publish-rubygems`** (needs `build`, environment `rubygems`,
   `id-token: write`) —
   - Verifies the checksums.
   - Configures RubyGems **trusted publishing** credentials via OIDC
     (`rubygems/configure-rubygems-credentials`) — no long-lived API token.
   - Signs the gem with `sigstore-cli`, then pushes with `gem push --attestation`
     (Sigstore attestation) unless that version is already on RubyGems (a re-run
     skips the push but still signs), and uploads the `.sigstore.json` bundle as
     the `sigstore` artifact.

4. **`upload-release-assets`** (needs `build` + `publish-rubygems`,
   `contents: write`) — attaches the `.gem`, both checksum files, and the
   Sigstore bundle to the GitHub Release.

If any job fails, the gem is not published; fix the cause, then re-cut with
`bin/release X.Y.Z --force`.

## Post-release notes

- **RubyGems trusted publishing** must be configured for `pgbus` on rubygems.org
  (the `rubygems` GitHub Environment is the trusted publisher). There is no
  `RUBYGEMS_API_KEY` secret — OIDC replaces it.
- Confirm the version appears on
  [rubygems.org/gems/pgbus](https://rubygems.org/gems/pgbus) and `release.yml`
  went green.
- The docs site (`pgbus.zoolutions.llc`) deploys from its own workflow; a gem
  release does not redeploy docs.
- **`SLACK_WEBHOOK_URL`** — the dependency-watch workflow
  (`.github/workflows/dependency-watch.yml`, added by #287) posts to Slack when a
  watched upstream (pgmq-ruby, PGMQ) releases. Configure the `SLACK_WEBHOOK_URL`
  repository secret so those notifications reach the team; without it the workflow
  degrades to a no-op notification step. Not part of the release flow itself.
