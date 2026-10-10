---
description: "Use when a PR needs full review — resolves merge conflicts with the base first, then fixes CI failures, then addresses unresolved review comments. Conflicts first so CI diagnoses the post-merge reality; failures before comments because comment fixes trigger new CI runs that obscure the original failures."
model: opus
argument-hint: "PR number (e.g., 156 or #156)"
allowed-tools: Bash(gh pr list:*), Bash(gh pr view:*), Bash(gh pr checks:*), Bash(gh pr checkout:*), Bash(gh pr diff:*), Bash(gh pr comment:*), Bash(gh api:*), Bash(gh run view:*), Bash(git log:*), Bash(git blame:*), Bash(git diff:*), Bash(git status:*), Bash(git switch:*), Bash(git fetch:*), Bash(git merge:*), Bash(git merge-tree:*), Bash(git rev-parse:*), Bash(git push:*), Bash(git commit:*), Bash(git add:*), Bash(bundle exec:*), Bash(bundle install:*), Bash(BUNDLE_GEMFILE=*), Bash(bun:*), Bash(bunx:*), Bash(cd:*), Read, Write, Edit, Glob, Grep, Agent
---

# Review GitHub PR (full pass): $ARGUMENTS

You are running a full review pass on a pull request. The pass has three phases that MUST run in this order:

1. **Phase A0: merge conflicts** — bring the branch up to date with its base and resolve any conflicts before anything else.
2. **Phase A: CI failures** — fix anything red before touching review comments.
3. **Phase B: review comments** — only after Phase A leaves CI green (or pending green after a push).

## Why this order matters

**Conflicts before failures**: CI results only matter for the code that will actually merge. On a conflicted (or stale) branch you'd diagnose failures against a base that no longer exists — and the conflict resolution itself changes code, invalidating the run you just fixed. Resolving conflicts first means Phase A reads CI for the post-merge reality, and you spend exactly one extra CI cycle instead of two.

**Failures before comments**: if you fix review comments first, every commit pushes a new CI run. By the time the review-comment fixes finish, the original failure logs are buried under new pipeline runs. Symptoms:

- The "specs (8, 4) failed" log you needed to read is now from a stale run; the latest run is still in progress on top of your unrelated comment fixes.
- A review-comment fix accidentally repairs the CI failure as a side effect, and you lose the chance to verify the failure was real.
- A review-comment fix accidentally INTRODUCES a CI failure, and you can't tell whether the new failure was pre-existing or your fault.

Conflicts-first, then failures-first eliminates this confusion. CI is either green or red on a known commit against the current base; the review-comment fixes layer cleanly on top.

## Phase 0: Determine the PR Number

The user may provide a PR number as `$ARGUMENTS`. Parse it flexibly:

- `PR156`, `PR 156`, `pr156` → PR 156
- `156` → PR 156
- `#156` → PR 156
- Empty/blank → auto-detect from current branch

**If no PR number is provided**, detect it automatically:

```bash
gh pr list --author=@me --head="$(git branch --show-current)" --state=open --json number,title
```

If exactly one open PR exists for the current branch, use it. If none or multiple, ask the user.

Once you have the PR number, confirm it:

```bash
gh pr view <PR_NUMBER> --json title,state,url
```

---

## Phase A0: Merge conflicts

Check whether the branch merges cleanly into its base:

```bash
gh pr view <PR_NUMBER> --json mergeable,mergeStateStatus,baseRefName
```

| `mergeable` | Action |
|-------------|--------|
| `MERGEABLE` | Skip to Phase A. |
| `UNKNOWN` | GitHub is recomputing (common right after pushes, and it can stay UNKNOWN for minutes). Don't poll it — verify **locally**, against the PR's actual head (NOT `HEAD`, which may be some other checked-out branch): `git fetch origin <base>` and `git fetch origin pull/<PR>/head`, verify both refs resolve (`git rev-parse --verify origin/<base>^{commit}` and `git rev-parse --verify FETCH_HEAD^{commit}` — a bad ref also exits 1 from merge-tree, so exit code alone can't be trusted), then `git merge-tree --write-tree --name-only origin/<base> FETCH_HEAD`. Clean exit → no conflicts, skip to Phase A. Exit 1 **with conflict output** → resolve below (the `--name-only` file list is your work list). |
| `CONFLICTING` | Resolve, below. |

### Resolution procedure

1. Check out the PR's branch (`gh pr checkout <PR_NUMBER>`) with a clean tree (`git status`). Stash nothing — if the tree is dirty, stop and ask the user.
2. `git fetch origin <base>` then **`git merge origin/<base>`** — MERGE, never rebase. The branch is shared (it has a PR); a rebase would require a force-push, which `.claude/rules/git-workflow.md` forbids on shared branches.
3. Resolve every conflicted file **semantically** — read both sides and produce the version that preserves BOTH changes' intent. Never blanket `--ours`/`--theirs` a source file. Repo-specific rules:
   - **`CHANGELOG.md` (Unreleased)**: union — keep BOTH sides' entries (main's landed bullets and this branch's), most recent first, without duplicating the `### Breaking Changes`/`### Added`/`### Changed`/`### Fixed`/`### Security` subheads. Losing either side is a real regression reviewers rarely catch.
   - **`lib/pgbus/version.rb`**: releases land DIRECTLY on `main` via `bin/release` (no PR), and `RELEASING.md` says even the release-prep PR leaves `version.rb` alone — so an ordinary branch never edits this file. A conflict here means the branch edited it against convention: take the BASE's version unless the branch's own commits show a deliberate, explained bump; if the intent isn't obvious, stop and ask.
   - **The frozen lockfiles** — `Gemfile.lock`, `gemfiles/rails_7_1.gemfile.lock` and `docs/Gemfile.lock` (all three tracked and installed frozen in CI): NEVER hand-merge conflict markers, and NEVER run `bundle lock` — the docs lock's broad PLATFORMS list makes a full re-resolve fail on platform-only gems (see `rakelib/release.rake` and `spec/pgbus/frozen_lockfile_sync_spec.rb`). If the conflict is only the `pgbus (X.Y.Z)` path-gem pin, take the base's file and edit the pin line(s) by hand to match `lib/pgbus/version.rb` (the spec's failure message shows exactly this fix). If the branch changed real dependencies, take the base's file and re-resolve on top: `bundle install` for the root lock, `cd docs && bundle install` for the docs lock, `BUNDLE_GEMFILE=gemfiles/rails_7_1.gemfile bundle install` for the Rails 7.1 lock.
   - **`bun.lock` / `docs/bun.lock`**: never hand-merge; take the base's file, then `bun install` (in the repo root or `docs/` respectively) so the branch's own JS dependency changes re-resolve.
   - **Generated/vendored dashboard assets** — `app/frontend/pgbus/style.css` is COMPILED Tailwind output (source: `app/frontend/pgbus/tailwind.css` + the views), and the repo ships NO build command for it (no tailwindcss in the root `package.json`, no rake/bin task — the only Tailwind CLI here is `docs/`'s, a newer version wired to the docs app's own CSS). So treat it like the vendored builds: never hand-merge the compiled file — resolve the source `tailwind.css` semantically, then take ONE side of `style.css` whole (normally the branch's, if the branch is the side that changed the dashboard). Regenerate only if BOTH sides genuinely changed the dashboard's class usage, pinning the version from the committed file's header comment so version drift doesn't rewrite the whole file: `bunx @tailwindcss/cli@<version from style.css line 1> -i app/frontend/pgbus/tailwind.css -o app/frontend/pgbus/style.css --minify`. Either way, `spec/pgbus/web/vendored_assets_spec.rb` sanity-checks the result (existence + size only). `app/frontend/pgbus/vendor/turbo.js` and `vendor/apexcharts.js` are vendored upstream builds — never hand-merge; take one side whole (normally the newer vendored version) or re-vendor from upstream.
   - **Append-only registries** (`docs/app/models/doc.rb`, `config/routes.rb`, `docs/config/routes.rb`, `spec/dummy/config/routes.rb`): both sides usually appended — keep both lines, in base order first.
4. Run the verification gates BEFORE pushing the merge — scoped to what the conflict touched, at minimum:
   ```bash
   # what CI's Lint job actually runs (a bare `rubocop` misses targets):
   bundle exec rubocop app benchmarks config gemfiles lib spec Gemfile Rakefile pgbus.gemspec
   # what CI's unit job runs (spec/pgbus/ci_coverage_spec.rb keeps it in sync with the spec tree):
   bundle exec rspec spec/pgbus/ spec/generators/ spec/requests/ spec/rubocop/ spec/benchmarks/ spec/i18n_spec.rb spec/pgbus_spec.rb
   # views (.html.erb) involved:
   bun run lint:herb
   # integration/system areas involved (need PGBUS_DATABASE_URL / Playwright — run if available, else note it):
   bundle exec rspec spec/integration/
   bundle exec rspec spec/system/
   # docs/ files involved (docs CI is its own workflow: rake lint + rspec inside docs/):
   cd docs && bundle exec rake lint && bundle exec rspec
   ```
5. Commit the merge (keep git's standard merge-commit message; add a body line naming any non-obvious resolution choice) and `git push` — a merge commit never needs force.

### Phase A0 exit criteria

- The PR reports `MERGEABLE` (or the local `git merge-tree` check is clean), AND the merge commit (if one was needed) is pushed.
- If the merge produced changes, CI is now re-running — that's expected; Phase A reads the fresh run.
- If a conflict cannot be resolved with confidence (both sides rewrote the same logic and the correct combination isn't decidable from the code), **stop and ask the user** — a guessed resolution that compiles is worse than a question.

---

## Phase A: Run `/github-review-failures`

Invoke the existing `/github-review-failures` slash command with the same `$ARGUMENTS` value. Its purpose: fix every failing CI check, push, leave the branch in a state where CI is either green or running-pending-toward-green.

Follow that command's full process — phases 1–6 of the failures runbook. The slash command is at `.claude/commands/github-review-failures.md`. Its workflow:

1. Identify failing checks via `gh pr checks <PR>`.
2. Fetch failure logs.
3. Diagnose root cause for each.
4. Fix locally — lint first (fast, deterministic), then specs, then build issues.
5. Verify locally before commit (`bundle exec rspec <files>`, `bundle exec rubocop`).
6. Commit + push + report which checks are now running.

### Phase A exit criteria

Before moving to Phase B, one of these must be true:

- All CI checks are green on the latest pushed commit. OR
- All CI checks are pending (running) on the latest pushed commit, AND no checks failed in the most recent completed run on this commit. OR
- A persistent CI failure exists that is **not caused by changes on this branch** (e.g., a flaky test on `main`, a secret-scanning job that fails for environmental reasons). Report this explicitly and proceed to Phase B with the caveat noted.

If failures persist on this branch's changes, **do NOT proceed to Phase B**. Report what's still failing, what's been tried, and ask the user how to proceed.

---

## Phase B: Run `/github-review-comments`

Once Phase A's exit criteria are met, invoke `/github-review-comments` with the same `$ARGUMENTS`. Its purpose: address every unresolved review thread on the PR, push fixes, reply with commit SHAs, and resolve the threads.

The slash command is at `.claude/commands/github-review-comments.md`. Its workflow:

1. Fetch all unresolved review threads via the GitHub GraphQL API.
2. Read and categorise each comment (valid fix / invalid suggestion / unclear).
3. Implement accepted fixes; verify locally (specs, validators, rubocop).
4. Commit all fixes together with a clear message; push.
5. Reply to every thread with the commit SHA (for accepted fixes) or technical reasoning (for rejections).
6. Resolve each thread via the GraphQL `resolveReviewThread` mutation.
7. Verify no unresolved threads remain.

### Phase B exit criteria

- All unresolved review threads have been replied to and resolved (or the user has explicitly approved leaving a specific thread open).
- The branch has been pushed with all accepted fixes.

---

## Phase C: Final report

Before reporting, re-check mergeability once more (`gh pr view <PR> --json mergeable`, or the local `git merge-tree` check if UNKNOWN) — the base can move underneath a long pass. If a NEW conflict appeared, loop back to Phase A0.

After all phases complete, report:

1. **Phase A0 summary**: whether the branch was conflicted, which files conflicted, how each was resolved (and the merge commit SHA) — or "clean merge, no action".
2. **Phase A summary**: which CI failures were diagnosed and fixed. Note the commit SHAs for the fixes.
3. **Phase B summary**: which review comments were accepted (with commit SHAs), which were pushed back on (with reasoning), and the final unresolved-thread count (should be 0).
4. **End state**: final mergeability + CI status on the latest commit.
5. **Outstanding work**: anything that still needs attention — e.g., CI was pending at the end of Phase B and the user should verify the latest run after the comment fixes.

---

## Important Notes

- **Do not interleave the phases.** Don't fix a CI failure, then a review comment, then another CI failure. The whole point of this command is the strict ordering.
- **A new CI failure emerging during Phase B** (e.g., a comment fix breaks a spec) means looping back to Phase A — fix the new failure before continuing comment work. Likewise, **a new conflict appearing mid-pass** (the base moved) means looping back to Phase A0. These loop-backs are the only allowed reverse directions.
- **If the PR is already merged**, there is nothing to review — report that and stop. (A stale `$ARGUMENTS` or a just-merged PR shows up as `state: MERGED` in Phase 0's confirm step.)
- **If the PR merges cleanly, has no failures AND no unresolved comments**, report "PR is clean" and stop.
- **If `$ARGUMENTS` is the same as the current open PR**, the two child slash commands will see the same PR. They share state through the git branch and the GitHub API, not through any in-process variable.
- **Don't re-implement the child slash commands' logic**. Invoke them and let them do their work. This command is the orchestrator.
