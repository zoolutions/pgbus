---
description: "Use when CI checks are failing on a PR — fetches failure logs, diagnoses root causes, implements fixes, and pushes until CI is green."
model: sonnet
argument-hint: "PR number (e.g., 41 or #41)"
allowed-tools: Bash(gh pr view:*), Bash(gh pr checks:*), Bash(gh pr diff:*), Bash(gh api:*), Bash(gh run view:*), Bash(git log:*), Bash(git diff:*), Bash(git push:*), Bash(git commit:*), Bash(git add:*), Bash(bundle exec:*), Read, Write, Edit, Glob, Grep, Agent
---

# Fix GitHub CI Failures: $ARGUMENTS

You are diagnosing and fixing CI failures on a GitHub pull request. Work systematically: identify failures, read logs, diagnose root causes, fix locally, verify, push.

## Phase 0: Determine the PR Number

The user may provide a PR number as `$ARGUMENTS`. Parse it flexibly:

- `PR41`, `PR 41`, `pr41` -> PR 41
- `41` -> PR 41
- `#41` -> PR 41
- Empty/blank -> auto-detect from current branch

**If no PR number is provided**, detect it automatically:

```bash
gh pr list --author=@me --head="$(git branch --show-current)" --state=open --json number,title
```

If exactly one open PR exists for the current branch, use it. If none or multiple, ask the user.

Once you have the PR number, confirm it:

```bash
gh pr view <PR_NUMBER> --json title,state,url,mergeable
```

**Pre-flight: merge conflicts (detection only).** If `mergeable` is `CONFLICTING`, STOP — do not diagnose CI on a conflicted branch (the merge itself may fix or cause the failures). Report the conflict and hand off to `/github-review-pr`, whose Phase A0 owns the resolution runbook — this command's toolset deliberately does not include the merge machinery. If `mergeable` is `UNKNOWN`, note it and proceed: the orchestrator resolves the ambiguity; a standalone run shouldn't block on GitHub's recompute.

---

## Phase 1: Identify Failing Checks

```bash
gh pr checks <PR_NUMBER>
```

Categorise each failing check:

| Check Type | Examples | How to Get Logs |
|------------|----------|----------------|
| Dependency audit | `Security` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Lint (rubocop + herb) + gem build | `Lint`, `Lint (Ruby 3.3 floor)` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Unit specs (matrix) | `Ruby 3.3 (Rails 8.x)` … `Ruby 4.0 (Rails 7.1)` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Integration specs (real PG + PGMQ) | `Integration (PG 17)` / `(PG 18)` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Browser system specs (Playwright) | `System Tests` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Docs site (runs only on `docs/**` changes) | `Build & test` | `gh run view <RUN_ID> --job=<JOB_ID> --log-failed` |
| Benchmarks | `Benchmarks (report only)` | report-only artifact — never a merge blocker |

Extract the run ID and job IDs from the check URLs. The URL format is:
`https://github.com/zoolutions/pgbus/actions/runs/<RUN_ID>/job/<JOB_ID>`

If all checks pass or are pending, report that and stop.

---

## Phase 2: Fetch Failure Logs

For each failing check, get the logs:

```bash
# Get the failed job logs (condensed output)
gh run view <RUN_ID> --job=<JOB_ID> --log-failed
```

If `--log-failed` output is too large or unclear, try:

```bash
# Full log for a specific job
gh run view <RUN_ID> --job=<JOB_ID> --log 2>&1 | tail -100
```

---

## Phase 3: Diagnose Each Failure

For each failure, determine the root cause:

### Lint Failures

Look for:
- RuboCop offenses: file path, line number, cop name, message

**Key**: RuboCop failures can often be auto-fixed with `bundle exec rubocop -A <file>`.

### Spec Failures

Look for:
- Test name and file path
- Error class and message
- Relevant backtrace lines (ignore framework noise)
- Whether it's a test environment issue vs actual code bug

**Key patterns**:
- `NameError: uninitialized constant` -> missing require or renamed class
- `NoMethodError: undefined method` -> API change, missing method
- `ActiveRecord::StatementInvalid` -> migration issue, missing column
- `expected: X, got: Y` -> logic bug or test needs updating

### Build Failures

Look for:
- Gem build errors: missing files in gemspec, syntax errors
- Bundle install failures: dependency conflicts

---

## Phase 4: Fix Locally

For each diagnosed failure:

1. **Read the relevant file** to understand context before fixing
2. **Make the fix** -- edit the file
3. **Verify locally** before committing:

```bash
# For rubocop failures
bundle exec rubocop <changed_files>

# For spec failures
bundle exec rspec <failing_spec_files>

# For full validation
bundle exec rake
```

### Fix Priority Order

1. **Lint/style fixes** first (fast, deterministic)
2. **Spec failures** second (may require understanding the code change)
3. **Build issues** third (usually gemspec or dependency)

---

## Phase 5: Commit and Push

```bash
git add <specific_files>
git commit -m "$(cat <<'EOF'
fix(ci): <brief description of what was fixed>

- Fix 1 description
- Fix 2 description
EOF
)"
git push
```

---

## Phase 6: Verify

After pushing, check if CI has been re-triggered:

```bash
gh pr checks <PR_NUMBER>
```

If there are still pending checks, report which checks are running and what was fixed. Do NOT poll in a loop -- report the status and let the user know.

If you can identify that certain failures will persist (e.g., a test that requires a real PostgreSQL with PGMQ extension), flag that explicitly.

---

## Important Notes

- **Read before fixing** -- always read the actual failing code before attempting a fix
- **Fix the root cause** -- don't add `# rubocop:disable` to bypass lint; fix the actual issue
- **Don't fix unrelated failures** -- if a spec was already failing on main, note it but don't fix it in this PR
- **CI environment differences** -- CI may not have PostgreSQL with PGMQ extension. Tests that require PGMQ will be pending (gated by PGBUS_DATABASE_URL).
- **Flaky tests** -- if a test passes locally but fails in CI, note it as potentially flaky rather than adding workarounds
- **Don't retry CI blindly** -- diagnose first, fix, then push. Each push triggers a full CI run.

Now begin by determining the PR number and fetching the failing checks.
