---
description: "Executes full autonomous engineering workflow with verification. Use when implementing complete features, tackling GitHub issues, or running end-to-end development cycles."
model: opus
argument-hint: "GitHub issue number/URL or feature description"
allowed-tools: Bash(gh issue view:*), Bash(gh search:*), Bash(gh issue list:*), Bash(gh pr create:*), Bash(gh pr edit:*), Bash(gh pr view:*), Bash(bin/labels infer:*), Bash(bin/labels sync), Bash(bin/labels sync --dry-run), Bash(bundle exec:*), Bash(git:*), Read, Write, Edit, Glob, Grep, Agent
---

# LFG - Full Autonomous Workflow

Execute a complete engineering workflow with verification at each phase.

## Phase 0: Branch Setup

**BEFORE any other work, prepare the git branch:**

1. Check the current branch: `git branch --show-current`
2. If NOT on `main`, switch: `git checkout main`
3. Pull latest: `git pull origin main`
4. Create feature branch: `git checkout -b issue-{number}-{brief-description}` (or `feature/{description}` if no issue number)

---

## Phase 1: Understand

### Step 1: Gather Requirements

If `$ARGUMENTS` is a GitHub issue number or URL:

```bash
gh issue view <number> --json title,body,labels,assignees,comments
```

If `$ARGUMENTS` is a description, use it directly.

**Keep the issue's `type` and `area` labels** — Phase 7 puts them on the pull request. `/lfg` never edits the issue's own labels; the issue's lifecycle is the user's to manage. A `docs/plans/*.md` plan carries them on its `Labels:` line. If either group is missing, or you were given a description, pin the `type` now (one, per `.github/LABELS.md`); the areas come from the actual changed paths when the PR is opened, via `bin/labels infer`.

### Step 2: Define Acceptance Criteria

**MANDATORY:** Write explicit acceptance criteria:

- **GIVEN** [context/setup]
- **WHEN** [action taken]
- **THEN** [expected outcome]

You MUST NOT proceed until you can articulate these clearly.

### Step 3: Comprehension Gate

Before proceeding, you must:

1. State the problem/feature in one sentence
2. Explain WHY this is needed (business context)
3. List what will change from the user's perspective
4. Identify edge cases not explicitly mentioned
5. Explain the data flow or code path involved

If you cannot complete ALL five items, investigate further.

### Step 4: Create Task List

Create a TaskCreate todo list with specific implementation steps.

---

## Phase 2: Explore

1. Find related files (Glob/Grep or Explore agent, `model: haiku`)
2. Read existing patterns in similar features
3. Understand dependencies and integration points
4. Check existing test coverage
5. Review PGMQ client interactions in `lib/pgbus/client.rb`
6. Check ActiveJob adapter in `lib/pgbus/active_job/`
7. Review event bus patterns in `lib/pgbus/event_bus/`
8. Check process model in `lib/pgbus/process/`

---

## Phase 3: Plan

1. List files to modify with specific changes
2. List new files to create with purpose
3. Identify migration changes needed (PGMQ queues, metadata tables)
4. Plan test coverage (TDD: tests FIRST)
5. Update task list with implementation steps
6. Consider backwards compatibility with existing queue configurations

---

## Phase 4: Implement (TDD)

### The deviation log (keep it from the first edit)

The plan is the map; the codebase is the territory. The moment reality forces a choice the plan or issue didn't settle, log it in `implementation-notes.md` at the repo root — one line, at the moment it happens, not reconstructed later:

- **Deviations** — the plan said X, you did Y, because Z
- **Discoveries** — facts about the codebase the plan didn't know
- **Judgment calls** — choices the user might have made differently (defaults, naming, scope cuts)

Pick the conservative option and keep going. The log is how the user audits your judgment afterwards. Never commit the file: its contents move into the PR body (Phase 7), then the file is deleted.

For each logical unit:

### 4.1: Write Failing Test First

Create a test that demonstrates the expected behavior. Run it to confirm it FAILS:

```bash
bundle exec rspec <spec_file>
```

### 4.2: Implement Minimum Code

Write the MINIMUM code to make the test pass. Follow project patterns:

| Never Do | Always Do |
|----------|-----------|
| Direct PGMQ SQL calls | Use `Pgbus::Client` wrapper |
| Skip queue prefixing | Always use `config.queue_name()` |
| Hardcode queue names | Use configuration |
| Skip visibility timeout | Use PGMQ's native VT mechanism |
| Ignore dead letter routing | Check `read_ct` against `max_retries` |
| Raw SQL in controllers | Use `Web::DataSource` for dashboard |

### 4.3: Refactor

Once green, refactor while keeping tests passing.

### 4.4: Validate

```bash
bundle exec rubocop <changed_files>
```

### 4.5: Repeat

Move to next logical unit. Mark task items complete.

---

## Phase 5: Deep Root Cause Analysis (Bug Fixes Only)

**If this is a bug fix, apply deep investigation before implementing:**

### Trace the Data Lifecycle

For the message/job/event causing the issue:
- Where and when was the message enqueued? By what adapter call?
- What visibility timeout was set? Has it expired?
- What ASSUMPTIONS does the code make at the failure point?
- Which assumption was violated, and WHY?

### Use Git History

```bash
git log --oneline -20 <file>
git blame <file>
```

- When was the code written? What was the original intent?
- Has something ELSE changed that invalidated the original assumptions?

### Map All Callers

Don't just look at the method that failed:
- Use Grep to find all call sites
- Different contexts (ActiveJob adapter vs event bus vs dashboard)?
- Does the error only happen in ONE context? Why?

### Five Whys

Keep asking WHY until you reach a meaningful fix point:

1. Error: X happened -> Why?
2. Because Y -> Why was Y in that state?
3. Because Z -> Why wasn't Z prevented?
4. Because no check existed -> Why not?
5. **THIS** is where the fix belongs

### Fix Location Principle

The best fix is usually NOT where the error is raised:
- Nil message in executor -> fix in worker that should provide non-nil
- Queue not found -> fix in code that should ensure_queue first
- Race condition -> PGMQ-level visibility timeout
- Dead letter overflow -> fix the retry/DLQ routing logic

**Ask: "Where is the EARLIEST point I could prevent this error?" Fix there.**

### Unacceptable Superficial Fixes -- DO NOT DO THESE

- `rescue nil` without understanding why the exception occurs
- `&.` to silence nil errors without investigating why nil occurs
- `if object.present?` guards without understanding why missing
- `return if message.nil?` to silently skip processing
- Wrapping everything in `begin/rescue` to swallow errors

**These HIDE bugs. The root cause continues causing issues elsewhere.**

---

## Phase 6: Verify

**ALL of these must pass before committing:**

```bash
bundle exec rubocop              # Style
bundle exec rspec <relevant_specs>  # Tests
```

### Solution Verification

Re-read the original requirements and verify:
- "If I were the requester, would I consider this fully resolved?"
- "Have I addressed the ROOT CAUSE, not just the symptom?"
- "Do my tests prove the issue is ACTUALLY fixed, not just suppressed?"
- "Does this maintain backwards compatibility?"

---

## Phase 6.5: Fable validation

Spawn the `fable-validator` agent (it is pinned to Fable) with the issue, the acceptance criteria from Phase 1 and the base branch. On **BLOCK**, fix every blocker (back to Phase 4 for code, with a failing test first), re-verify, and run the validator again. On **PASS WITH NOTES**, fix the risks you agree with and list the rest in the pull request under "Accepted risks". Put the validator's one-line verdict and its "Not verified" list in the pull request body. Do not open the pull request before a PASS or PASS WITH NOTES.

---

## Phase 7: Commit & PR

### Commit

```bash
git add <specific_files>
git commit -m "$(cat <<'EOF'
feat(scope): brief description

## Summary
[What changed and why]

## Test Coverage
- spec 1: validates requirement X
- spec 2: validates edge case Y

## Verification
- [x] bundle exec rubocop passes
- [x] bundle exec rspec passes
EOF
)"
```

### Push & PR

```bash
git push -u origin $(git branch --show-current)

gh pr create --title "feat(scope): brief description" --label <type> --label <area> --body "$(cat <<'EOF'
## Summary
- Key change 1 touching `lib/foo.rb`
- Key change 2

Closes #<issue_number>

## Test plan
- [ ] Scenario 1
- [ ] Scenario 2
EOF
)"
```

**Markdown inside the quoted heredoc is literal — do not escape.** The single-quoted `<<'EOF'` delimiter disables shell expansion on the body, so:

- Write backticks as backticks: `` `foo` ``. Do NOT write `\`foo\``; that writes a literal backslash-backtick and breaks the code span.
- Write dollar signs as-is: `$HOME`. No escaping needed.
- Write backslashes as-is: `\n` stays `\n`.

The body is copied verbatim into the PR / commit message. If you would not type a backslash in a GitHub comment, do not type one in the heredoc.

If the body is long or contains many backticks / tables, prefer writing it to a temp file and passing `--body-file`:

```bash
cat > /tmp/pr-body.md << 'EOF'
## Summary
...any markdown...
EOF
gh pr create --title "..." --label <type> --label <area> [--label <area>…] --body-file /tmp/pr-body.md
rm /tmp/pr-body.md
```

The `--body-file` path avoids the double-layer of shell interpretation entirely and makes long PR bodies easier to read in the terminal buffer.

**Label the PR — every time.** The `--label` flags are the issue's `type` + `area` labels from Phase 1, never a `status` label (`plan`, `epic`, …). For a description-only run, infer them: one `type` (`.github/LABELS.md` maps conventional-commit prefixes to types) plus `bin/labels infer $(git diff --name-only origin/main...HEAD)`. Exactly one type, at least one area: when `infer` prints nothing (changes confined to unmapped paths such as specs, the README or the Gemfile), pick the closest area by hand — never zero. `gh pr create` fails on a label that doesn't exist on GitHub: run `bin/labels sync` first, then re-run the create (`gh pr edit <n> --add-label …` labels a PR that is already open, once the labels exist).

The PR body MUST end with a `## Deviations & judgment calls` section copied from
`implementation-notes.md` (then delete the file). If the plan held completely,
write "None — the plan held." This section is read FIRST in review — it is the
audit trail for every decision the plan didn't make.

---

## Phase 8: Comprehension Close-Out

The tests prove the CODE is right; this phase keeps the USER's mental model right. After the PR is up, end your final message with:

1. **The decisions, not the diff** — the 3–5 non-obvious choices in this change someone must understand to maintain it. Lead with anything from the deviation log; the user has never seen those.
2. **Three merge-gate questions** the user should be able to answer before merging. If any answer isn't obvious to them, offer a walkthrough — an unanswerable question is comprehension debt, and merging anyway is how it compounds.

---

## Verification Checklist

- [ ] All acceptance criteria met
- [ ] Tests written BEFORE implementation
- [ ] `bundle exec rubocop` passes
- [ ] `bundle exec rspec` passes
- [ ] Backwards compatibility maintained
- [ ] PGMQ operations go through Client wrapper
- [ ] `fable-validator` returned PASS or PASS WITH NOTES (Phase 6.5)
- [ ] PR created with description
- [ ] PR labelled: one `type` + at least one `area`, no `status` (`.github/LABELS.md`)
- [ ] PR body ends with `## Deviations & judgment calls` (from implementation-notes.md, since deleted)
- [ ] Comprehension close-out delivered (decisions + three merge-gate questions)

---

## Handoff

When complete:
- All phases executed
- Verification passed
- PR created and linked

Now, execute this workflow for the provided issue or feature.
