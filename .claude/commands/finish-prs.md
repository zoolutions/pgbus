---
description: "Drive a set of open PRs to merge-ready, one at a time, in a given order. Merges main into each branch (auto-resolving the recurring CHANGELOG [Unreleased] and docs/Gemfile.lock conflicts), runs /github-review-pr (CI failures then review comments) on each, then waits for the user to merge before advancing to the next. With 'automerge' it merges each PR itself once CI is green, every thread is resolved and cubic has approved the head commit. Use to clear a stack of stacked/parallel PRs without manual sync churn."
model: opus
argument-hint: "ordered PR list (e.g. '292 288 289 293 294 295'); optional 'automerge' to merge each PR once every check is green, no thread is open and cubic APPROVED the head commit (no gh auto-merge); empty = auto-discover your open PRs"
allowed-tools: Bash(gh pr list:*), Bash(gh pr view:*), Bash(gh pr checks:*), Bash(gh pr diff:*), Bash(gh pr comment:*), Bash(gh pr merge:*), Bash(gh api:*), Bash(gh run view:*), Bash(git:*), Bash(bundle:*), Bash(bundle exec:*), Bash(cd:*), Read, Write, Edit, Glob, Grep, Agent, Skill, TaskCreate, TaskUpdate, TaskGet, TaskList, ScheduleWakeup, ToolSearch, mcp__cubic__get_pr_issues, mcp__cubic__update_pr_issue_status, mcp__cubic__trigger_pr_review
---

# Finish PRs (ordered merge-ready loop): $ARGUMENTS

You are driving a set of open pull requests to **merge-ready** state, one at a time, in a defined order, minimizing the manual sync/CI back-and-forth that stacked or parallel PRs create.

The two things that make a batch of PRs churn on this repo are **recurring and mechanical**, so this command resolves them automatically instead of surfacing them to the user each time:

1. **CHANGELOG `[Unreleased]` conflicts** — every PR appends an entry under the same section, so each merge re-conflicts the rest. The resolution is always a *union at a known anchor* (`### Added` / `### Fixed` / `### Changed` / `### Removed` / `### Breaking Changes`).
2. **`docs/Gemfile.lock` frozen-install failure** — `docs/` has a path dep on the gem; after a version bump the pin drifts and the "Docs site" CI job fails with `gemspecs for path gems changed … frozen mode`. The fix is to bump the pin to the current gemspec version — **never revert it**. (See memory `project_docs_gemfile_lock_version_pin`.)

**This command does NOT merge PRs itself** unless the user passed `automerge`. The `main` ruleset lets GitHub squash-merge with no approval and with open review threads, so in `automerge` mode the gate in 2e (green checks, no open thread, cubic APPROVED on the head commit, `fable-validator` PASS) is enforced by this command, not by GitHub. Default behavior: make each PR merge-ready, then pause and let the user merge; when a merge lands, merge main into the remaining PRs and continue.

**Unattended runs.** In `automerge` mode there may be no user to answer (a spawned agent under `/follow-through` runs this command). Wherever a step below says "ask the user", `automerge` mode **parks** the PR instead (2g) and goes on with the next one.

---

## Phase 0: Parse the PR list and order

`$ARGUMENTS` may be:

- A space/comma-separated ordered list of PR numbers: `292 288 289 293 294 295` (also accepts `#292`, `PR292`).
- The word `automerge` anywhere in the args → merge each PR with `gh pr merge --squash` once the automerge gate (2e) holds (not `--auto`, which waits only for the required checks). Strip it out before parsing numbers.
- Empty → auto-discover: `gh pr list --author=@me --state=open --limit 100 --json number,title,headRefName,createdAt` and order **oldest-first** (`createdAt` ascending). The explicit `--limit` matters — `gh pr list` defaults to 30, so without it the discovery silently drops older PRs once the queue grows past 30. Oldest-first is the safe default: the earliest PR is usually the base others were cut from, so merging it first minimizes downstream conflicts. Show the discovered order and proceed.

**Order matters.** Each merge invalidates the others' merge base. Processing in a fixed order means you sync each remaining PR exactly once per upstream merge, not repeatedly. If the user gave an explicit order, honor it exactly — they may know a dependency the metadata doesn't show.

Create a task list (TaskCreate) with one task per PR, in order, so progress is visible. Mark the current PR `in_progress`.

Confirm the plan in one line: `Finishing N PRs in order: #a → #b → #c. Mode: <pause-for-merge | automerge>.`

---

## Phase 1: Locate each PR's working tree

For each PR you need a checkout of its branch to merge main into and push. Prefer, in order:

1. The current directory, if it is a worktree already on that branch (`/follow-through` runs this command inside the worktree `/lfg` worked in).
2. An existing worktree already on that branch: `git worktree list` — match the branch. (Parallel-agent runs leave worktrees under `.claude/worktrees/agent-*`.)
3. If none, create one: `git worktree add .claude/worktrees/finish-<PR> <branch>` (fetch the branch first: `git fetch origin <branch>`). Remember that you created it: Phase 3 removes it after the merge.

Never sync a branch that is currently checked out in the **main working directory** — operate in a worktree so the user's main checkout is undisturbed. Never touch `main` directly except the optional lockfile-drift fix in Phase 4 (below), and only if the user explicitly opts into it.

---

## Phase 2: Per-PR loop

Process PRs strictly in order. For the current PR:

### 2a. Sync with latest main (merge, auto-resolving the known conflicts)

```bash
cd <worktree>
git fetch origin main --quiet
git merge origin/main
```

**MERGE, never rebase.** The branch is shared with its review: a force-push orphans cubic's inline comments and makes it re-review the whole diff, and `.claude/rules/git-workflow.md` forbids force-pushing a shared branch. The PR is squash-merged, so the branch's own history never reaches main anyway. "Already up to date" → nothing to push; go to 2b.

If the merge stops on a conflict, resolve it with **`/github-review-pr` Phase A0's rules** (`.claude/commands/github-review-pr.md`, "Resolution procedure", step 3), which cover every file this repo conflicts on: CHANGELOG union, `lib/pgbus/version.rb`, the frozen lockfiles, `bun.lock`, the compiled dashboard assets and the append-only registries. The two common cases:

- **`CHANGELOG.md`** — a union. The conflict is diff3-shaped at the top of an `### <Category>` list: this branch's new entry on the HEAD side, `main`'s entries on the other. Keep **both**, without duplicating a category heading. For the common "both sides insert at the same anchor" case, strip the markers keeping both blocks:

  ```bash
  perl -0pi -e 's/^<<<<<<< HEAD\n//mg; s/^\|\|\|\|\|\|\| [^\n]*\n=======\n//mg; s/^>>>>>>> [^\n]*\n//mg;' CHANGELOG.md
  ```

  Then **read the result** and verify: no conflict markers remain (`grep -n '^<<<<<<<\|^=======\|^>>>>>>>\|^|||||||' CHANGELOG.md`), this PR's `Refs #<n>` entry is present exactly once, ordering reads cleanly, and no unrelated entry was dropped or duplicated. The perl is a fast path, not a substitute for reading — if the conflict is not the simple same-anchor shape, resolve it by hand.

- **`docs/Gemfile.lock`** — do not hand-merge. Take main's side: **during a merge `--ours` is this branch and `--theirs` is `origin/main`**, so `git checkout --theirs docs/Gemfile.lock`. Then make its `pgbus (X.Y.Z)` pin match `lib/pgbus/version.rb`: by hand when only the pin differs, `cd docs && bundle install && cd ..` when the branch changed docs dependencies (never `bundle lock`). Confirm the only change against main is the `pgbus (X.Y.Z)` pin.

- **Any other conflicted file** — resolve it semantically per Phase A0. If the right combination is not decidable from the code (both sides rewrote the same logic), `git merge --abort` and: default mode → report the file(s) and ask the user; `automerge` mode → park (2g). A guessed resolution that compiles is worse than a question.

Run Phase A0's verification gates for what the conflict touched (step 4 there), `git add` the resolved files, and `git commit --no-edit` (git's standard merge message; add a body line if a resolution choice was not obvious).

### 2b. Ensure the docs-lockfile pin is current even without a conflict

The frozen-install failure happens whenever the pin ≠ the gemspec version, conflict or not. After the merge, if this PR touches anything under `docs/`:

```bash
grep -m1 'VERSION = ' lib/pgbus/version.rb           # the released/target version
grep -m1 '^    pgbus (' docs/Gemfile.lock            # the current pin
```

If they differ, `cd docs && bundle install` to bump the pin, then commit it on this branch:

```
fix(docs): pin docs/Gemfile.lock to pgbus <version>

Frozen docs bundle install fails when the path-gem pin drifts from the gemspec.
```

(If the PR touches no `docs/` files, the Docs site CI job doesn't run — skip this.)

### 2c. Push the merged branch

```bash
git push origin <branch>
```

A merge commit never needs force. Never `--force` or `--force-with-lease` a PR branch.

### 2d. Run the full review pass (one round)

Invoke `/github-review-pr <PR>` (via the Skill tool). It runs **conflicts, then CI failures, then review comments** — do not re-implement its logic. It will:

- Fix any red CI checks (lint, specs, build) and push.
- Address every unresolved review thread (cubic or human): implement valid fixes, push back with reasoning on wrong ones, record the outcome in cubic, resolve threads.

Wait for it to finish. A review thread never needs a decision: `/github-review-comments` fixes it or answers it. A failure is work too. The only things surfaced to the user (default mode, `needs-user`) or parked (`automerge`, 2g) are **blockers outside the repository**: a runner or registry outage, a missing secret or credential, a GitHub permission. Do not block the whole queue on one blocked PR; continue to the next PR, then return.

**Rounds.** A round is one `/github-review-pr` pass, the CI run its push causes, and cubic's re-review of the new head. **There is no round limit.** Every round fixes everything open, whatever its severity (P1 to P3, docs, spec-precision nits): nothing is deferred to a follow-up, and nothing is offered to the user as a choice. Two rules keep the rounds converging:

- A finding re-raised after it was fixed or answered gets one reply pointing at that commit or answer, and is resolved. It is not re-litigated.
- A CI failure that repeats after a fix (same check, same error) means the diagnosis was wrong. Never push the same fix again: re-read the log, re-diagnose (consult the advisor), and push a different fix. Park only when the cause is outside the repository.

### 2e. Verify merge-ready

Wait first while anything is still running (Phase 3, "Waiting for CI and cubic"): every gate below reads a finished state.

**Default (pause) mode.** Merge-ready means all of:

```bash
gh pr view <PR> --json mergeable,mergeStateStatus,reviewDecision --jq '{mergeable,mergeStateStatus,reviewDecision}'
gh pr checks <PR> --json name,state,bucket
```

`mergeable=MERGEABLE`; every check finished and green (each bucket `pass` or `skipping`, none `pending`, none `fail`); `reviewDecision` is `APPROVED` or empty (not `CHANGES_REQUESTED`); and zero unresolved review threads (gate 3 below). A `BLOCKED` mergeStateStatus with everything else green usually means a gate GitHub enforces itself, such as a required approval in a repo that has one — that is the user's/reviewer's gate, not a defect.

**`automerge` mode.** Merge-ready means **all five**, checked in this order (cheapest first). `reviewDecision` is ignored here: cubic's approval never feeds it (it stays `""`).

1. **Mergeable.** `gh pr view <PR> --json mergeable --jq .mergeable` is `MERGEABLE`. `UNKNOWN` → the local `git merge-tree` check from `/github-review-pr` Phase A0. `CONFLICTING` → back to 2a.
2. **Checks green.** `gh pr checks <PR> --json name,state,bucket`: every bucket `pass` or `skipping`, none `pending`, none `fail`. Trust the JSON, not `rtk`'s condensed view (it files `SKIPPED` under "Failed"). A `fail`, or any other bucket (`cancel`) → the next round (2d).
3. **No open thread, from any author.**

   ```bash
   gh api graphql -F owner='{owner}' -F repo='{repo}' -F pr=<PR> -f query='query($owner: String!, $repo: String!, $pr: Int!) { repository(owner: $owner, name: $repo) { pullRequest(number: $pr) { reviewThreads(first: 100) { totalCount nodes { isResolved } } } } }' --jq '[.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved | not)] | length'
   ```

   Must print `0` (past 100 threads, page with `after:`). Anything else → the next round (2d).
4. **Cubic approved the head commit.** The most recent review by a login starting with `cubic-dev-ai` is `APPROVED` **and** its `commit_id` equals the PR's `headRefOid`:

   ```bash
   gh pr view <PR> --json headRefOid --jq .headRefOid
   gh api 'repos/{owner}/{repo}/pulls/<PR>/reviews' --paginate --jq '.[] | select(.user.login | startswith("cubic-dev-ai")) | "\(.submitted_at) \(.state) \(.commit_id)"' | sort | tail -n 1
   ```

   When the cubic MCP is loaded (`ToolSearch` with `select:mcp__cubic__get_pr_issues,mcp__cubic__update_pr_issue_status,mcp__cubic__trigger_pr_review`), cross-check with `get_pr_issues`: `review.status == "completed"`, `review.hasNewCommitsSinceReview == false`, `openIssueCount == 0`. A disagreement between the two is a failed gate. A `COMMENTED` review — even "0 issues found" — is **not** approval. Gates 1–3 hold and only this one fails → 2e′.

   Example (PR #537): the latest cubic review is `APPROVED` on `841df5d`, which is the head → approved. Had the head been `64c00ba`, the latest cubic review would have been `COMMENTED` on it → not approved → 2e′.
5. **`fable-validator` PASS** on the final diff, or **PASS WITH NOTES** with every note fixed (2f).

### 2e′. Cubic quiet (`automerge`)

Gates 1–3 hold, but the latest cubic review on the head is `COMMENTED`, or cubic has not reviewed the head at all:

1. With the cubic MCP loaded and no trigger yet **for this head SHA**: call `trigger_pr_review` once (note the SHA), wait for the `cubic · AI code reviewer` check to finish (Phase 3), and re-run 2e.
2. Still not `APPROVED` on the head, or no cubic MCP → park (2g) with the reason `cubic did not approve <short sha>`.

Never merge on a `COMMENTED` review. One trigger per head SHA: a new push (a new round) earns a new one.

### 2f. Hand off for merge

Run the `fable-validator` agent on the combined diff first (`git diff origin/main...HEAD` in the worktree; give it the PR's linked issue and the acceptance criteria from the PR body). On BLOCK, or on PASS WITH NOTES, fix every blocker and every note (a failing test first for code) and go back to 2d for a new round. A note that is factually wrong gets a one-line rebuttal in the PR body instead. Merge only on PASS, or on PASS WITH NOTES whose notes are all fixed or rebutted.

- **`automerge` mode:** `gh pr merge <PR> --squash --delete-branch`, only once gates 1–5 hold. Do not arm `--auto`: GitHub would merge as soon as the *required* checks pass, while a check that is not required (Security, Gitar) may still be pending or about to fail. `--delete-branch` is redundant where the repo deletes merged branches, but keeps the command correct where it does not. Inside a worktree gh may fail to delete or switch the **local** branch after merging; that is cleanup, not a failed merge. Judge the merge by `gh pr view <PR> --json state,mergeCommit --jq '{state, sha: .mergeCommit.oid}'` (`MERGED` + a SHA), never by gh's exit code. Then go to Phase 3.
- **Default (pause) mode:** report this PR as ✅ merge-ready with its URL and a one-line "what's in it," and tell the user it's ready to merge. Then **wait** (Phase 3).

Mark the PR's task `completed` (merge-ready or merged) — or `needs-user` with the reason.

### 2g. Park a PR

Parking is for a blocker outside the repository (2d), or for cubic withholding approval with nothing left open (2e′). It leaves the PR for the user without blocking the queue:

1. `gh pr comment <PR> --body '…'` naming the blocker (the failing check and its error line, or cubic's latest review state and SHA) and what removes it. A park comment never lists findings to fix later and never offers options ("another round or merge as is", "take these as a follow-up"). A finding is fixed in the round it appears, never parked.
2. Mark its task `needs-user` with a one-line reason.
3. Leave the PR open and its worktree as it is. Continue with the next PR.

---

## Phase 3: Waiting

### Waiting for CI and cubic

Every push starts CI (this repo's `main.yml` run takes ~4 min) and a cubic re-review; `cubic · AI code reviewer` is itself a check, so waiting for the checks also waits for cubic's review to post.

- **`ScheduleWakeup` available** (an interactive session): wake after **300 s**, then re-check 2e. Never a busy sleep.
- **No `ScheduleWakeup`** (a spawned agent): `gh pr checks <PR> --watch --fail-fast` in the foreground with the Bash timeout at 600000; re-run it if it times out. Right after a push the new head may not have checks yet: if it reports none, run it again. If the condensed live output is unusable, `rtk proxy gh pr checks <PR> --watch --fail-fast`.

### Waiting for the merge, then advance

The loop is **gated on the target PR merging**, because each merge is what invalidates the next PR's base.

- **automerge mode:** the squash merge lands at once; confirm `MERGED` (2f). If this command created `.claude/worktrees/finish-<PR>` (Phase 1), remove it from the main working directory: `git worktree remove .claude/worktrees/finish-<PR>` and `git branch -D <branch>` (the remote branch is already gone). A worktree you were given belongs to its creator: leave it. Then advance.
- **default mode:** the user merges manually and will tell you (or you are re-invoked). On the next turn, re-check `gh pr view <PR> --json state`. If `MERGED`, advance to the next PR in the list and repeat Phase 2 (its merge of main now picks up the just-merged changes). If not yet merged, report current status and stop — do not spin.

When you advance, **always re-fetch and merge the new main into the next PR** (Phase 2a) before doing anything else — the merge that just landed is exactly the change it needs to absorb.

If the user merges a PR **out of the planned order**, adapt: drop it from the remaining list and sync whatever is now next.

---

## Phase 4 (optional): fix the lockfile drift at its source

The `docs/Gemfile.lock` pin drift originates from a version bump commit that didn't update the lockfile (the bump commit doesn't touch `docs/`, so the Docs site CI never runs on it and the drift lands on main silently). If the user is doing release hygiene, the durable fix is to bump the pin **on main** as part of the release, not in every feature PR. Mention this once if you see the drift recurring; don't do it unprompted (it's a commit to main).

---

## Phase 5: Final report

When the queue is drained (all merged, or all merge-ready-and-handed-off, or parked):

| PR | Result | Note |
|----|--------|------|
| #a | ✅ merged / ✅ merge-ready / ⏳ awaiting-merge / ⚠️ needs-user | one line (rounds used, the blocker) |

Then: what the user must do next (merge the ready ones, remove the blocker of any `needs-user` items), and whether any recurring drift (CHANGELOG, docs lockfile) is worth fixing at the source per Phase 4.

---

## Important notes

- **Never rebase a PR branch and never force-push it** — sync by merging `origin/main` (2a).
- **Never sync the branch checked out in the main working directory** — use a worktree.
- **Resolve conflicts by `/github-review-pr` Phase A0's rules.** One that is not decidable from the code: ask the user (default) or park (`automerge`).
- **Never merge in default mode** — the user merges; you make ready and wait.
- **Never merge on a `COMMENTED` cubic review** — `automerge` needs `APPROVED` on the head commit (2e gate 4).
- **Never `gh pr merge --auto`.**
- **Don't re-implement `/github-review-pr`, `/github-review-failures`, or `/github-review-comments`** — invoke them.
- **One stuck PR must not block the rest** — mark it `needs-user` (park it in `automerge`), continue the queue, return to it in the final report.
- **Read every auto-resolved CHANGELOG** before pushing — the perl fast-path is not a substitute for verifying the entry survived and reads correctly.
