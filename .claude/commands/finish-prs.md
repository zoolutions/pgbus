---
description: "Drive a set of open PRs to merge-ready, one at a time, in a given order. Auto-resolves the recurring CHANGELOG [Unreleased] and docs/Gemfile.lock conflicts, runs /github-review-pr (CI failures then review comments) on each, then waits for the user to merge before rebasing and advancing to the next. Use to clear a stack of stacked/parallel PRs without manual rebase churn."
model: opus
argument-hint: "ordered PR list (e.g. '292 288 289 293 294 295'); optional 'automerge' to merge each PR once every check is green (no gh auto-merge); empty = auto-discover your open PRs"
allowed-tools: Bash(gh pr list:*), Bash(gh pr view:*), Bash(gh pr checks:*), Bash(gh pr diff:*), Bash(gh pr comment:*), Bash(gh pr merge:*), Bash(gh api:*), Bash(gh run view:*), Bash(git:*), Bash(bundle:*), Bash(bundle exec:*), Bash(cd:*), Read, Write, Edit, Glob, Grep, Agent, Skill, TaskCreate, TaskUpdate, TaskGet, TaskList, ScheduleWakeup
---

# Finish PRs (ordered merge-ready loop): $ARGUMENTS

You are driving a set of open pull requests to **merge-ready** state, one at a time, in a defined order, minimizing the manual rebase/CI back-and-forth that stacked or parallel PRs create.

The two things that make a batch of PRs churn on this repo are **recurring and mechanical**, so this command resolves them automatically instead of surfacing them to the user each time:

1. **CHANGELOG `[Unreleased]` conflicts** — every PR appends an entry under the same section, so each merge re-conflicts the rest. The resolution is always a *union at a known anchor* (`### Added` / `### Fixed` / `### Changed` / `### Removed` / `### Breaking Changes`).
2. **`docs/Gemfile.lock` frozen-install failure** — `docs/` has a path dep on the gem; after a version bump the pin drifts and the "Docs site" CI job fails with `gemspecs for path gems changed … frozen mode`. The fix is to bump the pin to the current gemspec version — **never revert it**. (See memory `project_docs_gemfile_lock_version_pin`.)

**This command does NOT merge PRs itself** unless the user passed `automerge`. Branch protection requires review approval, and the user typically wants to eyeball each merge. Default behavior: make each PR merge-ready, then pause and let the user merge; when a merge lands, rebase the remaining PRs and continue.

---

## Phase 0: Parse the PR list and order

`$ARGUMENTS` may be:

- A space/comma-separated ordered list of PR numbers: `292 288 289 293 294 295` (also accepts `#292`, `PR292`).
- The word `automerge` anywhere in the args → merge each PR with `gh pr merge --squash` once every check is finished and green and it is approved (not `--auto`, which waits only for the required checks). Strip it out before parsing numbers.
- Empty → auto-discover: `gh pr list --author=@me --state=open --limit 100 --json number,title,headRefName,createdAt` and order **oldest-first** (`createdAt` ascending). The explicit `--limit` matters — `gh pr list` defaults to 30, so without it the discovery silently drops older PRs once the queue grows past 30. Oldest-first is the safe default: the earliest PR is usually the base others were cut from, so merging it first minimizes downstream rebases. Show the discovered order and proceed.

**Order matters.** Each merge invalidates the others' merge base. Processing in a fixed order means you rebase each remaining PR exactly once per upstream merge, not repeatedly. If the user gave an explicit order, honor it exactly — they may know a dependency the metadata doesn't show.

Create a task list (TaskCreate) with one task per PR, in order, so progress is visible. Mark the current PR `in_progress`.

Confirm the plan in one line: `Finishing N PRs in order: #a → #b → #c. Mode: <pause-for-merge | automerge>.`

---

## Phase 1: Locate each PR's working tree

For each PR you need a checkout of its branch to rebase and push. Prefer, in order:

1. An existing worktree already on that branch: `git worktree list` — match the branch. (Parallel-agent runs leave worktrees under `.claude/worktrees/agent-*`.)
2. If none, create one: `git worktree add .claude/worktrees/finish-<PR> <branch>` (fetch the branch first: `git fetch origin <branch>`).

Never rebase a branch that is currently checked out in the **main working directory** — operate in a worktree so the user's main checkout is undisturbed. Never touch `main` directly except the optional lockfile-drift fix in Phase 4 (below), and only if the user explicitly opts into it.

---

## Phase 2: Per-PR loop

Process PRs strictly in order. For the current PR:

### 2a. Sync onto latest main (rebase, auto-resolving the two known conflicts)

```bash
git fetch origin main --quiet
cd <worktree>
git rebase origin/main
```

If the rebase stops on a conflict:

- **`CHANGELOG.md`** — resolve as a union. The conflict is diff3-shaped at the top of an `### <Category>` list: `main`'s entries on the HEAD side, this branch's new entry on the other. Keep **both**, in a sensible order (feature/fix entries before the pre-existing docs-site entry; this branch's own entry adjacent to the others in its category). Practically, for the common "both sides insert at the same anchor" case, strip the markers keeping both blocks:

  ```bash
  perl -0pi -e 's/^<<<<<<< HEAD\n//mg; s/^\|\|\|\|\|\|\| [^\n]*\n=======\n//mg; s/^>>>>>>> [^\n]*\n//mg;' CHANGELOG.md
  ```

  Then **read the result** and verify: no conflict markers remain (`grep -n '^<<<<<<<\|^=======\|^>>>>>>>\|^|||||||' CHANGELOG.md`), this PR's `Refs #<n>` entry is present exactly once, ordering reads cleanly, and no unrelated entry was dropped or duplicated. The perl is a fast path, not a substitute for reading — if the conflict is not the simple same-anchor shape, resolve it by hand.

- **`docs/Gemfile.lock`** — do not hand-merge. Take main's side, then regenerate. **During a rebase the sides are inverted** — `--ours` is the base (main) you are replaying onto and `--theirs` is your branch's commit — so main's side is `--ours`: `git checkout --ours docs/Gemfile.lock 2>/dev/null || true`, then `cd docs && bundle install` (updates the pin to the current gemspec version), `cd ..`. (The `bundle install` re-derives the pin regardless, so this mainly avoids a spurious hand-merge; the side only matters if `bundle install` is skipped.) Confirm the only change is the `pgbus (X.Y.Z)` pin.

- **Any other conflicted file** — this command's auto-resolution covers only the two known-mechanical files. For anything else, STOP the rebase (`git rebase --abort`), report the conflicted file(s) to the user, and ask how to proceed. Do not guess at semantic conflicts.

`git add` the resolved files and `git rebase --continue` (set `GIT_EDITOR=true` to accept the message). Repeat until the rebase completes.

### 2b. Ensure the docs-lockfile pin is current even without a conflict

The frozen-install failure happens whenever the pin ≠ the gemspec version, conflict or not. After the rebase, if this PR touches anything under `docs/`:

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

### 2c. Push the rebased branch

```bash
git push --force-with-lease origin <branch>
```

`--force-with-lease` (never bare `--force`) so a concurrent push from the user aborts the overwrite instead of clobbering it.

### 2d. Run the full review pass

Invoke `/github-review-pr <PR>` (via the Skill tool). It runs **CI failures first, then review comments** — do not re-implement its logic. It will:

- Fix any red CI checks (lint, specs, build) and push.
- Address every unresolved review thread (CodeRabbit or human): implement valid fixes, push back with reasoning on wrong ones, resolve threads.

Wait for it to finish. If it reports a persistent failure it could not fix (or a review thread it could not resolve without a decision), surface that to the user for this PR and move it to a `needs-user` state — do not block the whole queue on one stuck PR; note it and continue to the next PR, then return.

### 2e. Verify merge-ready

```bash
gh pr view <PR> --json mergeable,mergeStateStatus,reviewDecision --jq '{mergeable,mergeStateStatus,reviewDecision}'
gh pr checks <PR>
```

Merge-ready means: `mergeable=MERGEABLE`, every check finished and green (`gh pr checks <PR> --json name,bucket`: each bucket `pass` or `skipping`, none `pending`, none `fail`; wait while any is pending), and `reviewDecision` is `APPROVED` or empty (not `CHANGES_REQUESTED`). A `BLOCKED` mergeStateStatus with everything else green usually means "awaiting required approval" — that is expected and fine; it is the user's/reviewer's gate, not a defect.

### 2f. Hand off for merge

Run the `fable-validator` agent on the combined diff first. On BLOCK do not open or merge: mark it `needs-user` and report the blockers instead of calling it ready.

- **`automerge` mode:** `gh pr merge <PR> --squash`, only once every check is finished and green as above. Do not arm `--auto`: GitHub would merge as soon as the *required* checks pass, while a check that is not required (Security, Gitar) may still be pending or about to fail. Then go to Phase 3 to wait for the merge to land before advancing.
- **Default (pause) mode:** report this PR as ✅ merge-ready with its URL and a one-line "what's in it," and tell the user it's ready to merge. Then **wait** (Phase 3).

Mark the PR's task `completed` (merge-ready) — or `needs-user` via a metadata note if it got stuck in 2d.

---

## Phase 3: Wait for the merge, then advance

The loop is **gated on the target PR merging**, because each merge is what invalidates the next PR's base.

- **automerge mode:** poll `gh pr view <PR> --json state --jq .state` until `MERGED`. Use `ScheduleWakeup` with a delay matched to CI duration (this repo's checks run ~1–3 min; poll ~180s, staying inside the prompt-cache window) rather than a busy sleep. When merged, advance.
- **default mode:** the user merges manually and will tell you (or you are re-invoked). On the next turn, re-check `gh pr view <PR> --json state`. If `MERGED`, advance to the next PR in the list and repeat Phase 2 (its rebase now picks up the just-merged changes). If not yet merged, report current status and stop — do not spin.

When you advance, **always re-fetch and rebase the next PR onto the new main** (Phase 2a) before doing anything else — the merge that just landed is exactly the change it needs to absorb.

If the user merges a PR **out of the planned order**, adapt: drop it from the remaining list and rebase whatever is now next.

---

## Phase 4 (optional): fix the lockfile drift at its source

The `docs/Gemfile.lock` pin drift originates from a version bump commit that didn't update the lockfile (the bump commit doesn't touch `docs/`, so the Docs site CI never runs on it and the drift lands on main silently). If the user is doing release hygiene, the durable fix is to bump the pin **on main** as part of the release, not in every feature PR. Mention this once if you see the drift recurring; don't do it unprompted (it's a commit to main).

---

## Phase 5: Final report

When the queue is drained (all merged, or all merge-ready-and-handed-off, or blocked-on-user):

| PR | Result | Note |
|----|--------|------|
| #a | ✅ merged / ✅ merge-ready / ⏳ awaiting-merge / ⚠️ needs-user | one line |

Then: what the user must do next (merge the ready ones, decide on any `needs-user` items), and whether any recurring drift (CHANGELOG, docs lockfile) is worth fixing at the source per Phase 4.

---

## Important notes

- **Never bare `git push --force`** — always `--force-with-lease`.
- **Never rebase the branch checked out in the main working directory** — use a worktree.
- **Never auto-resolve a conflict outside the two known-mechanical files** (`CHANGELOG.md`, `docs/Gemfile.lock`). Stop and ask.
- **Never merge in default mode** — the user merges; you make ready and wait.
- **Don't re-implement `/github-review-pr`, `/github-review-failures`, or `/github-review-comments`** — invoke them.
- **One stuck PR must not block the rest** — mark it `needs-user`, continue the queue, return to it in the final report.
- **Read every auto-resolved CHANGELOG** before pushing — the perl fast-path is not a substitute for verifying the entry survived and reads correctly.
