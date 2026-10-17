---
description: "Drive an epic (or any issues) from plan to merged, unattended. Resolves the dependency order of the linked issues, runs /lfg on each in its own worktree (independent issues concurrently, dependent ones after their prerequisite has merged), then /finish-prs <pr> automerge until CI is green, every thread is resolved and cubic has approved, and squash-merges. Parks what it cannot settle and carries on with whatever does not depend on it."
model: opus
argument-hint: "epic or issue number(s), e.g. '#539' or '540 541'; add --dry-run to print the wave order only"
allowed-tools: Bash(gh issue view:*), Bash(gh issue comment:*), Bash(gh issue close:*), Bash(gh pr list:*), Bash(gh pr view:*), Bash(gh api:*), Bash(git fetch:*), Bash(git worktree:*), Bash(git branch:*), Bash(git rev-parse:*), Bash(git log:*), Bash(bun install:*), Read, Glob, Grep, Agent, Skill, TaskCreate, TaskUpdate, TaskList
---

# Follow through: $ARGUMENTS

`/lfg <issue>` takes one issue to an open pull request and stops. `/follow-through` does the rest, for
one issue or a whole epic: it orders the issues by their dependencies, runs `/lfg` on each in its own
worktree, and drives each pull request to a squash merge with `/finish-prs <pr> automerge`. A run left
alone ends with every issue merged, or parked with a reason, and one final table.

You are a **thin orchestrator**. You resolve the order, prepare a worktree per issue, spawn one agent
per issue, and keep the books. All repository knowledge (TDD, conflicts, CHANGELOG, lockfiles, labels,
screenshots, the cubic gate) lives in `/lfg`, `/finish-prs` and the commands they call.

---

## Input

`$ARGUMENTS`: one or more issue numbers (`#539`, `539`, `540 541`, `540,541`), and optionally
`--dry-run`. All `gh api` paths below use `{owner}`/`{repo}`, which `gh` fills in from the current
repository.

---

## Phase 0: Resolve the work graph

### 0.1 Classify each issue

For each argument: `gh issue view <n> --json number,title,state,labels,body`.

- **Umbrella**: labelled `epic`, or its body has task-list items `- [ ] #N` / `- [x] #N`, or it has
  sub-issues (`gh api 'repos/{owner}/{repo}/issues/<n>/sub_issues' --jq '.[] | "\(.number) \(.repository_url)"'`; empty means
  none). Its children are those task-list items plus those sub-issues. Classify each child the same
  way (a child umbrella expands too).
- **Leaf**: any other issue. Only leaves are run; an umbrella never gets an `/lfg`.

### 0.2 Find each leaf's umbrella

A leaf named on its own (`/follow-through 540`) still belongs to its epic, whose body may order it.
A leaf's umbrellas are: the umbrella it was reached from (0.1); its GitHub parent,
`gh api 'repos/{owner}/{repo}/issues/<n>/parent' --jq '"\(.number) \(.repository_url)"'` (a non-zero exit, a 404, means none);
and the **first** `#N` after `Part of`, up to the next `;` or `.` ("Part of the workflow epic (#539);
builds on …" → #539 only). Read each such umbrella's body and its list of children (0.1) only to
apply 0.3: **its other children do not join the run** (a run of `540` alone never starts #541).
**"Part of" names an umbrella, never a prerequisite.**

### 0.3 Read the prerequisites

Join wrapped lines before matching: a sentence often wraps mid-reference ("builds on the core engine
(#540) and the batch DSL\n(#538)"). Join within a paragraph or list item only, never across a blank
line, a new list item or a heading. Phrases match case-insensitively. A sentence ends at a `.` or
`;` followed by a space, or at the end of its paragraph or list item; a `.` inside backticks
(`Pgbus.batch`) does not end one.

Only issues of this repository count. An `owner/repo#N` reference, and a sub-issue or parent whose
`repository_url` is another repository, is never read as this repository's `#N`: leave it out of the
run and list it in the report as not followed.

In a **leaf's** body, each of these adds an edge `N → leaf`:

- `builds on`, `depends on`, `blocked by`: every `#N` from the phrase to the end of its sentence
  ("builds on the core engine (#540) and the batch DSL (#538)." → #540 and #538).
- `after #N`: only with the `#N` right after the word (so "after-commit enqueue" is no edge).
- `#N` or `(#N)` followed by `lands first` or `lands before`.

In an **umbrella's** body, the same phrases apply to the umbrella's children:

- A `lands first` / `lands before` sentence about `#N` ("The batch DSL issue (#538) lands first",
  "issue #538 lands before the first") makes `N` a prerequisite of every child of that umbrella in
  the run other than `N` itself. `N` need not be a child: it joins the run as below.
- A sentence naming a child and its prerequisite ("#542 builds on #541") adds that edge.

Only these phrases make edges. "In order", a numbered list, "built on" and a dependency written
without an issue number ("after the batch DSL issue has merged") do not: an order that matters must
name its `#N` with one of the phrases above.

A prerequisite that is **closed** is satisfied: no edge. One that is **open and not yet in the run**
joins it as a leaf, and its own body and umbrellas are read the same way (this is how #538 joins a run
of #539, or of `540` alone). A prerequisite pulled in by an umbrella's sentence belongs to that
umbrella for the comments in 1.3. Repeat 0.2–0.3 until no issue joins. An umbrella and the issue a
"Part of" names are never prerequisites.

### 0.4 Status and resume

For each leaf in the run:

| Found | Status | What runs |
|-------|--------|-----------|
| The issue is closed | `done` | nothing; it satisfies its dependents |
| An open PR closes it | `resume-pr #<pr>` | `/finish-prs <pr> automerge` only, no second `/lfg` |
| Worktree `.claude/worktrees/follow-<N>`, or a local `issue-<N>-*` branch, with commits ahead of `origin/main` (or uncommitted changes), no PR | `resume-worktree` | `/lfg` resumed at Phase 4 on that branch, then `/finish-prs` |
| None of the above | `todo` | the full run |

Check the rows in that order: the first that matches wins.

- Open PRs that close an issue (GitHub's own reading of `Closes #N`, so `#54` never matches `#544`):
  `gh pr list --state open --limit 100 --json number,headRefName,closingIssuesReferences --jq '.[] | select(any(.closingIssuesReferences[]; .number == <N>)) | {number, headRefName}'`.
- Worktrees: `git worktree list --porcelain`; branches: `git branch --list 'issue-<N>-*'`; commits
  ahead: `git log --oneline origin/main..<branch>`. A `follow-<N>` worktree with nothing ahead may be
  stale: `git worktree remove` it (never `--force` here) and `git branch -D` its branch, and count the
  issue as `todo`. If git refuses because the tree has uncommitted changes, it is not stale: a run
  died mid-`/lfg`, so count it as `resume-worktree`. In a dry run remove nothing: report it as
  `todo (stale worktree?)`.

### 0.5 Order into waves and print

Topologically sort the leaves that are not `done` (Kahn's algorithm): wave 1 is every leaf with no
unmet prerequisite, wave 2 every leaf whose prerequisites are all in wave 1 or `done`, and so on. A
cycle is an error: print the issues in it and stop.

Print the order on one line, in exactly this form (issues within a wave ascending, separated by `, `):

```
Wave 1: #538 → Wave 2: #540 → Wave 3: #541 → Wave 4: #542
```

Then one line per leaf, in wave order: `#<N> <title> — <status>`, `done` leaves last.

**`--dry-run` stops here.** Phase 0 only reads GitHub and the local repository: no `git fetch`, no
worktree, branch or task, no agent.

Otherwise create one task per leaf (TaskCreate), in wave order.

---

## Phase 1: Run a wave

For every issue in the current wave whose prerequisites have all merged (or were `done`), and that is
not blocked: prepare it, then spawn all of the wave's agents **in one message** so they run
concurrently.

### 1.1 Prepare the worktree

- `todo`: slug = the title in lowercase kebab-case, at most 5 words, ASCII only.

  ```bash
  git fetch origin main
  git worktree add .claude/worktrees/follow-<N> -b issue-<N>-<slug> origin/main
  ```

- `resume-worktree`: use the existing worktree as it is; for a branch without one,
  `git worktree add .claude/worktrees/follow-<N> <branch>` (a branch checked out elsewhere: as for
  `resume-pr` below).
- `resume-pr`: `.claude/worktrees/follow-<N>` if it is on the PR's branch. If `git worktree list`
  shows the branch checked out anywhere else (the user's own tree, a sibling checkout), do not touch
  it: book the issue `needs-user` ("branch checked out at <path>", 1.3) and spawn nothing. Otherwise
  `git fetch origin <headRefName>` and `git worktree add .claude/worktrees/follow-<N> <headRefName>`.

If the repository root has a `package.json`: `bun install --cwd <worktree>` (worktrees do not share
`node_modules`, and some lint steps need them).

### 1.2 Spawn the agent

`Agent` with `subagent_type: "general-purpose"`, **`model: "opus"`** (spawned agents otherwise run on
the session's default subagent model, not on Opus), `description: "follow-through #<N>"`, and this
brief with the placeholders filled in:

```
You are running issue #<N> of a /follow-through run, unattended. Nobody will answer a question.

Issue: #<N> — <title>
Main working directory: <absolute repo root>
Your worktree: <absolute repo root>/.claude/worktrees/follow-<N> (branch <branch>)
Start: <lfg | lfg-resume | finish-prs #<PR>>

Work only inside your worktree: cd into it in every Bash call, or use absolute paths. Never touch
the main working directory, main, or another issue's worktree.

1. Start = lfg: run /lfg <N> with the Skill tool. The branch is prepared; its Phase 0 keeps it.
   Start = lfg-resume: the branch already has this issue's work (git log --oneline origin/main..HEAD,
   git status). Run /lfg <N> but SKIP its Phase 0: stay on this branch, never create another. Work
   through Phases 1-3 to rebuild the acceptance criteria and plan, then continue Phase 4 where the
   work stops; do not redo committed work.
   Start = finish-prs: skip to step 3.
   /lfg runs fable-validator (Phase 6.5) and opens the PR (Phase 7).
2. Post /lfg's Phase 8 close-out (the decisions and the three merge-gate questions) on the PR as a
   comment (gh pr comment <PR> --body-file <file>): nobody reads this session before the merge.
3. Run /finish-prs <PR> automerge with the Skill tool. You have no ScheduleWakeup: wait for CI and
   cubic with gh pr checks <PR> --watch --fail-fast, as its Phase 3 says.
4. Never ask a question, and never offer the user a choice. A finding, a failing check or a
   validator note is work: fix it now, in this PR, whatever its severity. Never defer it to a
   follow-up. Park only for a blocker outside the repository (an outage, missing credentials, a
   GitHub permission), or where a command says to stop and ask: comment on the PR (on issue #<N>
   if no PR exists yet) with the blocker and what removes it, then stop. If the
   Agent tool is unavailable, park with reason "validator-unavailable": never skip fable-validator.
   If the Skill tool cannot find a command, read .claude/commands/<name>.md and follow it.
5. Your final message is exactly one JSON object and nothing else:
   {"issue": <N>, "pr": <number or null>, "url": "<url or null>", "state": "merged" or "needs-user",
    "merge_sha": "<sha or null>", "reason": "<one line; empty when merged>"}
```

Wait for the agents' completion notifications; do not poll them.

### 1.3 Book each result

Check the agent's claim before acting on it:
`gh pr view <pr> --json state,mergeCommit --jq '{state, sha: .mergeCommit.oid}'`. A reply that is not
the JSON object, or a `merged` the PR does not confirm, counts as `needs-user` with that as the reason.

- **`merged`**: `git worktree remove .claude/worktrees/follow-<N>` (`--force` only if it refuses over
  leftover untracked files: the work is merged), then `git branch -D <branch>` (the remote branch is
  already gone). On each umbrella the issue belongs to (0.2, or the umbrella that pulled it in, 0.3):
  `gh issue comment <umbrella> --body "#<N> merged in #<pr> (<short sha>)"`. Mark the task completed.
- **`needs-user`**: leave the worktree and the PR as they are; update the task with the reason. Every
  issue that depends on it, directly or transitively, is `blocked by parked #<N>` and is never started.

Then run the next wave, until no runnable issue remains.

**Parallelism.** Concurrency is per wave: independent issues of one wave run side by side, and `/lfg`
parallelises inside itself as it always has. An epic whose issues form a chain (each wave one issue)
runs serially by construction. Concurrent agents share this machine's local services (a test database,
a fixed dev-server port); an agent that finds one busy works around it or parks, it never stops
another agent's process.

---

## Phase 2: Close out

1. Close an umbrella only if it was **named in the arguments** and **every one of its children** (0.1,
   not only those in this run) is now closed: `gh issue view <child> --json state`. Then
   `gh issue close <umbrella> --comment "<one line per child: #N → #pr>"`. The children were closed by
   their PRs' `Closes #N`; if one is still open, report it, do not close it by hand, and leave the
   umbrella open. A run of `540` alone never closes #539.
2. Final report, one row per leaf:

   | Issue | PR | Result |
   |-------|----|--------|
   | #538 | #600 | merged ✅ `abc1234` |
   | #540 | #601 | parked ⚠️ cubic did not approve `def5678` |
   | #541 | — | blocked ⛔ by #540 |

3. For each parked issue: what the user must do (from its PR comment). Then the exact command to
   resume once they have: `/follow-through <same arguments>`. Phase 0.4 picks up every open PR and
   worktree where it was left.

---

## What `/follow-through` never does

- Write or edit application code, or resolve a conflict: the agents' commands do that.
- Merge a PR itself: `/finish-prs automerge` does, behind its gate.
- Change `/lfg`'s phases, close a child issue by hand, or edit an umbrella's body (comments only).
- Ask the user mid-run, or let one parked issue stop the issues that do not depend on it.

---

## Portability

This file names no repository, path or language of its own. It does assume the target repository
has `/lfg` with the same phases (a worktree-safe Phase 0, the `fable-validator` agent at 6.5, the
Phase 8 close-out), `/finish-prs` and `/github-review-comments`. To use it elsewhere, copy it and
apply the same changes to those commands there: `/finish-prs` merges main instead of rebasing and
has the cubic gate in `automerge`; `/github-review-comments` treats cubic threads as first-class. In a
repository without cubic, `automerge` parks every PR at gate 4, by design.
