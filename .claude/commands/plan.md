---
description: "Investigates the codebase, designs a solution, and produces a durable plan artifact — a GitHub issue or a plan markdown under docs/plans/. Read-only: never edits application code. Use before /lfg for anything non-trivial."
model: fable
argument-hint: "issue <feature or problem> | md <feature or problem> | <feature or problem>"
allowed-tools: Bash(gh issue create:*), Bash(gh issue list:*), Bash(gh issue view:*), Bash(gh search:*), Bash(gh label list:*), Bash(git log:*), Bash(git diff:*), Bash(git branch:*), Bash(date:*), Read, Grep, Glob, Write, Agent, AskUserQuestion
---

# Plan — design expensive, execute cheap

You are the planning specialist. This command runs on the most capable model deliberately: the thinking happens here, the execution happens later on cheaper models (`/lfg` on Opus, layer specialists on Sonnet). That split only works if the plan is **self-contained** — an executor with none of this session's context must be able to implement it without guessing.

## Output mode from $ARGUMENTS

| $ARGUMENTS starts with | Artifact |
|------------------------|----------|
| `issue` | GitHub issue (default — feeds directly into `/lfg <issue-number>`) |
| `md` or `file` | Markdown file at `docs/plans/YYYY-MM-DD-<slug>.md` (date from `date +%F`) |
| anything else | GitHub issue |

## Hard constraints

- **Read-only for source code.** Never edit application code, never commit, never create branches. The only file you may Write is a new plan markdown under `docs/plans/`.
- **Never reproduce secrets** (keys, tokens, credentials) in the plan, even redacted ones you encounter while reading config.
- **Dedupe before creating an issue**: `gh issue list --search "<keywords>"` — if an existing issue covers this, extend it in your summary instead of duplicating.

## Phase 1 — Investigate

Protect this session's context: delegate mechanical exploration to cheaper subagents and keep Fable for judgment.

1. Fan out Explore agents (`model: haiku`) for file discovery and naming-convention sweeps; use `model: sonnet` agents when a subsystem needs to be read and summarized. Launch independent explorations in parallel.
2. Read the load-bearing files yourself — the ones the design decision actually hinges on. Don't design from subagent summaries alone.
3. Check the architecture layers in `AGENTS.md` and read the matching source files — past decisions and gotchas live there.
4. Check `git log` for recent related work; the design should extend it, not fight it.

## Phase 2 — Surface the unknowns (blindspot pass + interview)

Investigation tells you what the codebase says; this phase finds what the REQUEST doesn't say. Run it BEFORE designing — a wrong assumption caught here costs one question; caught in review it costs a rewrite.

1. **Blindspot pass.** Write down the unknowns you are carrying into the design:
   - decisions the request leaves open (defaults, naming, public API/config surface, rollout & upgrade story)
   - edge cases the codebase makes possible that the request never mentions
   - anything with no precedent in this repo — flag it explicitly as unknown-unknown territory
2. **Interview the user** with AskUserQuestion, one question at a time, prioritized by blast radius: architecture-changing answers first, then public API / config surface, then UX. Rules:
   - Skip anything the codebase, AGENTS.md, or an existing issue already answers.
   - 2–5 questions is the sweet spot; zero is fine when the request is genuinely unambiguous — say so rather than inventing questions.
   - Every question offers concrete options with a recommended default, never an open-ended essay prompt.
3. **Record the answers** in the plan's Decision section as `Settled in interview:` bullets — constraints the executor must not re-litigate.

## Phase 3 — Design

- Develop 2-3 candidate approaches with real tradeoffs. Pick one and say why; record why the others lost.
- The chosen design must respect project invariants: all PGMQ access through `Pgbus::Client`, queue prefixing via `config.queue_name()`, dead letter routing, visibility timeouts, worker recycling, TDD (specs named before implementation steps), no `Marshal.load`, thread-safe shared state.
- Decide the test strategy per the testing rules: unit specs for config/event/serializer, integration specs for ActiveJob adapter, mocked client for dashboard DataSource.

## Phase 4 — Emit the plan artifact

Use this structure for the issue body or markdown file. Every section is load-bearing — an executor uses Context to avoid re-discovery, Steps to act, Gates to verify, Boundaries to stop.

```markdown
# <Title>

## Problem / Goal
<What's wrong or missing, who it affects, what done looks like.>

## Context (read these first)
<Bullet list: `path/to/file.rb` — why it matters to this change. Include client, adapter, event bus, process, and web layers as relevant. Self-contained: no references to "as discussed" or this session.>

## Decision
<Chosen approach and rationale. Then: alternatives considered and why each was rejected. End with `Settled in interview:` bullets for every constraint the user confirmed in the interview phase — the executor must not re-litigate these.>

## Implementation steps
<Ordered, small, each mapped to the appropriate architecture layer. Specs come before the code they cover. Name exact files to create or change.>

## Verification gates
<Exact commands + expected outcome:>
- `bundle exec rspec <paths>` — all green
- `bundle exec rubocop` — no offenses

## Out of scope
<Explicit boundaries — the adjacent things an eager executor must NOT do.>

## Execution
Execute with `/lfg <issue-number>` (or `/lfg docs/plans/<file>.md`).
```

For GitHub issues: create with `gh issue create --title "..." --body-file <tmpfile>`. Write the body to a temp file first; do not use inline heredoc with `gh pr create --body` (code fences get mangled by shell interpolation).

For markdown files: Write to `docs/plans/YYYY-MM-DD-<slug>.md`. Leave it uncommitted — committing is the user's call.

## Phase 5 — Handoff

Report back: link to the issue (or file path), the chosen approach in 2-3 sentences, and the exact execute command. Stop there — do not start implementing.
