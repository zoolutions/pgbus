# Plans

pgbus tracks planned work in **GitHub issues** on `zoolutions/pgbus`. The
CHANGELOG entries and the code comments both reference them by number
(`issue #423`, `Refs #461`), and that is the trail a reader follows.

There is no `docs/plans/` directory in this repository, and `docs/` is the
published documentation site, not a plan store — do not put plans there.

- A plan that needs a durable artefact goes in a GitHub issue.
- A working plan for the current session goes in `lode/tmp/` (git-ignored) and
  is never committed.
- `/lode:plan --file` should write to `lode/tmp/` unless the user asks for an
  issue.
