How `Pgbus::ActiveJob::Executor` completes a job — the archive step is the exact-once claim, and a retried archive is ambiguous, not a duplicate.

### `archive_from` distinguishes a first-attempt `false` (another worker won) from a `false` after its own retry (ambiguous)
- **Holds because:** archiving is idempotent, so `#archive_from` retries once on a connection error. A `false` on the first attempt means the message is already gone: another worker archived it, the outcome is `:already_archived`, no completion signals fire and the `:while_executing` lock is released only if still bound to this queue and msg_id. A `false` *after* our own retry cannot tell whether our first attempt landed before the connection dropped, so the outcome is `:ambiguous` and the completion signals proceed — treating it as a duplicate would silently lose a completion that did happen.
- **Where:** `lib/pgbus/active_job/executor.rb#archive_from` (returns `:archived`, `:already_archived` or `:ambiguous`; `attempts.positive? ? :ambiguous : :already_archived`)
- **Safe direction:** when the outcome cannot be known, keep the signals and the lock (`spec/pgbus/active_job/executor_spec.rb:"does not release the lock if archive_from fails (retry path)"`); a wrongly kept hold is recoverable, a wrongly dropped completion is not.
- **Proven by:** `spec/pgbus/active_job/executor_spec.rb:"does not release the lock if archive_from fails (retry path)"`
- **Origin:** cubic learning 84bcd3c2
