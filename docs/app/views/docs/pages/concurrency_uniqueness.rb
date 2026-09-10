# frozen_string_literal: true

# Two related-but-distinct guarantees: uniqueness (at most one such job exists)
# and concurrency limits (at most N run at once). The comparison is the payoff.
class Views::Docs::Pages::ConcurrencyUniqueness < DocsUI::Page
  title "Concurrency & uniqueness"
  eyebrow "Guide"

  def lead = "Prevent duplicate jobs with uniqueness, or cap simultaneous runs with concurrency limits."

  def content
    uniqueness
    strategies
    concurrency
    choosing
  end

  private

  def uniqueness
    DocsUI::Section("Job uniqueness", description: "At most one job with a given key exists, from enqueue to done.") do
      md <<~'MD'
        `ensures_uniqueness` guarantees that at most one job with a given key
        exists in the system at a time. The lock is held in
        `pgbus_uniqueness_keys` — never released by a timer, only when the job
        completes, is dead-lettered, or is found orphaned by the reaper.
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/jobs/import_order_job.rb")
        class ImportOrderJob < ApplicationJob
          ensures_uniqueness strategy: :until_executed,
                             key: ->(order_id) { "import-order-#{order_id}" },
                             on_conflict: :reject

          def perform(order_id)
            # Only ONE instance per order_id can exist — enqueue through completion.
          end
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "A successful first enqueue records the logical queue and PGMQ msg_id; retries do not " \
              "re-bind, and a bind failure leaves a placeholder for reaper recovery. Crash recovery " \
              "checks the PGMQ queue, not a timer: the dispatcher's reaper releases a lock only when " \
              "its message is gone. Placeholder rows (pending / msg_id=0) are scanned across live " \
              "queues — a missing synthetic pending table is never treated as proof the job is gone. " \
              "A lock backed by a message still in any queue is never touched, however old it looks."
      end
    end
  end

  def strategies
    DocsUI::Section("Strategies and conflict policies") do
      md <<~'MD'
        The **strategy** decides when the lock is taken; the **conflict policy**
        decides what happens to a duplicate.
      MD
      DocsUI::Table(
        [ "Strategy", "Lock acquired", "Prevents" ],
        [
          [ [ :code, ":until_executed" ], "At enqueue, held until success or dead-letter", "Duplicate enqueue AND execution" ],
          [ [ :code, ":while_executing" ], "At execution start, released on completion or failure", "Duplicate execution only" ]
        ]
      )
      md <<~'MD'
        A `:while_executing` lock is bound to the message being executed: a
        failed attempt releases it so the retry can run, and a row left behind
        by a crashed attempt is re-acquired by the same message on its next
        read rather than treated as a duplicate.
      MD
      DocsUI::Table(
        [ "Conflict policy", "Behavior" ],
        [
          [ [ :code, ":reject" ], [ :md, "Raise `Pgbus::JobNotUnique` (default)" ] ],
          [ [ :code, ":discard" ], "Silently drop the duplicate" ],
          [ [ :code, ":log" ], "Log a warning and drop" ]
        ]
      )
      md <<~'MD'
        Add the table with `rails generate pgbus:add_uniqueness_keys` (append
        `--database=pgbus` for a separate database).
      MD
    end
  end

  def concurrency
    DocsUI::Section("Concurrency limits", description: "At most N jobs with the same key run at once.") do
      md <<~'MD'
        Where uniqueness is binary (one or none), `limits_concurrency` is a
        counting semaphore — up to N jobs with the same key run simultaneously,
        the rest wait, drop, or raise:
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/jobs/process_order_job.rb")
        class ProcessOrderJob < ApplicationJob
          limits_concurrency to: 1,
                             key: ->(order_id) { "ProcessOrder-#{order_id}" },
                             duration: 15.minutes,
                             on_conflict: :block

          def perform(order_id)
            # Only one job per order_id runs at a time.
          end
        end
      RUBY
      DocsUI::Table(
        [ "on_conflict", "Behavior" ],
        [
          [ [ :code, ":block" ], "Park the job; promoted the moment a slot frees. Never dropped, however long it waits." ],
          [ [ :code, ":discard" ], "Silently drop the job." ],
          [ [ :code, ":raise" ], [ :md, "Raise `Pgbus::ConcurrencyLimitExceeded`." ] ]
        ]
      )
      md <<~'MD'
        `:block` is the option to reach for when a job must be **constrained
        without being lost** — a per-record pipeline that may only run one
        at a time but must run for every enqueue. Uniqueness cannot do that:
        every `ensures_uniqueness` conflict drops or rejects the duplicate.

        The guarantees behind `:block`:

        - **A parked job never ages out.** The only way out of
          `pgbus_blocked_executions` is promotion, so a park that outlives
          `duration` is still promoted when its slot frees.
        - **A promotion always takes a real slot.** The finishing job
          releases its slot and hands it to the next parked job in one
          transaction; the dispatcher's sweep promotes behind a dead holder
          the same way. Neither can push a key past `to:`.
        - **A job parked while the holder is finishing is promoted, not
          stranded.** The semaphore check and the park commit together under
          the semaphore's row lock, so a concurrent release waits and then
          sees the parked row.
        - **A duplicate delivery never releases a slot twice.** Archiving the
          message is the exact-once claim; a worker whose message was already
          archived elsewhere (its heartbeat lapsed) skips the release.
      MD
      DocsUI::Callout(:info) do
        plain "`duration` bounds silence, not run time. A slot is taken at enqueue "
        plain "and its lease is renewed by the visibility heartbeat once a worker "
        plain "picks the message up, so a job that runs for an hour keeps its slot "
        plain "for an hour, and a holder whose process died is presumed dead "
        plain "`duration` after its last beat. A scheduled job's lease covers its "
        plain "delay too. `duration` is floored at twice the heartbeat interval, so "
        plain "a lease can never expire before the first beat."
      end
      DocsUI::Callout(:warning) do
        plain "One window the lease cannot cover: nothing renews it between enqueue "
        plain "and the moment a worker picks the message up. If a queue is backed up "
        plain "for longer than `duration`, a waiting holder can be presumed dead and "
        plain "a second job promoted for the same key. Set `duration` above the worst "
        plain "queue wait you tolerate, or disable the sweep's promotion for that key."
      end
    end
  end

  def choosing
    DocsUI::Section("Which one do I want?") do
      DocsUI::Table(
        [ "Use", "When" ],
        [
          [ [ :code, "ensures_uniqueness" ], "\"This exact job must not run twice\" — payments, order import, unique emails." ],
          [ [ :code, "limits_concurrency" ], "\"At most N of these at once\" — rate-limited APIs, resource-constrained tasks." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "They use separate tables, so you can combine both on one job class "
        plain "with no conflict."
      end
    end
  end
end
