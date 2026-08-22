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
          [ [ :code, ":until_executed" ], "At enqueue", "Duplicate enqueue AND execution" ],
          [ [ :code, ":while_executing" ], "At execution start", "Duplicate execution only" ]
        ]
      )
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
          [ [ :code, ":block" ], "Hold in a blocked queue; released when a slot opens or the semaphore expires." ],
          [ [ :code, ":discard" ], "Silently drop the job." ],
          [ [ :code, ":raise" ], [ :md, "Raise `Pgbus::ConcurrencyLimitExceeded`." ] ]
        ]
      )
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
