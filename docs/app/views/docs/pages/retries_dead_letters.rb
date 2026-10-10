# frozen_string_literal: true

# How a failed job is retried (visibility-timeout backoff) and where it goes when
# it keeps failing (the dead-letter queue).
class Views::Docs::Pages::RetriesDeadLetters < DocsUI::Page
  title "Retries & dead letters"
  eyebrow "Guide"

  def lead = "Failures retry with exponential backoff, then route to a dead-letter queue instead of spinning forever."

  def content
    how_retries_work
    backoff
    per_job
    dlq
    why_it_died
  end

  private

  def how_retries_work
    DocsUI::Section("Retries ride the visibility timeout") do
      md <<~'MD'
        pgbus doesn't re-enqueue a failed job. When a job raises, the worker lets
        the PGMQ **visibility timeout** expire — the message simply becomes visible
        again and another worker picks it up. PGMQ's `read_ct` counts each delivery,
        so pgbus always knows how many attempts a message has had.
      MD
      DocsUI::Callout(:note) do
        plain "This is why there's no separate retry table: the queue itself is the "
        plain "retry mechanism. A message that fails is never lost — it's just "
        plain "invisible until its timeout lapses."
      end
    end
  end

  def backoff
    DocsUI::Section("Exponential backoff with jitter", description: "Spread retries out instead of bunching them.") do
      md <<~'MD'
        Rather than retry at a fixed interval, pgbus extends the visibility timeout
        with exponential backoff and a little jitter, so a thundering herd of
        failures doesn't all retry at the same instant:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.retry_backoff        = 5    # base delay (seconds)
          config.retry_backoff_max    = 300  # cap at 5 minutes
          config.retry_backoff_jitter = 0.15 # ±15% randomization
        end
      RUBY
      md <<~'MD'
        The delay is `base * 2^(attempt-1) * (1 + jitter)`. With the defaults, a job
        that fails four times waits roughly 5s, 10s, 20s, 40s before it hits the
        DLQ on the fifth read.
      MD
    end
  end

  def per_job
    DocsUI::Section("Per-job overrides") do
      md <<~'MD'
        A fragile job — say one calling a flaky third-party API — can widen its own
        backoff without changing the global setting:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/fragile_api_job.rb")
        class FragileApiJob < ApplicationJob
          include Pgbus::RetryBackoff::JobMixin

          pgbus_retry_backoff base: 10, max: 600, jitter: 0.2

          def perform(...)
            # ...
          end
        end
      RUBY
    end
  end

  def dlq
    DocsUI::Section("The dead-letter queue", description: "read_ct > max_retries → <queue>_dlq.") do
      md <<~'MD'
        When `read_ct` exceeds `max_retries` (default 5), the message stops
        retrying and moves to a dead-letter queue named `<queue>_dlq`. It sits there
        for inspection instead of consuming worker capacity forever.
      MD
      render Components::Diagrams::MessageLifecycle.new
      md <<~'MD'
        Dead-lettered messages show up in the [dashboard](/docs/dashboard), where
        you can inspect the payload and the failure. Tune the threshold with
        `max_retries`:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.max_retries = 3 } # DLQ after 3 failed reads
      RUBY
    end
  end

  def why_it_died
    DocsUI::Section("Why a message died", description: "The pgbus_dead_letter header block.") do
      md <<~'MD'
        The process that moves a message to its DLQ — the job worker or the event
        consumer — writes why into the DLQ copy's PGMQ **headers**, under the
        `pgbus_dead_letter` key (`Pgbus::DeadLetterHeader::KEY`). Headers the
        message already carried (trace ids, `x-pgmq-group`) stay next to it. The
        dashboard's DLQ page, `pgbus dlq list|show` and the MCP `pgbus_dlq` tools
        read it back.
      MD
      DocsUI::Table(
        [ "Key", "Value" ],
        [
          [ [ :code, "version" ], "1 — bumped when the shape changes." ],
          [ [ :code, "reason" ], [ :md, "`max_retries_exceeded`, the only reason today." ] ],
          [ [ :code, "source" ], [ :md, "`worker` (ActiveJob) or `consumer` (event bus)." ] ],
          [ [ :code, "source_queue" ], [ :md, "The physical queue it was deleted from, a priority sub-queue (`…_p2`) included." ] ],
          [ [ :code, "attempts" ], [ :md, "`read_ct` of the read that routed it." ] ],
          [ [ :code, "max_retries" ], [ :md, "`max_retries` at that moment." ] ],
          [ [ :code, "dead_lettered_at" ], "UTC ISO 8601." ],
          [ [ :code, "retries_from_dlq" ], "How many times it was retried out of a DLQ before; absent the first time." ],
          [ [ :code, "error_class" ], [ :md, "The last recorded failure, from `pgbus_failed_events`; absent when none was recorded." ] ],
          [ [ :code, "error_message" ], "Truncated to 1,000 characters." ],
          [ [ :code, "backtrace" ], "The first 10 lines, at most 2,000 characters." ],
          [ [ :code, "error_attempt" ], "The attempt the error came from. A worker that crashed mid-perform records nothing, so it can be earlier than the last one." ],
          [ [ :code, "error_recorded_at" ], "When that failure was recorded." ]
        ]
      )
      md <<~'MD'
        **Retry strips it.** Retrying from the DLQ (dashboard, `pgbus dlq retry`,
        Retry All) re-enqueues the message without the block — a live message must
        not claim to be dead — and sets `pgbus_dlq_retries` to the trip count, so a
        second death reads "retried from the DLQ once before".

        Messages dead-lettered before pgbus 0.18.0 have no block and read "Reason
        not recorded"; they retry and discard as before. An event's handler error is
        recorded once the consumer writes `pgbus_failed_events` rows; until then a
        dead event says "no handler error was recorded".
      MD
    end
  end
end
