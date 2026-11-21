# frozen_string_literal: true

# The drop-in ActiveJob adapter: how jobs enqueue, run, and follow a message
# through PGMQ. The message-lifecycle diagram anchors the mental model.
class Views::Docs::Pages::ActiveJob < DocsUI::Page
  title "ActiveJob adapter"
  eyebrow "Guide"

  def lead = "Set the adapter to :pgbus and your existing jobs run on PGMQ, unchanged."

  def content
    setup
    enqueueing
    coalescing
    lifecycle
    current_attributes
    what_you_keep
  end

  private

  def setup
    DocsUI::Section("Set the adapter") do
      md <<~'MD'
        pgbus is a standard ActiveJob queue adapter. Point Rails at it and every
        job in your app enqueues through PGMQ:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.active_job.queue_adapter = :pgbus
      RUBY
      DocsUI::Callout(:note) do
        plain "Because it's a standard adapter, nothing about your job classes changes — "
        plain "no base-class swap, no per-job include. "
        code { "queue_as" }
        plain ", "
        code { "retry_on" }
        plain ", and "
        code { "discard_on" }
        plain " all work as usual."
      end
    end
  end

  def enqueueing
    DocsUI::Section("Enqueue and schedule") do
      md <<~'MD'
        Your existing jobs work with no edits:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/order_confirmation_job.rb")
        class OrderConfirmationJob < ApplicationJob
          queue_as :mailers

          def perform(order)
            OrderMailer.confirmation(order).deliver_now
          end
        end

        OrderConfirmationJob.perform_later(order)                    # enqueue now
        OrderConfirmationJob.set(wait: 5.minutes).perform_later(order) # scheduled
      RUBY
      md <<~'MD'
        A scheduled job is sent with a PGMQ delay, so it stays invisible until its
        time arrives — no separate scheduler poll table.
      MD
    end
  end

  def coalescing
    DocsUI::Section("Coalescing enqueues", description: "One send for a unit of work's jobs.") do
      md <<~'MD'
        Every `perform_later` is one send, so one database round trip. Inside
        `Pgbus.coalesce` the adapter holds the jobs it can send in bulk and sends
        them when the block ends: one `send_batch` per queue, priority and delay
        (per chunk of `config.coalesce_chunk_size`).
      MD
      DocsUI::Code(<<~RUBY, filename: "app/controllers/orders_controller.rb")
        Pgbus.coalesce do                 # on_failure: :raise (default) | :report | ->(jobs, error) { }
          EventJob.perform_later("a")
          EventJob.perform_later("b")
        end                               # one send here
      RUBY
      md <<~'MD'
        For every request, an opt-in Rack middleware runs the request inside the
        scope and sends before it returns the response, never after it:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/application.rb")
        config.pgbus.coalesce_requests = true               # inserted after ActionDispatch::Executor
        config.pgbus.coalesce_requests_on_failure = :report # default :raise
      RUBY
      md <<~'MD'
        - **Sent at once, not held:** jobs with uniqueness or a concurrency limit,
          `retry_on` re-enqueues, deploy task runs,
          batch members outside their own built batch's block, workflow steps, jobs
          from another thread or fiber, and
          jobs enqueued once the flush has started. On Rails 7.2+ a job that
          Rails defers to after commit (`enqueue_after_transaction_commit`)
          reaches the scope at commit, so one from a rolled-back transaction
          never does; a job that does not defer is held like any other.
        - **Bulk-eligible future jobs (`set(wait:)`)** are held too and sent with the delay
          left at the flush, one `send_batch` per queue, priority and delay.
        - **Serialized at `perform_later`:** an argument that cannot be
          serialized raises there, and a later change to the job is not sent.
        - **The return value:** `perform_later` returns the job with
          `successfully_enqueued?` true and `provider_job_id` nil until the flush,
          as Rails does for a job deferred to after commit.
        - **The flush:** one `send_batch` per queue, priority and delay (per chunk), then each job
          left unsent is retried on its own; what still fails goes to
          `on_failure`. `:raise` raises `Pgbus::EnqueueError` naming each job.
          When the block raises, the buffer is still sent and the block's error
          propagates.
        - **Chunks:** a scope sends what it holds every
          `config.coalesce_chunk_size` jobs (default 1 000) and at the end. A
          built batch's scope keeps holding while a transaction is open on the
          batch's connection; `on_failure` runs once, at the end, over every
          chunk.
        - **Nesting and batches:** a nested scope joins the outer one. A
          `Pgbus.batch` built in a pgbus process opens a scope of its own that
          holds only its members, sends them by the batch block's end and
          never joins an outer scope (see [Batches](/docs/batches)).

        An endpoint that must not answer 500 after committing its writes should use
        `on_failure: :report` or a callable that does not raise.
      MD
    end
  end

  def lifecycle
    DocsUI::Section("What happens to a message", description: "Enqueue, read, execute, archive — or dead-letter.") do
      md <<~'MD'
        Each job becomes one PGMQ message. The adapter serializes it to JSON and
        sends it to the queue; a worker claims it under a visibility timeout, runs
        it inside the Rails executor, and archives it on success. On failure the
        visibility timeout expires and the message is retried — until `read_ct`
        crosses `max_retries`, when it routes to the dead-letter queue.
      MD
      render Components::Diagrams::MessageLifecycle.new
      md <<~'MD'
        The retry backoff and dead-letter details are on
        [Retries & dead letters](/docs/retries-dead-letters).
      MD
    end
  end

  def current_attributes
    DocsUI::Section("Current attributes", description: "Request context travels with the job.") do
      md <<~'MD'
        `ActiveSupport::CurrentAttributes` (`Current.tenant`, `Current.user`,
        `Current.request_id`) is reset around every job, so inside `perform` it is
        empty — unless you ask pgbus to carry it. One switch:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.current_attributes = :auto                  # every ActiveSupport::CurrentAttributes subclass
          # config.current_attributes = [Current, "Admin::Current"]          # or an explicit list
          # config.current_attributes = { Current => { except: [:request] } } # or per-class only:/except:
        end
      RUBY
      md <<~'MD'
        At `perform_later` the assigned attributes of each persisted class are
        serialized with `ActiveJob::Arguments` (a record becomes a GlobalID, a
        Symbol stays a Symbol) into the job payload under `pgbus_current`. When the
        job runs, pgbus wraps the **whole** `perform_now` in `Current.set(...)` —
        so `before_perform`, `perform`, `rescue_from`, `retry_on` / `discard_on`
        blocks and a job enqueued from inside `perform` all see the context, and
        the previous values come back afterwards. Because it lives in the job
        hash (not in pgbus metadata), it behaves the same under Rails' `:test` and
        `:inline` adapters and a bare `job.perform_now`.
      MD
      DocsUI::Table(
        [ "Path", "What the job sees" ],
        [
          [ [ :code, "retry_on" ], "The context captured at the original enqueue — a retry never picks up whatever Current happened to be during the failed attempt." ],
          [ [ :code, "limits_concurrency on_conflict: :block" ], "Promotion re-sends the stored payload; context preserved." ],
          [ "Dead-letter / dashboard retry", "Same payload, same context." ],
          [ [ :code, "perform_all_later" ], "Every job tagged." ],
          [ "Batch callbacks", "Captured from the job that finished the batch (they are enqueued from its executor)." ],
          [ "Recurring tasks", "Nothing persisted — the scheduler has no request context." ]
        ]
      )
      md <<~'MD'
        **Safety.** GlobalIDs inside the context are gated by the same
        `allowed_global_id_models` allowlist as job arguments. An attribute that
        cannot be serialized (`Current.request` holding an `ActionDispatch::Request`,
        say) raises `Pgbus::CurrentAttributesError` at `perform_later` naming the
        class, the attribute and the `except:` fix — nothing is dropped silently.
        One deliberate exception: an **unpersisted** record (`persisted?` falsey —
        a dev-mode fallback record, a model captured before `save`, a destroyed
        record) is skipped with a debug log instead of raising, because with no
        id it could never be restored anyway and its momentary state should not
        abort the enqueue.
        Per job class: `self.pgbus_persist_current_attributes = false` (never persist)
        or a list/hash in the config shapes (replace the list for this class).
        Under `execution_mode: :async` remember `CurrentAttributes` is per isolation
        unit — set `config.active_support.isolation_level = :fiber`
        (see [Running workers](/docs/running-workers)).

        The failed-job and dead-letter pages in the dashboard show the persisted
        context in a **Context** card (through the same parameter filter as the
        payload). Pairs naturally with
        [fair share](/docs/routing-ordering): `config.fair_share = ->(job) { Current.tenant_id }`
        now also sees the restored tenant on a retry re-enqueue.
      MD
    end
  end

  def what_you_keep
    DocsUI::Section("Serialization and safety") do
      md <<~'MD'
        Payloads are **JSON only** — pgbus never uses `Marshal`, so a malicious or
        corrupt payload can't deserialize into arbitrary Ruby. GlobalID arguments
        (an Active Record record) resolve through the same
        `config.allowed_global_id_models` allowlist that EventBus payloads use:
        when the allowlist is set, a crafted `_aj_globalid` job argument whose
        model is not listed raises `Pgbus::SerializationError` before Rails'
        unrestricted `GlobalID::Locator` runs. Leave the config `nil` (default)
        for allow-all; set `[]` to deny all. Apps that enqueue ActiveStorage
        analyze/purge/transform jobs need `ActiveStorage::Blob` (and related
        models) on the allowlist.
      MD
      DocsUI::Callout(:tip) do
        plain "Need at-most-once semantics or a concurrency cap? See "
        a(href: "/docs/concurrency-uniqueness", class: "link") { "Concurrency & uniqueness" }
        plain " — "
        code { "ensures_uniqueness" }
        plain " and "
        code { "limits_concurrency" }
        plain " layer straight onto a job class."
      end
    end
  end
end
