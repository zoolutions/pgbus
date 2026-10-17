# frozen_string_literal: true

# Coordinating a fan-out of jobs and firing a callback when they all finish.
class Views::Docs::Pages::Batches < DocsUI::Page
  title "Batches"
  eyebrow "Guide"

  def lead = "Enqueue a group of jobs and run a callback when the whole batch completes."

  def content
    creating
    inside_a_job
    open_batches
    callbacks
    configured_callbacks
    lower_level_api
    how_it_works
  end

  private

  def creating
    DocsUI::Section("Start a batch") do
      md <<~'MD'
        A batch tracks a group of related jobs and fires a callback when they
        have all finished. `Pgbus.batch` opens one, yields a builder, and
        starts the batch when the block returns:
      MD
      DocsUI::Code(<<~RUBY)
        batch = Pgbus.batch(description: "Import users", properties: { initiated_by: current_user.id }) do |b|
          b.on_complete ImportFinishedJob
          b.on_success  ImportSucceededJob
          b.on_failure  ImportFailedJob
          b.add(*users.map { |user| ImportUserJob.new(user.id) })
        end
      RUBY
      md <<~'MD'
        `b.add` takes job instances, or arrays of them, and sends them in one
        bulk enqueue. Each instance keeps its own options, so
        `ImportUserJob.new(id).set(wait: 5.minutes, queue: :low)` works.
        `b.add { ... }` with a block also works, and any `perform_later` inside
        the `Pgbus.batch` block joins the batch as well.

        The batch row exists before the block runs and stays `pending` until
        the block returns, so a job that finishes while the block is still
        adding jobs cannot finish the batch early. Callbacks can be set at any
        point in the block, but set them first: if the block raises after an
        `add`, the jobs already added still run, and the batch finishes later
        (through the dispatcher sweep) with only the callbacks set by then. A
        block that goes `config.batch_stall_threshold` (default 5 minutes)
        without adding a job is treated as stalled and started by that sweep,
        so keep slow work outside the block. `b.add` must run in the block's
        own thread or fiber; it raises anywhere else, since jobs sent from
        there would not join the batch. `Pgbus.batch` returns the started `Pgbus::Batch`; its
        keywords are the ones `Pgbus::Batch.new` takes (`description:`,
        `properties:`, `uniqueness_key:`, `on_conflict:`, and the callbacks).
        `Pgbus::Batch.start` is the same method.

        A callback is an ActiveJob class or a configured instance, never a
        block: it is stored on the batch row and enqueued later, from another
        process. The builder only works inside its block. To add jobs later,
        call `batch.add` on the batch `Pgbus.batch` returned.
      MD
    end
  end

  def inside_a_job
    DocsUI::Section("Start a batch from a job") do
      md <<~'MD'
        Inside a job, `batch` with a block is `Pgbus.batch`. It opens a new
        batch, independent of any batch the running job belongs to:
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/jobs/import_job.rb")
        class ImportJob < ApplicationJob
          def perform(source_id)
            source = Source.find(source_id)

            batch(description: "Import #{source_id}") do |b|
              b.on_complete ImportFinishedJob
              b.add(*source.rows.map { |row| ImportRowJob.new(row.id) })
            end
          end
        end
      RUBY
      md <<~'MD'
        Without a block, `batch` reads the batch the job belongs to, or the
        batch a callback job reports on.
      MD
    end
  end

  def open_batches
    DocsUI::Section("Open batches") do
      md <<~'MD'
        A batch stays open until it finishes. A job running inside it reaches
        its own batch through `batch` and can add siblings with `batch.add`.
        `total_jobs` grows and the callbacks wait for the new jobs too:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/extract_job.rb")
        class ExtractJob < ApplicationJob
          def perform
            batch.add(*extract.map { |row| TransformJob.new(row.id) })
          end
        end
      RUBY
      md <<~'MD'
        Membership stays explicit: only jobs passed to `add`, or enqueued
        inside a batch block, join the batch. A job that merely runs as part of
        a batch can enqueue unrelated work without extending it.

        `Pgbus::Batch.find(batch_id)` returns the same handle from anywhere —
        with `description`, `properties`, `status`, `total_jobs`,
        `completed_jobs`, `failed_jobs`, `pending_jobs`, `progress_percentage`
        and `finished?`. Adding to a batch that has already finished raises
        `Pgbus::Batch::AlreadyFinished` — at `perform_later`, before the job
        is sent, even if the handle you hold is stale.
      MD
      DocsUI::Callout(:warn) do
        plain "Breaking change (pre-1.0): "
        code { "Pgbus::Batch.find" }
        plain " used to return the raw attributes Hash. Read the values off the handle, or query "
        code { "Pgbus::BatchEntry" }
        plain " directly for a row."
      end
    end
  end

  def configured_callbacks
    DocsUI::Section("Configured callback jobs") do
      md <<~'MD'
        A callback can be a configured ActiveJob instance instead of a bare
        class, so it runs on the queue — and with the delay — you choose:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.batch do |b|
          b.on_complete BatchFinishedJob.new.set(queue: :critical, wait: 5.minutes)
          b.add(*jobs)
        end
      RUBY
      md <<~'MD'
        `.set` options resolve when the batch is created, and the serialized job
        is stored on the batch row. At fire time it is enqueued on its
        configured queue with `callback_batch_id` pointing at the finished
        batch, so the callback reads the batch through `batch` rather than
        through a properties argument:
      MD
      DocsUI::Code(<<~'RUBY', filename: "app/jobs/batch_finished_job.rb")
        class BatchFinishedJob < ApplicationJob
          def perform(*)
            Rails.logger.info "#{batch.completed_jobs}/#{batch.total_jobs} done"
            User.find(batch.properties["initiated_by"])
          end
        end
      RUBY
      md <<~'MD'
        A callback is never a member of the batch it reports on — its own
        `batch_id` is nil, so enqueueing it can never keep the batch open.
      MD
      DocsUI::Callout(:note) do
        plain "Existing installs need "
        code { "rails generate pgbus:add_batch_callback_jobs" }
        plain " (or "
        code { "pgbus:update" }
        plain ") for the jsonb callback columns. Bare callback classes keep the "
        code { "perform_later(properties)" }
        plain " signature, deprecated at 1.0."
      end
    end
  end

  def callbacks
    DocsUI::Section("Callbacks") do
      DocsUI::Table(
        [ "Callback", "Fired when" ],
        [
          [ [ :code, "on_complete" ], "The batch finished (no outstanding execution rows remain), including after a dispatcher sweep repair. (`on_finish` is a deprecated alias until 1.0.)" ],
          [ [ :code, "on_success" ], "The batch finished with zero failed jobs." ],
          [ [ :code, "on_failure" ], "The batch finished with at least one dead-lettered job. (`on_discard:` is a deprecated alias until 1.0.)" ]
        ]
      )
      md <<~'MD'
        Each name works as a builder method (`b.on_complete MyJob`) and as a
        `Pgbus::Batch.new` keyword (`on_complete: MyJob`). `on_finish` still
        works in both places and logs a deprecation warning once per process.
        The batch row keeps its `on_finish_class` and `on_finish_job` columns
        until 1.0, so batches created before the rename still fire.
      MD
      md <<~'MD'
        A callback job receives the batch `properties` hash as its argument:
      MD
      DocsUI::Code(<<~RUBY, filename: "app/jobs/batch_finished_job.rb")
        class BatchFinishedJob < ApplicationJob
          def perform(properties)
            user = User.find(properties["initiated_by"])
            ImportMailer.complete(user).deliver_later
          end
        end
      RUBY
    end
  end

  def lower_level_api
    DocsUI::Section("Pgbus::Batch.new and enqueue") do
      md <<~'MD'
        `Pgbus.batch` is built on `Pgbus::Batch.new` plus `#enqueue`, which
        stay available. Callbacks are keywords, and every job enqueued inside
        the `enqueue` block joins the batch:
      MD
      DocsUI::Code(<<~RUBY)
        batch = Pgbus::Batch.new(on_complete: ImportFinishedJob, description: "Import users")
        batch.enqueue do
          users.each { |user| ImportUserJob.perform_later(user.id) }
        end
      RUBY
      md <<~'MD'
        A second `enqueue` (or `add`) on the same handle adds to the batch
        while it is unfinished.
      MD
    end
  end

  def how_it_works
    DocsUI::Section("How batches work") do
      md <<~'MD'
        1. The top of the `Pgbus.batch` block (or the first `enqueue`) takes
           the uniqueness key, if there is one, and creates a row in
           `pgbus_batches` with `status: "pending"`. `Batch.new` alone writes
           nothing.
        2. Each job enqueued in the block is tagged with the batch id and,
           in one transaction *before* the message is sent, increments
           `total_jobs` and inserts a `pgbus_batch_executions` row (identity is
           the ActiveJob `job_id`). The increment is guarded on an unfinished
           batch — that guard is what raises `AlreadyFinished`. A
           `perform_all_later` counts once for the whole bulk.
        3. As each job is archived or dead-lettered, the executor deletes that
           execution row and bumps `completed_jobs` / `failed_jobs`.
           `total_jobs == outstanding rows + completed + failed` holds at every
           commit point. A job that re-enqueues itself with `retry_on` keeps
           its single row across every attempt — the batch waits for the
           terminal outcome, so `on_success` cannot fire while a retry is
           pending and `on_failure` fires if the retries exhaust.
        4. The batch finishes when no execution rows remain and the counters
           add up (single-winner update). A dispatcher sweep repairs crash
           windows — a worker that dies between archive and row-delete, an
           enqueue that dies between insert and send, a `pending` batch whose
           block never returned. An unsent row is only un-counted once the
           sweep has checked the queue and DLQ for its message; a batch is
           only "stalled" after `config.batch_stall_threshold` (default
           5 minutes) without a new execution row.
        5. The dispatcher cleans up finished batches older than
           `config.batch_retention` (default 7 days).
      MD
      DocsUI::Callout(:note) do
        plain "Existing installs need "
        code { "rails generate pgbus:add_batch_executions" }
        plain " (or "
        code { "pgbus:update" }
        plain "). Fresh "
        code { "pgbus:install" }
        plain " already includes the executions table."
      end
    end
  end
end
