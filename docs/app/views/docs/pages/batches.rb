# frozen_string_literal: true

# Coordinating a fan-out of jobs and firing a callback when they all finish.
class Views::Docs::Pages::Batches < DocsUI::Page
  title "Batches"
  eyebrow "Guide"

  def lead = "Enqueue a group of jobs and run a callback when the whole batch completes."

  def content
    creating
    callbacks
    how_it_works
  end

  private

  def creating
    DocsUI::Section("Create and enqueue a batch") do
      md <<~'MD'
        A batch tracks a group of related jobs. Enqueue the jobs inside
        `batch.enqueue` and each is tagged with the batch id:
      MD
      DocsUI::Code(<<~RUBY)
        batch = Pgbus::Batch.new(
          on_finish: BatchFinishedJob,
          on_success: BatchSucceededJob,
          on_failure: BatchFailedJob,
          description: "Import users",
          properties: { initiated_by: current_user.id }
        )

        batch.enqueue do
          users.each { |user| ImportUserJob.perform_later(user.id) }
        end
      RUBY
    end
  end

  def callbacks
    DocsUI::Section("Callbacks") do
      DocsUI::Table(
        [ "Callback", "Fired when" ],
        [
          [ [ :code, "on_finish" ], "The batch finished (no outstanding execution rows remain), including after a dispatcher sweep repair." ],
          [ [ :code, "on_success" ], "The batch finished with zero failed jobs." ],
          [ [ :code, "on_failure" ], "The batch finished with at least one dead-lettered job. (`on_discard:` is a deprecated alias until 1.0.)" ]
        ]
      )
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

  def how_it_works
    DocsUI::Section("How batches work") do
      md <<~'MD'
        1. `Batch.new(...)` creates a row in `pgbus_batches` with
           `status: "pending"`.
        2. `batch.enqueue { ... }` tags each enqueued job with the batch id and
           inserts a `pgbus_batch_executions` row (identity is the ActiveJob
           `job_id`) *before* the message is sent.
        3. As each job is archived or dead-lettered, the executor deletes that
           execution row. Counters stay as dashboard data.
        4. The batch finishes when no execution rows remain (single-winner
           update). A dispatcher sweep repairs crash windows — a worker that
           dies between archive and row-delete, an enqueue that dies between
           insert and send, a `pending` batch whose block never returned.
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
