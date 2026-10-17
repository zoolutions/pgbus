# frozen_string_literal: true

# One-time work owed by a release: the YAML, version vs every_deploy, the
# revision chain, the rollover gate, manual control, and failure semantics.
class Views::Docs::Pages::DeployTasks < DocsUI::Page
  title "Deploy tasks"
  eyebrow "Guide"

  def lead = "One-time work owed by a release — a backfill, a data fix, a cache warm — run once, on the new code, after the deploy has rolled over."

  def content
    overview
    yaml_file
    version_vs_every_deploy
    revision
    gate
    who_runs_it
    manual_control
    ran_and_failures
    history
    setup
  end

  private

  def overview
    DocsUI::Section("What a deploy task is") do
      md <<~'MD'
        A deploy task runs once per `(key, version)`, and only after every live pgbus
        process reports the new app revision and at least one worker on that revision
        is alive. So it:

        - never runs on old code during the [deploy overlap window](/docs/rolling-restarts);
        - runs once per rollover in normal operation, however many hosts you have —
          two schedulers never both fire it;
        - is not forgotten — a task that is owed waits until the fleet has rolled over.

        It is not an exactly-once guarantee: a failure between the enqueue and
        recording the run can repeat it after a scheduler restart (see "What
        ran means" below), so make deploy tasks idempotent.
      MD
    end
  end

  def yaml_file
    DocsUI::Section("Declare tasks in deploy_tasks.yml", description: "Same shape as recurring.yml, minus schedule:.") do
      md <<~'MD'
        Each task is a key with either a `class` (an ActiveJob class) or a `command`
        (`Const.method`, run by `Pgbus::Recurring::CommandJob`, with the same rules as
        a [recurring](/docs/recurring-tasks) `command:`). `queue`, `args`, `priority`
        and `description` are optional. A `class:` that is not an ActiveJob is skipped
        with a warning when the scheduler boots.
      MD
      DocsUI::Code(<<~YAML, filename: "config/deploy_tasks.yml", lexer: :yaml)
        production:
          backfill_order_totals:
            class: BackfillOrderTotalsJob      # or command: "Orders.backfill_totals!"
            version: 2                          # bump to run again after the next rollover (default 1)
            queue: maintenance
            args: [{ batch_size: 500 }]
            description: Recompute totals after the currency migration
          warm_fragment_caches:
            command: "FragmentCacheWarmer.run"
            every_deploy: true                  # runs after every rollover, dedup'd on the app revision
      YAML
      DocsUI::Callout(:note) do
        plain "The file is ERB-evaluated and can be environment-scoped or flat. List several files with "
        code { "config.deploy_tasks_files" }
        plain "; explicit files replace the default one. "
        code { "config.deploy_tasks" }
        plain " takes a hash of the same shape."
      end
    end
  end

  def version_vs_every_deploy
    DocsUI::Section("version: or every_deploy:") do
      DocsUI::Table(
        [ "Setting", "Runs", "Use for" ],
        [
          [ [ :code, "version: 1" ], "Once per version, after the next rollover. Compared as a string; default \"1\".", "A data fix. Bump the version in the same commit as a new fix to run it again." ],
          [ [ :code, "every_deploy: true" ], "After every rollover, dedup'd on the app revision.", "A cache warm. A rollback deploy re-fires it; a scheduler crash-restart does not." ]
        ]
      )
      md <<~'MD'
        `version` is ignored for an `every_deploy` task (a warning is logged).
      MD
    end
  end

  def revision
    DocsUI::Section("The app revision", description: "How pgbus knows which release a process runs.") do
      md <<~'MD'
        The revision is the first non-blank value of:

        1. `config.app_revision` (a String, or a callable returning one)
        2. `ENV["PGBUS_REVISION"]`
        3. `ENV["DASH_VERSION"]`
        4. `ENV["KAMAL_VERSION"]`
        5. the `REVISION` file in `Rails.root` (Capistrano)

        dash injects `KAMAL_VERSION` into every app container, and Kamal sets it, so
        neither needs configuring. Every pgbus process (supervisor, worker, dispatcher,
        scheduler, consumer, outbox poller) stamps the revision into its heartbeat row
        (`metadata["revision"]`); the Processes page shows it in the expanded row.
      MD
      DocsUI::Callout(:warning) do
        plain "With no revision the gate stays closed: tasks wait, the scheduler warns once at boot, and the dashboard says \"app revision not configured\". Only Run now enqueues."
      end
    end
  end

  def gate
    DocsUI::Section("The rollover gate", description: "Checked by the scheduler at most every 5 seconds.") do
      md <<~'MD'
        A live process is one with a heartbeat in the last 5 minutes. The gate is
        closed while any live process reports another revision, or while no live
        worker runs the new revision. A process with no revision at all (a
        pre-upgrade pgbus) counts as lagging.

        A gracefully stopped process deletes its heartbeat row at once, so a clean
        dash or Kamal deploy opens the gate within a scheduler tick after the last old
        container drains. A hard-killed old container keeps the gate closed until its
        row goes stale, up to 5 minutes.

        Every scheduler that boots syncs its own declarations. If an old-revision
        scheduler crash-restarts inside the overlap window, it removes the rows of
        tasks only the new release declares; the new scheduler restores them on its
        next boot.
      MD
    end
  end

  def who_runs_it
    DocsUI::Section("Who runs it") do
      md <<~'MD'
        The scheduler, the process that already runs recurring tasks. The supervisor
        forks it when recurring tasks or deploy tasks are configured
        (`config.deploy_tasks_enabled`, default true); `--scheduler-only` works as
        before. At boot the scheduler syncs the declared tasks into
        `pgbus_deploy_tasks` and arms the owed ones. A compare-and-set claim on the
        row means two schedulers (the overlap window, several hosts) never both fire,
        and an arm can only be fired by a scheduler declaring that version or revision.

        Tasks that are due together are enqueued in key order, with no completion
        ordering between them. If one depends on another, make it a single job.
      MD
    end
  end

  def manual_control
    DocsUI::Section("Run now and Disable", description: "On the Deploy tasks page, /pgbus/deploy_tasks.") do
      md <<~'MD'
        The page shows a banner with the rollover status (ready, waiting for N
        processes on another revision, waiting for a worker, app revision not
        configured, or no scheduler running) and live process counts per revision.
        Each task says in words whether it ran (when, on which revision), is waiting
        (and why), is due ("runs on the scheduler's next tick"), is disabled, or never
        ran. A task's page shows its configuration and its last 25 runs.

        - **Run now** enqueues immediately, with no gate and no claim, and records a
          manual run. It does not cancel a pending rollover run: Disable the task for that.
        - **Enable / Disable** is honoured by the scheduler on every tick.
      MD
    end
  end

  def ran_and_failures
    DocsUI::Section("What \"ran\" means", description: "Enqueued, like recurring tasks.") do
      md <<~'MD'
        The task is enqueued through ActiveJob `perform_later`, so uniqueness,
        batches, concurrency, priority, retries and the dead-letter queue behave as
        for any job. The job's outcome is on the Jobs page.

        If the enqueue fails (`perform_later` raises, returns false, or does not
        enqueue the job), the task is re-armed, the failure is logged and reported to
        your error reporters, and the scheduler retries it after 5 seconds, then 10,
        20, and so on up to 5 minutes. Disable the task to stop the retries. Each
        enqueue emits `pgbus.deploy_task.enqueue` (`task`, `version`, `revision`,
        `triggered_by`).

        The run row is what marks a version as done. If it cannot be written right
        after a successful enqueue (logged and reported), the job still runs, but the
        next scheduler boot sees the version as owed and runs it again: make deploy
        tasks idempotent.
      MD
    end
  end

  def history
    DocsUI::Section("Run history") do
      md <<~'MD'
        Runs are kept in `pgbus_deploy_task_runs` for `config.deploy_task_run_retention`
        (default 90 days; `nil` keeps them forever). The dispatcher prunes hourly.
        The read-only MCP tool `pgbus_deploy_tasks` lists tasks, their state and the
        rollover status.
      MD
    end
  end

  def setup
    DocsUI::Section("Install the tables") do
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_deploy_tasks                  # migration + config/deploy_tasks.yml
        rails generate pgbus:add_deploy_tasks --database=pgbus # for a separate database
        rails db:migrate
      SHELL
      md <<~'MD'
        `rails generate pgbus:update` detects the missing tables and queues the
        generator for you. Fresh installs (`pgbus:install`) already have them.
      MD
    end
  end
end
