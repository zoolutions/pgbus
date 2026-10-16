# frozen_string_literal: true

# Cron-style recurring tasks: the migration, the two ways to declare tasks
# (a YAML file or the config hash), the class-vs-command distinction, and the
# schedule syntax.
class Views::Docs::Pages::RecurringTasks < DocsUI::Page
  title "Recurring tasks"
  eyebrow "Guide"

  def lead = "Run jobs on a cron schedule — a YAML file (SolidQueue-compatible) or the config hash."

  def content
    setup
    yaml_file
    config_hash
    schedules
    class_vs_command
  end

  private

  def setup
    DocsUI::Section("Install the tables") do
      md <<~'MD'
        Recurring tasks need two tables — one for the task definitions, one for the
        execution ledger that keeps a schedule from firing twice. Add them:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:add_recurring                  # add the migration + config/recurring.yml
        rails generate pgbus:add_recurring --database=pgbus # for a separate database
        rails db:migrate
      SHELL
      md <<~'MD'
        The generator also writes a starter `config/recurring.yml`. A scheduler
        process (part of the supervisor) reads your tasks, syncs them to the
        database, and enqueues each on its schedule. For one-time work after a
        deploy, see [Deploy tasks](/docs/deploy-tasks).
      MD
    end
  end

  def yaml_file
    DocsUI::Section("Declare tasks in recurring.yml", description: "SolidQueue-compatible; environment-scoped or flat.") do
      md <<~'MD'
        The generated file is compatible with SolidQueue's `recurring.yml`, so a
        migration is mostly a copy. Each task is a key with a `schedule` and either
        a `class` (an ActiveJob class) or a `command` (Ruby to run):
      MD
      DocsUI::Code(<<~YAML, filename: "config/recurring.yml", lexer: :yaml)
        production:
          periodic_cleanup:
            class: CleanupJob
            queue: maintenance
            args: [1000, { batch_size: 500 }]
            schedule: every hour

          daily_report:
            class: DailyReportJob
            schedule: "0 8 * * mon-fri"
            description: Generate the daily business report
      YAML
      DocsUI::Callout(:note) do
        plain "The file is ERB-evaluated and can be environment-scoped (a "
        code { "production:" }
        plain " top-level key) or flat. Point at it with "
        code { "config.recurring_tasks_file" }
        plain " if it lives elsewhere."
      end
    end
  end

  def config_hash
    DocsUI::Section("Or declare them in the initializer") do
      md <<~'MD'
        If you'd rather keep everything in Ruby, set `config.recurring_tasks` to a
        hash of the same shape:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.recurring_tasks = {
            periodic_cleanup: {
              class: "CleanupJob",
              schedule: "every hour",
              queue: "maintenance",
              args: [1000, { batch_size: 500 }]
            }
          }
        end
      RUBY
    end
  end

  def schedules
    DocsUI::Section("Schedule syntax", description: "Cron expressions or natural language, parsed by fugit.") do
      md <<~'MD'
        Schedules are parsed by [fugit](https://github.com/floraison/fugit), so both
        cron expressions and natural-language phrases work:
      MD
      DocsUI::Table(
        [ "Schedule", "Means" ],
        [
          [ [ :code, '"0 2 * * *"' ], "Every day at 2:00 AM" ],
          [ [ :code, '"*/5 * * * *"' ], "Every 5 minutes" ],
          [ [ :code, '"every hour"' ], "Every hour at :00" ],
          [ [ :code, '"every day at 2am"' ], "Daily at 2:00 AM" ],
          [ [ :code, '"@daily"' ], "Daily at midnight" ],
          [ [ :code, '"0 9 * * mon-fri"' ], "Weekdays at 9:00 AM" ]
        ]
      )
    end
  end

  def class_vs_command
    DocsUI::Section("class: vs command:") do
      md <<~'MD'
        A task runs **either** a job class **or** an inline command — one is
        required. Use `class:` to enqueue an ActiveJob (the usual case); use
        `command:` for a one-off snippet with no job class:
      MD
      DocsUI::Code(<<~YAML, filename: "config/recurring.yml", lexer: :yaml)
        cleanup_old_records:
          command: "OldRecord.where('created_at < ?', 30.days.ago).delete_all"
          schedule: every day at 3am
      YAML
      DocsUI::Callout(:warning) do
        plain "A "
        code { "command:" }
        plain " string is evaluated as Ruby in the worker — keep it to trusted, "
        plain "version-controlled snippets. For anything non-trivial, prefer a "
        code { "class:" }
        plain " job you can test."
      end
    end
  end
end
