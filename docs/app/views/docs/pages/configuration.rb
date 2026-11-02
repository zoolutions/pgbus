# frozen_string_literal: true

# How pgbus is configured: the initializer, the config groups that matter most,
# and the capsule DSL for defining workers. The exhaustive option list lives on
# the Configuration reference page.
class Views::Docs::Pages::Configuration < DocsUI::Page
  title "Configuration"
  eyebrow "Getting started"

  def lead = "One initializer drives everything — queues, retries, recycling, and worker capsules."

  def content
    initializer
    common_knobs
    eager_validation
    capsules
    full_example
    upgrading
  end

  private

  def initializer
    DocsUI::Section("The initializer", description: "Call Pgbus.configure once at boot.") do
      md <<~'MD'
        pgbus reads its settings from a `Pgbus.configure` block. Every option has
        a sensible default, so an empty block (or no initializer at all) gives you
        a working setup on your primary database.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.queue_prefix       = "myapp"        # all queues → myapp_<name>
          c.max_retries        = 5              # failed reads before the DLQ
          c.visibility_timeout = 30.seconds
        end
      RUBY

      DocsUI::Callout(:note) do
        plain "Queue names are always prefixed: "
        code { "{queue_prefix}_{name}" }
        plain " (default "
        code { "pgbus_default" }
        plain "). Dead-letter queues append "
        code { "_dlq" }
        plain ". Names are lower case: PGMQ spells its tables that way, so "
        code { "Orders" }
        plain " and "
        code { "orders" }
        plain " are one queue, "
        code { "pgbus_orders" }
        plain " (since 0.23.0; an older install folds its queues with "
        code { "rails generate pgbus:fold_queue_name_case" }
        plain ")."
      end
    end
  end

  def common_knobs
    DocsUI::Section("The knobs you'll reach for first") do
      md <<~'MD'
        These are the settings most apps touch. Durations accept an integer number
        of seconds or an `ActiveSupport::Duration` (`30.seconds`, `1.hour`).
      MD
      DocsUI::PropTable([
        [ [ :code, "queue_prefix" ], "String", [ :code, '"pgbus"' ], "Prefix for every PGMQ queue name." ],
        [ [ :code, "max_retries" ], "Integer", [ :code, "5" ], "Failed reads before a message routes to the dead-letter queue." ],
        [ [ :code, "visibility_timeout" ], "Duration", [ :code, "30" ], "How long a read message stays invisible before it can be retried." ],
        [ [ :code, "workers" ], "String / Array", [ :code, "default: 5" ], "Worker capsule definitions — see the capsule DSL below." ],
        [ [ :code, "max_jobs_per_worker" ], "Integer, nil", [ :code, "nil" ], "Recycle a worker after N jobs." ],
        [ [ :code, "max_memory_mb" ], "Integer, nil", [ :code, "nil" ], "Recycle a worker when RSS exceeds N MB." ],
        [ [ :code, "max_worker_lifetime" ], "Duration, nil", [ :code, "nil" ], "Recycle a worker after N seconds." ],
        [ [ :code, "idempotency_ttl" ], "Duration, nil", [ :code, "7.days" ], "How long processed-event records are kept for dedup." ]
      ])
      md <<~'MD'
        The complete list — outbox, streams, metrics, health, and the rest — is on
        the [Configuration reference](/docs/configuration-reference).
      MD
    end
  end

  def eager_validation
    DocsUI::Section("Configuration is validated eagerly", description: "A bad setting fails boot, not a worker mid-run.") do
      md <<~'MD'
        `Pgbus.configure` runs `configuration.validate!` right after your block
        yields. An invalid value — `visibility_timeout = 0`, for
        example — now raises `ArgumentError` at Rails boot instead of sitting
        dormant until a worker code path finally consumes it, far from the
        misconfiguration. `validate!` stays DB-free, so eager validation adds no
        boot-time database dependency.
      MD
      md <<~'MD'
        This is backward-incompatible in one direction: an invalid-but-previously-
        unread config now raises at boot instead of silently later. That's the
        intended fix. For an exotic setup that intentionally holds a transiently-
        invalid config (built up across several sequential `configure` calls, say),
        set the escape hatch:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.eager_validation = false # default true; suppresses the automatic validate!
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "Explicit "
        code { "Pgbus.configuration.validate!" }
        plain " calls always still run — "
        code { "eager_validation" }
        plain " only suppresses the automatic call after "
        code { "configure" }
        plain "."
      end
    end
  end

  def capsules
    DocsUI::Section("Worker capsules", description: "Which queues each worker serves, and how many threads.") do
      md <<~'MD'
        A **capsule** is a group of worker threads bound to a set of queues. The
        shortest form is the string DSL — Sidekiq-style `queues: threads`,
        semicolons separating capsules:
      MD
      DocsUI::Code(<<~RUBY)
        # default + mailers on 10 threads; critical on its own 5 threads
        c.workers = "default, mailers: 10; critical: 5"
      RUBY
      md <<~'MD'
        When you need advanced options — a single active consumer for strict
        ordering, or a consumer priority — use named capsules:
      MD
      DocsUI::Code(<<~RUBY)
        c.capsule :ordered, queues: %w[ordered_events], threads: 1, single_active_consumer: true
      RUBY
      md <<~'MD'
        The ordering and priority options are covered in
        [Routing & ordering](/docs/routing-ordering); recycling is in
        [Running workers](/docs/running-workers).
      MD
    end
  end

  def full_example
    DocsUI::Section("A full initializer", description: "Every subsystem turned on, for reference.") do
      md <<~'MD'
        Most apps set a handful of these. This is the kitchen-sink version — an app
        using separate databases, priority queues, the outbox, realtime streams, and
        Prometheus metrics all at once — so you can see how the groups fit together.
        Copy the lines you need; every setting has a working default if you omit it.
      MD
      DocsUI::Code(<<~'RUBY', filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          # --- Database & connection pool ------------------------------------
          c.queue_prefix = "myapp"
          c.connects_to  = { database: { writing: :pgbus } }  # dedicated database
          c.pool_timeout = 5                                    # pool_size auto-tunes from thread counts

          # --- Wake-up, visibility, retries ----------------------------------
          c.listen_notify      = true          # LISTEN/NOTIFY instant wake-up
          c.visibility_timeout = 30.seconds
          c.max_retries        = 5             # reads before the dead-letter queue
          c.idempotency_ttl    = 7.days

          # --- Priority queues -----------------------------------------------
          c.priority_levels  = 3   # enable 3 priority sub-queues per queue
          c.default_priority = 1

          # --- Workers -------------------------------------------------------
          c.capsule :default,  queues: %w[critical default], threads: 5
          c.capsule :low,      queues: %w[low],              threads: 2
          c.capsule :ordered,  queues: %w[ordered_events],   threads: 1, single_active_consumer: true

          # --- Worker recycling ----------------------------------------------
          c.max_jobs_per_worker = 10_000
          c.max_memory_mb       = 512
          c.max_worker_lifetime = 1.hour

          # --- Event bus consumers -------------------------------------------
          c.event_consumers = [
            { topics: ["orders.#"],        threads: 3 },
            { topics: ["notifications.#"], threads: 1 }
          ]

          # --- Transactional outbox ------------------------------------------
          c.outbox_enabled       = true
          c.outbox_poll_interval = 0.5
          c.outbox_retention     = 1.day

          # --- Realtime streams (turbo-rails) --------------------------------
          c.streams_enabled         = true
          c.streams_broadcast_queue = "realtime"                  # isolate broadcast jobs (#311)
          c.capsule :realtime, queues: %w[realtime], threads: 3   # ...and a worker to drain them
          c.streams_retention = {
            "orders.*"        => 30.days,   # keep order streams for replay
            "notifications.*" => 1.day      # ephemeral
          }

          # --- Metrics (Prometheus / StatsD) ---------------------------------
          c.metrics_backend = :prometheus

          # --- Recurring tasks -----------------------------------------------
          c.recurring_enabled          = true
          c.recurring_schedule_interval = 30.seconds
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "The exhaustive option list — every streams, health, and metrics knob — is on the "
        a(href: "/docs/configuration-reference", class: "link") { "Configuration reference" }
        plain "."
      end
    end
  end

  def upgrading
    DocsUI::Section("Upgrading an existing install") do
      md <<~'MD'
        `rails generate pgbus:update` inspects your live database and adds any
        missing pgbus migrations. It detects a separate database automatically, so
        you don't re-specify `--database=pgbus`.

        YAML config (`config/pgbus.yml`) was removed in 1.0 — it is no longer
        loaded, and pgbus warns once at boot if the file is still present. Port
        its settings into `config/initializers/pgbus.rb` and delete the YAML.
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:update            # add missing migrations
        rails generate pgbus:update --dry-run  # print the plan, create nothing
      SHELL
    end
  end
end
