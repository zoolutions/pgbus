# frozen_string_literal: true

# Operating the worker fleet: the CLI, split-role deployments, recycling, the
# circuit breaker, prefetch, and the async fiber mode.
class Views::Docs::Pages::RunningWorkers < DocsUI::Page
  title "Running workers"
  eyebrow "Operations"

  def lead = "Start the supervisor, split roles across containers, and keep workers from leaking with recycling."

  def content
    cli
    boot_banner
    shared_listen
    roles
    recycling
    processes
    circuit_breaker
    connection_circuit_breaker
    read_timeouts
    prefetch
    read_ahead
    async
  end

  private

  def cli
    DocsUI::Section("The CLI") do
      DocsUI::Code(<<~SHELL, lexer: :shell)
        pgbus start     # supervisor: workers + dispatcher + scheduler + consumers
        pgbus status    # show running processes
        pgbus queues    # list queues with depth/metrics
        pgbus version   # print the version
      SHELL
    end
  end

  def boot_banner
    DocsUI::Section("Boot diagnostics banner", description: "See what actually booted, at a glance.") do
      md <<~'MD'
        `Supervisor#run` logs a one-block banner right after the heartbeat starts
        and before queues bootstrap — every `"[Pgbus] boot:"`-prefixed line renders
        cleanly under both the `:text` and `:json` log formatters:
      MD
      DocsUI::Code(<<~LOG, lexer: :text, filename: "log output")
        [Pgbus] boot: pgbus 0.9.8 pid=42317
        [Pgbus] boot: connection=host/dbname pool=12
        [Pgbus] boot: pgmq_schema_mode=auto pgmq_version=1.4.0
        [Pgbus] boot: listen_notify=true worker_notify_wakeup=true worker_notify_scope=supervisor
        [Pgbus] boot: roles=workers,dispatcher,scheduler
        [Pgbus] boot: capsule=critical queues=critical threads=5 processes=1 mode=threads
        [Pgbus] boot: capsule=default queues=default,mailers threads=10 processes=1 mode=threads
      LOG
      md <<~'MD'
        It states the version, the connection target (host/dbname only — never the
        password, across the `database_url`, `connection_params`, and
        ActiveRecord-derived connection forms), the resolved pool size, the PGMQ
        schema mode and installed version, LISTEN/NOTIFY status, the roles that
        will actually boot, and one line per worker capsule and event consumer.
      MD
      DocsUI::Callout(:note) do
        plain "Every DB-dependent field degrades to "
        code { "unknown" }
        plain " on a transient failure — the banner can never abort boot."
      end
    end
  end

  def shared_listen
    DocsUI::Section("Shared LISTEN connection", description: "One direct connection per host, not per fork.") do
      md <<~'MD'
        Workers wake instantly on job inserts via a persistent `LISTEN`
        connection. That connection must bypass a transaction-pooling PgBouncer
        (LISTEN does not survive transaction boundaries), so it lands on the
        direct port's scarce `max_connections` budget.

        By default (`worker_notify_scope = :supervisor`) the **supervisor owns
        one shared listener for the whole host**: it LISTENs on the union of
        every capsule's and consumer's queue channels and wakes the right
        fork(s) over a per-fork pipe. A job host pins **exactly one** direct
        LISTEN connection, regardless of how many capsules and consumers it
        runs.
      MD
      DocsUI::Code(<<~'RUBY', lexer: :ruby, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.worker_notify_scope = :supervisor  # default: 1 LISTEN connection per host
          # c.worker_notify_scope = :fork      # previous behavior: 1 per worker/consumer fork
        end
      RUBY
      md <<~'MD'
        Health is fail-safe in both directions: if the shared connection dies,
        the supervisor broadcasts a degraded signal and every fork drops to
        fast polling until the listener reconnects (typically well under a
        second); if the supervisor itself disappears, the pipes reach EOF and
        forks keep processing on plain polling. `pgbus doctor`'s **Connection
        budget** check prints the pinned count for the current config, and the
        connections are census-tagged for capacity audits:
      MD
      DocsUI::Code(<<~'SQL', lexer: :sql)
        SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pgbus-listen';
      SQL
      DocsUI::Callout(:note) do
        plain "Use "
        code { "worker_notify_scope = :fork" }
        plain " to restore the per-fork listeners — for example while bisecting "
        plain "a wake-latency regression, or on single-capsule hosts where the "
        plain "footprint is identical anyway."
      end
    end
  end

  def roles
    DocsUI::Section("Split-role deployments", description: "One role per container.") do
      md <<~'MD'
        By default `pgbus start` boots every role in one supervisor. For
        containerized deployments where each role is its own process, use the
        role flags (mutually exclusive) and, optionally, a single capsule:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        pgbus start --workers-only              # only worker processes
        pgbus start --scheduler-only            # only the recurring-task scheduler
        pgbus start --dispatcher-only           # only the maintenance dispatcher
        pgbus start --workers-only --capsule critical  # one capsule per container
      SHELL
      DocsUI::Callout(:note) do
        plain "The auto-tuned "
        code { "pool_size" }
        plain " follows the role: a "
        code { "--scheduler-only" }
        plain " process opens only the connections it actually needs, not one per configured worker thread."
      end
    end
  end

  def recycling
    DocsUI::Section("Worker recycling", description: "The fix for the memory-bloat problem.") do
      md <<~'MD'
        pgbus workers retire themselves before they leak — the main reliability
        difference from backends that leave workers alive forever. When a limit is
        hit, the worker drains its thread pool, exits, and the supervisor forks a
        fresh process:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.max_jobs_per_worker = 10_000 # restart after 10k jobs
          config.max_memory_mb       = 512    # restart above 512 MB RSS
          config.max_worker_lifetime = 1.hour # restart after an hour
        end
      RUBY
      md <<~'MD'
        RSS is sampled from `/proc/self/statm` on Linux and `ps -o rss` on macOS.

        A capsule can set its own limits. They apply to that capsule's workers only;
        any limit it leaves unset (or `nil`) falls back to the global value:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.max_memory_mb = 512 # every other capsule
          config.capsule :render, queues: %w[render], threads: 1, processes: 4,
                         max_memory_mb: 1_536, max_worker_lifetime: 6.hours
        end
      RUBY
    end
  end

  def processes
    DocsUI::Section("Threads vs processes", description: "For CPU-bound jobs.") do
      md <<~'MD'
        Ruby threads in one process share the GVL. Threads help when jobs wait on
        I/O; they do not help when jobs burn CPU (rendering, reports, PDFs, images),
        where a capsule with `threads: 8` still uses about one core. `processes: N`
        forks N identical workers for one capsule, each with its own GVL:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.capsule :render, queues: %w[render], threads: 1, processes: 4
        end
      RUBY
      md <<~'MD'
        - **I/O-bound jobs:** `threads:` (or `execution_mode: :async`), `processes: 1`.
        - **CPU-bound jobs:** `processes:` up to the cores you can give the capsule, `threads: 1`–`2`.

        All forks read the same queues with `FOR UPDATE SKIP LOCKED`, so uniqueness
        and concurrency controls behave as with one process. Each fork has its own
        restart backoff, liveness pipe and `pgbus_processes` row; a crashed fork
        restarts alone. The boot banner prints `processes=N`, readiness counts every
        fork, and the Processes page labels each row `capsule: render`,
        `process: 2/4`.

        Budget `boot RSS + peak job RSS` per process; the forks share the booted
        app copy-on-write until they write to it. Every process has its own pool,
        so a host holds about `processes × pool_size` connections for the capsule.
        Under the default `worker_notify_scope: :supervisor` the host still holds
        one LISTEN connection; under `:fork` each fork holds its own. With
        `single_active_consumer: true` each queue is locked by one fork at a
        time; a fork can still process the capsule's other queues, so in a
        single-queue capsule the extra forks are hot standbys.
      MD
    end
  end

  def circuit_breaker
    DocsUI::Section("Circuit breaker", description: "Auto-pause a failing queue.") do
      md <<~'MD'
        A queue that fails repeatedly is auto-paused with exponential backoff, so a
        broken dependency doesn't burn the whole fleet retrying. It auto-resumes
        after the backoff and resets; continued failures double the backoff:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.circuit_breaker_enabled = true } # default
      RUBY
      DocsUI::Callout(:note) do
        plain "Pause state lives in "
        code { "pgbus_queue_states" }
        plain " and survives restarts; you can also pause/resume manually from the dashboard. "
        plain "Add the table with "
        code { "rails generate pgbus:add_queue_states" }
        plain "."
      end
    end
  end

  def connection_circuit_breaker
    DocsUI::Section("Client-level circuit breaker (database-down)",
                     description: "A different breaker from the one above — trips on connection failure, not job failure.") do
      md <<~'MD'
        The circuit breaker above (`Pgbus::CircuitBreaker`) is **per-queue** and
        persists its pause state in the database — so it's useless when the
        database itself is down; its `check_paused` rescues and returns `false`,
        tripping nothing. `Pgbus::Client::ConnectionHealth` is a **separate,
        in-memory, process-local** latch owned by `Pgbus::Client` for exactly that
        case: it trips on repeated connection failures, not job failures, and
        needs no database access to operate (it can't — the database is down).
      MD
      DocsUI::Table(
        [ "", [ :code, "Pgbus::CircuitBreaker" ], [ :code, "Client::ConnectionHealth" ] ],
        [
          [ "Scope", "Per queue", "Per client (whole process)" ],
          [ "Trips on", "Job execution failures", [ :md, "Consecutive `PGMQ::Errors::ConnectionError`" ] ],
          [ "State lives in", [ :md, "`pgbus_queue_states` (DB)" ], "In-process memory (Mutex-guarded)" ],
          [ "Survives restart", "Yes", "No — resets on process start" ],
          [ "Purpose", "Isolate a queue whose job code keeps failing", "Stop hammering a database that is down" ]
        ]
      )
      md <<~'MD'
        `ConnectionHealth` trips open after 5 consecutive connection errors across
        *any* operation. Once open, read paths (`read_message`, `read_batch`,
        `read_multi`, `read_grouped*`, `read_with_poll`) fail fast with
        `Pgbus::ConnectionCircuitOpenError` **without checking out a pool
        connection** — no wasted connection attempt, no `ErrorReporter` noise per
        poll. A single half-open probe is admitted after a monotonic backoff (1s
        base, doubling per re-open, capped at 60s); its success closes the breaker,
        its failure re-opens it with a doubled window. Enqueues (`send_message` /
        `send_batch`) are **never** short-circuited — callers must see enqueue
        failures rather than have them silently swallowed.
      MD
      DocsUI::Callout(:note) do
        plain "This costs an outage exactly two log lines total instead of one per "
        plain "worker per poll: a "
        code { "warn" }
        plain " when the breaker opens, an "
        code { "info" }
        plain " when it closes. There is no configuration for this breaker — the "
        plain "thresholds are constants, mirroring "
        code { "Pgbus::CircuitBreaker" }
        plain "."
      end
    end
  end

  def read_timeouts
    DocsUI::Section("Read timeouts (libpq-native)", description: "Bounded reads without Ruby Timeout.") do
      md <<~'MD'
        `config.read_timeout` (default `30` seconds) caps how long a single PGMQ
        read can block. On a **dedicated connection** (`database_url` or
        `connection_params`), pgbus bakes two libpq-native bounds into the
        connection at boot — no Ruby `Timeout.timeout`, which can interrupt
        mid-libpq-call and leave a pooled connection corrupted for the next
        checkout:
      MD
      DocsUI::Table(
        [ "Bound", "How", "Effect" ],
        [
          [ "Server-side", [ :md, "`statement_timeout` (via `options=-c statement_timeout=<ms>`)" ],
            [ :md, "Postgres cleanly cancels an overrunning query → `Pgbus::ReadTimeoutError`" ] ],
          [ "Client-side", [ :md, "`tcp_user_timeout` + `keepalives` (sized `read_timeout + 5s`)" ],
            "A dead/hung socket raises `PG::ConnectionBad` synchronously" ]
        ]
      )
      md <<~'MD'
        The client-side bound only applies on Linux with libpq ≥ 12 (older libpq
        rejects the `tcp_user_timeout` conninfo keyword; non-Linux hosts no-op it),
        detected automatically at connection init — no configuration needed. Ruby
        `Timeout` remains only as a narrow last resort on a dedicated connection
        where libpq can't bound the socket (non-Linux or libpq < 12).
      MD
      DocsUI::Callout(:warning) do
        plain "The "
        code { "Proc" }
        plain "-based shared-AR connection path (`-> { ActiveRecord::Base.connection.raw_connection }`) "
        plain "gets neither bound automatically — pgbus doesn't own that socket. "
        plain "Configure the same libpq timeouts yourself in "
        code { "database.yml" }
        plain ":"
      end
      DocsUI::Code(<<~YAML, filename: "config/database.yml", lexer: :yaml)
        production:
          primary:
            <<: *default
            variables:
              statement_timeout: 30000 # ms — match config.read_timeout
            # tcp_user_timeout / keepalives: set at the connection-string or
            # OS/driver level; ActiveRecord passes libpq options straight through.
      YAML
    end
  end

  def prefetch
    DocsUI::Section("Prefetch flow control") do
      md <<~'MD'
        Cap the number of in-flight (claimed but unfinished) messages per worker to
        keep a burst from overwhelming a slow downstream:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.prefetch_limit = 20 } # nil = unlimited (default)
      RUBY
    end
  end

  def read_ahead
    DocsUI::Section("Read-ahead", description: "For a database on another host.") do
      md <<~'MD'
        By default a worker reads as many messages as it has free threads. Once the
        pool is busy, threads free one at a time, so the worker makes one read round
        trip per job. Next to the database that is free. Across a network, that
        round trip becomes the ceiling. `read_ahead` claims up to N messages beyond
        the free threads and holds them until a thread frees, so one read feeds
        many jobs:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.read_ahead = 12 # 0 = off (default)
          config.capsule :api, queues: %w[api], threads: 12, read_ahead: 12
          config.event_consumers = [{ topics: ["orders.#"], threads: 8, read_ahead: 8 }]
        end
      RUBY
      md <<~'MD'
        Start with `read_ahead` equal to `threads`. Buffered messages stay invisible
        (the visibility heartbeat holds them) until their job starts. On drain,
        recycle, pause or shutdown they go straight back to the queue, with
        `read_ct` already counted. They also count as in flight, so `prefetch_limit`
        still caps the total. The hold needs `visibility_heartbeat` (on by default);
        with it off, a message buffered past `visibility_timeout` can run twice.
      MD
    end
  end

  def async
    DocsUI::Section("Async execution mode (fibers)", description: "For I/O-bound work.") do
      md <<~'MD'
        Workers can run jobs as fibers instead of threads — ideal for I/O-bound
        workloads (HTTP calls, email, LLM APIs) where jobs spend their time waiting.
        Because fibers yield during I/O, many share a handful of connections:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.execution_mode = :async # all workers, or per-capsule:
          config.workers = [
            { queues: %w[webhooks emails], threads: 100, execution_mode: :async },
            { queues: %w[default], threads: 5 } # stays thread-based
          ]
        end
      RUBY
      DocsUI::Callout(:warning) do
        plain "Async needs "
        code { 'gem "async"' }
        plain " and "
        code { "config.active_support.isolation_level = :fiber" }
        plain ". Don't use it for CPU-bound jobs — they block the reactor. Messages stay "
        plain "protected by the visibility timeout regardless of mode."
      end
    end
  end
end
