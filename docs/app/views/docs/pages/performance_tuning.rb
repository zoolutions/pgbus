# frozen_string_literal: true

# Keeping a busy pgbus install fast: autovacuum tuning for the high-churn queue
# tables, archive retention, and the health metrics that tell you when tuning is
# needed.
class Views::Docs::Pages::PerformanceTuning < DocsUI::Page
  title "Performance & tuning"
  eyebrow "Operations"

  def lead = "Tune autovacuum for the high-churn queue tables, size archive retention, and watch the health metrics."

  def content
    autovacuum
    archive
    job_burst_tuning
    remote_database
    streams_master_hub
    streams_pool_autoscaling
    fanout_throughput
    health_metrics
  end

  private

  def remote_database
    DocsUI::Section("Database on another host: turn on read-ahead",
                    description: "Under network latency the worker's single reader is the ceiling.") do
      md <<~'MD'
        A worker reads in one loop and runs jobs on its pool. Once the pool is
        busy, threads free one at a time, so each read claims one message: one
        read round trip per job. Add a millisecond of network to every round trip
        and that loop caps a worker at a fixed number of jobs per second, however
        many threads it has. `read_ahead` claims extra messages and holds them
        (kept invisible by the visibility heartbeat) until a thread frees, so one
        read feeds many jobs. Start with `read_ahead` equal to `threads`:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure { |config| config.read_ahead = 12 } # or per capsule / event consumer
      RUBY
      DocsUI::Callout(:note) do
        plain "Measure it on your own topology with "
        code { "rake bench:worker_profile" }
        plain " (set "
        code { "PGBUS_BENCH_PROXY_URL" }
        plain " to a Postgres behind toxiproxy, and "
        code { "WP_BENCH_READ_AHEAD" }
        plain " to the value to try). It drives a real worker and a real event "
        plain "consumer and reports jobs/s plus where each thread's time goes."
      end
    end
  end

  def job_burst_tuning
    DocsUI::Section("Job bursts: raise threads and the pool together",
                    description: "Under a job spike, the DB connection pool is the ceiling — not the thread count.") do
      md <<~'MD'
        When a queue floods, the instinct is to add worker threads. But a job
        holds a database connection only for the brief `read_batch` + `archive`
        round-trip — not for the job body — so a worker's throughput is capped by
        its **connection pool**, not its thread count. Adding threads past the
        pool size just makes them queue on connection checkout: latency climbs,
        throughput doesn't.
      MD
      md <<~'MD'
        So size the two **together**. Raising `threads` alone plateaus at the pool
        size; raising both scales throughput roughly linearly (measured 8× from
        2→16 when the pool matches). The connection pool auto-tunes from the thread
        count by default, so in practice you raise `threads` and let `pool_size`
        follow — but if you pin `pool_size`, keep it ≥ `threads`.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.worker "default", threads: 16   # more concurrency…
          config.pool_size = 20                  # …needs the connections to back it
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "This is the "
        strong { "static headroom" }
        plain " answer, and it's usually the right one: idle pool slots are lazy — "
        plain "they cost nothing until a burst uses them. pgbus deliberately does "
        plain "not autoscale the job pool, because elastic threads on a fixed "
        plain "connection pool can't push past the connection ceiling (measured "
        plain "with "
        code { "rake bench:job_burst" }
        plain "). Watch "
        code { "pgbus_worker_pool_utilization" }
        plain " — sustained near 1 means raise both."
      end
    end
  end

  def fanout_throughput
    DocsUI::Section("Fan-out throughput: raise the writer threads",
                    description: "When broadcasts fan out to many SSE clients, scale the writer pool statically.") do
      md <<~'MD'
        With `streams_writer_threads > 0`, durable broadcast socket writes move off
        the dispatcher into a pool of writer threads (each connection pinned to one
        worker so its frames stay ordered). If you fan out to a large fleet of
        connections, more writer threads flush them in parallel — throughput scales
        roughly linearly with the count.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.streams_writer_threads = 8   # more parallel socket writes
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "This is a "
        strong { "static" }
        plain " knob, and that's deliberate — pgbus does not autoscale the writer "
        plain "pool. A slow or congested client can't stall the fleet: the fan-out "
        plain "write deadline ("
        code { "streams_fanout_write_deadline_ms" }
        plain ", default 250 ms) evicts it, and it reconnects and replays from the "
        plain "durable archive. So the writer pool scales fan-out throughput; it "
        plain "doesn't need to grow to absorb slow clients. Size "
        code { "streams_writer_threads" }
        plain " for your peak fan-out fleet (measured with "
        code { "bench:one[writer_burst_bench]" }
        plain ")."
      end
    end
  end

  def streams_master_hub
    DocsUI::Section("Streams master hub", description: "One streams LISTEN connection per web host.") do
      md <<~'MD'
        Each Puma worker used to open its own dedicated streams `LISTEN`
        connection on first SSE use — one direct connection per worker, on the
        same scarce direct-port budget the job-side supervisor scope protects.
        By default (`streams_listen_scope = :master`) the `pgbus_streams`
        plugin now runs **one shared listener in the Puma master**; workers
        connect to it lazily over a Unix socket and receive every wake —
        including ephemeral broadcast payloads — as framed messages.
      MD
      DocsUI::Code(<<~'RUBY', lexer: :ruby, filename: "config/puma.rb")
        preload_app!            # required for :master — the hub waits for the pgbus initializer
        plugin :pgbus_streams
      RUBY
      md <<~'MD'
        Delivery semantics are unchanged: the synchronous subscribe/ack
        contract crosses the process boundary, durable wakes self-heal via
        `read_after`, and ephemeral wakes are never dropped by the transport.
        The measured cost of the extra hop is noise-level (single-broadcast
        SSE roundtrip p50 16.0ms via the hub vs 16.9ms per-worker on the same
        machine).
      MD
      DocsUI::Callout(:note) do
        plain "Fail-safe in every direction: if the hub is absent or dies "
        plain "(no "
        code { "preload_app!" }
        plain ", single-mode Puma, crash), each worker falls back to its own "
        plain "listener — the connection footprint balloons back to one per "
        plain "worker (visible in "
        code { "pgbus doctor" }
        plain "'s Connection budget and the "
        code { "pgbus-listen" }
        plain " census) but no broadcast is ever lost. Roll back with "
        code { "streams_listen_scope = :process" }
        plain "."
      end
    end
  end

  def streams_pool_autoscaling
    DocsUI::Section("Streams pool autoscaling",
                    description: "Let the SSE streams pool grow into spare connections under a burst, and shrink back when it's over.") do
      md <<~'MD'
        The dedicated streams pool (used for durable-broadcast publish and the
        dispatcher's replay reads) is normally a fixed size — `streams_pool_size`
        (default 5). Under a genuine burst of SSE clients that pool can saturate,
        and a saturated pool **serialises** replay reads (it doesn't error — the
        checkout just waits), so broadcasts fan out more slowly. For steady load,
        the right fix is simply a larger `streams_pool_size`. For **bursty** load,
        opt into autoscaling: a periodic maintenance check grows the pool into a
        fair share of live Postgres connection headroom while it's saturated and
        shrinks it back to `streams_pool_size` when the burst passes.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.streams_pool_autoscale          = true   # opt-in; default false
          config.streams_pool_size               = 5       # baseline + shrink floor
          config.streams_pool_max                = 12      # optional hard per-process cap
          config.streams_pool_autoscale_interval = 300     # check cadence, seconds (default 5 min)
        end
      RUBY
      md <<~'MD'
        There is **no connection-count target to tune**. Every threshold derives
        from live `max_connections`: each check reads `pg_stat_activity`, counts how
        many pgbus stream processes share the database, and grows only into its own
        fair share of the free connections. `streams_pool_max` is an optional hard
        ceiling — leave it `nil` and the dynamic fair share is the cap.
      MD
      DocsUI::Callout(:note) do
        plain "It adds "
        strong { "no extra database connections" }
        plain " and no extra thread: the check is a lightweight "
        code { "pg_stat_activity" }
        plain " query that runs on an existing idle connection every "
        code { "streams_pool_autoscale_interval" }
        plain " seconds — like pghero's periodic stats capture. In a web process "
        plain "serving SSE it rides the streamer's idle LISTEN connection; a "
        plain "background worker that only "
        em { "publishes" }
        plain " broadcasts triggers the same throttled check from the publish path "
        plain "(on the job pool), so pure-publisher processes autoscale too. One "
        plain "grow (or shrink) step per check; a sustained burst converges over a "
        plain "few checks."
      end
      DocsUI::Callout(:tip) do
        plain "It self-protects: if the database runs critically low on free "
        plain "connections, every process immediately shrinks its streams pool back "
        plain "to the baseline — protecting the database wins over keeping a busy "
        plain "pool full."
      end
      DocsUI::Callout(:warning) do
        plain "Autoscaling needs to see each process's own connections, so it must "
        plain "connect to Postgres "
        strong { "directly" }
        plain " — a transaction-pooling PgBouncer strips the "
        code { "application_name" }
        plain " it uses to count peers, so it falls back to assuming it's the only "
        plain "process (still connection-safe, just less precise). It's also a no-op "
        plain "on the shared-ActiveRecord connection path. When in doubt, a larger "
        plain "static "
        code { "streams_pool_size" }
        plain " is always the simpler choice."
      end
      DocsUI::Callout(:warning) do
        plain "If you pin the streamer's LISTEN connection to the direct Postgres "
        plain "port to bypass a transaction-mode pooler ("
        code { "streams_port = 5432" }
        plain "), the streams pool follows it by default — putting "
        code { "streams_pool_size" }
        plain " connections "
        em { "per process" }
        plain " on the direct port's low "
        code { "max_connections" }
        plain " ceiling. Only LISTEN needs the direct port; the pool's traffic is "
        plain "pooler-safe. Route the pool back through the pooler with "
        code { "streams_pool_port = 6432" }
        plain " (or "
        code { "streams_pool_host" }
        plain " / "
        code { "streams_pool_database_url" }
        plain ")."
      end
    end
  end

  def autovacuum
    DocsUI::Section("Autovacuum tuning", description: "Queue tables churn far faster than the Postgres defaults expect.") do
      md <<~'MD'
        PGMQ queue tables see heavy insert/delete churn — a message is inserted,
        read (an UPDATE), and archived (a DELETE) in seconds. PostgreSQL's default
        autovacuum is too conservative for that, so dead tuples accumulate and
        indexes bloat. pgbus applies aggressive per-table tuning automatically:
        new queues get it at creation, the install migration tunes the default
        queue, and `pgbus:update` detects untuned tables.
      MD
      md <<~'MD'
        `db:schema:load` drops `ALTER TABLE` settings, so re-apply after a schema
        load with the generator:
      MD
      DocsUI::Code(<<~SHELL, lexer: :shell)
        rails generate pgbus:tune_autovacuum                  # generate the migration
        rails generate pgbus:tune_autovacuum --database=pgbus # for a separate database
      SHELL
      DocsUI::Callout(:warning) do
        plain "A long-running transaction pins the MVCC horizon and stops vacuum from "
        plain "cleaning any dead tuple created while it's open — the most common cause of "
        plain "queue-table bloat. Watch "
        code { "pgbus_oldest_transaction_age_seconds" }
        plain "."
      end
    end
  end

  def archive
    DocsUI::Section("Archive retention", description: "Archive tables grow unbounded without it.") do
      md <<~'MD'
        Archived (successfully-processed) messages accumulate in the `a_<queue>`
        tables. The dispatcher compacts them hourly; size the window to your volume:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.archive_retention = 3.days # default 7.days; nil disables cleanup
        end
      RUBY
      DocsUI::Callout(:tip) do
        plain "High-volume queues (>100 msg/s) want a shorter window (1–3 days); "
        plain "audit-sensitive queues may need 30+. SSE stream retention is separate — "
        plain "see "
        a(href: "/docs/streams", class: "link") { "Real-time streams" }
        plain "."
      end
    end
  end

  def health_metrics
    DocsUI::Section("The metrics that tell you when to tune") do
      md <<~'MD'
        The dashboard's Queue Health panel and the Prometheus gauges surface the
        signals that predict trouble before it becomes an incident:
      MD
      DocsUI::Table(
        [ "Metric", "Watch for" ],
        [
          [ [ :code, "pgbus_table_dead_tuples" ], "Dead tuples climbing — vacuum isn't keeping up." ],
          [ [ :code, "pgbus_table_bloat_ratio" ], "Dead / (dead + live) rising toward 1." ],
          [ [ :code, "pgbus_table_last_vacuum_age_seconds" ], "Long gaps since the last vacuum." ],
          [ [ :code, "pgbus_oldest_transaction_age_seconds" ], "A long transaction pinning the MVCC horizon." ],
          [ [ :code, "pgbus_worker_pool_utilization" ], "Busy / capacity near 1 — you need more threads." ]
        ]
      )
      DocsUI::Callout(:note) do
        plain "This page covers running-system tuning. The gem's benchmark harness "
        plain "and allocation budgets — for contributors optimizing hot paths — live in "
        plain "the repo's "
        code { "docs/performance.md" }
        plain "."
      end
    end
  end
end
