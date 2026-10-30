# frozen_string_literal: true

# Keeping a busy pgbus install fast: autovacuum tuning for the high-churn queue
# tables, archive retention, and the health metrics that tell you when tuning is
# needed.
class Views::Docs::Pages::PerformanceTuning < DocsUI::Page
  title "Performance & tuning"
  eyebrow "Operations"

  def lead = "Start with the throughput checklist, then tune autovacuum for the high-churn queue tables, size archive retention, and watch the health metrics."

  def content
    throughput_checklist
    autovacuum
    archive
    job_burst_tuning
    multi_core
    remote_database
    batched_completion
    native_statements
    streams_master_hub
    streams_pool_autoscaling
    fanout_throughput
    health_metrics
  end

  private

  PERFORMANCE_MD = "https://github.com/zoolutions/pgbus/blob/main/docs/performance.md"

  def throughput_checklist
    DocsUI::Section("Throughput checklist",
                    description: "Every knob that moves jobs/s, with its measured effect and what it costs.") do
      md <<~'MD'
        More processes first, then fewer and cheaper Postgres statements per job,
        then the knobs for a database on another host, then Ruby CPU, then the
        Postgres side. Each item gives the setting and its default, what it measured in
        `docs/performance.md`, and what it costs. Those are no-op jobs on one
        12-core M-series laptop with Postgres on the same machine; "proxied" adds
        1 ms per round trip through toxiproxy, the shape of a database on another
        host. A real job's own work dilutes every number, so measure your own shape
        with `rake bench:fleet` and `rake bench:worker_profile` before and after.
        Every pgbus option it names is in the [configuration reference](/docs/configuration-reference);
        `RUBY_YJIT_ENABLE`, `config.yjit` and `config.active_job.logger` belong to Ruby and Rails.
      MD
      md <<~'MD'
        1. **More processes.** `processes:` per capsule (default 1).
           *Measured:* locally (YJIT, 12 threads) 7 790–7 986 jobs/s for one process
           and 14 277–17 879 for four; two give 1.5–2.2× and four 1.8–2.6× the same
           round's one, and eight (6-thread workers) run below four. Proxied, four
           run 3.9–4.6× one.
           *Costs:* every process brings its own pgmq pool, ActiveRecord connections
           and memory (peak connections 16 for one 12-thread process, 58–62 for four);
           locally Postgres is what bends.
           [More cores](#more-cores-run-more-processes) ·
           [Threads vs processes](/docs/running-workers#threads-vs-processes).
        2. **Prepared statements.** `native_statements` (default `true`): leave it on.
           *Measured:* median Postgres time per job falls about 70 % at one process
           and 34–42 % at four; median jobs/s +11 % (one process) and +20 % (four)
           with YJIT, unchanged without a JIT.
           *Costs:* behind a transaction-mode PgBouncer it needs PgBouncer 1.21+ with
           `max_prepared_statements`. Turning it off also gives up the check that a
           re-run archive or delete which finds its ids gone may have committed: the
           function path keeps pgmq-ruby's plain reconnect retry.
           [Prepared statements](#prepared-statements-and-connection-poolers).
        3. **Batched completion.** `completion_batching` (default `false`;
           `completion_batch_size` 100), threads only.
           *Measured:* at four local processes jobs/s +17–35 %, Postgres time per job
           0.26–0.34 → 0.06–0.07 ms, statements per job ~1.23 → ~0.4; two processes
           gained nothing. One process: up to a third slower. With `read_ahead`
           across a network: 10–30 % slower.
           *Costs:* a hard crash re-runs the jobs of the current flush, not one job.
           Turn it on for several processes on one Postgres when the fleet bench's
           waits show `LWLock:WALWrite` on top.
           [Batch the archives](#several-processes-on-one-postgres-batch-the-archives).
        4. **Delete instead of archive.** `completed_jobs = :delete` (default `:archive`).
           *Measured:* batched delete at four local processes is the fastest shape
           measured, +11–13 % jobs/s over batched archive by median (19 830 → 22 000
           with YJIT). Unbatched, Postgres time per job falls in five of six rows
           (0.058 → 0.022 ms at one YJIT process) and jobs/s rises 10–35 % in four
           of six rows by median, with overlapping ranges.
           *Costs:* no completed-job history in the `a_` tables (with job stats on,
           Insights still counts every success). `Batch::Sweep` counts a stale batch
           job whose message is in neither the queue nor the DLQ as completed, so a
           purged queue's batch jobs count as completed too. ActiveJob only.
           [Archive retention](#archive-retention).
        5. **Read-ahead, for a database on another host.** `read_ahead = threads`
           (default 0), per capsule and per event consumer too.
           *Measured:* proxied, 1.4–1.9× jobs/s for workers and event consumers;
           locally within noise.
           *Costs:* buffered messages count as in flight against `prefetch_limit`;
           on drain, recycle or shutdown they go back with `read_ct` already
           counted; the hold needs `visibility_heartbeat` (default `true`).
           [Read-ahead](#database-on-another-host-turn-on-read-ahead).
        6. **The pool, sized with read-ahead.** `pool_size` (default `nil`: sized
           from the thread counts).
           *Measured:* with `read_ahead` 12, job threads spent 14–15 % of their time
           waiting to check out a pgmq connection from the default pool (~7 for 12
           threads), up from 2–3 %.
           *Costs:* connections. Budget `resolved_pool_size + streams_pool_size` per
           process against `max_connections`.
           [Job bursts](#job-bursts-raise-threads-and-the-pool-together).
        7. **LISTEN/NOTIFY.** `listen_notify` (default `true`): keep it on; polling
           every `polling_interval` is the fallback.
           *Measured:* not as a throughput lever; a wake-up's p50 latency is
           1.66ms through the supervisor's shared listener.
           *Costs:* one direct LISTEN connection per host under the default
           `worker_notify_scope: :supervisor`.
        8. **YJIT.** `RUBY_YJIT_ENABLE=1`, or `config.yjit` on Rails 7.2+; pgbus never
           turns a JIT on, and `pgbus doctor` reports which one runs.
           *Measured:* ~14–20 % locally (the single-worker profile); nothing
           measurable proxied.
           *Costs:* the memory of YJIT's compiled code (not measured here).
        9. **ActiveJob's per-job log lines.** At `info`, ActiveJob formats and writes
           `Performing …` and `Performed …` for every job. Give ActiveJob its own
           logger at `warn` in `config/environments/production.rb`:
           `config.active_job.logger = ActiveSupport::TaggedLogging.new(ActiveSupport::Logger.new($stdout, level: :warn))`.
           Not `ActiveJob::Base.logger.level = :warn`: by default that logger is
           `Rails.logger`, and the line silences the whole app.
           *Measured:* not measured in `docs/performance.md`.
           *Costs:* no per-job lines in the log; a failing job still logs at `error`,
           with its job tags as long as the logger is wrapped in `TaggedLogging`. The
           new logger writes where you point it, not where `Rails.logger` goes.
        10. **Job stats.** `stats_enabled` (default `true`): keep it on.
            *Measured:* in the mocked executor bench, execute 65.8k / 66.4k i/s and
            execute + stat recording 60.1k / 59.4k i/s (39.9k / 40.5k before #604).
            *Costs:* turning it off empties Insights, which under `completed_jobs =
            :delete` is the only record of a success.
        11. **Already in the default path, no knob:** the enqueue no longer runs a
            `pgbus_batch_executions` UPDATE for a job in no batch ("adapter: plain
            job" 4 579 / 4 563 → 7 401 / 7 983 i/s), and the executor reads
            `enqueued_at` with `Time.new` (+5 % jobs/s without a JIT). Upgrade to get them.
        12. **The Postgres side.** Fillfactor (`rails generate pgbus:tune_fillfactor`),
            [autovacuum](#autovacuum-tuning) (`rails generate pgbus:tune_autovacuum`),
            `archive_retention` (default `7.days`) and the `max_connections` budget.
            *Measured:* not measured as throughput levers; they keep the queue
            tables from bloating as volume grows.
      MD
      md <<~MD
        Not levers ([why, in `docs/performance.md`](#{PERFORMANCE_MD}#native-compilation-rust-c-extensions-and-ractors)):

        - **Ractors:** the host's job classes, ActiveRecord, Zeitwerk and ActiveJob
          are not Ractor-safe, so `perform_now` cannot move into one.
        - **A C or Rust extension:** it reaches only pgbus's own share of on-CPU
          time (35 % in #484), a ≤ 1.5× bound for one process, and makes pgbus a
          compiled gem for every host.
        - **Spinel or Roundhouse:** Spinel runs no RubyGems or C extensions (so no
          `pg`), and Roundhouse turns the whole app into a non-Ruby program pgbus is
          not part of.
      MD
    end
  end

  def multi_core
    DocsUI::Section("More cores: run more processes",
                    description: "One worker process runs about one core of Ruby; processes: N multiplies it.") do
      md <<~'MD'
        Under the GVL a worker process uses about one core however many threads it
        has, so throughput past one core comes from `processes:`. Each process
        brings its own pgmq connection pool and its own ActiveRecord
        connections, so add both up for every process and keep the total under
        `max_connections`. On one queue and one Postgres the measured fleet scales
        almost linearly with 1 ms of simulated network latency per round trip
        (toxiproxy; each process is capped by its reader's round trips), and
        bends at 2–4 processes with Postgres on the same machine: every job's
        archive is its own commit, and every process works on the same hot pages
        of the one queue table. The table
        and the Postgres cost per job are in `docs/performance.md` → "Fleet
        scaling".
      MD
      DocsUI::Callout(:note) do
        plain "Measure your own topology with "
        code { "rake bench:fleet" }
        plain ": it drains one queue with "
        code { "WP_BENCH_PROCESSES" }
        plain " (default 1,2,4) worker processes and reports jobs/s, and Postgres "
        plain "milliseconds per job from pg_stat_statements when it is available "
        plain "(n/a otherwise)."
      end
    end
  end

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

  def batched_completion
    DocsUI::Section("Several processes on one Postgres: batch the archives",
                    description: "One archive_batch per flush instead of one pgmq.archive per job. Opt-in, threads only.") do
      md <<~'MD'
        Every successful job archives its message, and each archive is its own
        commit. With several worker processes on one Postgres those commits queue
        on the WAL flush, and that is where the fleet bends. `completion_batching`
        gives each worker one completer that archives the jobs finishing close
        together with one `archive_batch`. There is no timer: a lone job is
        archived at once, and whatever finishes during a flush goes in the next.
        `completion_batch_size` (default 100) caps the ids per statement.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.completion_batching = true # threads only; rejected with execution_mode :async
        end
      RUBY
      md <<~'MD'
        Measured with four processes on a local Postgres: about 5 jobs per
        statement, Postgres time per job down 4–5×, jobs/s up 17–35 %; two
        processes gained nothing. Turn it on for several processes on one
        Postgres when the fleet bench's waits show `LWLock:WALWrite` on top.
        Leave it off for a single process, where
        it gains nothing and measured up to a third slower, and with `read_ahead`
        on a remote database, where each job thread waits a full round trip for
        its flush (10–30 % slower). The numbers are in `docs/performance.md` →
        "Batched completion".
      MD
      DocsUI::Callout(:warning) do
        plain "It widens the at-least-once window from one job to one flush. On a "
        plain "hard crash (SIGKILL, OOM) the jobs waiting in the current flush run "
        plain "again after "
        code { "visibility_timeout" }
        plain ", and a flush that committed just before the crash leaves its jobs "
        plain "completed but unsignalled (uniqueness lock, batch counter, concurrency "
        plain "slot) until pgbus's reapers and sweeps heal them. On a QUIT the worker "
        plain "stops the completer before it kills the running jobs, so a flush that "
        plain "answers in time resolves every waiter. Shutdown waits for a flush at most "
        code { "immediate_shutdown_grace" }
        plain " (QUIT) or what is left of the 5-second residual (TERM). A flush still "
        plain "wedged after that is abandoned and its jobs run again; if Postgres had "
        plain "committed it and only the reply stalled, those jobs are left completed "
        plain "but unsignalled, as after a crash."
      end
      DocsUI::Callout(:note) do
        plain "Try it on your own shape with "
        code { "WP_BENCH_COMPLETION_BATCHING=1 rake bench:fleet" }
        plain " against the same run without it."
      end
    end
  end

  def native_statements
    DocsUI::Section("Prepared statements and connection poolers",
                    description: "The hot queue operations are planned once per connection.") do
      md <<~'MD'
        pgmq's `read`, `archive`, `delete`, `send` and `send_batch` are plpgsql
        functions that build their SQL and run it with `EXECUTE`, which Postgres
        parses and plans again on every call. pgbus runs the same statements as
        its own SQL in named prepared statements on each pooled connection
        (`config.native_statements`, on by default), so Postgres plans each
        one once per connection. Everything a caller sees is unchanged: return
        values, `read_ct`, `vt`, headers, and the queue's NOTIFY trigger, which
        fires on the same `INSERT`. A queue whose table lacks a column the
        statements use (a pgmq extension older than the `headers` column) keeps
        calling the pgmq functions by itself. The measurements are in
        `docs/performance.md` → "Native statements".
      MD
      DocsUI::Callout(:warning) do
        plain "Behind a transaction-mode PgBouncer, prepared statements need PgBouncer 1.21 or later with "
        code { "max_prepared_statements" }
        plain " set. Without it, the pooler drops them between transactions; a process that sees that "
        plain "switches to sending the same SQL unprepared and logs a warning once; with "
        code { "prepared_statements: false" }
        plain " in database.yml (and no pgbus database_url or connection_params Hash of its own) it sends it "
        plain "unprepared from the start. "
        code { "config.native_statements = false" }
        plain " uses the pgmq functions everywhere. See also "
        code { "connection_guc_mode" }
        plain " for the same pooler."
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
    DocsUI::Section("Archive retention", description: "Archive tables grow unbounded without it, or skip them with completed_jobs :delete.") do
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
      md <<~'MD'
        If nothing reads the completed jobs' archive, skip it. `completed_jobs = :delete`
        finishes a successful job with `pgmq.delete`, the DELETE alone, instead of the
        archive's DELETE + INSERT (`delete_batch` instead of `archive_batch` with
        `completion_batching`). Newly completed jobs add no `a_` rows and no archive
        index entries; rows archived before the switch stay until `archive_retention`
        compacts them. The message is gone once the statement commits either way. You
        lose the completed-job history in the archive; with job stats on (`stats_enabled`
        and the `pgbus_job_stats` table migrated), the Insights page still counts every
        success. One thing the archive did tell apart:
        `Batch::Sweep` counts a stale batch job whose message is in neither the queue nor
        the DLQ as completed, so a purged queue's batch jobs count as completed too.
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |config|
          config.completed_jobs = :delete # default :archive
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "ActiveJob only. EventBus handler queues keep archiving, because event replay "
        plain "reads their archive. Failed jobs stay in their queue for the retry and dead "
        plain "letters still move to the DLQ, under either setting. The numbers are in "
        code { "docs/performance.md" }
        plain " → \"Delete mode\"."
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
