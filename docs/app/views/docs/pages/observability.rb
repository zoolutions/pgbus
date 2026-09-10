# frozen_string_literal: true

# Seeing what pgbus is doing: error reporting to your APM, structured logs,
# ActiveSupport::Notifications events, the metrics adapter, and health endpoints.
class Views::Docs::Pages::Observability < DocsUI::Page
  title "Observability"
  eyebrow "Operations"

  def lead = "Route errors to your APM, emit metrics, structure your logs, and expose health probes."

  def content
    error_reporting
    instrumentation
    metrics
    dashboard_gauges
    pool_metrics
    appsignal
    logging
    stats_buffering
    health
  end

  private

  def error_reporting
    DocsUI::Section("Error reporting", description: "Route caught exceptions to your APM.") do
      md <<~'MD'
        By default pgbus logs caught exceptions and continues. Push callable
        reporters onto `config.error_reporters` to forward them to Sentry,
        Honeybadger, AppSignal, or anything else. Each reporter receives
        `(exception, context_hash)`:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.error_reporters << ->(ex, ctx) { Sentry.capture_exception(ex, extra: ctx) }
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "Reporters wire into every critical rescue path — job execution, worker "
        plain "fetch/process, dispatcher maintenance, fork failures, circuit-breaker trips, "
        plain "outbox publish. "
        code { "ErrorReporter.report" }
        plain " never raises: a broken reporter can't take down the thread that called it."
      end
    end
  end

  def instrumentation
    DocsUI::Section("Instrumentation events", description: "Everything is an ActiveSupport::Notifications event.") do
      md <<~'MD'
        pgbus emits `ActiveSupport::Notifications` events on every hot path.
        Subscribe directly for a bespoke sink (New Relic, OpenTelemetry, a custom
        aggregator):
      MD
      DocsUI::Code(<<~'RUBY')
        ActiveSupport::Notifications.subscribe(/^pgbus\./) do |name, start, finish, _id, payload|
          duration_ms = (finish - start) * 1_000
          YourApm.record(name, duration_ms, payload)
        end
      RUBY
      md <<~'MD'
        The events, with payload keys documented in `lib/pgbus/instrumentation.rb`:
      MD
      DocsUI::Table(
        [ "Event", "Fires on" ],
        [
          [ [ :code, "pgbus.executor.execute" ], "Every job execution (wraps the run)." ],
          [ [ :code, "pgbus.job_completed" ], "A job finished successfully." ],
          [ [ :code, "pgbus.job_failed" ], "A job raised." ],
          [ [ :code, "pgbus.job_dead_lettered" ], "A job exceeded max_retries." ],
          [ [ :code, "pgbus.event_processed" ], "An event handler ran." ],
          [ [ :code, "pgbus.event_failed" ], "An event handler raised." ],
          [ [ :code, "pgbus.client.send_message" ], "A message was enqueued." ],
          [ [ :code, "pgbus.client.send_batch" ], "A batch was enqueued." ],
          [ [ :code, "pgbus.client.read_batch" ], "A worker read a batch." ],
          [ [ :code, "pgbus.client.pool" ], "Connection pool snapshot, emitted per worker heartbeat." ],
          [ [ :code, "pgbus.stream.broadcast" ], "A stream broadcast fired." ],
          [ [ :code, "pgbus.outbox.publish" ], "An outbox entry was published." ],
          [ [ :code, "pgbus.recurring.enqueue" ], "A recurring task was enqueued." ],
          [ [ :code, "pgbus.worker.recycle" ], "A worker recycled itself." ],
          [ [ :code, "pgbus.consumer.recycle" ], "An event consumer recycled itself." ]
        ]
      )
    end
  end

  def metrics
    DocsUI::Section("Metrics adapter (Prometheus / StatsD)", description: "Off by default; consumes the same events.") do
      md <<~'MD'
        The built-in metrics adapter consumes those events and forwards them to a
        backend — no hand-written subscribers. It's off by default (zero overhead)
        and runs independently of AppSignal:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.metrics_backend = :prometheus # in-process registry, scraped
          # or :statsd (DogStatsD UDP), or a custom Pgbus::Metrics::Backend instance
        end
      RUBY
      md <<~'MD'
        For Prometheus, mount the exporter — a self-contained Rack app:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Metrics::PrometheusExporter.new => "/metrics"
      RUBY
      md <<~'MD'
        Emitted metrics are all `pgbus_`-prefixed with low-cardinality tags:
        `pgbus_queue_job_count`, `pgbus_job_duration_ms`, `pgbus_event_count`,
        `pgbus_messages_sent` / `_read`, `pgbus_stream_broadcast_count`,
        `pgbus_outbox_published`, `pgbus_recurring_enqueued`,
        `pgbus_worker_recycled` (tagged `reason` and `kind` — `worker` or
        `consumer`), and `pgbus_pool_size` / `pgbus_pool_available` (tagged
        `hostname`).
      MD
    end
  end

  def dashboard_gauges
    DocsUI::Section("Dashboard gauges", description: "Point-in-time table state, sampled on scrape.") do
      md <<~'MD'
        The metrics adapter above is event-driven — it counts things as they happen.
        Some numbers are not events but *state*: how many jobs are parked behind a
        concurrency key right now, how long the oldest has waited, how many slots
        are held. Those are sampled instead, and served from the dashboard's own
        data source on two surfaces: the `/pgbus/api/metrics` endpoint (gated by
        `config.metrics_enabled`) and the AppSignal probe.

        | Gauge | Means |
        |---|---|
        | `pgbus_concurrency_blocked_executions` | Jobs parked behind a concurrency key, waiting for a slot |
        | `pgbus_concurrency_blocked_oldest_age_seconds` | How long the longest-waiting parked job has waited |
        | `pgbus_concurrency_slots_held` | Concurrency slots held across all keys |

        They carry no labels. Concurrency keys are per-record — one per order, one
        per sync group — so a per-key label would be an unbounded series. Alert on
        the oldest wait: a backlog that keeps growing means a holder died without
        releasing its slot. The per-key detail is on the dashboard's Locks page and
        in the `pgbus_concurrency` MCP tool.
      MD
    end
  end

  def pool_metrics
    DocsUI::Section("Connection pool metrics", description: "See utilization before you hit a pool timeout.") do
      md <<~'MD'
        `Pgbus::Client#pool_stats` returns `{ size:, available:, pool_timeout: }` —
        pgmq-ruby's live pool counters merged with the configured timeout. It's
        purely observational (rescues internally to `{}`), so reading the pool can
        never break job processing:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.client.pool_stats
        # => { size: 10, available: 3, pool_timeout: 5 }
      RUBY
      md <<~'MD'
        The worker heartbeat emits a `pgbus.client.pool` instrumentation event once
        per beat (never on a per-job hot path) carrying that payload, and the
        AppSignal minutely probe reports `pgbus_pool_size` / `pgbus_pool_available`
        gauges tagged by hostname (the pool is per-process, unlike the cluster-wide
        queue gauges). A pool-timeout error is re-raised with the live pool state
        and an actionable hint — raise `pool_size` or reduce worker threads —
        appended to the message.
      MD
    end
  end

  def appsignal
    DocsUI::Section("AppSignal", description: "Auto-installs when the gem is present.") do
      md <<~'MD'
        Load the `appsignal` gem and pgbus auto-installs a subscriber and a minutely
        probe — background-job transactions for every job and handler, `pgbus_`
        counters and distributions, and gauges for queue depth, oldest-message age,
        DLQ depth, dead tuples, and MVCC horizon. `pgbus_queue_latency` is computed
        from the oldest *claimable* message (visibility timeout elapsed), so a
        queue holding only scheduled or backoff-parked jobs reads 0 — alert on it
        without false positives from one delayed job; the raw
        `pgbus_queue_oldest_message_age_seconds` gauge keeps enqueue-time
        semantics, and `pgbus_queue_oldest_claimable_age_seconds` reports the
        claimable age itself. Four importable dashboards ship
        with the gem — `pgbus dashboard --list` enumerates them and
        `pgbus dashboard <name>` prints import-ready JSON for AppSignal's
        "Import dashboard" dialog. Opt out with `config.appsignal_enabled = false`.
      MD
    end
  end

  def logging
    DocsUI::Section("Structured logging") do
      md <<~'MD'
        Switch to JSON logs for aggregators; the formatter extracts the `[Pgbus]`
        component into its own field and keeps thread-local context under `ctx`:
      MD
      DocsUI::Code(<<~RUBY)
        Pgbus.configure { |c| c.log_format = :json } # or :text (default)
      RUBY
    end
  end

  def stats_buffering
    DocsUI::Section("Stats buffering", description: "Tune the SIGKILL loss window.") do
      md <<~'MD'
        Job stats (for the Insights dashboard) are buffered in memory per worker and
        bulk-inserted rather than written one row per job. A worker flushes when its
        buffer fills to `stats_flush_size` entries, when `stats_flush_interval`
        seconds have elapsed, and immediately on entering drain — a graceful `TERM`
        shutdown or a recycle threshold (`max_jobs`/`max_memory`/`max_lifetime`):
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.stats_flush_size = 500     # flush after 500 buffered stats (default 100)
          c.stats_flush_interval = 2   # ...or every 2 seconds (default 5), whichever first
        end
      RUBY
      DocsUI::Callout(:note) do
        plain "Stats are advisory — dashboard/insights only, never a job payload. A "
        plain "clean stop flushes the buffer, so nothing is lost. Only an un-trappable "
        plain "supervisor "
        code { "SIGKILL" }
        plain " on a stalled worker can lose up to "
        code { "stats_flush_interval" }
        plain " seconds / "
        code { "stats_flush_size" }
        plain " entries accumulated since the last flush. Tune both down on a "
        plain "high-throughput deployment for a tighter loss window, or up to reduce "
        plain "insert frequency."
      end
    end
  end

  def health
    DocsUI::Section("Health endpoints", description: "Liveness and readiness for Kubernetes.") do
      md <<~'MD'
        Pgbus exposes two HTTP probes: `/livez` (is the serving process up?) and
        `/readyz`. Readiness means different things in the two places it is served.
        Mounted in Rails, `/readyz` runs the same cluster-wide `OK` / `DEGRADED` /
        `STALLED` verdict as the MCP `pgbus_health` tool — `DEGRADED` deliberately
        stays ready; only the silent-wedge `STALLED` signal fails readiness. Served
        standalone from the supervisor (`health_port`), `/readyz` is
        **container-local**: did *this* supervisor finish booting, and are all the
        children it forked alive? That is the signal a rolling deploy's health gate
        needs — see [Rolling restarts](/docs/rolling-restarts).
      MD
      DocsUI::Table(
        [ "Path", "Method", "200", "503", "Touches DB" ],
        [
          [ [ :code, "/livez" ], "GET", [ :md, "always (`ok`)" ], "never", "no" ],
          [ [ :md, "`/readyz` (mounted)" ], "GET", [ :md, "verdict `OK` or `DEGRADED`" ],
            [ :md, "verdict `STALLED`, or DB unreachable (`{\"status\":\"ERROR\"}`)" ], "yes" ],
          [ [ :md, "`/readyz` (supervisor)" ], "GET", [ :md, "`OK` — booted, all children live" ],
            [ :md, "`BOOTING`, `DEGRADED` (child down), `DRAINING` (stopping)" ], "no" ]
        ]
      )
      md <<~'MD'
        Unknown paths return `404`; non-`GET` methods return `405`. The `/readyz`
        body is the verdict JSON, so a probe failure is self-describing in the
        pod's event log.
      MD
      md <<~'MD'
        `Pgbus::Web::HealthApp` is a plain Rack app — mount it wherever your web
        pods already serve HTTP. It needs no auth (it exposes only aggregate
        health, never payloads), but keep it on an internal network:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/routes.rb")
        mount Pgbus::Web::HealthApp.new => "/pgbus/health"
      RUBY
      DocsUI::Code(<<~YAML, filename: "kubelet probes (Deployment spec)", lexer: :yaml)
        livenessProbe:
          httpGet: { path: /pgbus/health/livez, port: 3000 }
          periodSeconds: 10
        readinessProbe:
          httpGet: { path: /pgbus/health/readyz, port: 3000 }
          periodSeconds: 10
          failureThreshold: 3
      YAML
      md <<~'MD'
        Worker pods that run only the supervisor (`bin/pgbus`, no Puma) have no
        Rails server to mount into. Set `health_port` and the supervisor serves
        both paths itself over a tiny TCP server (no Rails, no dashboard), so a
        kubelet can probe the process that actually forks and watches workers:
      MD
      DocsUI::Code(<<~RUBY, filename: "config/initializers/pgbus.rb")
        Pgbus.configure do |c|
          c.health_port = 9394       # nil (default) disables the standalone endpoints
          c.health_bind = "0.0.0.0" # default "127.0.0.1"
        end
      RUBY
    end
  end
end
