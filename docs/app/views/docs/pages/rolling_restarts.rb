# frozen_string_literal: true

# Zeitwerk resolves this compact reference through the directory-implied
# namespaces (app/views/docs/pages/ → Views::Docs::Pages), so there's no need
# for the 4-level nested-module ceremony.
class Views::Docs::Pages::RollingRestarts < DocsUI::Page
  title "Rolling restarts"
  eyebrow "Operations"

  def lead = "Zero-lost-capacity deploys for the job container: health-gate the new supervisor, drain the old one."

  def content
    how_it_works
    readiness_gate
    probe
    shutdown_budget
    overlap_window
    hard_kill_costs
  end

  private

  def how_it_works
    DocsUI::Section("How a health-gated rolling restart works",
                    description: "Start new, prove healthy, then stop old.") do
      md <<~'MD'
        Orchestrators with per-role health checks — Kamal distributions such as the
        [dash](https://github.com/zoolutions/dash), or any docker-level
        `HEALTHCHECK`-driven deploy — replace a job container in three steps: start
        the new container, poll its health check until it reports healthy, and only
        then `docker stop` the old one. If the new container never goes healthy, the
        old one keeps running.

        Pgbus participates on both ends: the supervisor's standalone `/readyz` is the
        health gate for the *new* container, and the graceful-drain pipeline
        (`drain_timeout` / `shutdown_timeout`) bounds the stop of the *old* one.
      MD
    end
  end

  def readiness_gate
    DocsUI::Section("The readiness gate is container-local",
                    description: "The standalone /readyz answers for THIS supervisor, not the fleet.") do
      md <<~'MD'
        When `health_port` is set, the supervisor's `/readyz` answers from its own
        state — never the database. That distinction matters precisely during a
        rolling deploy: a cluster-wide verdict would let a freshly-booted container
        pass the gate on the strength of the *old* container's still-heartbeating
        workers, and the orchestrator would stop the old container before the new
        one had forked a single child. (The Rails-mounted `Pgbus::Web::HealthApp`
        keeps the cluster-wide verdict — the two probes answer different questions.)

        The body is a snapshot of the supervisor's fork table:
      MD
      DocsUI::Code(<<~'JSON', filename: "GET /readyz", lexer: :json)
        { "status": "OK", "expected": 3, "live": 3 }
      JSON
      DocsUI::Table(
        [ "Status", "HTTP", "Meaning" ],
        [
          [ [ :code, "BOOTING" ], "503",
            "Connection not yet verified, queues not bootstrapped, or children not yet forked." ],
          [ [ :code, "OK" ], "200", "Every child forked at boot is currently alive." ],
          [ [ :code, "DEGRADED" ], "503",
            "A child died and is waiting out crash-restart backoff." ],
          [ [ :code, "DRAINING" ], "503", "A stop signal arrived; the container is leaving." ]
        ]
      )
      DocsUI::Callout(:tip) do
        plain "A crash-looping replacement container never reaches "
        code { "OK" }
        plain " — the deploy gate fails and the orchestrator keeps the old container "
        plain "running. That is the failure mode you want."
      end
      md <<~'MD'
        A clean worker recycle (`max_jobs_per_worker`, `max_memory_mb`,
        `max_worker_lifetime`) never flaps readiness: the snapshot refreshes after
        the reap-and-restart step of each monitor pass, so a recycled worker is
        already replaced by the time the next probe reads it.
      MD
    end
  end

  def probe
    DocsUI::Section("pgbus-health: the HEALTHCHECK probe",
                    description: "A dependency-free probe cheap enough for 1–5s intervals.") do
      md <<~'MD'
        The gem ships a `pgbus-health` executable: plain Ruby and stdlib sockets,
        loading neither Bundler, nor Rails, nor the rest of the gem — so a docker
        `HEALTHCHECK` can run it every few seconds, and it works in images without
        curl. It GETs `127.0.0.1:<port>/readyz` and exits `0` on HTTP 200, `1` on
        anything else (non-200, refused, timeout), `2` on usage errors.
      MD
      DocsUI::Code(<<~'SH', filename: "shell", lexer: :shell)
        pgbus-health --port 9394                     # or PGBUS_HEALTH_PORT=9394 pgbus-health
        pgbus-health --port 9394 --path /livez --timeout 2
      SH
      md <<~'MD'
        Wire it into a Kamal role (`bundle binstubs pgbus` generates
        `bin/pgbus-health`):
      MD
      DocsUI::Code(<<~'YAML', filename: "config/deploy.yml", lexer: :yaml)
        servers:
          job:
            hosts: [...]
            cmd: bin/pgbus start
            healthcheck:
              cmd: bin/pgbus-health --port 9394
              interval: 5s
              start_period: 30s   # cover Rails boot + queue bootstrap
            stop_timeout: 45      # must exceed pgbus shutdown_timeout
        env:
          clear:
            PGBUS_HEALTH_PORT: 9394
      YAML
    end
  end

  def shutdown_budget
    DocsUI::Section("Aligning the shutdown budget",
                    description: "stop_timeout > shutdown_timeout > drain_timeout.") do
      md <<~'MD'
        On `docker stop`, SIGTERM reaches the supervisor and readiness flips to
        `DRAINING`. Children stop claiming new work and finish in-flight jobs for up
        to `drain_timeout` (default 30s). The supervisor then waits
        `shutdown_timeout` — default `drain_timeout + 5` — before SIGKILLing
        stragglers. The orchestrator's stop grace period sits outside both:
      MD
      DocsUI::Code(<<~'TEXT', filename: "budget alignment", lexer: :text)
        orchestrator stop_timeout  >  pgbus shutdown_timeout  >  pgbus drain_timeout
                45s                       35s (derived)                30s
      TEXT
      DocsUI::Callout(:warning) do
        plain "If the orchestrator's stop grace period is shorter than "
        code { "shutdown_timeout" }
        plain ", docker SIGKILLs the whole tree mid-drain and the graceful path "
        plain "never finishes. Raising "
        code { "drain_timeout" }
        plain " raises the derived "
        code { "shutdown_timeout" }
        plain " automatically — raise the orchestrator's stop timeout to match."
      end
      md <<~'MD'
        An explicit `shutdown_timeout` below `drain_timeout` logs a boot warning:
        it guarantees mid-drain kills.
      MD
    end
  end

  def overlap_window
    DocsUI::Section("The overlap window",
                    description: "Two supervisors briefly share the database — by design, safely.") do
      md <<~'MD'
        Between "new container healthy" and "old container stopped", two supervisors
        run against the same database. Nothing double-fires:

        - Queue claims use `FOR UPDATE SKIP LOCKED` — a message goes to exactly one
          worker regardless of how many are reading.
        - `single_active_consumer` queues arbitrate through session-level advisory
          locks, released by Postgres the instant a killed process's connection dies.
        - Two live recurring schedulers dedup on the `(task_key, run_at)` unique
          record — the loser of the insert race skips the occurrence.
        - Dispatcher maintenance is idempotent; two dispatchers just do some
          redundant work.

        "One scheduler per deployment" is a steady-state rule; a deploy window may
        briefly violate it without consequence.

        [Deploy tasks](/docs/deploy-tasks) are the fleet-wide complement of the
        container-local `/readyz` gate: they wait for the overlap window to close,
        so one-time work never runs on old code.
      MD
    end
  end

  def hard_kill_costs
    DocsUI::Section("What a hard kill still costs",
                    description: "At-least-once holds, but read_ct counts deploy kills as failures.") do
      md <<~'MD'
        Jobs killed past the drain window are redelivered after their visibility
        timeout — at-least-once holds. But PGMQ's `read_ct` increments exactly like
        a logical failure, so a long-running job that straddles *repeated* deploy
        kills can be pushed to the dead-letter queue without its code ever raising.
        `zombie_detection` logs exactly this pattern (`read_ct > 1` with no recorded
        failure for the message).

        Keep jobs shorter than `drain_timeout`, or raise it (together with the
        orchestrator's stop timeout) for queues that can't be. For `idempotent!`
        event handlers there is a separate crash-window caveat tracked in
        [pgbus#385](https://github.com/zoolutions/pgbus/issues/385).
      MD
    end
  end
end
