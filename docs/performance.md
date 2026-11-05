# Performance

Pgbus aims to be fast in the places that run on every job: the enqueue path
(`Client#send_message`), the dequeue path (`Client#read_batch`), and the
execution path (`Executor#execute`). This page documents how those paths are
kept fast, how to measure them yourself, and how performance is part of every
change.

The honest summary: **the gem overhead (serialization, queue routing, stats) is
what we control; the PGMQ round-trip and database I/O dominate end-to-end
latency.** So the unit benchmarks isolate gem overhead with a mocked PGMQ
client, while the integration benchmarks (when available) measure the full
stack. Measure before you optimize; the harness below exists so you never have
to guess.

**Checklist:** every knob that moves jobs/s, in order, with the number below that
measured it and what it costs, is the docs site's
[throughput checklist](https://pgbus.zoolutions.llc/docs/performance-tuning#throughput-checklist).

## The hot paths

| Path | When it runs | Bench |
|------|--------------|-------|
| `Client#send_message` / `#send_batch` | every job enqueue | `client_bench.rb` |
| `Client#read_batch` | every worker poll cycle | `client_bench.rb` |
| `Client#archive_message` | every job completion (a native prepared statement, issue #605) | `worker_profile_bench.rb` (`WP_BENCH_ROLE=fleet`) |
| `Client#read_batch_fair` | every worker poll cycle when `config.fair_share` is set (issue #426) | `fair_read_bench.rb` |
| `Executor#execute` | every job execution (deserialize + dispatch + archive) | `executor_bench.rb` |
| `StatBuffer#flush` | every `stats_flush_size` jobs (default 100) or `stats_flush_interval`, stats on (the default): one multi-row INSERT into `pgbus_job_stats` (issue #622) | `stat_flush_bench.rb` |
| JSON serialization | every enqueue/dequeue (payload encoding) | `serialization_bench.rb` |
| Connection pool checkout | every PGMQ operation under concurrency | `connection_pool_bench.rb` |
| Execution pool dispatch | every job dispatched to thread/async pool | `execution_pool_bench.rb` |
| SSE streaming | every Turbo Stream broadcast | `streams_bench.rb` |
| Streamer at scale | connect storm, fan-out, dispatcher wake, heartbeat tick and RSS at 100–1 000 SSE clients on one web process (issue #620); frame once per wake and the dispatcher profile (issue #621) | `streams_scale_bench.rb`, `streams_frame_bench.rb`, `streams_fanout_bench.rb` |
| Streamer replay read | every durable-stream wake + SSE connect (`Client#read_after`) | `streams_read_pool_bench.rb` |
| Streams-pool hot-swap | elastic resize under load (issue #323) | `pool_swap_bench.rb` |
| Streams-pool autoscaler | per-tick decision cost + grow-under-load (issue #323) | `pool_autoscale_bench.rb` |
| Job-burst limiter gate | is the job pool or the DB pool the burst ceiling? (issue #323 phase 3) | `job_burst_bench.rb` |
| Fan-out writer throughput | does the writer pool scale with thread count? (issue #323 phase 1) | `writer_burst_bench.rb` |
| NOTIFY wake path | every job-insert wake-up (direct listener vs supervisor hub, issue #381) | `notify_wake_bench.rb` |
| NotifyHub failure modes | killed LISTEN backend, wedged fork, FD churn, fan-out cost (issue #381) | `notify_chaos_bench.rb` |
| Streams master-hub hop | broadcast→SSE roundtrip, per-worker vs master hub (issue #382) | `streams_hub_bench.rb` |
| Workflow step (`Workflow::StepExecution#call`) | every workflow step attempt: claim, guards, body, outcome, next row, events (issue #540) | `workflow_step_bench.rb` |

## Measuring

Everything is driven from `rake`:

```bash
rake bench              # the unit benchmark suite (alias for bench:all)
rake bench:all          # serialization, client, executor — saves report to tmp/benchmarks/
rake bench:one[name]    # a single benchmark by name (e.g. client_bench)
rake bench:memory       # detailed memory profiling with allocation breakdown
rake bench:integration  # real PostgreSQL + PGMQ (requires PGBUS_DATABASE_URL)
rake bench:streams      # real Puma + SSE fan-out (requires PGBUS_DATABASE_URL)
rake bench:streams_scale    # 100 / 1 000 SSE clients on one Puma process: connect storm, fan-out p99, wake, heartbeat, RSS (requires PGBUS_DATABASE_URL)
rake bench:one[streams_read_pool_bench]  # streamer replay-read pool (requires PGBUS_DATABASE_URL)
rake bench:execution_modes  # threads vs async DB-connection consumption (requires PGBUS_DATABASE_URL)
rake bench:notify_wake      # NOTIFY wake latency + LISTEN census (requires PGBUS_DATABASE_URL)
rake bench:notify_chaos     # NotifyHub failure-mode measurements (requires PGBUS_DATABASE_URL)
rake bench:streams_hub      # streams master-hub hop cost + census (requires PGBUS_DATABASE_URL)
rake bench:worker_profile   # real Worker: jobs/s + where a job's time goes (requires PGBUS_DATABASE_URL)
rake bench:fleet            # N worker processes on one queue: jobs/s + Postgres ms/job (requires PGBUS_DATABASE_URL)
```

- **Unit benches** (`benchmarks/*_bench.rb`) isolate gem overhead with a mocked
  PGMQ client using [`benchmark-ips`](https://github.com/evanphx/benchmark-ips)
  (throughput) and [`memory_profiler`](https://github.com/SamSaffron/memory_profiler)
  (allocations).
- **Integration benches** (`benchmarks/integration_bench.rb`) hit a real
  PostgreSQL + PGMQ instance for end-to-end latency.
- **Streams benches** (`benchmarks/streams_bench.rb`) boot a real Puma server
  and measure SSE broadcast fan-out.
- **Streamer replay-read bench** (`benchmarks/streams_read_pool_bench.rb`)
  isolates the per-wake `Client#read_after` cost against a real DB, comparing
  a fresh `PG.connect` per call (the pre-#315 `with_raw_connection` behavior) to
  the dedicated streams pool.

### Where a job's time goes (issue #484)

`benchmarks/worker_profile_bench.rb` (`rake bench:worker_profile`) drives a
**real `Pgbus::Process::Worker`** (12 threads, `execution_mode: :threads`)
draining 5 000 no-op ActiveJob jobs from a real PGMQ queue, with stats on. Each
cell is its own subprocess. jobs/s comes from an unprofiled drain; the time
split comes from a second drain under [vernier](https://github.com/jhawthorn/vernier),
which samples every thread and tags each sample as running, idle (GVL released:
blocked in libpq or on a condition variable) or stalled (runnable, waiting for
the GVL). `proxied` puts Postgres behind toxiproxy with **+1 ms per round trip**,
the shape of a database on another host (setup in the bench header).

Measured on an M-series laptop, local PostgreSQL 18, Ruby 3.4.2, at normal
machine load. Baseline is `main`; "after" is the per-job DELETE removal below.
Local rows are the mean of two runs, which repeated within 1 %. Proxied rows
show the range of the two runs instead: the proxy adds real jitter, and the
proxied/YJIT "after" pair spread 27 % (1 324 vs 1 679). Read the proxied rows
as "no measurable change", not as a YJIT ranking. The bench sizes the pgmq pool
from pgbus defaults (`resolved_pool_size` ~7 for these runs) under a 12-thread
worker; the `pool_wait` bucket below stays at 1-3 %, so that shape did not cap
these numbers.

| cell | baseline jobs/s | after jobs/s | Δ | CPU/wall (after) |
|---|---:|---:|---:|---:|
| local / YJIT | 4 569 | 5 823 | **+27 %** | 84–86 % |
| local / no JIT | 3 797 | 5 119 | **+35 %** | 92 % |
| proxied / YJIT | 1 228–1 292 | 1 324–1 679 | within noise | 33–35 % |
| proxied / no JIT | 1 382–1 409 | 1 361–1 412 | within noise | 37–39 % |

Where the time goes (after; "pool" = the 12 job threads, "loop" = the worker
thread that reads batches and hands them to the pool):

| cell | pool idle (no work) | pool DB wait | pool GVL wait | loop DB wait | loop GVL wait | on-CPU: pgbus / driver / Rails / other |
|---|---:|---:|---:|---:|---:|---|
| local / YJIT | 65 % | 7 % | 24 % | 21 % | 46 % | 35 / 24 / 37 / 5 % |
| proxied / YJIT | 63 % | 24 % | 10 % | 68 % | 19 % | 30 / 28 / 36 / 7 % |

What the numbers say:

- **Local: the worker is CPU-bound under the GVL.** CPU/wall sits near one
  core, the loop spends ~half its time waiting for the GVL, and pool threads
  are mostly idle. Per-job Ruby CPU is the ceiling, which is why removing a
  query's *client-side* cost moved jobs/s by a third and why **YJIT is worth
  ~14–20 %** here. "pgbus" in the on-CPU column includes JSON parsing and
  client wrappers attributed to their pgbus caller; pgbus-owned allocation is
  only ~8 of ~70 objects per job.
- **Proxied: the single reader is the ceiling, not per-job round trips.** The
  loop is ~68 % waiting on Postgres while pool threads sit ~63 % idle. The loop
  reads `qty = free slots`, and at steady state slots free one at a time, so it
  degenerates to about one read round trip per job: ~1.3–1.7 k jobs/s per
  worker regardless of thread count. YJIT does nothing measurable here.
  Raising this needs a read-ahead / minimum-batch change to the claim model —
  a separate issue, deliberately not in #484. Done in #486: see "Read-ahead"
  below.
- These are method/system results for **no-op jobs**. A real job's own work
  (its queries, HTTP calls) adds to every row and dilutes every percentage.

**Changes made from these numbers (issue #484):**

- **Dropped the per-job `pgbus_failed_events` DELETE on first delivery.** Every
  successful job ran `DELETE FROM pgbus_failed_events WHERE queue_name = $1 AND
  msg_id = $2` through ActiveRecord. Only `FailedEventRecorder.record!` writes
  that table, and only after a failed attempt, so a job on `read_ct == 1`
  cannot have a row; the executor now clears only on redelivery. Local
  +27 % / +35 % jobs/s above; the single-threaded `rake bench:integration`
  "execute: plain job" cycle (enqueue + read + execute) went 1 904 → 2 074 i/s
  (+9 %) and 153 → 147 objects/cycle.
- **The executor's debug tag string is built only when debug logging is on**
  (9 → 8 pgbus-owned objects per job; a hard budget in
  `spec/pgbus/active_job/executor_allocation_budget_spec.rb`). No throughput
  change is measurable; it is kept because it is free.
- **`pgbus doctor` reports the Ruby JIT** and the supervisor boot banner logs
  `jit=yjit|zjit|none`. pgbus never enables a JIT; Rails does through
  `config.yjit`.

**Enqueue path (issue #603):**

- **Dropped the per-enqueue `pgbus_batch_executions` UPDATE for jobs in no
  batch.** After the PGMQ send, the adapter called `Batch.backfill_execution`
  for every job, which ran `UPDATE pgbus_batch_executions … WHERE job_id = $1`
  through ActiveRecord on every install that ran the batch-executions
  migration: one extra round trip and commit per `perform_later`, N per
  `perform_all_later` group. Execution rows exist only for payloads tagged
  `pgbus_batch_id`, so the backfill now returns before touching the database
  for any other payload. Same-machine `rake bench:integration`, two runs each
  (main, branch, main, branch; load 0.24–0.34× cores, other worktrees'
  benches on the same Postgres): "adapter: plain job" 4 579 / 4 563 →
  7 401 / 7 983 i/s (219 → 125–135 µs); "adapter: until_executed" 2 139 /
  1 923 → 2 572 / 2 681 i/s (it is not a batch member either); "plain
  enqueue" (raw `send_message`, the control) 8 534 / 8 370 → 8 256 / 8 609,
  unchanged. `integration_bench.rb` now bootstraps `pgbus_batch_executions`;
  without it `executions_migrated?` is false and the bench skips the path.
  main's bench file has no such bootstrap, so its "before" runs used the
  table the branch run had already created in the same database.

**Gate verdicts (not done, with the number):**

- *Further executor allocation trims* (guarding instrument payload hashes):
  ~1 object per job each, not measurable in throughput. Not pursued.
- *A prepared statement for `pgmq.archive`*: pool threads have spare capacity
  under latency, so a faster archive cannot raise jobs/s; and on one
  connection `exec_prepared` vs `exec_params` for `SELECT pgmq.archive(…)` was
  "same-ish" within error both locally (~85–135 µs) and proxied (~1.6–2.0 ms),
  with the winner flipping when run order was swapped. `pgmq.archive` is
  plpgsql, so its inner plan is cached either way. Not pursued.

### Read-ahead (issue #486)

`config.read_ahead = N` (also per capsule and per `event_consumers` entry) lets
a worker or event consumer claim up to N messages beyond its free threads.
They wait in a `Process::ClaimBuffer`, kept invisible by a visibility-heartbeat
hold, until a thread frees. The loop feeds buffered claims to free slots, then
reads `free slots + read_ahead - buffered` (capped by `prefetch_limit -
in_flight`), then feeds again. It waits on the wake signal only when a step
moved nothing. The default is 0, which reads exactly the free slots, as before.

`rake bench:worker_profile` now also has a **consumer cell**: a real
`Process::Consumer` draining an event-bus topic queue through a plain (not
`idempotent!`) handler, so the transport is measured and the dedup table is not.
`WP_BENCH_ROLE` picks `worker`, `consumer` or `both`, and
`WP_BENCH_READ_AHEAD` sets the value.

Measured on the same M-series laptop, local PostgreSQL 18, Ruby 3.4.2, 12
threads, 5 000 no-op jobs/events, **+1 ms per round trip** for `proxied`. The
machine was shared and loaded: load average 29–45 on 12 cores, from other
sessions' test suites. Each round therefore ran baseline (`main`'s
worker/consumer with the bench files copied in), `read_ahead` 0 and
`read_ahead` 12 back to back. The rows are the range of rounds 2 and 3 (load
~29–36). Round 1 ran at load ~43; its local cells were depressed across the
board (baseline local worker 1 413 jobs/s), and it is listed separately below.

| cell | baseline jobs/s | read_ahead 0 | read_ahead 12 | Δ (12 vs baseline) |
|---|---:|---:|---:|---:|
| worker / local / YJIT | 4 664–5 952 | 5 446–5 843 | 5 994–6 057 | within noise |
| worker / local / no JIT | 4 342–4 953 | 4 602–4 896 | 4 929–4 965 | within noise |
| worker / proxied / YJIT | 1 323–1 731 | 1 302–1 546 | 2 300–2 462 | **+33 % to +86 %** |
| worker / proxied / no JIT | 1 286–1 680 | 1 384–1 525 | 2 423–2 441 | **+44 % to +90 %** |
| consumer / local / YJIT | 7 800–8 145 | 7 751–7 876 | 7 868–8 013 | within noise |
| consumer / local / no JIT | 6 275–7 066 | 6 780–6 899 | 6 905–6 975 | within noise |
| consumer / proxied / YJIT | 1 359–1 779 | 1 347–1 565 | 2 590–2 599 | **+46 % to +91 %** |
| consumer / proxied / no JIT | 1 326–1 576 | 1 283–1 295 | 2 288–2 300 | **+45 % to +73 %** |

Round 1 (load ~43), proxied only: worker baseline 1 313 / 1 277 (YJIT / no
JIT), `read_ahead` 0 1 271 / 1 108, `read_ahead` 12 **2 382 / 1 121**; consumer
baseline 1 027 / 1 258, `read_ahead` 0 1 292 / 1 001, `read_ahead` 12 1 615 /
2 527. The worker/no-JIT `read_ahead` 12 cell is the one run in 12 where
read-ahead showed no gain. It ran at the highest load of the session.

Where the time goes, proxied / YJIT, round 3 (pool = the 12 job threads, loop
= the reader thread):

| cell | pool idle | pool DB wait | pool checkout wait | loop DB wait |
|---|---:|---:|---:|---:|
| worker, baseline | 62 % | 27 % | 2 % | 73 % |
| worker, read_ahead 12 | 39 % | 33 % | 14 % | 64 % |
| consumer, baseline | 60 % | 28 % | 3 % | 78 % |
| consumer, read_ahead 12 | 32 % | 37 % | 15 % | 76 % |

What the numbers say:

- **Under latency, read-ahead is the lever #484 predicted.** One read now feeds
  several jobs: pool threads that sat idle waiting for work are busy, and jobs/s
  rises 1.4–1.9× for both loops against the same round's baseline (rounds 2–3). This is a system-level
  result for no-op jobs; a real job's own work dilutes it.
- **Locally it changes nothing measurable.** There the worker is CPU-bound
  under the GVL (#484), so a faster reader has nothing to feed. `read_ahead` 0
  overlaps the baseline's run-to-run range in 7 of 8 cells. The exception,
  consumer / proxied / no JIT, sits 2–18 % below (1 283–1 295 vs 1 326–1 576),
  and round 1's worker / proxied / no JIT `read_ahead` 0 cell was 13 % below
  its baseline (1 108 vs 1 277). Under load 29–45 that is plausibly noise, but
  it was not proven to be. `read_ahead` 0 still routes each claim through the
  buffer (one heartbeat hold and release per message).
- **The next ceiling is the connection pool.** With the pool busy, job threads
  now spend 14–15 % of their time waiting to check out a pgmq connection (the
  bench sizes the pool from pgbus defaults, ~7 for 12 threads), up from 2–3 %.
  Raise `pool_size` together with `read_ahead` under latency.
- **Sizing:** start at `read_ahead = threads`. `prefetch_limit` still caps the
  total, because buffered claims count as in flight.

Known limits, documented rather than solved:

- `read_multi` (a multi-queue worker without priority, fair share or group
  mode, and a multi-queue consumer) may claim up to `qty` rows per queue and
  discard those past the `LIMIT`. A discarded row stays invisible until its
  timeout. A larger `qty` discards more. This predates read-ahead; the fix is a
  different read statement.
- A message handed back on drain, recycle, pause or shutdown (`set_vt` 0) has
  already had its `read_ct` incremented, the same cost a crash or stale claim
  pays. With `zombie_detection` on, the next claimant logs it as a zombie
  redelivery (`read_ct > 1`, no failed-event row): up to `read_ahead` such
  warnings per recycle.
- With `visibility_heartbeat = false` there is no hold, so a message buffered
  longer than `visibility_timeout` can be claimed and run by another worker.
  `Configuration#validate!` warns when `read_ahead > 0` meets a disabled
  heartbeat.

### Fleet scaling (issue #602)

Multi-core throughput exists only across processes (`processes: N`, #506):
under the GVL one worker process runs about one core of Ruby (#484). The
**fleet cell** (`WP_BENCH_ROLE=fleet bundle exec rake bench:worker_profile`, or
`rake bench:fleet`) measures that shape: N real `Pgbus::Process::Worker`
subprocesses (12 threads each) drain **one** queue of `5 000 × N` no-op jobs on
one Postgres.

- Each drain child configures pgbus without purging, warms up on a private
  queue (500 jobs, so YJIT is warm as in the worker cell), builds its worker and
  waits at a start barrier. When every child is ready, the parent resets
  `pg_stat_statements` for the bench database and releases them together.
  jobs/s = total / (the last job finished in any child − the parent's start
  signal), all `CLOCK_REALTIME` on one machine. The start-skew column (0.0–0.2
  ms in every run) shows the barrier held.
- A child stops once its worker has nothing in flight and the queue reads empty
  three polls in a row; `Σ processed == total`, no failed job and an empty queue
  are checked after every row.
- **PG ms/job** = Σ `total_exec_time` / jobs and **statements/job** = Σ `calls`
  / jobs, over the top-level `pgmq.*` and `pgbus_*` statements of the bench
  database only (`pg_stat_statements` is cluster-wide; other databases' stats
  are neither read nor reset). The children's `pgmq.metrics` polls are
  excluded. **PG busy backends** = Σ `total_exec_time` / wall: how many
  backends were executing pgbus statements at any moment, on average. The
  parent also samples `pg_stat_activity` every 10 ms for what the bench
  database's active client backends were waiting on.
- Each child sets `pool_size = threads + 2` (the job threads, the loop thread,
  the heartbeat ticker). The worker cell leaves it to pgbus, which sizes it ~7
  under a 12-thread worker because the bench configures no `workers` (#484).
  Each child also caps its ActiveRecord pool at 2 (stats flushes, its process
  row), so a row whose `N × (pool_size + 2) + 10` exceeds the free connections
  (`max_connections` minus the superuser reserve and the client backends
  already connected) is skipped with the reason. Each row prints the peak
  connections it held: 16 for one 12-thread child (the parent's included),
  58–62 for four. `WP_BENCH_PROCESSES` defaults to `1,2,4`. `8` is opt-in:
  8 × 16 + 10 = 138 does not fit a stock `max_connections = 100`. The
  8-process numbers below come from 6-thread workers (8 × 10 + 10 = 90).

Measured 2026-10-29 on an Apple M2 Max (12 cores), PostgreSQL 18.6 on the same
machine (`synchronous_commit = on`), Ruby 3.4.2, `read_ahead` not passed,
`proxied` = **+1 ms per round trip** through toxiproxy. The machine was shared:
load average 5–17 on 12 cores, from other sessions' benches and test suites
against the same Postgres. Local rows are the range of five rounds, proxied
rows of two, 6-thread rows of two. Ranges are wide because of that load. Read
them as the shape of the curve, not as decimals.

| cell | processes | jobs/s | jobs/s per process | PG ms/job | PG busy backends |
|---|---:|---:|---:|---:|---:|
| local / YJIT | 1 | 7 790–7 986 | 7 790–7 986 | 0.111–0.124 | 0.9–1.0 |
| local / YJIT | 2 | 11 636–14 079 | 5 818–7 039 | 0.145–0.203 | 1.8–2.4 |
| local / YJIT | 4 | 14 277–17 879 | 3 569–4 470 | 0.299–0.415 | 5.3–5.9 |
| local / no JIT | 1 | 5 242–6 143 | 5 242–6 143 | 0.096–0.115 | 0.6–0.7 |
| local / no JIT | 2 | 9 052–11 336 | 4 526–5 668 | 0.119–0.230 | 1.3–2.1 |
| local / no JIT | 4 | 12 016–16 149 | 3 004–4 037 | 0.317–0.438 | 5.1–5.4 |
| proxied / YJIT | 1 | 1 923–2 216 | 1 923–2 216 | 0.308–0.391 | 0.7–0.8 |
| proxied / YJIT | 2 | 4 701–4 804 | 2 351–2 402 | 0.268–0.279 | 1.3 |
| proxied / YJIT | 4 | 8 562–8 617 | 2 141–2 154 | 0.358–0.364 | 3.1 |
| proxied / no JIT | 1 | 1 841–1 883 | 1 841–1 883 | 0.359–0.408 | 0.7–0.8 |
| proxied / no JIT | 2 | 4 298–4 525 | 2 149–2 263 | 0.255–0.306 | 1.2–1.3 |
| proxied / no JIT | 4 | 8 290–8 691 | 2 072–2 173 | 0.308–0.344 | 2.7–2.9 |
| local / YJIT, 6 threads | 4 | 12 907–15 462 | 3 227–3 865 | 0.243–0.310 | 3.8–4.0 |
| local / YJIT, 6 threads | 8 | 11 411–14 241 | 1 426–1 780 | 0.735–0.923 | 10.5 |
| local / no JIT, 6 threads | 4 | 12 636–14 087 | 3 159–3 522 | 0.227–0.282 | 3.2–3.6 |
| local / no JIT, 6 threads | 8 | 11 524–11 646 | 1 441–1 456 | 0.833–0.866 | 9.6–10.1 |

Statements per job stay at 1.19–1.26 in every 12-thread row (one
`pgmq.archive` per job, one `pgmq.read` per 4–5 jobs, the batched
`pgbus_job_stats` insert) and 1.37–1.47 with 6 threads (smaller reads). The
statement with the most total time is `pgmq.archive` in every 12-thread YJIT
row and most other rows (`pgmq.read` in the rest). In the local 12-thread rows,
`pgmq.archive` takes 0.06–0.09 ms per call with one process and 0.18–0.25 ms
with four.

What the active backends of the bench database were waiting on (local, YJIT;
share of `pg_stat_activity` samples; the five 12-thread rounds, and the two
6-thread rounds for 8 processes). The output lists the top six wait kinds per
row, so "≤" bounds a kind that missed the top six in some rounds:

| processes | on CPU | `LWLock:WALWrite` (waiting for a WAL flush) | `IO:WalWrite` (doing it) | `LWLock:BufferContent` | `Lock:transactionid` |
|---:|---:|---:|---:|---:|---:|
| 1 | 47–63 % | 11–28 % | 16–21 % | ≤ 2 % | 0 % |
| 4 | 29–41 % | 19–25 % | ≤ 7 % | 9–16 % | ≤ 6 % |
| 8 (6 threads) | 30–36 % | 18–20 % | ≤ 4 % | 11–15 % | 11–16 % |

What the numbers say:

- **Locally, scaling bends at 2–4 processes and turns down past 4.** Against
  the same round's one process, two processes give 1.5–2.2×, four give
  1.8–2.6×, and eight (6-thread workers) run below four. With YJIT the
  fleet's 1-process row sits 6–11 % above the worker cell of the same round
  (7 116–7 405, rounds 2–5); without a JIT the two overlap (−9 % to +17 %
  against 5 246–5 741). Round 1's YJIT worker cell hit a load spike (312
  jobs/s at 7.7 % CPU) and is left out. The one configuration difference is
  `pool_size`, 14 instead of ~7.
- **Postgres is what bends, not Ruby.** Statements per job do not change with
  N, but Postgres time per job roughly triples from one process to four
  (2.4–3.8× against the same round's one process) and grows 4.6–7.5× by
  eight (6-thread workers, against their own one-process row). Each
  statement takes longer, and the waits say why. Every
  `pgmq.archive` is its own transaction, so every job waits for a WAL flush
  (`synchronous_commit = on`), and concurrent commits queue on `WALWrite`.
  Every reader and archiver works on the same few hot pages of the one queue
  table and its indexes (`BufferContent`). At eight processes, row-lock waits
  on other transactions (`transactionid`) appear too. It is the single queue
  table and the per-job commit, not Postgres running out of statements.
- **On this machine Postgres also shares the cores.** At four processes about
  4 cores run Ruby and 5–6 backends are busy in Postgres, on 12 cores already
  loaded by other sessions. Part of the bend is CPU contention a dedicated
  database host would not have. A team with Postgres on its own host should
  rerun the fleet cell there before reading these rows as its ceiling.
- **Under latency the fleet scales almost linearly.** Proxied, four processes
  run 3.9–4.6× one: each process is capped by its single reader's round trips
  (#484, #486), not by Postgres, and Postgres stays at ~3 busy backends. This
  is the production shape for a database on another host. `read_ahead` (#486)
  and `processes:` multiply there.
- **The baseline the #601 levers are measured against**, local, 12 threads:
  ~7 800–8 000 jobs/s for one process and 14 300–17 900 for four (YJIT).
  What raises the ceiling is fewer commits and less contention on the queue
  table per job, not less Ruby per job.
- These are no-op jobs. A real job's own queries add to every row, and its
  run time spreads the archive commits out.

### Native statements (issue #605)

pgmq's `read`, `archive`, `delete`, `send` and `send_batch` are plpgsql
functions that build their statement with `FORMAT(...)` and run it with
`EXECUTE`. plpgsql never caches a dynamic `EXECUTE`'s plan, so every job paid
a parse and a plan for its archive, and every read for its CTE. `Client` now
runs the same statements as its own SQL in named prepared statements on the
pooled connection (`lib/pgbus/client/native_statements.rb`,
`config.native_statements`, default on).

Two findings on the way:

- **`LIMIT $1` keeps a prepared read on custom plans for good.** The generic
  plan assumes the LIMIT fetches a tenth of the table and hash-joins a full
  scan of it, so Postgres re-plans every read (0.07–0.11 ms of planning per
  read at four processes, `pg_stat_statements.track_planning`). Reads of up
  to 64 messages (`LITERAL_READ_LIMIT`; a worker reads at most its free
  threads plus `read_ahead`, so only `threads + read_ahead` above 64 reaches
  the `LIMIT $1` statement) run a statement with the LIMIT written in, one per qty,
  which settles on the cached generic plan after Postgres's five custom
  plans. Each pooled connection pays those five per qty it sees, so a short
  bench still shows some read planning; a long-running worker does not.
  Each prepared statement keeps its plan in the backend for the
  connection's life: per queue, the seven statements plus one read per qty
  a worker asks for (at most `threads + read_ahead`).
- **The fleet cell undercounted planning.** A function's planning of its
  dynamic `EXECUTE` is inside its execution time, while a top-level
  statement's planning is `total_plan_time`, recorded only with
  `track_planning` on. PG ms/job now adds `total_plan_time` (0 when the
  setting is off). Both trees below ran that harness, with
  `ALTER DATABASE … SET pg_stat_statements.track_planning = on`.

`WP_BENCH_ROLE=fleet WP_BENCH_PROCESSES=1,4`, 12 threads, 5 000 jobs per
process, Apple M2 Max (12 cores), PostgreSQL 18.6 on the same machine, Ruby
3.4.2. `main` and the branch alternated, three rounds each, in one session.
Other sessions shared the machine and the Postgres: load average 6.3–8.0 on
12 cores (0.53–0.67×) throughout. Ranges are over the three rounds:

| cell | processes | `main` jobs/s | branch jobs/s | `main` PG ms/job | branch PG ms/job |
|---|---:|---:|---:|---:|---:|
| local / YJIT | 1 | 6 345–6 928 | 7 365–7 976 | 0.137–0.159 | 0.036–0.059 |
| local / YJIT | 4 | 12 892–13 615 | 14 468–16 745 | 0.370–0.441 | 0.206–0.299 |
| local / no JIT | 1 | 6 079–6 514 | 5 885–6 941 | 0.104–0.138 | 0.026–0.095 |
| local / no JIT | 4 | 13 145–14 045 | 11 199–14 129 | 0.279–0.372 | 0.172–0.287 |

An earlier three rounds, with the reads still on `LIMIT $1` and load
0.17–1.03×, agree: PG ms/job 0.107–0.117 → 0.037–0.049 at one process and
0.274–0.326 → 0.188–0.261 at four. Statements per job do not change
(1.20–1.26 on both sides): the same statements, cheaper.

The top statements, YJIT, one round (ms per call, execution + planning):

| statement | 1 process `main` | 1 process branch | 4 processes `main` | 4 processes branch |
|---|---:|---:|---:|---:|
| archive (one per job) | 0.112 | 0.018 | 0.258 | 0.149 |
| read (one per 4–5 jobs) | 0.192 | 0.062 | 0.559 | 0.319 |
| `pgbus_job_stats` insert (batched) | 0.401 | 0.388 | 0.858 | 0.773 |

What the numbers say:

- **Median Postgres time per job falls about 70 % at one process (0.148 →
  0.045 ms with YJIT, 0.123 → 0.036 without) and 34–42 % at four (0.382 →
  0.222, 0.344 → 0.228).** At one process the archive and read were mostly planning. At four
  what remains is what #602 found: one commit per job waiting on the WAL
  flush (`LWLock:WALWrite` is 38–51 % of the sampled waits on `main` and
  43–71 % on the branch, whose backends spend less of their time on CPU) and
  contention on the queue table's hot pages. Native statements take the
  planning out; they do not take the commit out.
- **jobs/s moves less than PG ms/job.** With YJIT the branch's range sits
  wholly above `main`'s in the three rounds above, at one and at four
  processes; in the earlier rounds the four-process ranges overlap
  (15 117–15 690 against 15 622–16 255). Medians of the three rounds above:
  +11 % at one process (6 709 → 7 427) and +20 % at four (13 472 → 16 185).
  Without a JIT the medians do not move (6 416 → 6 339, 13 347 → 13 147):
  there Ruby is the larger share of a job. The load on the machine is part of
  the spread.
- `config.native_statements = false` restores the function calls exactly.

`rake bench:integration` (one process, one connection, real Postgres), two
runs per side, alternated, load 0.34–0.40×:

| row | `main` | branch |
|---|---:|---:|
| plain enqueue (`send_message`) | 8 109 / 8 531 i/s | 9 607 / 9 864 i/s (**+13–22 %**) |
| adapter: plain job | 7 369 / 7 564 i/s | 9 018 / 8 535 i/s (**+13–22 %**) |
| execute: plain job (read + archive) | 1 945 / 1 597 i/s | 2 255 / 2 193 i/s (**+13–41 %**) |
| send_batch(100) | 1 552 / 1 707 i/s | 1 763 / 2 080 i/s |
| acquire + release (no queue statement) | 4 561 / 4 624 i/s | 4 288 / 4 379 i/s (inside the noise) |

`rake bench:fair_read`'s `read_batch` row resets the claimed rows after
every read on a 100 000-message table; its ±18–65 % spread on this machine
hides any difference either way, so it is left out.

### Native compilation, Rust, C extensions and Ractors

In one line: none is a throughput lever. The worker runs the host's Rails app
(nothing standalone to compile, no Ractor-safe ActiveRecord to move). One local
worker is CPU-bound under the GVL, but on CPU it shares with Rails and the driver;
several local processes bend on Postgres work per job; under latency the ceiling
is one read round trip.

Evaluated 2026-10-05 for issue #484; recorded so the question stays answered.

- **The worker is not a standalone program.** The generated binstub requires
  the host's `config/environment`, the supervisor eager-loads the app, and the
  executor runs the *host's* job classes inside `Rails.application.executor`.
  "Compile the pgbus process" means compiling the host Rails app and every gem
  it loads.
- **[Spinel](https://github.com/matz/spinel)** (Matz's AOT compiler, first
  release 2026.09.12) rules that out: no RubyGems or C extensions at runtime
  (so no `pg`), no `eval`, `method_missing`, runtime `send` or computed
  `define_method` — all of which ActiveJob, ActiveRecord and Zeitwerk rely on.
  Its benchmarks are CPU-bound single programs; none waits on a database.
- **[Roundhouse](https://github.com/rubys/roundhouse)** transpiles a Rails
  *application* to another runtime. Its [published bench](https://rubys.github.io/roundhouse/bench/)
  is SQLite-only and HTTP-only, with no ActiveJob. An app that went through it
  would no longer run Ruby, so pgbus-the-gem would not be involved.
- **Compiling only the fetch loop** would hand each message back to CRuby for
  `perform_now`, adding an IPC hop per job rather than removing a round trip.
- **A Rust extension inside the gem** could only speed up pgbus-owned Ruby CPU
  (libpq already releases the GVL, `JSON.parse` is already C, ActiveJob is
  Rails code) and would make pgbus a compiled gem for every host. A standalone
  Rust worker that shares pgbus's database contract, for apps that do not run
  Ruby, is tracked separately in #483.
- The table above is the reason none of this is the lever: locally the CPU that
  a compiler could speed up is shared with Rails and the driver, and under real
  network latency the ceiling is a read round trip, which no compiler removes.

Ractors and a C extension, evaluated 2026-10-29 for issue #602:

- **Ractors.** The host's job classes run inside `Rails.application.executor`,
  and ActiveRecord, Zeitwerk and ActiveJob are not Ractor-safe, so
  `perform_now` cannot move into a Ractor. Decoding messages in a Ractor would
  copy every message across the boundary (a non-shareable object sent with
  `Ractor#send` is deep-copied) to parallelize less than the ~24 % driver share of on-CPU time (#484).
- **A C extension inside the gem.** The same reasoning as the Rust row: only
  the pgbus-owned share of on-CPU time (35 % in #484) is reachable. That is a
  ≤ 1.5× theoretical bound for one process, and it would make pgbus a compiled
  gem for every host. The fleet multiplies processes instead. On the measured
  shape (Postgres on the same machine) the fleet bends on Postgres work per job
  (WAL flushes and the one queue table) and on cores shared with Postgres; a
  Ruby-side saving would only hand those cores back, and does nothing for the
  per-job commit.

Campfire on Ractors, evaluated 2026-11-02 for #617:
[tobi/campfire-once-ruby-ractor](https://github.com/tobi/campfire-once-ruby-ractor)
serves a Rails app's pages up to 27× faster than its Rails version (the room page; less on other routes) by running Falcon in worker
Ractors over plain modules and SQLite (through a vendored Extralite), with
Rails, Rack and ActiveRecord gone. It got there by deleting the framework, not
by shimming it: `rails_compat/` holds wire formats only (cookies,
`MessageEncryptor`, GlobalID, CSRF), and its `Jobs` module sends frozen
`[kind, *args]` arrays to in-process job Ractors with no retries and no
persistence, so a crash loses every queued job, the opposite of pgbus's
contract. A pgbus worker exists to run the host's ActiveJob classes, so it
cannot drop Rails, and Ractors are not where campfire's fan-out speed comes
from either: its own cable fan-out at 1 000 clients is 117 deliveries/s. What
transfers is discipline, measured step by step in #617: the Rails share of a
job as a measured bucket ("The Rails share of a job" below), a commit that does
not wait on the WAL flush for the completion statement only (#619), and SSE
frames serialized once and fanned out as frozen bytes (#620, #621).

The Ractor fan-out gate, evaluated 2026-11-02 for #617 step 5 after #621: shut.
At 1 000 subscribers the dispatcher's wake is almost entirely the socket-write
loop (`IO#write_nonblock` 74–81 % of its running samples, framing 0.1–0.2 %),
but the wake is under half the last client's p99 in every cell (6–20 ms against
14–30 ms), so the write loop is not the fan-out ceiling. Moving it into Ractors
(sockets re-opened by fd, frozen frames by reference, a Ractor-local registry,
Ruby 4.0 only) or turning `streams_writer_threads` on by default could save at
most the wake's share of the time to the last client. The numbers are in "Frame
once per wake (issue #621)" below; the question reopens if a later bench shows
the wake above half the p99.

### Workflow steps (issue #540)

Every workflow step is one `Pgbus::Workflow::StepJob`, and its `perform` is one
`StepExecution#call`: the claim under the run's row lock, the guards
(`cancel_if`, `skip_if`), the body, the outcome, the next step row and the
audit events. `rake bench:one[workflow_step_bench]` runs it with the three
models replaced by in-memory fakes, so it measures pgbus's own cost per step,
not the database round trips (claim, finish, next-row insert, events) that
dominate a real step. A new hot path, so **measured after only** (local arm64,
Ruby 3.4.2 + YJIT):

| | Per step |
|---|---|
| Throughput | 81.0k i/s (12.3 µs) |
| Allocated | ~85 objects, ~9.7 KB |
| Retained | 0 objects (4 across 1 000 steps: one-time memos) |

Method-level, not system-level: a step that does nothing still pays several
round trips and the StepJob's own enqueue and execution, so this is the part
pgbus adds on top of those, not a step rate.

Waiting steps (issue #541) add a signal gate before a `wait_for:` step's body
and a signal settle after every attempt's outcome. A step that does not wait
skips the gate, and its settle returns before any query (it holds no signal);
a `wait_for:` attempt that finds no signal parks at the gate and never reaches
the settle. Same machine,
same (fixed) bench, two runs each: `main` 82.3k / 81.4k i/s, the #541 branch
81.7k / 81.2k i/s (inside the run-to-run noise); 85 objects / 9 744 bytes per
step and 0 retained on both. The bench's fakes now answer
`WorkflowEntry.connection` (`Runner.locked` asks it whether an app transaction
is open); without it the bench raised `ConnectionNotDefined` on `main`.

`Executor#execute` exposes the running message to the StepJob
(`Pgbus::ActiveJob::Delivery`, for crash detection on redelivery). Same
machine, `rake bench:one[executor_bench]`: `main` 50.7k / 51.3k i/s, this branch
58.9k i/s (inside the ±16% run-to-run noise); 69 objects / 6 608 bytes per job
on both; pgbus-owned objects per job 9 on both (`executor_allocation_budget_spec.rb`).
The three fiber-locals it sets allocate nothing; an earlier Hash-per-job
version cost one object and tripped the budget.

### Uniqueness release by holder (issue #564)

`Executor#execute` now sets two more delivery fiber-locals (`enqueued_at`,
`job_id`) and releases an `:until_executed` / `:while_executing` key with
`UniquenessKey.release_held_by!` instead of the key-only `release!`. Same
machine, the same fixed harness in both trees, three interleaved runs:
`main` 58.0k / 46.0k / 43.4k i/s, this branch 57.5k / 45.7k / 44.8k i/s
(inside the ±9–26 % run-to-run noise); 69 objects / 6 608 bytes per job and 0
retained on both. The bench job has no uniqueness key, so the release is not
in that number; it is a database round trip either way, and both DELETEs
plan as one `idx_pgbus_uniqueness_keys_key` index scan, the new one with a
filter on the single matching row.

The harness was wrong before this: its message was a `Struct` without
`enqueued_at`, so once the executor read that field every iteration raised
`NoMethodError` before `perform` and the bench timed the failure path (it
reported a 28 % "speed-up" and 6 fewer objects). On `main` the same `Struct`
made the stats-enabled run skip recording, since `compute_enqueue_latency`
raised inside `record_stat`'s rescue. Both benches now build a real
`PGMQ::Message` (`BenchStubs.build_message`) and abort unless the job returns
`:success` (`BenchStubs.expect_success!`).

### Executor enqueued_at decode (issue #604)

With stats on (the default) every job computes `enqueue_latency_ms` from the
message's `enqueued_at`, which pgmq-ruby hands over as PostgreSQL's TIMESTAMPTZ
text (`2026-10-29 10:11:12.123456+00`: it sets no result type map). The
executor parsed it with `Time.parse`. It now reads it with core Ruby's
`Time.new(String)`, which does the work in C. One call each, same machine
(Ruby 3.4.2 + YJIT):

| reader | per call |
|---|---:|
| `Time.parse` (before) | 6.9 µs |
| `PG::TextDecoder::TimestampWithTimeZone#decode` | 2.9 µs |
| `Time.new(str)` (after) | 0.26 µs |

The pg decoder was the plan. Profiled in the real worker it still took 4.9 % of
on-CPU time, so it was dropped for `Time.new`. A string without an offset gets a
`Z` before `Time.new`. That case only arises for input that does not come from
PostgreSQL, and it is checked with `match?` rather than `in: "UTC"`, because the
keyword costs a Hash per call. The second change: `VisibilityHeartbeat` builds
each entry's `[queue, msg_id]` key once, where `#register`, `#unregister` and
`#registered?` each used to build their own. The allocation budget spec's
message used to carry `enqueued_at: nil`, so it never measured the latency
read. It now carries the String PostgreSQL sends and checks the latency was
recorded. With that message `main` measures 10 pgbus-owned objects per job and
fails the `< 10` gate; this branch measures 9. The one object saved is the
heartbeat key: `Time.parse` and `Time.new` each cost one pgbus-owned object (the
Time), and `Time.parse`'s ~39 other objects per call land in the standard
library, which the gate does not count. What keeps `Time.parse` off this path is
the executor spec's `expect(Time).not_to have_received(:parse)`.

`rake bench:one[executor_bench]` (mocked client), same machine, two runs each,
interleaved:

| | `main` | this branch |
|---|---:|---:|
| execute | 63.2k / 61.3k i/s | 65.8k / 66.4k i/s (inside the noise) |
| execute + stat recording | 39.9k / 40.5k i/s | 60.1k / 59.4k i/s (**+48 %**) |
| objects per job (stats off) | 69 | 68 |

`rake bench:worker_profile`, local worker cells (`WP_BENCH_ROLE=worker`,
`WP_BENCH_PROFILE_JOBS=10000`). Each run started once no other bench process was
running and Postgres had no other active client backend. Load was 0.4–0.6× the
12 cores. Three clean runs per side, interleaved:

| cell | `main` jobs/s | branch jobs/s | `main` pgbus on-CPU | branch pgbus on-CPU |
|---|---:|---:|---:|---:|
| local / YJIT | 6 831–7 283 | 6 631–7 297 | 34.0–36.5 % | 25.6–27.4 % |
| local / no JIT | 5 702–5 758 | 6 003–6 061 (**+5 %**) | 33.0–35.6 % | 27.4–28.6 % |

pgbus's share of on-CPU time fell by about 9 points with YJIT and about 6
points without it. Without a
JIT that shows up as jobs/s, outside the run-to-run spread. With YJIT the
jobs/s change is inside the noise: the loop thread spends 54–76 % of its time
waiting on Postgres in those runs, and that wait sets the rate. Two more runs
landed in slow windows, one per side (`main` 5 251, branch 5 226 jobs/s with
YJIT, loop DB wait 73–75 %). They are left out of the table, as is one branch
run taken while another worktree's bench was hitting the same Postgres.

pgbus frames at 2 % or more of on-CPU time, from the 10 000-job profiles
(`main` / branch ranges; "self" counts stdlib frames under a pgbus caller as
that caller's, as the harness does):

| frame | `main` | branch | verdict |
|---|---:|---:|---|
| `Executor#parse_enqueue_latency_from_string` (`Time.parse`) | 12.0–14.1 % | gone | replaced; `compute_enqueue_latency` is now 1.3–1.8 % inclusive |
| `ExecutionPools::ThreadPool#post` | 3.6–4.2 % | 4.1–6.2 % | concurrent-ruby's `Thread::Queue#push` waking a pool thread. Counted as pgbus because the harness's patterns do not name `concurrent-ruby`. Not removable |
| `Executor#execute` (self) | 3.0–3.7 % | 3.7–4.7 % | mostly `JSON.parse` of the payload. Needed |
| `VisibilityHeartbeat.unregister` | 1.7–2.3 % | 1.2–1.5 % | the key built per call is gone. What remains is the `Hash#delete` under the mutex, which is the heartbeat's job |
| `Client#with_read_timeout` | 1.1–1.6 % | 1.5–2.2 % | Ruby's `Timeout` fallback. It runs only where libpq cannot bound the socket (this macOS machine). On Linux the read takes the libpq path. Kept |

The plan's other candidates do not make the 2 % line. The
`Instrumentation.instrument` wrapper's own time is ≤ 0.35 % (its 51–60 %
inclusive is the job running inside it). `Concurrency.extract_key` and
`Uniqueness.extract_key` are ≤ 0.2 % each. The read wrappers' own time is
below 1 %. All three stay, per #484's finding that trims below 1 % do not
show up in throughput.

### Batched completion (issue #606)

`config.completion_batching = true` (opt-in, default off) gives each worker a
`Process::Completer`: the jobs that finish close together are archived with one
`pgmq.archive(queue, bigint[])` per physical queue instead of one
`pgmq.archive` each. There is no timer. An idle completer archives a lone job at
once; whatever finishes while a flush is in flight goes in the next one, so a
batch is "what finished during the previous round trip".
`completion_batch_size` (default 100) only caps the ids per statement. The job
thread waits for its own result, so the duplicate guard and the post-archive
signals run on it as before. Threads only.

`WP_BENCH_COMPLETION_BATCHING=1` runs every cell of `rake bench:worker_profile`
with it on (fleet drain children included). Measured 2026-10-29 on the machine
of "Fleet scaling" (Apple M2 Max, 12 cores, PostgreSQL 18.6 on the same machine,
Ruby 3.4.2, 12 threads, `proxied` = +1 ms per round trip through toxiproxy),
off and on interleaved. The fleet rows are two rounds; the 1-process rows add
three more (five in all). Load during the fleet rounds was 0.39–0.61× the 12
cores; #605's session shared the machine and the Postgres.

Fleet cell (`WP_BENCH_ROLE=fleet`, 5 000 jobs per process, `read_ahead` not
passed), off → on:

| cell | processes | jobs/s | PG ms/job | statements/job | peak connections |
|---|---:|---|---|---|---|
| local / YJIT | 1 | 6 839–7 895 → 5 974–7 480 | 0.107–0.124 → 0.048–0.062 | 1.23–1.24 → 0.38–0.41 | 16 → 6 |
| local / YJIT | 2 | 10 739–12 140 → 11 898–12 391 | 0.137–0.152 → 0.051–0.056 | 1.23 → 0.40–0.41 | 30 → 10 |
| local / YJIT | 4 | 14 196–15 598 → **18 572–19 161** | 0.288–0.343 → **0.065–0.071** | 1.21–1.22 → 0.38 | 59 → 19 |
| local / no JIT | 1 | 6 000–6 415 → **4 258–6 085** | 0.102–0.136 → 0.047–0.086 | 1.22–1.26 → 0.37–0.39 | 16 → 6 |
| local / no JIT | 2 | 10 497–10 702 → 9 928–10 161 | 0.126–0.128 → 0.051–0.066 | 1.25 → 0.37–0.39 | 32 → 10 |
| local / no JIT | 4 | 14 279–14 330 → **16 780–17 090** | 0.258–0.277 → **0.060–0.064** | 1.23 → 0.38–0.39 | 62 → 18 |
| proxied / YJIT | 1 | 1 596–2 000 → 1 522–1 769 | 0.240–0.507 → 0.121–0.211 | 1.22–1.23 → 0.39–0.41 | 16 → 7 |
| proxied / YJIT | 2 | 3 855–4 042 → 3 851–3 932 | 0.274–0.314 → 0.127–0.164 | 1.23 → 0.37–0.40 | 32 → 10 |
| proxied / YJIT | 4 | 6 539–8 775 → 7 956–8 762 | 0.199–0.277 → 0.099–0.110 | 1.22 → 0.39–0.40 | 60 → 21 |
| proxied / no JIT | 1 | 1 539–2 201 → 1 387–1 914 | 0.213–0.454 → 0.100–0.214 | 1.22–1.23 → 0.39–0.45 | 16 → 7 |
| proxied / no JIT | 2 | 3 928–4 396 → 3 645–4 352 | 0.254–0.266 → 0.090–0.170 | 1.23 → 0.39–0.40 | 31 → 11 |
| proxied / no JIT | 4 | 8 330–8 913 → 8 651–8 981 | 0.191–0.192 → 0.090–0.096 | 1.23 → 0.40–0.42 | 58 → 19 |

Peak connections are round 2's (the bench database's client backends, the
parent's included). Every on row averages 4.9–5.5 ids per `archive_batch`, close
to the jobs one `pgmq.read` claims, at 0.09–0.17 ms per call locally. At four
local processes (YJIT, round 2) the backends sampled busy fell from 12.9 to 3.6,
and their top wait from `LWLock:WALWrite` 54 % to 37 % (on CPU 19 % → 39 %).

Worker cell (`WP_BENCH_ROLE=worker`, one process, pool sized by pgbus defaults,
~7 for 12 threads), two rounds each, load 0.38–1.54× (round 2's off run ended
in a spike from another session):

| cell | jobs/s | pool DB wait | pool checkout wait | pool idle |
|---|---|---|---|---|
| local / YJIT | 6 350–6 724 → 6 478–6 771 | 12.0–13.9 % → 0.3–0.4 % | 0.4–0.6 % → 0.0 % | 69 % → 86 % |
| local / no JIT | 4 919–5 975 → 5 188–5 591 | 8.9–9.3 % → 0.4 % | 0.3 % → 0.0 % | 65 % → 82–83 % |
| proxied / YJIT | 1 395–1 577 → 1 394–1 701 | 27.3–30.7 % → 0.4–0.5 % | 1.5–4.5 % → 0.0 % | 60–63 % → 90 % |
| proxied / no JIT | 1 337–1 697 → 1 341–1 688 | 25.3–27.7 % → 0.4–0.5 % | 1.5–2.4 % → 0.0 % | 63–64 % → 87–91 % |
| proxied / YJIT, `read_ahead` 12 | 2 340–2 524 → **1 877–2 108** | 32.8–34.4 % → 0.6–0.7 % | 8.6–11.9 % → 0.0 % | 42–44 % → 83–85 % |
| proxied / no JIT, `read_ahead` 12 | 2 469–2 608 → **1 717–2 072** | 31.2–32.0 % → 0.6–0.7 % | 7.3–8.8 % → 0.0 % | 40–41 % → 83 % |
| local / YJIT, `read_ahead` 12 | 7 024–7 176 → 7 333–7 674 | 14.9–15.3 % → 0.4 % | 4.6 % → 0.0 % | 50 % → 80–83 % |
| local / no JIT, `read_ahead` 12 | 5 993–6 138 → 6 394–6 834 | 11.2–11.9 % → 0.4 % | 3.2 % → 0.0 % | 48–49 % → 76–79 % |

The `read_ahead` 12 rounds ran at 0.61–1.24× load (round 1 started at 1.24×).

What the numbers say:

- **Statements per job fall from ~1.23 to ~0.4 in every row**: one
  `archive_batch` per ~5 jobs replaces one `pgmq.archive` per job. What is left is
  the `pgmq.read` (one per ~5 jobs) and the batched `pgbus_job_stats` insert.
- **With several processes on one local Postgres it is the lever "Fleet
  scaling" asked for.** Postgres time per job falls 4–5× at four processes
  (0.26–0.34 → 0.06–0.07 ms), the busy backends from 3.7–4.9 to 1.0–1.3, and jobs/s rises
  17–35 % over the same round's off row. Fewer commits per job is what moves it:
  the WAL flush queue was the bend.
- **One process on a local Postgres gains nothing and can lose up to a third.**
  Round by round, the no-JIT 1-process rows run +1 % to −33 % against the same
  round's off row (4 258–6 085 against 6 000–6 415 in five rounds), the YJIT rows
  +2 % to −19 %. Locally Postgres is not one process's bottleneck, so there is
  nothing to save, and every completion now costs two thread wakeups under the
  GVL (the job thread hands its id to the completer, the completer wakes it back).
- **Under latency without `read_ahead` it is inside the run-to-run spread** at
  1, 2 and 4 processes, while Postgres time per job halves: it frees database
  capacity, not worker throughput.
- **With `read_ahead` across a network it costs 10–30 %** round by round
  (locally it is +4 % to +14 %). The plan predicted
  the pool checkout wait (8–12 % here, 14–15 % in "Read-ahead") would fall: it
  falls to 0 %, but the wait moves onto the completer and shows up as pool idle
  (83–85 %). Read-ahead keeps every thread fed, so job threads are what limits
  the rate there, and each one now waits for a flush that is a full round trip
  and runs one at a time. Leave batching off for that shape.
- **When to turn it on:** several processes (`processes: N`) on one Postgres
  where the fleet cell's waits show `LWLock:WALWrite` on top. Here that was four
  processes; two gained nothing (local / no JIT 10 497–10 702 → 9 928–10 161). Leave it
  off for a single process and with `read_ahead` on a remote database. Measure
  your own shape with `WP_BENCH_COMPLETION_BATCHING=1`.
- **Reliability.** A hard crash (SIGKILL, OOM) loses the archives of the jobs
  waiting in the current flush; they re-run after `visibility_timeout`, as a job
  killed between `perform` and its archive does without batching. A flush that
  committed just before the crash leaves its jobs completed but unsignalled
  (uniqueness lock, batch counter, concurrency slot) until the uniqueness reaper,
  the stalled-batch sweep and the semaphore expiry heal them. Both windows grow
  from one job to one flush. `Worker#immediate_shutdown` stops the completer
  before it kills the pool, so a flush that answers in time resolves every
  waiter. Shutdown waits for a flush in flight at most `immediate_shutdown_grace`
  (QUIT) or what the pool wait left of its 5 s residual (TERM,
  `POOL_TERMINATION_WAIT`, so shutdown stays inside the supervisor's default
  `drain_timeout + 5`). A flush wedged past that is killed, and its jobs fail
  with `Completer::Stopped` and re-run after `visibility_timeout`; if Postgres had
  committed it and only the reply stalled, they are left completed but
  unsignalled, the crash's second window.

### Delete mode (issue #607)

`config.completed_jobs = :delete` (opt-in, default `:archive`) finishes a
successful job with `pgmq.delete` (pgbus's native `DELETE … RETURNING msg_id`)
instead of the archive's `DELETE … RETURNING` + `INSERT INTO a_<queue>`; with
`completion_batching` the completer flushes with `delete_batch`. Newly completed
jobs add no `a_` rows and no `archived_at` index entries, and leave no
completed-job history in the archive; rows archived before the switch stay until
the hourly compaction trims them. ActiveJob only; failures and dead letters are
unchanged.

`WP_BENCH_COMPLETED_JOBS=archive|delete` sets it in every cell of
`rake bench:worker_profile` (fleet drain children included). Measured
2026-10-30 on the machine of "Fleet scaling" (Apple M2 Max, 12 cores,
PostgreSQL 18.6 on the same machine, Ruby 3.4.2, 12 threads, local only),
`WP_BENCH_ROLE=fleet`, 5 000 jobs per process, `read_ahead` not passed. Four
variants interleaved per round (archive, delete, batched archive, batched
delete); three rounds at load 0.6–1.1× the 12 cores. Two more rounds ran under
an unrelated load spike (2.7–4.4×) and are left out; their medians point the
same way. Medians, with the three-round range:

| cell | processes | jobs/s archive → delete | PG ms/job archive → delete | statements/job |
|---|---:|---|---|---|
| local / YJIT | 1 | 7 037 (5 792–7 487) → 7 986 (7 219–8 259) | 0.058 → 0.022 | 1.19–1.24 → 1.21–1.24 |
| local / YJIT | 2 | 10 430 (9 245–10 714) → 14 067 (5 085–14 107) | 0.122 → 0.034 | 1.19–1.20 → 1.21–1.23 |
| local / YJIT | 4 | 15 056 (9 519–15 819) → 13 127 (6 970–17 645) | 0.207 → 0.264 | 1.19–1.22 → 1.18–1.21 |
| local / no JIT | 1 | 5 929 (5 811–5 999) → 6 620 (6 120–6 712) | 0.027 → 0.019 | 1.25 → 1.21–1.26 |
| local / no JIT | 2 | 10 787 (10 126–10 904) → 10 586 (8 201–11 400) | 0.039 → 0.030 | 1.24 → 1.19–1.24 |
| local / no JIT | 4 | 14 547 (14 195–15 551) → 16 281 (15 880–16 704) | 0.153 → 0.102 | 1.22–1.23 → 1.22–1.23 |

With `completion_batching` (`WP_BENCH_COMPLETION_BATCHING=1`), batched archive →
batched delete:

| cell | processes | jobs/s | PG ms/job | peak connections |
|---|---:|---|---|---|
| local / YJIT | 1 | 6 548 (6 276–7 546) → 6 686 (4 445–7 843) | 0.026 → 0.027 | 6 → 6–7 |
| local / YJIT | 2 | 11 976 (11 885–12 383) → 14 180 (6 395–14 292) | 0.025 → 0.019 | 10 → 10–12 |
| local / YJIT | 4 | 19 830 (18 775–19 948) → **22 000 (20 768–22 375)** | 0.036 → 0.032 | 18–20 → 19–20 |
| local / no JIT | 1 | 5 107 (4 539–5 243) → 4 951 (4 856–5 566) | 0.043 → 0.033 | 6 → 6–7 |
| local / no JIT | 2 | 10 461 (7 822–10 809) → 11 750 (6 273–11 964) | 0.026 → 0.016 | 10–11 → 10–12 |
| local / no JIT | 4 | 16 620 (15 456–16 920) → **18 770 (11 502–20 006)** | 0.047 → 0.031 | 18–20 → 18–22 |

What the numbers say:

- **Statements per job do not change**: one `pgmq.delete` replaces one
  `pgmq.archive`. What changes is what each one costs. By median ms per call,
  the delete takes about half the archive's time unbatched (0.008 against
  0.015 ms at one process, 0.053 against 0.101 at four without a JIT, 0.097
  against 0.130 with YJIT), and `delete_batch` about a third to a half of
  `archive_batch` (0.026 against 0.061 ms, 0.030 against 0.056 at four
  processes): no INSERT into `a_`, no `a_` index entries, less WAL.
- **Unbatched, the gain is mostly Postgres time.** Jobs/s rises 10–35 % in
  four of six rows by median, but the ranges overlap and two rows (YJIT at four
  processes, no JIT at two) are inside the noise or below it, with one bad run
  each. Postgres time per job falls in five of six rows.
- **Batched delete at four processes is the fastest shape measured**: +11–13 %
  jobs/s over batched archive by median, on top of what batching already gave
  ("Batched completion" above). Each `delete_batch` replaces an `archive_batch`
  that inserts the same ~5 rows into `a_`.
- **When to turn it on:** when nothing reads the archive of your job queues
  (the dashboard does not; with job stats on, `stats_enabled` and the
  `pgbus_job_stats` table migrated, Insights counts successes either way). It never costs throughput in these runs beyond the run-to-run spread,
  and it ends `a_` table growth for job queues. Keep `:archive` when you query
  `a_` tables for audit or debugging.

### Async completion commit (issue #619)

`config.completion_synchronous_commit = false` (opt-in, default `true`) commits a
job's completion (the executor's archive or delete, the completer's
`archive_batch` or `delete_batch`) with `synchronous_commit` off for that
transaction only. The completion goes out in libpq pipeline mode as four
queries and one sync, so it is still one round trip:

```
BEGIN
SET LOCAL synchronous_commit TO off
<the prepared completion statement>   <- the result the caller gets
COMMIT
```

The statement stays prepared (the planning #605 took out stays out), `SET LOCAL`
ends with the transaction, and a transaction-mode pooler sees one explicit
transaction. A failed query leaves the explicit `BEGIN` open in a failed state
after the sync, so pgbus leaves pipeline mode, rolls it back and raises; a
statement the session lost is prepared and run outside the pipeline for that
call (so the first completion of a queue on a new connection prepares its
statement and commits synchronously once). A completion inside a caller's
transaction is left to that transaction. Only callers passing `job_completion: true` (the executor and the completer)
get it: the EventBus consumer's archive (its replay reads the archive) and the
dashboard's archives and deletes stay durable. Native statements on pgbus's own
connections only: with `native_statements = false`, or on ActiveRecord's shared
connection (a `connection_params` Proc), the option has no effect and
`pgbus doctor` warns.

**Semantics.** With `completion_synchronous_commit = false`, a completion is
acknowledged before its WAL record reaches disk. A Postgres server crash (not a
client crash, not a planned switchover), or a failover to a standby that has
not yet received that WAL, within about three times `wal_writer_delay`
(default 200 ms) can lose completions committed in that window. Each lost
completion redelivers its job after the visibility timeout, so the job runs
again. Under synchronous replication it also stops waiting for the standby for
that transaction, which is what lets a failover lose it. Sends, retries and
dead-letter moves keep full durability.

The precise exposure is smaller than that window. A durable commit on *any*
connection to the same server flushes WAL up to its LSN, including an earlier
asynchronous completion record, so a job whose completion is followed by a
synchronous write (a concurrency signal, a batch completion, a uniqueness
release, the stat buffer's flush) closes its own window within microseconds.
The job's own ActiveRecord writes run in `perform`, before the completion, so
they do not close it. The exposure is a plain job with no follow-up write, whose re-run
is the consequence above. That also answers "can a lost archive double-signal a
semaphore?": the signal's own commit makes the archive durable first.

Turn it on for a fleet on one Postgres whose fleet bench shows `WALWrite`
waits. Leave it on (`true`) for jobs that are not idempotent and cannot
tolerate one extra run after a database crash or failover. `commit_delay` (with
`commit_siblings`) on the server is the alternative that keeps every commit
durable: it groups concurrent commits into one flush.

**PgBouncer.** No PgBouncer release notes (read through 1.26.1, 2026-10-14)
mention libpq pipeline mode; the closest are 1.7's "Support pipelining count
expected ReadyForQuery packets" and 1.26.0's fix for batches that mix `Parse`
and `Close` without a `Sync`. Untested behind PgBouncer in transaction mode:
use session mode or leave the option on.

**Measured** 2026-11-02 on the machine of "Fleet scaling" (Apple M2 Max, 12
cores, PostgreSQL 18.6 on the same machine, Ruby 3.4.2, local only),
`WP_BENCH_ROLE=fleet WP_BENCH_PROCESSES=1,4`, 5 000 jobs per process,
**8 threads** (`WP_BENCH_THREADS=8`: the server's 100 connections were shared
with other sessions, and a 12-thread four-process row needs 74), `read_ahead`
not passed. Six variants alternated per round: `main`, the branch with
`WP_BENCH_COMPLETION_COMMIT=sync` and with `async`, and the three again with
`WP_BENCH_COMPLETION_BATCHING=1 WP_BENCH_COMPLETED_JOBS=delete` at four
processes. Five rounds, each run started only once the load average was under
the 12 cores and no other database had an active backend (waiting at most
120 s); load 0.64–1.55× the cores across the runs. Other sessions benched the
same Postgres during the rounds, so single runs dropped 3–6× at random:
`main` and the branch at `sync` run the same code and still differ by up to 2×
by median in jobs/s, and the branch at `sync` trails `main` in PG ms/job in all
six rows, which server-side time cannot owe to a Ruby keyword (the default path
is the same `exec_prepared` call, pinned by the unchanged
`spec/pgbus/client/native_statements_spec.rb`). Read the jobs/s columns as the
noise floor, not as the effect.
Medians, with the five-round range:

| cell | processes | jobs/s main | jobs/s branch, sync | jobs/s branch, async | PG ms/job main → sync → async |
|---|---:|---|---|---|---|
| local / YJIT | 1 | 4 879 (4 454–8 021) | 4 817 (4 245–6 660) | 7 102 (4 839–7 481) | 0.141 → 0.149 → 0.057 |
| local / YJIT | 4 | 9 550 (5 558–13 485) | 7 386 (4 471–12 235) | 10 620 (7 503–14 458) | 0.417 → 0.536 → 0.331 |
| local / no JIT | 1 | 5 357 (5 095–6 205) | 4 595 (3 998–5 751) | 3 317 (2 885–5 770) | 0.099 → 0.138 → 0.075 |
| local / no JIT | 4 | 9 233 (7 206–16 363) | 4 698 (3 069–13 504) | 6 506 (4 254–13 731) | 0.403 → 0.823 → 0.480 |

Batched delete (`completion_batching` + `completed_jobs = :delete`), four
processes:

| cell | jobs/s main | jobs/s branch, sync | jobs/s branch, async | PG ms/job main → sync → async |
|---|---|---|---|---|
| local / YJIT | 10 692 (3 735–17 878) | 6 621 (3 197–20 992) | 15 302 (8 692–23 894) | 0.223 → 0.518 → 0.138 |
| local / no JIT | 15 621 (3 179–15 876) | 6 781 (2 761–20 327) | 16 663 (2 952–19 366) | 0.141 → 0.505 → 0.057 |

What the bench database's active backends waited on (share of
`pg_stat_activity` samples, median of the five rounds with the range; the
output lists the top six wait kinds per row, so 0 means "not in the top six"):

| cell | processes | `LWLock:WALWrite` main / sync / async | `IO:WalWrite` main / sync / async |
|---|---:|---|---|
| local / YJIT | 1 | 9 (4–22) / 12 (5–19) / 0 (0–0) | 22 (15–24) / 19 (17–20) / 14 (2–21) |
| local / YJIT | 4 | 18 (8–23) / 17 (9–29) / 2 (0–8) | 5 (5–6) / 4 (4–6) / 5 (3–6) |
| local / no JIT | 1 | 5 (0–19) / 7 (0–13) / 0 (0–0) | 28 (16–43) / 26 (16–36) / 14 (7–20) |
| local / no JIT | 4 | 12 (11–26) / 23 (6–31) / 2 (2–7) | 5 (4–6) / 5 (4–10) / 5 (3–6) |
| batched delete / YJIT | 4 | 11 (6–17) / 10 (2–16) / 3 (0–6) | 11 (8–11) / 7 (3–14) / 11 (3–15) |
| batched delete / no JIT | 4 | 11 (5–13) / 14 (2–16) / 4 (3–9) | 9 (8–13) / 7 (4–18) / 9 (4–16) |

The single call (`rake bench:integration` "execute: plain job": enqueue, read
and execute one plain job on one connection; `PGBUS_BENCH_COMPLETION_COMMIT=async`
for the async rows), three alternated rounds, load 0.38–0.99× the cores:
`main` 1 782 i/s median (756–2 652), the branch at the default 2 614
(2 280–2 697), async 2 598 (821–2 664); every run reports ±8–37 %. The three
extra statements in the pipeline add no visible cost to the round trip.

What the numbers say:

- **The flush wait goes away.** `LWLock:WALWrite`, a backend waiting for
  another backend's WAL flush, falls from 12–23 % of samples (median, `main`
  and `sync`, four processes) to 2 %, and at one process it drops out of the
  top six. `IO:WalWrite` (a backend doing the write) falls at one process
  (19–28 % → 14 %) and stays at 4–11 % elsewhere: the read's `UPDATE`, the
  stats insert and the batched flushes still commit durably, and WAL still has
  to be written, by the WAL writer instead of the job.
- **Postgres time per job falls where the completion dominates it:** by median
  in five of six rows against `main` (0.141 → 0.057 ms at one YJIT process,
  0.417 → 0.331 at four, 0.223 → 0.138 batched), and in every row against the
  branch at `sync`, with overlapping ranges.
- **Jobs/s:** the async median is the highest of the three in both YJIT rows
  and both batched-delete rows (15 302 and 16 663 jobs/s batched), and lowest
  at one no-JIT process; every range overlaps, and the `main`/`sync` spread
  shows the machine's noise is as large as any difference here. The expected
  shape, a sharp fall in the WAL-flush wait with Postgres time per job falling
  at four processes, is what the waits and PG ms/job show; whether that buys
  jobs/s needs a quieter machine or a dedicated database host to measure.
- **Batched delete:** batching already cut the per-job commits to one per
  flush (~5 jobs), so the flush wait there was smaller to begin with
  (10–14 %); async takes it to 3–4 %.

### The Rails share of a job (issue #618)

The worker profile above puts 37 % of a no-op job's on-CPU time in one `rails`
bucket. The harness now splits that bucket by who owns the work and prints it as
an "inside rails" table under the on-CPU row. The leaf Rails frame names it when
it is specific; otherwise the nearest owner frame toward the root does:

| sub-bucket | what lands there |
|---|---|
| `aj_log` | ActiveJob's log subscriber, `ActiveJob::Logging` (the `tag_logger` wrapper), and any ActiveSupport log subscriber or logger frame (in this bench only ActiveJob's lines reach one: the app logs at `info` and pgbus's logger is a plain `Logger`) |
| `aj_arguments` | `ActiveJob::Arguments`, the serializers, GlobalID |
| `aj_callbacks` | ActiveSupport callbacks run by ActiveJob (`perform` callbacks) |
| `notif:activejob` | `ActiveSupport::Notifications` work for ActiveJob's `perform_start` / `perform` events, the listeners included |
| `notif:pgbus` | the same for pgbus's own `instrument` calls |
| `executor_wrap` | `Rails.application.executor.wrap` and the hooks it runs (query cache, connection return, async-query tracker, `CurrentAttributes` reset) |
| `pgbus_rails` | Rails code pgbus itself calls: the stat buffer's `insert_all`, the heartbeat's process row |
| `aj_other` | everything else: ActiveJob's own per-job core (`Core#initialize`, `deserialize_time`, `ExecutionState#perform_now`, `ExecutionContext`), a job's own ActiveRecord work, a leaf with no owner |

**Discovery: #484's profile never contained the executor wrap.** The bench boots
no Rails application, so `Executor#execute_job` takes its bare `job.perform_now`
branch. `WP_BENCH_RAILS_APP=1` (new) boots a minimal `Rails::Application`
(ActiveRecord + ActiveJob railties, `load_defaults`, log level `info`) in each
cell, so jobs run inside `Rails.application.executor.wrap` as in a host app.
`WP_BENCH_AJ_LOGS=default|warn|quiet` (new) picks how ActiveJob's per-job lines
are handled: `default` keeps the bench's logger (a plain `Logger` to `IO::NULL`:
every line formatted, none written, no tags), `warn` is the throughput
checklist's recipe (ActiveJob's own `TaggedLogging` logger at `warn`), `quiet`
sets `config.quiet_active_job_logs = true` (new).

Measured 2026-11-02 on the M-series laptop, local PostgreSQL 18, Ruby 3.4.2,
12 threads, 5 000 timed jobs and a 2 000-job profiled drain per cell. Two rounds,
alternated (round 1: Rails off, default, warn, quiet; round 2 the reverse), load
0.32–0.34× cores before and after every invocation, with two other worktrees on
the same machine. Ranges are the two rounds.

Inside rails, Rails app off (#484's shape) and on, default log lines. Each cell
is the share of the `rails` bucket; the on-CPU row gives the bucket itself:

| | off / YJIT | off / no JIT | on / YJIT | on / no JIT |
|---|---:|---:|---:|---:|
| jobs/s | 6 346–7 220 | 5 810–5 865 | 6 473–6 593 | 4 824–4 888 |
| `rails` share of on-CPU | 36–37 % | 47 % | 46–51 % | 55–56 % |
| `aj_log` | 2–3 % | 8–9 % | 2–3 % | 4–5 % |
| `aj_arguments` | 0 % | 0–1 % | 0 % | 0–1 % |
| `aj_callbacks` | 0–1 % | 0–2 % | 0.5 % | 0–1 % |
| `notif:activejob` | 17–20 % | 14–20 % | 7–8 % | 12–13 % |
| `notif:pgbus` | 0–1 % | 1–2 % | 1.5–2 % | 1 % |
| `executor_wrap` | — | — | 28–31 % | 29–33 % |
| `pgbus_rails` | 52–53 % | 35–36 % | 33–35 % | 25–26 % |
| `aj_other` | 24–27 % | 34–39 % | 23–25 % | 21–27 % |

ActiveJob's per-job log lines, Rails app on. "Log share" is `aj_log` +
`notif:activejob` as a share of all on-CPU time: the second column is the
LogSubscriber's listener being invoked, so `aj_log` alone understates the lines:

| cell | jobs/s YJIT | jobs/s no JIT | log share YJIT | log share no JIT |
|---|---:|---:|---:|---:|
| default | 6 473–6 593 | 4 824–4 888 | 4.7–5.0 % | 9.0–9.8 % |
| warn (checklist recipe) | 6 369–6 967 | 5 182–5 307 | 2.3–2.9 % | 1.9–4.0 % |
| `quiet_active_job_logs` | 6 598–6 649 | 5 250–5 299 | 0.2–0.5 % | 1.0 % |

What the numbers say:

- **Of #484's 37 % (the Rails-off cells), 35–53 % is pgbus's own ActiveRecord
  work, 19–29 % the two log lines, 24–39 % ActiveJob's per-job core.**
  ActiveJob's framework overhead is smaller than the bucket made it look. `pgbus_rails` is mostly `StatBuffer#write_to_database` (job stats'
  `insert_all`, 100 rows per flush): ~31–41 % of the bucket, about 12–15 % of
  all on-CPU time (a frame tally of one 3 000-job profiled drain per mode). The rest of
  `pgbus_rails` is `Heartbeat#register_process` / `#deregister_process`, which run
  once per drain (each drain builds a new Worker), a bench artefact a long-lived
  worker does not pay per job. The flush is followed up in #622: it is
  pgbus-owned, over 10 % of on-CPU, and needs its own before/after.
- **The log lines: without a JIT, warn and quiet both buy +6–10 % jobs/s**
  (4 824–4 888 → 5 182–5 307 and 5 250–5 299, equal within noise); with YJIT all
  three cells are within noise (the Rails-off YJIT pair alone spread 14 %).
  `quiet` removes more of the profile than `warn` here because the bench's
  default logger has no tags: under `warn` the recipe's `TaggedLogging` logger
  still pays `ActiveJob::Logging#tag_logger` and the subscriber's `silenced?`
  checks. A host's `ActiveJob::Base.logger` is `Rails.logger`, which is tagged,
  so in a Rails app `quiet` keeps that `tag_logger` push and pop too. The knob's
  case is scope, not speed: it acts in worker processes only, keeps `Enqueued`,
  `Retrying`, `Stopped retrying` and `Discarded`, and leaves the web process and
  `Rails.logger` alone. It also drops ActiveJob's `Error performing …` line
  (the same `perform` listener writes it): an exception that escapes
  ActiveJob's handlers is still logged by pgbus's `ErrorReporter` and kept in
  `pgbus_failed_events`, one `retry_on` catches still logs ActiveJob's
  `Retrying …` line, and a job a `before_perform` callback halted logs nothing.
- **The executor wrap: 13–18 % of on-CPU time, kept.** Its hooks are
  ActiveRecord's async-query tracker, the query cache's enable/clear, the
  connection pool's return, and the callbacks that run them. The Rails app
  around the job (the wrap, plus `load_defaults` and ActiveRecord's silenced
  SQL subscriber) costs −16 to −18 % jobs/s without a JIT and is within noise
  with YJIT. It stays: the wrap is what returns the job's ActiveRecord
  connections, clears the query cache and lets development reload code, and
  SolidQueue wraps every job the same way.
- **`notif:pgbus` is 0–3 % of the bucket across all eight runs (at most 1.2 % of on-CPU time, the quiet / YJIT cell of round 2; the table above shows the default cells only)**, far under the 10 %
  that would justify touching pgbus's three `instrument` calls per job.

`config.quiet_active_job_logs` (default `false`) unsubscribes
`ActiveJob::LogSubscriber` from `perform_start.active_job` and
`perform.active_job` once, first thing in `Process::Worker#run`
(`Pgbus::ActiveJob::QuietLogs`), after loading `ActiveJob::Base` (which attaches
the subscriber) so a worker that has not loaded a job class yet still finds it. It uses `Fanout#all_listeners_for`, not
`listeners_for`, so a subscriber silenced at boot (a logger above `info`) is
removed too. The executor allocation budget is unchanged: the knob adds nothing
per job.

### Job stats flush (issue #622)

#618's split put `StatBuffer#write_to_database` at 12–15 % of a no-op job's
on-CPU time: `JobStat.insert_all` of 100 rows, where ActiveRecord checks every
attribute against the schema, type-casts every value, visits an Arel
`ValuesList`, and adds `ON CONFLICT DO NOTHING RETURNING "id"`, so the 100 new
ids come back and are cast into an `ActiveRecord::Result` nobody reads. The
values are Strings and Integers pgbus produced itself.

The flush now builds the one multi-row `INSERT` itself and runs it with
`execute` on a connection from `JobStat.connection_pool.with_connection`
(the model's database, `connects_to` included). The integer columns go through
`Integer()`, `NULL` for nil; an entry with a value `Integer()` refuses is
logged and dropped alone, and the rest of the flush is written; the strings (`job_class` comes from the message) go
through the connection's own quoting, memoized per flush, so 100 rows of three
job classes and two queues cost about seven `quote` calls. `pgbus_job_stats`
has no unique index besides the primary key, so dropping `ON CONFLICT DO
NOTHING` changes no outcome. The missing-latency-columns fallback is the same:
the six-column statement first, and on PostgreSQL's `undefined_column` error
(`PG::UndefinedColumn`, the cause ActiveRecord keeps) naming
`enqueue_latency_ms` or `retry_count`, the four-column one, warned once. main
retried on any `StatementInvalid` whose message contained a column name, which
an unrelated error's `LINE 1:` excerpt of the statement does; the error class
is also what keeps it working when the server's `lc_messages` is not English.

A constant `INSERT … SELECT * FROM unnest($1::varchar[], …)` with six array
binds (`PG::TextEncoder::Array`) was the other candidate. One scratch run, same
database, 100 rows, load 0.62× cores: `insert_all` 984 µs CPU and 3 193
objects per flush, multi-row `VALUES` 96 µs / 429, `unnest` 133 µs / 1 002.
`unnest` had the lower wall time (538 vs 734 µs, it is a prepared statement),
but that time is spent in libpq with the GVL released; a local worker is
GVL-bound, so Ruby CPU picked `VALUES`.

Measured 2026-11-02 on the M-series laptop, local PostgreSQL 18, Ruby 3.4.2,
YJIT, with three other worktrees' agents on the same machine; load is recorded
before and after every invocation. `rake bench:one[stat_flush_bench]` (new,
real database) pushes 100 worker-shaped entries and flushes. Ranges are rounds,
main and branch alternated. ActiveRecord 8.1.4, four rounds: two (load
0.63–0.92×) with an earlier version of the bench that created the table
without indexes (historical: the committed bench always creates the
migration's four indexes, so the "bare table" row cannot be reproduced with
it) and two with the committed bench (load 0.39–0.56×); ActiveRecord 7.1.6
(`gemfiles/rails_7_1.gemfile`), one round with the committed bench (load
0.39–0.42×):

| | main 8.1 | branch 8.1 | main 7.1 | branch 7.1 |
|---|---:|---:|---:|---:|
| Ruby CPU per flush | 936–1 999 µs | 120–229 µs | 1 440 µs | 101 µs |
| Ruby CPU per job (÷ 100) | 9.4–20 µs | 1.2–2.3 µs | 14 µs | 1.0 µs |
| objects per flush | 3 161 | 518 | 2 850 | 521 |
| bytes per flush | 265 KB | 50 KB | 252 KB | 50 KB |
| wall i/s, bare table (historical bench) | 282–483 | 954–1 449 | — | — |
| wall i/s, indexed table | 254–289 | 513–696 | 207 | 866 |

Ruby CPU per flush moves with machine load (main's four rounds spread 2×);
the objects per flush do not move at all.

`WP_BENCH_ROLE=worker rake bench:worker_profile`, two rounds alternated
(main, branch, main, branch), load 0.63–0.73×:

| cell | jobs/s main | jobs/s branch | `pgbus_rails` share of on-CPU, main | branch |
|---|---:|---:|---:|---:|
| local / YJIT | 5 139–6 452 | 7 499–7 840 | 17.7–18.1 % | 7.7–7.8 % |
| local / no JIT | 5 580–5 897 | 5 886–6 349 | 17.1–18.3 % | 0.7 % |

What the numbers say:

- **The flush costs 4.7–14× less Ruby CPU and allocates a sixth of the
  objects**, on ActiveRecord 8.1 and 7.1 alike. The ratio is per main/branch
  pair, run back to back: 947/120 and 936/200 on the bare table, 1 999/161
  and 1 118/229 indexed, 1 440/101 on 7.1. Per job that is ~9–20 → 1–2.3 µs.
- **`pgbus_rails` is no longer a meaningful share of a worker's on-CPU time.**
  What remains is `Heartbeat#register_process` / `#deregister_process`, which
  run once per drain (each drain builds a new Worker: a bench artefact), and a
  handful of flush samples in the `Rails` category (`execute`'s logging and the
  query cache's dirty hook). The profiled drains are 2 000 jobs, so each cell
  rests on a few hundred on-CPU samples: read the YJIT 7.7–7.8 % as "a few
  samples of the heartbeat", not as a YJIT-specific cost.
- **jobs/s: up with YJIT (the ranges do not overlap), within noise without a
  JIT.** The machine was shared and the main YJIT pair alone spread 26 %;
  the CPU-per-flush table is the measurement this change stands on.

### Connection leases on the bookkeeping paths (issue #649)

The executor runs `perform_now` inside `Rails.application.executor.wrap`,
whose ActiveRecord hook returns the job's lease. pgbus's own bookkeeping runs
after that wrap, on the same pool thread or fiber: the uniqueness release, the
concurrency signal, the batch counters, the failed-event row and the job stat.
Every one of those sites went through a bare `Model.connection`, which is a
lease nobody returns (`lease_connection` since Rails 7.2, and on 7.1 every
relation call too). In threads mode a job with a key, a limit or a batch left
one connection pinned to its pool thread until the worker stopped. In async
mode the fiber ended still owning it, and the pool's reaper (`ConnectionPool#reap`,
run inline by `checkout` once the pool has nothing free) took it back with
`reset!`: `ROLLBACK`, `DISCARD ALL` and the session's `SET`s, about once per
job. This is River's v0.16 bug ([riverqueue/river#1574](https://github.com/riverqueue/river/pull/1574)),
shape for shape.

Every such call now runs inside `connection_pool.with_connection`, which
reuses a lease the caller already holds and otherwise returns the one it took.
`Pgbus/BareConnectionLease` keeps the job-path files that way, and
`spec/integration/connection_lease_spec.rb` runs each operation, and a whole
`Executor#execute` per job shape, in a fresh thread and in a fresh fiber under
`:fiber` isolation (red on main: 16 of 23 examples on Rails 8.1, 22 of 23 on 7.1).

`rake bench:worker_profile` takes `WP_BENCH_JOB_SHAPE=plain|unique|limited|batch`
and `WP_BENCH_EXECUTION_MODE=threads|async`, and worker cells print the
ActiveRecord pool's stat when the timed drain finished plus the
`PostgreSQLAdapter#reset!` calls (nothing instruments `reset!`, and
`pg_stat_statements` never sees `verify!`'s `;`). Main and the branch
interleaved on one machine shared with other sessions (1-minute load 8–17 on
12 cores), three rounds, 3 000 timed jobs, 12 threads; ranges, not means.

| cell (worker, YJIT / no JIT) | main jobs/s | branch jobs/s | main busy / dead / resets | branch busy / dead / resets |
|---|---:|---:|---:|---:|
| plain, threads | 5 763–7 490 / 5 197–6 357 | 5 562–6 314 / 5 355–5 911 | 0 / 1 / 0 | 0 / 0 / 0 |
| unique, threads | 3 176–4 740 / 3 201–4 401 | 4 551–4 892 / 3 875–4 469 | 12 / 0 / 12 | 0 / 0 / 0 |
| limited, threads | 2 233–2 627 / 1 796–1 997 | 2 278–2 731 / 1 864–1 949 | 12 / 0 / 12 | 0 / 0 / 0 |
| batch, threads | 1 299–1 431 / 927–1 008 | 1 142–1 588 / 920–1 048 | 0 / 1 / 0 | 0 / 0 / 0 |
| **limited, async** | **1 039–1 143 / 434–873** | **1 059–2 761 / 1 805–2 151** | **0 / 6–12 / 3 000–3 005** | **0 / 0 / 0** |

- **Async, the headline row.** On main every job's fiber died owning a
  connection, the pool of 20 ran dry, and `checkout` reaped inline: 3 000
  `reset!`s for 3 000 jobs, 6–12 connections dead when the drain ended. The
  branch resets none and leaves none dead, and the cell runs about twice as
  fast (one YJIT round, 1 059, sat in a load spike).
- **Threads.** On main the 12 pool threads each kept one connection (busy 12),
  and each drain's first checkouts reset the 12 the previous drain's threads
  left behind when they ended. The branch holds none between jobs. jobs/s is
  inside the round-to-round spread for every shape, so the per-call `verify!`
  ping a fresh lease pays (one `;` before its first write) does not show: the
  single lease around a job's whole bookkeeping that #649's plan held in
  reserve was not needed.
- **plain** pins nothing on either side except main's one dead connection: the
  stat table's memoized `table_exists?` probe, also a bare `connection`, ran
  once per process on a pool thread (now inside `with_connection` too).

The fleet cell (`WP_BENCH_ROLE=fleet WP_BENCH_PROCESSES=1`, which honours the
shape: a shaped drain child gets an ActiveRecord pool of threads + 2) reads
the Postgres side from `pg_stat_statements`:

| shape | main statements/job | branch statements/job | main jobs/s (YJIT) | branch jobs/s (YJIT) |
|---|---:|---:|---:|---:|
| plain | 1.19–1.24 | 1.19–1.24 | 7 332–8 849 | 7 395–8 286 |
| unique | 2.21–2.28 | 2.21–2.26 | 4 982–6 181 | 4 883–5 650 |
| limited | 3.35–3.55 | 3.36–3.46 | 2 382–2 604 | 2 597–2 910 |
| batch | 6.99–7.01 | 6.85–7.00 | 1 502–1 650 | 1 298–1 493 |

Statements per job do not move: the change is which connection runs a
statement and when it goes back, not what runs. The one row whose jobs/s
ranges did not overlap, `batch` with YJIT (main 1 502–1 650, branch
1 298–1 493), was run again, three more interleaved rounds with the branch
first at a lower load (1-minute 6–12): main 1 549–1 745, branch 1 470–1 703,
statements per job 6.89–6.98 and 6.84–6.92. Every job in a batch slice updates
one batch row, so the cell is bound by that row lock (`Lock:tuple` is about
two thirds of sampled waits) and swings with load.

From River: the fix itself (scope every operation's lease, so a thread or
fiber that held none before holds none after, and keep a caller's own), the
spec shape (a fresh owner per operation, `active_connection?` falsy inside it,
`stat[:dead] == 0` after it ends, a second example for the caller's lease) and
the reset count as the measure. Already in place in pgbus: the job's own lease
(the executor wrap), the stat buffer's flush (`with_connection` since #622),
the semaphore heartbeat's `touch`, and the dispatcher, which returns its
maintenance leases between cycles. Left for later sweeps: the workflow runner
and models, streams and recurring tasks; the dashboard's lease is returned by
the web request's executor.

### Fair share reads (issue #426)

`Client#read_batch_fair` replaces `pgmq.read` on the worker when
`config.fair_share` is set. It is a single statement: a recursive loose index
scan enumerates the keys that currently have visible messages, a `LATERAL`
takes each key's oldest `qty` visible messages, and the `qty` lowest
`rank / weight` win (`FOR UPDATE SKIP LOCKED`, then `UPDATE … RETURNING`).
Cost scales with the number of keys that have visible work, not with backlog
depth — the `(key, vt, msg_id)` expression index lets keys whose messages are
all invisible (in flight / delayed / in retry backoff) be skipped at the index
level. Measured with `rake bench:fair_read` (local arm64, local Postgres, one
connection, `qty: 10`, the claimed rows are reset after every read so the
backlog is constant):

| Backlog shape | `read_batch` (pgmq.read) | `read_batch_fair` | Δ |
|---|---|---|---|
| 100k visible / 1 key | 1 486 i/s (0.67 ms) | 950 i/s (1.05 ms) | 1.56× slower |
| 100k visible / 200 keys | 1 401 i/s | 211 i/s (4.7 ms) | 6.6× slower |
| 10k visible + 50k invisible / 50 keys | 1 378 i/s | 492 i/s (2.0 ms) | 2.8× slower |

Method-level, not system-level: a worker reads at most once per poll tick when
idle and back-to-back only while it has free capacity, so a 5 ms read with 200
active tenants still supplies ~2 000 messages/s to one worker — far above what a
5–20 thread pool executes. Enabling `fair_share` costs nothing on the enqueue
path beyond one callable invocation; unkeyed installs (`fair_share` nil) never
run this query. The bench prints `EXPLAIN (ANALYZE, BUFFERS)` per shape so the
plan (Index Cond on `vt <= now()`, no Seq Scan) can be re-checked.

### Supervisor-owned shared LISTEN (issue #381)

One `NotifyListener` in the supervisor serves every fork over per-fork pipes
instead of one dedicated LISTEN connection per fork. Measured on an M-series
laptop against local PostgreSQL (n=100, sends spaced past the 250ms NOTIFY
throttle; `notify_wake_bench.rb`):

| Metric | main (per-fork listener) | #381 `:fork` scope | #381 `:supervisor` (hub → pipe) |
|--------|--------------------------|--------------------|---------------------------------|
| wake latency p50 | 1.63ms | 1.68ms | 1.66ms |
| wake latency p95 | 4.76ms | 5.47ms | 6.04ms |
| wake latency p99 | 7.54ms | 10.70ms | 15.10ms |
| direct LISTEN connections (5 forks) | 5 | 5 | **1** |

The supervisor hop is free at the median; the tails differ by single-digit
milliseconds on a shared machine (noise-level: the empty-read control drifted
±24% between the same two runs). This is a **connection-footprint win, not a
throughput win** — the point is the last row.

Failure modes (`notify_chaos_bench.rb`, same machine): killed LISTEN backend →
fresh backend + healthy in **18ms**, first post-recovery wake 8.8ms; a wedged
fork (full pipe) leaves sibling wake latency unaffected; 50 fork
register/deregister cycles leak **0** FDs; hub fan-out costs **10.5µs** per
NOTIFY to 10 forks (the pgmq trigger throttle caps real NOTIFY load at
4/s/queue, so the hub is never the bottleneck).

### Streams master hub (issue #382)

One `Web::Streamer::Listener` in the Puma master serves every worker over a
Unix socket instead of one dedicated LISTEN connection per worker. Measured
on the same machine (`streams_hub_bench.rb`, n=50, durable broadcasts,
single-broadcast SSE roundtrip):

| Mode | p50 | p95 | LISTEN connections (host) |
|------|-----|-----|---------------------------|
| `:process` (per-worker, pre-#382) | 16.93ms | 26.67ms | 1 per worker |
| `:master` (hub → socket hop) | 16.00ms | 19.19ms | **1** |

The master→worker frame hop is noise-level free — the DB round trips
(`read_after` + NOTIFY) dominate. Like #381, this is a
**connection-footprint win, not a latency win**. On hub outage every worker
falls back to its own listener (census-visible balloon, unchanged
semantics) — verified end-to-end by
`spec/integration/streams/master_hub_e2e_spec.rb` (census 1 → 2 across a
mid-stream hub death with zero missed broadcasts).

### Streamer connection model (issue #315)

The durable-stream publish and replay hot paths run on a **dedicated streams
DB pool** (`config.streams_pool_size` / `streams_pool_timeout`), separate from
the job pool (`pool_size`):

- `Client#send_stream_message` (the broadcast INSERT) draws from the streams
  pool, so a saturated job pool can't delay a broadcast on pool checkout.
- The dispatcher's per-wake reads (`read_after`, `stream_current_msg_id`,
  `stream_oldest_msg_id`) check out a persistent pooled connection via
  `Client#with_streams_connection` instead of a fresh `PG.connect` per call —
  measured ~23× faster per read against a real DB (191 μs vs 4.42 ms), removing
  the TCP + auth + TLS setup cost from the single dispatcher thread. This is a
  connection-setup win at the client layer, **not** end-to-end broadcast
  latency (which the PGMQ round-trip + LISTEN/NOTIFY + socket write dominate).

On the shared-ActiveRecord (Proc) connection path no separate pool is created
(libpq isn't thread-safe): streams share the single serialized connection, so
the isolation applies only to the dedicated `database_url` / `connection_params`
config.

**Capacity planning:** on the dedicated path this streams pool is *in addition*
to the job pool, and every forked process (worker, dispatcher, scheduler,
consumer) builds its own — even `--workers-only` processes that rarely stream.
Both pools are lazy, so an idle process holds few real connections, but when
sizing Postgres/PgBouncer `max_connections`, budget `pool_size (or the
auto-tuned resolved size) + streams_pool_size` per process rather than
`pool_size` alone.

### Fanout head-of-line blocking (issue #315 item 3)

A single dispatcher thread per web-server process fans out every broadcast to
every connected SSE client on the stream, writing to each socket **serially**.
A slow client's socket write blocks that thread up to a deadline before the
client is marked dead — and the connections queued behind it in the same wake
wait. With the full `streams_write_deadline_ms` (5s) that meant K slow clients
could stack a `K × 5s` stall on every other browser on the worker.

Fanout writes now use a separate, shorter `config.streams_fanout_write_deadline_ms`
(default **250 ms**), bounding the worst-case serial stall to `K × 250 ms`
(~20× lower). A slow-but-alive client that can't absorb a fanout frame in that
window is marked dead and reconnects; its `EventSource` `Last-Event-ID` then
replays the gap from the durable archive (or triggers a fresh re-render for
ephemeral streams). **Connect-replay** writes keep the full
`streams_write_deadline_ms`, so a new client catching up a large backlog isn't
evicted. Set `streams_fanout_write_deadline_ms = streams_write_deadline_ms` to
restore the pre-fix timing. Since #621 the heartbeat's writes use the same short
deadline (they are serial on the heartbeat thread, so K slow clients stalled a
tick `K × 5 s`; each heartbeat write is now bounded by 250 ms once it holds the connection's
mutex, which a concurrent fan-out write on that connection can delay); the same setting restores
their old timing too.

This is a **system-level** latency bound, not a per-write speedup: the fanout
happy path (all-fast clients) is unchanged — the `deadline_ms` keyword is a
zero-allocation pass-through (`streams_fanout_bench.rb`: 585k vs 583k i/s,
within noise; 0 retained/op). `config.streams_dispatch_queue_limit` (default
`0` = unbounded) optionally caps distinct-stream wake backlog by dropping
durable wakes (never ephemeral/connect) when the Listener sees the queue at the
cap — safe because the next durable wake re-reads from the min cursor.

### Durable fanout offload (issue #321)

The #315 deadline only *bounds* the serial stall (`K × 250 ms`); the dispatcher
still writes to every client in turn. `config.streams_writer_threads` (default
**0** = inline, the behavior above) moves the **durable** fanout socket write
off the dispatcher into a pool of N writer threads. Each connection is pinned to
one worker by `id.hash % N`, so its frames stay ordered and its per-io mutex is
never cross-contended. The dispatcher hands the write to the pump and moves on;
the pump reports back the highest msg_id it actually committed, and the
dispatcher — still the **sole owner** of the read cursor — advances it only on
that ack (so a blocked/partial write never advances the cursor past a frame that
didn't reach the socket). A failed write posts a `DisconnectMessage`, and the
dispatcher scrubs the connection on its own thread.

Measured decoupling (`streams_writer_offload_bench.rb`, dispatcher
`handle_durable_wake` wall time, 50 fast + K slow clients @ 50 ms each):

| K slow clients | inline (OFF) | offload (ON, 2 writers) |
|---|---|---|
| 0 | 1.1 ms | 1.1 ms |
| 1 | 55.6 ms | 0.2 ms |
| 5 | 268 ms | 0.5 ms |
| 20 | **1071 ms** | **0.09 ms** |

Inline scales linearly with K (a slow client blocks the thread); offload stays
flat — fast clients no longer wait behind slow ones. This is a **system-level
latency win, not a per-write speedup**: total bytes written are unchanged and a
slow client still takes its time on its own worker. The dispatcher does slightly
more bookkeeping per wake under offload (the post + ack round-trip), which is why
it stays **opt-in and default-off**; enable it only when slow-client
head-of-line latency is a measured problem.

**Ephemerals stay inline** regardless of `streams_writer_threads` — they have no
archive to replay, so they must not risk an async drop (the pump raises if one is
ever routed to it). `config.streams_writer_buffer_limit` (default `0` =
unbounded) caps a connection's outbound buffer, dropping its **oldest** durable
frame on overflow — safe because durable frames are re-read from the archive on
reconnect; it's an OOM guard for a pathologically slow-but-alive client, not a
delivery guarantee.

### Streamer at scale (issue #620)

Nothing measured the streamer above 50 clients until `benchmarks/streams_scale_bench.rb`
(`rake bench:streams_scale`). It connects M SSE clients to **one** Puma web process
(an in-process `Puma::Server`, `streams_listen_scope = :process`: no master hub) over real
PGMQ and real LISTEN/NOTIFY, then sends 20 broadcasts 250 ms apart, in ephemeral mode
(the NOTIFY payload is the data) and in durable mode (`read_after` from the stream queue).

- Each (M, mode) cell is its own subprocess, so no cell inherits another's heap growth or
  YJIT warm-up. The clients run in 4 spawned client processes (never forked, never in the
  server's process: 1 000 SSE-parsing threads under the server's GVL would time the bench).
  A client process waits at a start barrier, then starts every client at once.
- The server is timed by prepending wall-time wrappers on the instances the cell owns
  (the dispatcher's `handle_connect`, `handle_ephemeral_wake` and `handle_durable_wake`,
  `Heartbeat#tick`, `PeerCloseWatcher#watched_sockets`); nothing under `lib/` changes.
  "Connected" is the client's receipt of its `pgbus:connected` frame, written when the
  dispatcher registers it. Fan-out is the bench's `CLOCK_REALTIME` stamp just before the
  broadcast call to each client's receipt, first and last client per broadcast. One
  heartbeat tick is forced after the broadcasts. RSS and CPU are the cell process's.
- A client whose connect the kernel resets reconnects after 2 s, the `retry:` the
  streamer sends and an EventSource would wait; the "retried connects" column counts
  those. They are macOS's accept queue: `somaxconn` is 128 and caps Puma's backlog of
  1 024, so a burst of 1 000 connects overflows it (0 retries at M = 100, 583–724 at
  1 000). Linux's default is 4 096.
- The fd soft limit is raised in every process (to the hard limit, or `OPEN_MAX` = 10 240
  when that is infinite, as on macOS); an M whose `2 × M + 64` descriptors do not fit is
  skipped with the cap and the `ulimit -n` / `launchctl limit maxfiles` hint.
  `STREAMS_SCALE_SLOW=K` adds K clients that sleep 1 s after each event, for #621.

Measured 2026-11-02 on an Apple M2 Max (12 cores), PostgreSQL on the same machine, Ruby 3.4.2
with YJIT, Puma 8.0.2 (`max_threads` 8), every streams setting at its default
(`streams_heartbeat_interval` 15 s, `streams_peer_close_interval` 1 s,
`streams_listen_health_check_ms` 250, `streams_writer_threads` 0). Two rounds; load average
8.3–11.7 on 12 cores from other sessions' benches and suites. Ranges are those two rounds.
Every row: 0 missed broadcasts, 0 failed clients, 0 503s. Every 1 000-client cell also logged
2–29 Puma `Socket timeout writing data` thread-pool errors: Puma writing to connections the
kernel had reset, which those clients retried.

| mode | M | connect all (s) | fan-out first p50 (ms) | fan-out last p50 / p99 (ms) | dispatcher wake p50 / p99 (ms) | heartbeat tick (ms) | watcher build (ms) | RSS Δ / conn (KB) | CPU / wall |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ephemeral | 100 | 25.3–25.6 | 0.8 | 2.8–2.9 / 8.5–63.6 | 3.4–3.6 / 7.7–172.6 | 0.7–0.8 | 0.06 | 42.4–42.7 | 1 % |
| ephemeral | 1 000 | 254.5–256.7 | 0.6–1.3 | 16.8–24.4 / 32.6–57.2 | 17.4–23.0 / 20.1–42.1 | 5.7–21.6 | 0.38–1.06 | 26.7–26.8 | 5–8 % |
| durable | 100 | 25.2–25.6 | 1.5–2.4 | 3.4–4.5 / 7.8–9.8 | 1.2 / 5.8–6.2 | 0.7–0.8 | 0.06–0.09 | 40.3–41.9 | 1 % |
| durable | 1 000 | 253.9–256.6 | 1.8–4.2 | 15.5–21.7 / 28.9–35.4 | 9.2–9.3 / 11.1–19.6 | 6.7 | 0.39–0.43 | 26.6–27.4 | 4 % |

`STREAMS_SCALE_CLIENTS=2000` once (load 8.9 rising to 33.7): connect all 515 s (ephemeral) and
542 s (durable), fan-out last p50 / p99 34.8 / 73.5 and 78.5 / 150.9 ms, wake p50 31.7 and
25.8 ms, heartbeat tick 12.7 and 12.1 ms, 21–22 KB per connection, 0 misses. Calibration: 8
client processes instead of 4 at M = 1 000 left fan-out last p50 in the range above (13.4 ms
ephemeral, 21.7 ms durable) and made p99 worse (83 and 130 ms), so 4 client processes do not
cap these numbers.

What the numbers say:

- **The connect storm is the ceiling, and it is not fan-out.** Every connect costs one
  `streams_listen_health_check_ms` cycle on the single dispatcher thread: `handle_connect`
  p50 is 254–257 ms in every cell, so 1 000 clients take ~255 s to register and 2 000 take
  ~515 s. The dispatcher calls `Listener#ensure_listening` for each connect, and the listener
  acks the command only after its current `wait_for_notify(health_check_ms)` returns, even
  for a channel it already LISTENs on. Confirmed by the knob: 100 connects take 27.2 s at
  250 ms and 6.0 s at 50 ms. Until a client registers it gets no byte (not even a heartbeat),
  so a client with a read timeout under the queue wait gives up: the bench's client needed
  a 30-minute read timeout. A Puma restart behind 1 000 open EventSources is that storm.
- **Fan-out is linear and cheap per connection: ~10–20 µs.** The span from the first to the
  last client is ~2 ms at M = 100 and ~15–23 ms at 1 000, in both modes. Durable's wake
  (one `read_after`, then 1 000 filter + frame + write) is 9.2–9.3 ms; ephemeral's is
  17.4–23.0 ms in both rounds, about twice durable's for the same per-connection code. This
  bench does not explain that gap; #621's profile should.
- **Framing is a small share.** `Envelope.message` builds a 146-byte frame in ~1.07 µs
  (YJIT, best of 5 × 1 M), so building 1 000 frames per wake is ~1.1 ms: 5–12 % of the
  wake. The rest is the per-connection write (`IoWriter`, `write_nonblock`, the connection
  mutex) and the audience filter.
- **The heartbeat tick is 6–7 ms at 1 000 fast clients** (12 ms at 2 000; one 21.6 ms tick
  under load), serial on its own thread. Slow clients are #621's case.
- **The peer-close watcher's O(M) part is under 1.3 ms** per pass at 2 000 (the socket-set
  build; its `IO.select` wait is not counted).
- **A connection costs ~25 KB of server RSS.** The row figure carries a fixed ~1.7 MB
  (first-connection allocations), so the marginal number is (Δ at 1 000 − Δ at 100) / 900 =
  25.0–25.8 KB. 1 000 connections are ~27 MB on the web process; at the default
  `streams_max_connections` of 2 000 the bench measured 43–44 MB.

**Target for #621 (frame once per wake, one heartbeat frame per tick, copy-free registry
snapshots), derived from this baseline.** Framing is ~1.1 ms of the durable wake's 9.2–9.3 ms
at M = 1 000 and the heartbeat builds one comment String per connection per tick, so those
changes can remove about a ninth of the wake and part of the tick, not more. Measured
against this bench on the same machine at M = 1 000, durable: dispatcher wake p50
≤ 8.2 ms (baseline 9.2–9.3 minus the ~1.1 ms of framing); heartbeat tick ≤ 6.0 ms (baseline
6.7); fan-out last-client p99 no worse than the baseline's 35.4 ms (its 29–35 ms spread is
wider than the gain, so it gates regressions only). Ephemeral rows (wake spread 17–23 ms)
gate regressions only, and the profile explains the ephemeral/durable gap. The connect storm
(~255 ms per connect) is outside #621's scope and the largest number here. Skipping the
listener's ack for a channel already in `listening_to` (or waking the listener when a
command arrives) would bring 1 000 connects from ~255 s down to the dispatcher's own
per-connect cost: the step this baseline recommends next (both done in #630, "Connect storm"
below). Ractor-gate pre-read (#617: wake wall time above half the fan-out p99 at
1 000): durable 9.2 vs 14.5–17.7 ms, not met; ephemeral 17.4 vs 16.3 (round 1, met) and 23.0
vs 28.6 (round 2, not met); this bench does not profile, which is the gate's other half.

### Frame once per wake (issue #621)

Four changes to the streamer's per-wake and per-tick work, measured against the baseline
above: the dispatcher builds each envelope's SSE frame once per wake (`msg_id` → frozen
frame) and hands the same frozen Hash to every connection on the stream, inline or through
the pump, instead of every connection re-framing every envelope; `Envelope.data_lines` skips
its split/map/join for a one-line payload; the heartbeat builds one frame per tick and writes
it under `streams_fanout_write_deadline_ms` (250 ms) instead of the 5 s
`streams_write_deadline_ms`; the `Registry` hands out frozen snapshots it caches until the
next membership change instead of copying the connection list on every wake and tick. The
connect replay and the in-flight drain still frame per connection.

**Micro-benches** (no database, YJIT, `main` worktree and branch alternated, load 5.6–12.8 on
12 cores):

| bench | main | branch |
|---|---:|---:|
| durable wake, 3 broadcasts → 100 real `Connection`s (`streams_frame_bench`) | 377 µs, 2 641 objects | 102 µs, 256 objects |
| `Connection#enqueue`, frame built vs given (`streams_fanout_bench`) | 771 ns, 6 objects | 148 ns, 2 objects |
| `Envelope.message`, one-line payload | 990 ns, 8 objects | 620 ns, 4 objects |
| `Envelope.message`, multi-line payload | ~1 540 ns | ~1 520 ns |
| pgbus-owned objects per connection per wake (`fanout_allocation_budget_spec.rb`, gated `< 3`) | 22.5 | 1.8 |

The one-line check is two `String#include?` scans, not the regex `match?` the plan named:
232 vs 388 ns on a 170-byte line, and the regex check made multi-line payloads ~10 % slower
(match, then split), where `include?` stays within noise.

**The eager registry, superseded.** The first version replaced the frozen arrays on every
`register` / `unregister` and read them without a lock. Its first round showed RSS Δ/conn
36 KB at M = 1 000 against main's 26 KB, and a direct check put it at +35 MB of RSS and 80 ms
for 2 000 registers (main: +0.4 MB, 2.5 ms): a connect storm copies the M-element list M
times. The registry now builds a snapshot on the first read after a change and caches it
(+0.35 MB, 2.5 ms); that round is left out of the table below.

**1 000 clients** (`rake bench:streams_scale`, same machine and settings as the baseline,
2026-11-02, each round alternating main and branch; load average before each run in the
last column; `slow` rows add `STREAMS_SCALE_SLOW=20`; `×100` rows send 100 broadcasts
instead of 20). Every row: 0 missed broadcasts, 0 failed clients.

| mode | run | tree | fan-out last p50 / p99 (ms) | dispatcher wake p50 / p99 (ms) | heartbeat tick (ms) | RSS Δ / conn (KB) | CPU / wall | load |
|---|---|---|---:|---:|---:|---:|---:|---:|
| ephemeral | 1 | main | 37.8 / 51.6 | 27.4 / 36.2 | 8.8 | 27.6 | 10 % | 5.7 |
| ephemeral | 2 | main | 24.1 / 108.9 | 8.9 / 36.4 | 16.3 | 26.9 | 6 % | 5.2 |
| ephemeral | 2 | branch | 39.4 / 46.6 | 20.5 / 29.2 | 7.4 | 28.2 | 7 % | 2.0 |
| ephemeral | 3 | branch | 18.2 / 31.8 | 5.7 / 8.2 | 5.5 | 28.2 | 3 % | 5.6 |
| ephemeral | 3 | main | 26.4 / 48.6 | 15.3 / 23.0 | 12.7 | 27.0 | 6 % | 8.3 |
| durable | 1 | main | 32.9 / 58.6 | 20.3 / 40.0 | 5.8 | 26.1 | 10 % | 5.7 |
| durable | 2 | main | 49.5 / 65.1 | 33.8 / 37.0 | 25.4 | 26.9 | 12 % | 5.2 |
| durable | 2 | branch | 34.6 / 60.0 | 13.8 / 36.3 | 5.2 | 28.2 | 7 % | 2.0 |
| durable | 3 | branch | 33.4 / 60.2 | 16.3 / 19.0 | 15.2 | 28.4 | 6 % | 5.6 |
| durable | 3 | main | 33.0 / 58.4 | 15.3 / 40.1 | 6.2 | 27.3 | 7 % | 8.3 |
| durable | slow | main | 19.8 / 34.4 | 9.5 / 15.9 | 6.4 | 26.8 | 4 % | 4.8 |
| durable | slow | branch | 21.2 / 35.2 | 7.3 / 13.5 | 6.1 | 28.0 | 3 % | 4.2 |
| durable | ×100 #1 | main | 43.9 / 61.0 | 29.5 / 40.2 | 31.7 | 27.1 | 10 % | 4.2 |
| durable | ×100 #1 | branch | 20.1 / 27.2 | 6.9 / 12.5 | 5.3 | 28.5 | 3 % | 4.6 |
| durable | ×100 #2 | main | 22.9 / 56.4 | 9.2 / 28.3 | 6.6 | 26.3 | 4 % | 4.3 |
| durable | ×100 #2 | branch | 27.3 / 60.8 | 9.8 / 35.9 | 12.2 | 27.7 | 5 % | 5.0 |

What the numbers say:

- **This machine did not reproduce the baseline today, so the scale bench cannot settle the
  target.** Main's own durable wake p50 is 9.2–33.8 ms across six cells, against 9.2–9.3 ms
  in the baseline. The cells fall in two regimes, and the CPU / wall column tells them apart:
  at 3–5 % every number is near the baseline (main's wake 9.2–9.5 ms, branch's 6.9–9.8 ms), at
  6–12 % everything is 2–3× slower on both trees (the same work burning more CPU, which reads
  as the cell's threads landing on the efficiency cores, not as load average: the slowest main
  cell started at 4.2). Medians over every cell: durable wake p50 main 17.8 ms, branch 9.8 ms;
  heartbeat tick main 6.5 ms, branch 6.1 ms.
- **Against the targets**, read in the fast regime, where the baseline was measured: dispatcher
  wake p50 ≤ 8.2 ms — branch 6.9 and 7.3 ms in two of its three fast cells (9.8 in the third),
  main 9.2 and 9.5; heartbeat tick ≤ 6.0 ms — branch 5.3, 6.1 and 12.2 ms, main 6.4 and 6.6
  (one tick per cell, so a single sample each: met once, not settled). Fan-out last-client p99
  ≤ 35.4 ms, the regression gate: met by the branch in two of its three fast cells (27.2 and
  35.2 ms; 60.8 in the third) and by main in one of two (34.4; 56.4); not met by either tree in
  the slow regime (46–65 ms, main and branch alike). Since main misses the absolute gate as often
  as the branch today, the gate cannot be read as passed or failed on this machine; within each
  regime the branch is no worse than main. The per-wake saving is what the micro-bench shows: the
  ~1 ms of framing and copying per 1 000-connection wake is gone.
- **RSS per connection is 1–1.5 KB higher on the branch** (27.7–28.5 against 26.1–27.6 KB),
  within the spread of the lazy registry's own micro-check (+0.35 MB per 2 000 registers, the
  same as main). The bench reads RSS right after the connects without a GC, so fewer
  allocations during the storm (one heartbeat frame per tick instead of one per connection)
  also move when the next GC runs; this bench does not separate the two.
- **The slow-client rows do not exercise the heartbeat deadline.** Twenty clients reading one
  event a second never fill a socket buffer with 20 broadcasts and the heartbeat comment, so no
  heartbeat write blocks and both trees tick in ~6 ms. The bound (each heartbeat write gives up
  after 250 ms once it holds the connection's mutex, not 5 s) holds by construction, and `heartbeat_spec.rb`'s `:timeout` example
  proves the connection is dropped.
- **The profile** (`STREAMS_SCALE_PROFILE=<dir>`, new: vernier over the broadcast window, the
  dispatcher thread's running samples, branch, M = 1 000): durable — `io_writer.rb` 77.8 %
  inclusive, `IO#write_nonblock` 73.8 % self, framing 0.1 %, the audience filter 1.5 %,
  `read_after` 1.6 %, waiting for the GVL 0.3 % of running + stalled; ephemeral —
  `io_writer.rb` 89.0 %, `write_nonblock` 81.3 %, framing 0.2 %, JSON 0.0 %, GVL 0.8 %. The
  wake is the socket-write loop and nothing else.
- **The ephemeral/durable gap does not hold up.** The baseline's ephemeral wake was about twice
  durable's; today ephemeral ranges 5.7–27.4 ms and durable 6.9–33.8 ms, both following the
  regime, and the profile gives both the same composition (write loop ~80 %, no JSON or
  database share worth naming). The two baseline rounds most likely caught ephemeral cells in
  the slow regime.

**The #617 step-5 gate: shut.** The gate opens when, at M = 1 000 after these changes, the
dispatcher wake wall time is more than half the fan-out p99 *and* the profile's top frames are
in `io_writer.rb` / `write_nonblock`. The second half holds (74–81 % `write_nonblock`). The
first does not, in any branch cell: durable wake p50 against half the last-client p99 is 13.8
vs 30.0, 16.3 vs 30.1, 7.3 vs 17.6, 6.9 vs 13.6 and 9.8 vs 30.4 ms; ephemeral 20.5 vs 23.3 and
5.7 vs 15.9 ms; the profiled run 8.7 vs 21.5 (durable) and 6.4 vs 15.7 ms (ephemeral). Most of
the time to the last client is spent outside the dispatcher's wake (the NOTIFY round trip, and
1 000 clients parsed by 4 client processes), so parallelizing the write loop, by writer threads
or by Ractors, could remove at most the wake's share. The verdict is recorded with the Ractor
evaluations above.

### Connect storm (issue #630)

The baseline above named the connect storm as its largest number: every SSE connect cost one
`streams_listen_health_check_ms` cycle (250 ms) on the single dispatcher thread, so 1 000
clients took ~255 s to register and none got its `pgbus:connected` frame or a heartbeat before
it did (the response headers, `retry:` and the opening comment go out first). Two causes, two
changes:

- **A. No listener wait for a stream that already has a subscriber.** `handle_connect` called
  `Listener#ensure_listening` for every connect, and the listener acked it only when its
  current wait ended, even for a channel it already LISTENed on. It now calls it only when
  `@registry.empty?(stream)`. `remove_listening` is issued from one place,
  `cleanup_stream_if_unused`, and only when the registry holds no connection on the stream, so
  a registered connection proves the stream's last `ensure_listening` was issued after its last
  `remove_listening`, and the listener runs commands in order. When that `ensure_listening` was
  acked, the LISTEN is active; when it timed out (a stuck listener), the LISTEN is still queued
  ahead of any UNLISTEN and runs once the listener recovers, the same window that first connect
  had. An empty stream always takes the slow path. A connect while the listener reconnects keeps
  the fast path: waiting there would cost every connect the ack budget for as long as the
  database is unreachable, and its exposure is that of every registered connection (a durable
  stream's next wake re-reads from the min cursor).
- **B. The listener drains commands on a self-pipe wake.** The listener thread parked in
  `wait_for_notify(health_check_ms)` and drained its command queue only between waits, so a
  stream's first connect, every unlisten and every master-hub sub waited out the current wait.
  It now parks in `IO.select` on the LISTEN socket and a wake pipe (the `PeerCloseWatcher`
  pattern); `ensure_listening`, `remove_listening` and `stop` write a byte. It keeps pg's
  `wait_for_notify` order (buffered notifications, the socket, `consume_input`, the buffer
  again), and the keepalive runs on a deadline: one `SELECT 1` per idle cycle, scheduled from
  the end of the last one, which a stream of wakes cannot postpone. Only the pipe crosses
  threads (#375). `PG::Connection#notifies` reports a NOTIFY without payload as `extra: ""`
  where `wait_for_notify` yielded `nil`; the loop maps it back so durable wakes stay durable.

The worker-side `Process::NotifyListener` keeps its `wait_for_notify` loop on purpose: its
subscriptions change only at boot and on a wildcard refresh, so no command waits on it.

`STREAMS_SCALE_STREAMS=N` (new) deals the clients over N streams round-robin and publishes each
broadcast sample to all N in turn, its fan-out timed from the first publish. With one stream only
the first connect touches the listener, so that row shows A; with 100 streams there are 100 first
connects, each a listener round trip, so that row is the one that exercises B. `main` has no such
knob: its worktree ran the branch's bench files (nothing under `lib/`).

Measured 2026-11-02, same machine and settings as the baseline (Apple M2 Max, 12 cores,
PostgreSQL 18 on the same machine, Ruby 3.4.2 with YJIT, Puma 8.0.2, every streams setting at its
default), `main` and the branch alternated, each on its own database. Two rounds; load average
before each run: main 19.8 and 6.3 (one stream), 10.7 and 7.3 (100 streams); branch 4.1 and 11.3
(one stream), 5.4 and 8.4 (100 streams). Ranges are the two rounds. Every row: 0 missed
broadcasts, 0 failed clients.

**M = 1 000**

| mode | streams | tree | connect all (s) | connect p50 (s) | `handle_connect` p50 / p99 (ms) | fan-out first p50 (ms) | fan-out last p50 / p99 (ms) | retried connects |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| ephemeral | 1 | main | 255.3 | 127.8–128.0 | 254.1–254.2 / 308–315 | 0.7–0.9 | 16.7–24.8 / 31.6–43.7 | 811–906 |
| ephemeral | 1 | branch | 4.49–6.58 | 2.31–2.35 | 0.1–0.2 / 1.9–4.7 | 0.5–1.3 | 16.0–18.4 / 32.5–44.0 | 831–869 |
| durable | 1 | main | 255.6–259.3 | 127.3–132.4 | 254.1–255.3 / 261–431 | 1.9–4.6 | 20.5–22.2 / 33.7–45.0 | 632–821 |
| durable | 1 | branch | 4.46–4.56 | 2.31–2.36 | 0.1–0.2 / 3.3–3.7 | 2.5–4.3 | 19.1–20.5 / 36.4–59.2 | 676–821 |
| ephemeral | 100 | main | 259.3–260.2 | 130.7–131.9 | 255.6–257.2 / 372–410 | 0.6–1.1 | 28.0–39.8 / 46.0–80.4 | 683–729 |
| ephemeral | 100 | branch | 4.49–4.53 | 0.28–2.30 | 0.2–0.3 / 3.1–3.6 | 0.7–0.8 | 28.6–28.8 / 38.9–59.7 | 711–748 |
| durable | 100 | main | 255.3–266.9 | 127.9–133.8 | 254.4–254.7 / 278–618 | 1.8–4.1 | 72.5–89.4 / 197–829 | 691–897 |
| durable | 100 | branch | 4.51–4.53 | 2.32–2.34 | 0.2–0.3 / 4.5–4.6 | 2.4–6.4 | 97.6–128.5 / 120–215 | 732–826 |

**M = 100** (no connect is retried at this size)

| mode | streams | main: connect all (s) | branch: connect all (s) | main: `handle_connect` p50 (ms) | branch: `handle_connect` p50 (ms) |
|---|---:|---:|---:|---:|---:|
| ephemeral | 1 | 25.34–25.43 | 0.08–0.10 | 253.7 | 0.2 |
| durable | 1 | 25.17–25.80 | 0.08–0.09 | 254.0–254.3 | 0.1–0.3 |
| ephemeral | 100 | 25.28–25.71 | 0.16–0.17 | 255.0–257.1 | 0.7–1.0 |
| durable | 100 | 25.47–27.14 | 0.15–0.16 | 256.3–258.1 | 0.7–0.8 |

`rake bench:streams` section 4 (K concurrent connects on one stream, `streams_listen_health_check_ms`
100 in that bench) returned at the response headers, which `StreamApp` writes before the
dispatcher registers the connection, so it showed ~0.6 ms per connect on `main` as well; it now
waits for every client's `pgbus:connected` frame. Two rounds each, same bench file on both trees
(load 7.6–12.1): K = 50 registers in 5 181–5 191 ms on `main` (~104 ms per connect, one cycle
each) and in 49–64 ms on the branch; K = 5 / 20 / 50 cost 102–107 ms per connect on `main` and
1.0–2.6 ms on the branch. Its single-broadcast roundtrip, the NOTIFY → dispatcher → client path
the listener rewrite touched, is 11.7–12.7 ms median on both trees.

What the numbers say:

- **The storm is gone.** At M = 100, where the kernel resets no connect, every client registers
  in 0.08–0.17 s instead of 25 s. At M = 1 000 the branch registers everyone in 4.5–6.6 s instead
  of 255–267 s, and the dispatcher's share of that is `handle_connect` p50 0.1–0.3 ms, about 0.3 s
  for all 1 000. The rest is macOS's accept queue: `somaxconn` 128 resets 676–869 of the 1 000
  connects, and those clients retry after the 2 s `retry:` an EventSource would wait (connect p99
  ≈ 4.5 s, one or two retry clocks; connect p50 2.3 s in seven of the eight branch cells and
  0.28 s in one, where the median client was not reset). Linux's default of 4 096 resets none.
- **B is what the 100-stream rows show.** At M = 100 over 100 streams every connect is a stream's
  first, so every one goes to the listener: 0.7–1.0 ms each on the branch, 255–258 ms on `main`.
  At M = 1 000 the 100-stream rows land at 4.49–4.53 s, inside the one-stream rows' 4.46–6.58 s;
  without B the 100 first connects alone would add 100 × 250 ms = 25 s.
- **Against the targets** (#617's comment, restated in #630): 1 000 connects register in under
  10 s, one stream and 100 streams, both modes: met (4.46–6.58 s). The 100-stream row within 2 s
  of the one-stream row: met. `handle_connect` p50 at the cost of one `read_after` plus the replay
  write: 0.1–1.0 ms.
- **Fan-out is a regression gate, and it holds.** The first-client p50, the NOTIFY → dispatcher
  path the select loop replaced, is 0.5–6.4 ms on the branch against 0.6–4.6 ms on `main`, and the
  roundtrip above is unchanged. Last-client p99 is the slowest of 20 broadcasts and both trees
  throw outliers: `main` 829 ms in one durable 100-stream cell, the branch 1 571 ms in a durable
  100-stream cell at M = 100. The durable 100-stream rows sit 8–56 ms higher on the branch at
  p50 (M = 100: 94.4–99.6 against 81.8–82.6 ms); swapping the two databases moved that gap with the database, not the code (M = 100,
  durable, 100 streams, two runs per pairing, load 6.3–19.1: on `main`'s database 91.7 and
  106.8 ms for `main`, 86.8 and 85.1 ms for the branch; on the branch's database 124.6 and
  120.4 ms for `main`, 116.8 and 121.8 ms for the branch). That cell is load-sensitive on both
  trees: one of `main`'s runs on its own database at load 12.3 missed 64 broadcasts. In those rows
  the last client's time includes the bench publishing 100 broadcasts in turn.
- **RSS per connection reads lower on the branch** (15–16 against 28–29 KB at one stream,
  6.8–8.4 against 16–19 KB at 100 streams). The bench samples RSS right after the connects
  without a GC, and the storm now lasts seconds instead of minutes, so this is when the next GC
  runs, not a per-connection saving; the bench does not separate the two.
- **`rake bench:streams_hub`** (the hub's broadcast round trip, a regression gate; it does not time
  `handle_sub`, which B also shortens): p50 13.3–16.7 ms on `main` and 13.9–16.4 ms on the branch,
  `:process` and `:master` alike, over 11 runs per tree. Its client polls every 10 ms, so the
  figure is mostly that poll. The first five alternated runs had the branch 0.3–2.2 ms slower;
  six more per tree, alternated with each tree on both databases, put both at 16.0 ms in most
  runs, and in the last round the branch was the faster (13.9–14.9 against 15.2–16.0 ms).
- **`rake bench:notify_wake`** (control: the worker-side `NotifyListener` is untouched): direct
  listener p50 0.93 ms / p99 4.81 ms, hub → pipe p50 0.81 ms / p99 11.0 ms, load 5.4.

### Execution mode and DB pool sizing (threads vs async)

**Offered concurrency is not connections held.** A worker's `threads:` setting is
how many jobs run at once; `resolved_pool_size` is how many *DB connections* the
pgmq pool holds. They are not the same number, because a job holds a pgmq
connection only for the `read_batch` + `archive` SQL round-trip — `perform_now`
runs with **zero** pgmq connections checked out (`executor.rb`). If your job does
its own database work, that uses ActiveRecord's *separate* pool, not this one.

This means the folklore "async saves connections" is only half true, and the
`benchmarks/execution_modes_bench.rb` harness (`rake bench:execution_modes`,
requires `PGBUS_DATABASE_URL`) measures exactly when it holds. It runs the same
offered load (240 jobs at concurrency 12) through both pools and samples
`peak_busy = pool size − available` — the live checkout count — going through the
real pool, so pool *sharing* is what's measured. Representative local numbers:

| mode | pool | io profile | peak_busy | throughput | note |
|------|------|-----------|-----------|-----------|------|
| threads | 12 | io_light | 12 | ~216 job/s | baseline: ~1 conn per busy thread's checkout window |
| async | 3 | io_light | **3** | ~222 job/s | **12 fibers sustained on 3 connections, full throughput** |
| threads | 3 | io_light | 3 | ~208 job/s | short 10 ms checkouts cycle fast enough that 3 conns serve 12 threads too |
| threads | 12 | db_bound | 12 | ~320 job/s | connection-bound — one conn per concurrent DB call |
| async | 12 | db_bound | 12 | ~324 job/s | async matches threads when the pool is sized right |
| async | **3** | db_bound | 3 | **~95 job/s** | **under-provisioned: throughput collapses ~3.4×** (p50 37 ms → 125 ms) |

**What the numbers actually say:**

- **Async's connection-density win is real for I/O-light work** — where a job
  spends most of its time *outside* the checkout (HTTP calls, app compute, waits).
  There, a handful of connections serves many fibers with no throughput loss. This
  is why async workers are auto-sized to a flat `ASYNC_POOL_CONNECTIONS` (3) per
  capsule rather than one-per-fiber.
- **For DB-bound work, async is connection-bound just like threads.** A fiber
  holds a connection for the whole SQL call (a blocking libpq call does not yield
  the reactor), so a too-small async pool doesn't share — it **serialises**, and
  throughput collapses.
- **Under-provisioning degrades throughput; it does not necessarily error.**
  Because `pool_timeout` (5 s) dwarfs a typical checkout (10–30 ms), a
  too-small pool rarely times out — it just serialises work behind the available
  connections. Watch `throughput` and p95/p99 latency, not just error counts.

**Sizing guidance:**

| mode | recommended `pool_size` | under-provision symptom | over-provision cost |
|------|------------------------|-------------------------|---------------------|
| threads | ≈ `Σ worker threads` (+ dispatcher/scheduler/consumers) — the auto-tuned default | throughput collapse; eventually `pool_timeout` (`enrich_pool_timeout_error`), `available → 0` | wastes Postgres `max_connections`; `warn_if_oversized` fires above 50 |
| async | `ASYNC_POOL_CONNECTIONS` (3) for **I/O-light** work; **≈ your peak concurrent DB calls** for **DB-bound** work — measure with `rake bench:execution_modes` | reactor fibers serialise on checkout → throughput collapse (harder to spot; no error) | forfeits the density win async exists for |

This is a connection-**density** tool, not a throughput tuner: a smaller pool means
fewer Postgres backends for the same offered load, not faster jobs (PGMQ round-trip
and job I/O dominate wall time). The numbers are single-box, single-process, no
PgBouncer — a per-process, per-io-profile sizing guide, not a production capacity
guarantee. Budget `resolved_pool_size + streams_pool_size` per forked process (see
Capacity planning above).

### Reading the output

`benchmark-ips` reports **i/s** (iterations per second — higher is better).
`memory_profiler` reports **objects/bytes allocated** (transient GC pressure) and
**retained** (objects that survive — a steady climb here is a leak). For a
`send_message` call, *retained should be 0*; non-zero retained is the smell the
allocation budget specs catch.

### Measure BEFORE you change

The first rule: capture the baseline **before** touching code, or you can't
claim a delta. The cleanest way is an isolated worktree so `main` and your
branch run the *same script* on the *same machine*:

```bash
git worktree add --detach /tmp/pgbus-baseline main
cp -r benchmarks /tmp/pgbus-baseline/ && cp Gemfile Rakefile /tmp/pgbus-baseline/
(cd /tmp/pgbus-baseline && bundle install --quiet && bundle exec rake bench:all) > /tmp/before.txt
bundle exec rake bench:all > /tmp/after.txt
diff /tmp/before.txt /tmp/after.txt
git worktree remove --force /tmp/pgbus-baseline
```

There is no committed baseline file (shared CI runners are too noisy for a hard
regression gate), which is exactly why the before/after has to be a deliberate
same-machine measurement, not a comparison against a number from another box.

## Allocation budgets

The spec suite includes allocation budget tests (`spec/pgbus/allocation_budget_spec.rb`)
that enforce hard limits:

| Operation | Budget |
|-----------|--------|
| `Client#send_message` | < 50 objects/call |
| `Client#send_batch` (per item) | < 25 objects/item |
| `Client#read_batch` | < 30 objects/call |
| JSON round-trip | < 20 objects |
| Retained objects (leak detection) | 0 across 100 cycles |
| `Executor#execute`, pgbus-owned objects only, PostgreSQL-shaped `enqueued_at` (`executor_allocation_budget_spec.rb`) | < 10 objects/job (measured 9; 10 before issue #604) |

These run as part of `bundle exec rspec` on every PR. They are hard gates — a
regression that exceeds the budget fails the build.

## CI

The `bench` job in `.github/workflows/main.yml` runs the unit benchmark suite
on every PR and **uploads the report as the `benchmarks` artifact**. It is
**run-and-report, never a hard fail** — it surfaces trends, it does not gate
merges on a flaky threshold. Download the artifact from the PR's checks tab to
see the numbers for that branch.

The allocation budget specs in the `test` job ARE a hard gate — those fail the
build if allocations regress past the budget.

## Adding a benchmark

A new hot path gets a new `benchmarks/<name>_bench.rb`. Use the shared harness:

```ruby
require_relative "bench_helper"

BenchSupport.header("my hot path")
BenchSupport.ips { |x| x.report("thing") { thing_under_test } }
BenchSupport.allocations("thing") { thing_under_test }
```

For unit-level benches that should run in CI, add the filename to the
`unit_benches` list in the `Rakefile`'s `bench` namespace.
