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

## The hot paths

| Path | When it runs | Bench |
|------|--------------|-------|
| `Client#send_message` / `#send_batch` | every job enqueue | `client_bench.rb` |
| `Client#read_batch` | every worker poll cycle | `client_bench.rb` |
| `Client#archive_message` | every job completion (a native prepared statement, issue #605) | `worker_profile_bench.rb` (`WP_BENCH_ROLE=fleet`) |
| `Client#read_batch_fair` | every worker poll cycle when `config.fair_share` is set (issue #426) | `fair_read_bench.rb` |
| `Executor#execute` | every job execution (deserialize + dispatch + archive) | `executor_bench.rb` |
| JSON serialization | every enqueue/dequeue (payload encoding) | `serialization_bench.rb` |
| Connection pool checkout | every PGMQ operation under concurrency | `connection_pool_bench.rb` |
| Execution pool dispatch | every job dispatched to thread/async pool | `execution_pool_bench.rb` |
| SSE streaming | every Turbo Stream broadcast | `streams_bench.rb` |
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
restore the pre-fix timing.

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
