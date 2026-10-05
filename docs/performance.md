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
  matches the baseline within the run-to-run spread in every cell. It still
  routes each claim through the buffer (one heartbeat hold and release per
  message), and that cost does not show.
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
  pays.

### Native compilation (Spinel, Roundhouse) and Rust

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
| `Executor#execute`, pgbus-owned objects only (`executor_allocation_budget_spec.rb`) | < 10 objects/job (measured 8) |

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
