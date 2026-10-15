# Concurrency in pgbus

pgbus runs threads by design: a worker process has a claim loop, an execution
pool and several helper threads, and a web process running streams has a LISTEN
thread and a dispatcher. This page lists them, says which state is shared and
how it is guarded, what is fiber-local, and how far pgbus is from running parts
of itself inside a Ractor. It is held to these rules by
[conformance-kit](https://github.com/zoolutions/conformance-kit) (issue #527):
the `rubocop-thread_safety` cops, thread and fiber race specs, a Ractor canary
and `conformance-audit --strict`.

## Threads per process

Every thread has one owner that starts it once and a `stop`/`shutdown` that
joins it. The inline `# rubocop:disable ThreadSafety/NewThread` next to each
`Thread.new` names both.

| Process | Thread | Owner, and how it stops |
|---|---|---|
| Worker, consumer | the claim loop (main thread) | `Worker#run` / `Consumer#run`; TERM drains, QUIT stops |
| | execution pool: N pool threads (`:threads`) or one Async reactor thread (`:async`) | `ExecutionPools::ThreadPool` / `AsyncPool`; `shutdown` + `wait_for_termination`, `kill` as the last resort |
| | `pgbus-visibility-heartbeat` | `VisibilityHeartbeat`, started by the first tracked job; `Worker#shutdown` calls `stop` |
| | notify listener (fork scope) or wake pipe watcher (supervisor scope) | `Process::NotifyListener` / `Process::WakePipe`; `stop` joins it |
| | process heartbeat (`Concurrent::TimerTask`) | `Process::Heartbeat`; `stop` |
| Supervisor | the notify hub's one listener (supervisor scope: one LISTEN connection per host, forks are woken over pipes) | `Process::NotifyHub`; `stop` joins it |
| | health server accept loop | `Web::HealthServer`; `stop` joins it, then kills it if it hangs |
| Web (streams) | LISTEN thread, dispatcher, heartbeat, peer-close watcher, outbound pump writers | `Web::Streamer::Instance`, built once per process under `Streamer`'s lock; `shutdown!` stops and joins them |
| | hub client reader (Puma cluster mode) | `Web::Streamer::HubClient`; ends with its socket |
| Puma master | hub accept / fanout / status loops, one reader and writer per worker | `Web::Streamer::MasterHub`; `stop` joins them |

**Immediate shutdown (QUIT).** A worker or consumer stops taking work, gives the
running jobs `config.immediate_shutdown_grace` seconds (default 2) to finish,
and only then kills the pool. Both pool types wait for the killed jobs to
unwind, and every ActiveRecord connection whose thread or fiber is gone is
reaped, so none stays checked out by a dead job.

## Shared state, and how it is guarded

- **Built at load, never by a lazy `||=`.** A memo two first callers can race
  builds two objects: two clients (one pool orphaned), two filter registries
  (a registered filter lost), two Mutexes (no exclusion at all). Module state
  is created in the module body: `Pgbus.configuration`, the stream cache, the
  loaders, `Streams.filters` and `.coalescer`, `VisibilityHeartbeat`'s lock and
  entry table, every handler class's dedup cache. `Pgbus.client` is the one
  lazy memo left (it resolves connection options, which a `rake db:` task must
  not need), and it is built under a lock.
- **Locks live in class or module instance variables, never in constants**: a
  constant holding a Mutex cannot be read from a non-main Ractor.
- **Check-then-act is atomic**: installing the metrics and AppSignal
  subscribers, `DedupCache#mark!`, the warn-once flags.
- **Memos handed to other threads are frozen and replaced whole**, never
  mutated in place (`StreamQueue.all_names`), so a caller iterating one can
  never see it change.
- **Idempotent schema probes** (does this table or column exist yet?) may race:
  both callers store the same answer. Their inline disables say so.

## Fiber-local state, and the hop rule

`Thread.current[]` is fiber-local on every Ruby, and pgbus relies on that for
state that belongs to one unit of work:

- the broadcast options a Turbo helper passes down (`Streams::BroadcastOpts`:
  `durable:`, `exclude:`, `visible_to:`, `event:`, `coalesce:`);
- the batch a job is enqueued into, and a job's retry bookkeeping
  (`Batch`);
- the uniqueness key an enqueue acquired (`ActiveJob::Adapter`);
- the per-request watermark cache (`Streams::WatermarkCacheMiddleware`);
- a block-form `Pgbus::Testing.fake! { }`.

**The hop rule**: these values are visible only in the fiber (and thread) that
set them. Hop to another fiber or thread between setting and reading, for
example by broadcasting from an `Async` task spawned inside a `BroadcastOpts`
block, and the options are gone. That is by design: a sibling Async task must
not see another task's options. Set them in the fiber that broadcasts.

Process-wide settings are the opposite: the test mode set by `fake!` without a
block, the configuration, the filter registry. They are visible from every
thread and fiber.

Async execution mode needs `config.active_support.isolation_level = :fiber`
(see the README's "Async execution mode"), so ActiveRecord connections and
`CurrentAttributes` are fiber-scoped too.

## Ractor readiness

Nothing in pgbus runs in a non-main Ractor today, and nothing has to: jobs are
ActiveRecord-bound, and the loops are IO-bound and release the GVL. What is in
place:

- **Finalized at boot.** `Pgbus.finalize!` (the engine's `after_initialize`)
  freezes every plain-data setting into a Ractor-shareable value. Callables and
  the logger stay on the main Ractor.
- **A shareable seam.** The values the LISTEN thread hands the dispatcher, and
  the frames the dispatcher hands a connection (`WakeMessage`,
  `StreamEnvelope`, `ReadAfter::Envelope`), are `Ractor.shareable?`, so a
  future Ractor dispatcher could pass them by reference.
- **A canary** (`spec/pgbus/ractor_canary_spec.rb`, Ruby 4.0): stream names,
  stream-key fragments, durable and presence pattern resolution, the event
  serializer and the dispatch seam run inside a non-main Ractor.
- **The audit**: `bundle exec conformance-audit --strict` reports every constant
  and class/module instance variable under `Pgbus` that is not shareable. What
  is main-only by design is listed in `.conformance.yml` with its reason.

The blockers, in order:

1. `Pgbus.configuration` is main-only: it holds the logger and the app's
   callables, so a non-main Ractor cannot read it, and with it anything that
   reads a setting (`Streams::Key.stream_key` checks the queue-name budget
   against `queue_prefix`). Lifting this needs a shareable configuration
   snapshot for non-main Ractors.
2. The PGMQ client and its connection pools are per process.
3. ActiveRecord itself is reachable from non-main Ractors only through Rails'
   own proxying, which is not there yet.
