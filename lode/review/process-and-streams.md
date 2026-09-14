Rules about threads that own a `PG::Connection`: who may close it, how long a
stop may wait, and what a lifecycle test has to prove. See
[../process/summary.md](../process/summary.md) and
[../streams/summary.md](../streams/summary.md).

### A `PG::Connection` has one owner thread, and only that thread closes it
- **Holds because:** `PG::Connection#close` is `PQfinish` — it frees the PGconn and its OpenSSL state. Freeing it while another thread is inside `wait_for_notify`, `PQsendQuery` or any other libpq call is a process-killing SEGV, not a rescuable `PG::Error` (issue #375). So `#stop` clears the running flag and joins; it never touches the connection. The listener thread closes it in its own `ensure`, after its operations have finished. There is also no UNLISTEN round-trip on teardown — closing the session deregisters the LISTENs, and the round-trip is what raced the stopper's `PQfinish` into a SEGV.
- **Safe direction:** a wedged wait that survives until process exit is an accepted residual; force-closing to unblock it reintroduces the race.
- **Where:** `lib/pgbus/web/streamer/listener.rb#stop`, `#run_loop` (its `ensure`), `#close_quietly`; the same shape in `lib/pgbus/process/notify_listener.rb`
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"closes the connection only from the listener thread, never from the stopping thread"`, `:"never execs on a connection that has already been closed"`, `:"skips the UNLISTEN round-trip on teardown (closing the session deregisters LISTENs)"`, `:"releases the listener's connection exactly once so the LISTEN slot is freed"`
- **Origin:** cubic learnings 5ec66f53 and 5b5832ab (duplicates of one rule, merged here)

### A stop join budget is sized from the wait it is joining, never flat
- **Holds because:** the listener thread can be parked in one full `wait_for_notify(health_check_ms)` before it re-reads the cleared flag, so a flat 5-second timeout expires before a listener with a large `health_check_ms` has had a single chance to observe the stop — leaving shutdown handling premature or the worker orphaned. Both listeners use `health_check_ms / 1000.0 + STOP_JOIN_GRACE_SECONDS` (5). The same reasoning gives `ack_timeout` = `health_check_ms / 1000.0 + 1.0`.
- **Where:** `lib/pgbus/web/streamer/listener.rb#stop_join_timeout`, `#ack_timeout`; `lib/pgbus/process/notify_listener.rb#stop_join_timeout`
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"joins the listener thread within one health-check cycle"`, `:"spawns a thread on start and joins it on stop"`
- **Origin:** cubic learnings ff587565 and c86c6f17 (duplicates of one rule, merged here)

### A thread reference outlives `#stop` so a timed-out join stays observable
- **Holds because:** clearing the reference inside `stop` hides exactly the failure the caller needs to see — a listener thread that did not finish. `#threads` reports the live thread while running and none after stop, which is what `Pgbus::Testing`'s `StreamerLeakError` check reads (issue #443).
- **Where:** `lib/pgbus/web/streamer/listener.rb` (`#threads`, `#stop`); `lib/pgbus/testing.rb` (`StreamerLeakError`)
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"exposes its thread via #threads while running and none after stop (issue #443)"`
- **Origin:** PR #448

### A listener health-check failure rebuilds a **fresh** connection and re-LISTENs every known channel
- **Holds because:** a silently dropped LISTEN connection (NAT, a PG restart, a network blip) delivers nothing and raises nothing. `wait_for_notify` returning nil triggers a `SELECT 1` keepalive; if that raises, the listener builds a new connection through the injected `connection_factory` — a fresh connect, not a reset, so DNS is re-resolved and the listener converges on a promoted primary after a failover — and re-LISTENs `@listening_to`. A re-LISTEN that raises mid-loop must not shrink the canonical subscription set.
- **Where:** `lib/pgbus/web/streamer/listener.rb#run_loop`, the reconnect path (`RECONNECT_BACKOFF_SECONDS` 0.5)
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"runs SELECT 1 when wait_for_notify times out"`, `:"rebuilds a fresh connection and re-LISTENs every previously-known channel"`, `:"preserves the canonical subscription set when a re-LISTEN raises mid-loop"`
- **Origin:** the design doc §11 note quoted in the class comment

### A durable wake may be dropped under backpressure; an ephemeral one never may
- **Holds because:** a durable wake is a hint — the frame is in PGMQ and the next wake (or a replay from the cursor) still delivers it. An ephemeral wake *is* the frame: dropping it loses the message outright. The listener's queue limit therefore applies only to wakes with a nil payload.
- **Where:** `lib/pgbus/web/streamer/listener.rb` (the wake-enqueue path)
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"drops a durable wake (payload nil) when the queue is at/over the limit"`, `:"NEVER drops an ephemeral wake (payload present) even at the limit"`, `:"enqueues every wake when the limit is 0 (unbounded, the default)"`
- **Origin:** the listener's own contract, confirmed while reviewing the shutdown work

### A LISTEN lifecycle is exercised across repeated start/stop cycles, not one shutdown
- **Holds because:** the code is reusable worker-lifecycle code, and a leak that only shows on the second cycle is invisible to a one-shot teardown test. The check is that no LISTEN backend or session resource remains after repeated cycles — `Pgbus::Testing`'s streamer-leak assertion exists for exactly this.
- **Where:** `lib/pgbus/web/streamer/listener.rb`, `lib/pgbus/web/streamer/instance.rb`; `lib/pgbus/testing/assertions.rb`
- **Proven by:** `spec/pgbus/web/streamer/listener_spec.rb:"releases the listener's connection exactly once so the LISTEN slot is freed"`; the repeated-cycle stress lives in the integration streams specs
- **Origin:** cubic learning 9048d10e

### A shutdown test asserts the ownership contract, not the final symptom
- **Holds because:** a symptom-only test ("it eventually stopped") passes against a use-after-free, a post-close query and a multi-second hang alike. The tests assert which thread closed the connection, that nothing execs after close, and that the join returns inside one health-check cycle — and the fakes model a real blocking wait with a real timeout, so an unrealistic fake cannot hide a hang.
- **Where:** `spec/pgbus/web/streamer/listener_spec.rb`
- **Proven by:** itself — the three examples named in the single-owner rule above
- **Origin:** cubic learnings 4bcb656f and d7720197 (duplicates of one rule, merged here)

### A broadcast inside an open ActiveRecord transaction is deferred to `after_commit`, and the probe never checks out a connection
- **Holds because:** a client must never see a change the database rolled back. The probe reads `ActiveRecord::Base.connection_pool.active_connection?` — the lease this thread already holds — because both `ActiveRecord::Base.connection` and a `with_connection` block leaked the caller's connection when its thread died, deterministically exhausting an exactly-sized pool (Zazu fan-out incident, 2026-08-05). Semantics are unchanged: a transaction is per-lease, so a thread holding no connection can have no open transaction to defer on. The ephemeral path never deferred (pg_notify runs on the PGMQ pool connection, outside the request's transaction), so `#durable_fallback` does not defer either.
- **Where:** `lib/pgbus/streams.rb#broadcast`, `#current_open_transaction`, `#durable_fallback`
- **Proven by:** `spec/pgbus/streams_spec.rb:"defers send_stream_message until the transaction commits"`, `:"accumulates multiple broadcasts and fires them all on commit in order"`, `:"does NOT submit if the transaction never commits (rolls back)"`, `:"submits an ephemeral coalesced frame immediately, even inside the transaction"`
- **Origin:** the method's own comment; the accessor choice is the same one `EventBus::StaleConnectionRetry#reconnect_leased!` makes

### `IO#timeout=` is core Ruby, not an extension — never `require "io/timeout"`
- **Holds because:** `IO#timeout=` is a C method on `IO` added in Ruby 3.2 (Feature #18630), and the gemspec's floor is 3.3. There is no `io/timeout` file to require, so adding the require raises `LoadError` when `health_server.rb` loads and takes the supervisor's whole health surface down. A one-line comment at the call site records this so the suggestion is not re-made.
- **Where:** `lib/pgbus/web/health_server.rb#handle_client`
- **Proven by:** `spec/pgbus/web/health_server_spec.rb` drives a real `TCPSocket` against a real bound port; a `NoMethodError` on the setter would hang the silent-client example rather than pass it
- **Origin:** cubic learning b0c1fa73; PR #456 (a rejected review suggestion — see the *Not a bug* entry in [testing.md](testing.md))
