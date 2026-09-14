What a pgbus spec is allowed to claim, and the two review findings that were
rejected with a reason. See
[../testing-and-ci/summary.md](../testing-and-ci/summary.md).

### A unit spec that stubs the query may only claim the lookup, not the behaviour
- **Holds because:** an example that asserts "the sweep asked the model for `promotable_keys`" proves nothing about starvation — the behavioural claim depends on the SQL, which the stub replaced. Such an example is named for what it checks ("asks the model for promotable keys, not for every parked key") and points at the integration spec that runs the real scan; the starvation claim lives there.
- **Where:** `spec/pgbus/concurrency/blocked_execution_spec.rb`; `spec/integration/concurrency_block_durability_spec.rb:"skips keys whose slots are all held, so a promotable key behind them is still serviced"`
- **Proven by:** itself
- **Origin:** cubic learning f7595c37; PR #460

### A fake rejects what production would reject
- **Holds because:** a fake that swallows extra keywords into `**` cannot fail when the thing under test leaks one, so the "does not leak into turbo's rendering kwargs" examples were vacuous — they passed with `BroadcastOpts.extract!` removed. `FakeTurboStreamHelpers#reject_leaked_kwargs!` raises on any key in `Pgbus::Streams::BroadcastOpts::KEYS`. It is deliberately a **denylist** of pgbus's own option names rather than an allowlist of turbo's: that is the invariant under test ("no pgbus option reaches turbo"), and an allowlist would false-fail on `broadcast_refresh_to`, which takes arbitrary keys as HTML attributes. Asserting on the rendered content instead would not help — the fake builds that string from `action` and `target` alone and never reads the rest.
- **Where:** `spec/support/fake_turbo_stream_helpers.rb#reject_leaked_kwargs!`; `spec/pgbus/streams/turbo_broadcastable_spec.rb`, `spec/pgbus/streams/broadcastable_override_spec.rb`
- **Proven by:** mutation-tested — with `BroadcastOpts.extract!` removed the spec fails with `ArgumentError: leaked into turbo's rendering kwargs: [:coalesce]`
- **Origin:** PR #466

### A fake that stands in for a real conversion calls the real conversion
- **Holds because:** `TurboBroadcastable#pgbus_coalesce_target` resolves the coalescing key through the fake's `convert_to_turbo_stream_dom_id`, so an approximation lets a spec assert a key production never produces — `Admin::Order` keys on `admin_order_7`, a Class target on `new_order`. The fake is verbatim from `Turbo::Streams::ActionHelper` and delegates to the real `ActionView::RecordIdentifier.dom_id`, so there is no divergence left to drift.
- **Where:** `spec/support/fake_turbo_stream_helpers.rb#convert_to_turbo_stream_dom_id`
- **Proven by:** `spec/pgbus/streams/turbo_broadcastable_spec.rb` pins `order_7`, `admin_order_7` and `new_order`
- **Origin:** cubic learning 3675ad7f; PR #466

### A double derives a physical queue name the way production does
- **Holds because:** hardcoding the suite's prefix makes the double agree with the code no matter what the code does to prefixes or priority suffixes. `PgmqDoubles`' `target_queue` builds the name from `Pgbus.configuration.queue_prefix`, `priority_levels` and `default_priority`, and bypasses suffixing only for a name that both **starts with the configured prefix** and already ends in `_pN` — ending in `_pN` alone is not enough, since a logical queue may legitimately be named `orders_p0`.
- **Where:** `spec/support/pgmq_doubles.rb`
- **Proven by:** itself — `spec/pgbus/active_job/adapter_spec.rb:"sends one batch per priority level so priority routing is preserved"` reads through it
- **Origin:** cubic learnings 4d1512cf, 043e2ddd; PR #420

### Every shared override is snapshotted and restored at the scope it was set
- **Holds because:** a value set in `before(:all)` and left set makes every later example and suite order-dependent, and integration runs are where that surfaces as an unreproducible failure. Snapshot each overridden value and restore each one in teardown at the same scope (`after(:all)` for a suite-scoped override). Values scoped to one example and reset by the framework are not the target.
- **Where:** `spec/integration/` suite setups; `spec/support/`
- **Proven by:** no single test; it is a review rule enforced at reading time
- **Origin:** cubic learnings 7ba053ef and bdd6491c (duplicates of one rule, merged here)

### A cross-thread wait waits for a state, not a duration
- **Holds because:** a fixed `sleep` makes the assertion scheduler-dependent — it passes on a fast machine for the wrong reason and flakes on a loaded CI runner. The serialization examples wait until thread B has *settled* (blocked on the install mutex, or terminated), track whether the wait succeeded, and **fail explicitly** ("thread B never settled within the wait budget") when the bounded wait expires, instead of falling through to the serialization assertions.
- **Where:** `spec/pgbus/client_spec.rb:"serializes installs process-wide across client instances (#397)"`
- **Proven by:** itself
- **Origin:** cubic learning f91ea28c

### A resource-cleanup example cleans up in an `ensure`
- **Holds because:** the streams-pool autoscale integration examples hold real connections and real threads; an example that fails before its cleanup leaves them behind and the *next* example fails for an unrelated reason. Close the release queue, stop the workers and join the holder/publisher threads in an `ensure`, before client teardown.
- **Where:** `spec/integration/streams_pool_publisher_autoscale_spec.rb`
- **Proven by:** itself
- **Origin:** cubic learning 8aaa12f5

### A regression spec asserts the behaviour, not the absence of an exception
- **Holds because:** "does not raise" passes for every reason including the bug coming back in a quieter form. The stream-queue notify-race regression asserts exactly one `enable_notify_insert` call with `throttle_interval_ms: 0`. Mismatch-retry coverage stays in `client_spec` rather than being duplicated into the `ensure_stream_queue` regression.
- **Where:** `spec/pgbus/client/ensure_stream_queue_spec.rb` (the `.once` assertion on `throttle_interval_ms: 0`); `spec/pgbus/client_spec.rb`
- **Proven by:** itself
- **Origin:** cubic learning de0fc304; PR #457

### A schema-cache example warms and asserts the thing it is *not* testing first
- **Holds because:** a poisoned pool `@data_sources` entry affects cached **index** resolution, not the live `table_exists?` query — so an example that does not warm and assert `table_exists?` first cannot say which of the two it caught.
- **Where:** `spec/pgbus/stream_queue_spec.rb`
- **Proven by:** itself
- **Origin:** cubic learning 343d8e39

### A spec that models a swallowed rollback stubs the transaction to swallow it
- **Holds because:** `Batch.try_finish!` raises `ActiveRecord::Rollback` inside its transaction block and relies on Rails swallowing it, returning nil. A spec that lets the exception escape asserts a different code path than production. Stub the transaction to swallow it and assert `just_finished` is false.
- **Where:** `spec/pgbus/batch_execution_spec.rb:"does not report finished when a fresh exists? check finds rows (READ COMMITTED hazard)"`
- **Proven by:** itself
- **Origin:** cubic learning 3368ac96; PR #420

### A constant's source location comes from `Object.const_source_location`, never `Pgbus.loader.cpath`
- **Holds because:** `cpath` is a private Zeitwerk internal, and coupling a spec to it breaks on a loader upgrade; `Object.const_source_location` is the supported API and has existed since Ruby 2.7, well below this gem's 3.3 floor. **No caller exists in the tree today** — `grep -rn 'const_source_location\|cpath' lib spec` returns nothing — so this is a standing constraint for the next spec that needs the lookup (`spec/pgbus/selective_loading_spec.rb` is where that need arose), not a description of current code.
- **Where:** would-be callers in `spec/`; `Pgbus.loader` (`lib/pgbus.rb`)
- **Proven by:** no test — the rule is that the private API stays unused
- **Origin:** cubic learnings 39de03bb and f2659872 (duplicates of one rule, merged here)

### Not a bug: `IO#timeout=` in `HealthServer` needs no `require "io/timeout"`
- **Holds because:** `IO#timeout=` is a core C method on `IO` (Ruby 3.2, Feature #18630) — `IO.instance_method(:timeout=).owner` is `IO` and its `source_location` is nil even under `--disable-gems`. There is no `io/timeout` file, so the suggested require raises `LoadError` when `health_server.rb` loads and takes the whole health surface down. The predicted symptom is also contradicted by the tests, which drive a real `TCPSocket` against a real bound port: a `NoMethodError` on the setter would hang the silent-client example rather than let it pass. No functional change was made; a comment at the call site records why.
- **Where:** `lib/pgbus/web/health_server.rb#handle_client`; `spec/pgbus/web/health_server_spec.rb`
- **Proven by:** `spec/pgbus/web/health_server_spec.rb` (12 examples, green on Ruby 3.3, 3.4 and 4.0)
- **Origin:** cubic learning b0c1fa73; PR #456

### Not a bug: the discard-parked cleanup runs after its transaction commits, and the orphan it can leave is the right trade
- **Holds because:** a process killed between the commit and the cleanup leaves a `BatchExecution` row that `Batch::Sweep#classify_orphan` resolves as completed rather than failed. Moving the cleanup inside the transaction would swap that for the failure this codebase has already judged worse: `Batch.job_discarded` can finish a batch and enqueue its callbacks, pgbus sends to PGMQ on its own connection, and a callback fired for a batch that then rolls back is unrecoverable — while an orphan row is not (`Concurrency::BlockedExecution#backfill` states the same precedent). The `FOR UPDATE SKIP LOCKED` claim that makes the discard safe against a concurrent promote also only holds inside the transaction. Exposure is limited: each row is cleaned under its own rescue, so one bad payload never costs the rest. The proper fix is `classify_orphan` learning about deliberately-discarded parked jobs.
- **Where:** `lib/pgbus/web/data_source.rb#discard_parked_jobs`, `#cleanup_discarded_parked_job`; `lib/pgbus/batch/sweep.rb#classify_orphan`
- **Proven by:** `spec/integration/dashboard_concurrency_spec.rb:"deletes the parked rows and leaves the semaphore alone"`, `:"resolves a parked batch child as failed so the batch stops waiting"`
- **Origin:** PR #463 (a reasoned rejection, recorded in the method's comment)
