How a batch counts its jobs, finishes exactly once, and what the sweep may and
may not repair. See [../batch/summary.md](../batch/summary.md).

### A job is counted in and given its execution row before its message is sent
- **Holds because:** it is what makes `total_jobs == outstanding rows + completed_jobs + failed_jobs` true at every commit point, and what lets a finish never race an add: `BatchEntry.increment_total_jobs!` raises `Batch::AlreadyFinished` at `perform_later` — before the send — when the row is already finished. `Batch.track_enqueue` does both in one `BatchEntry.transaction`, and a bulk send passes the whole array so the increment happens once.
- **Where:** `lib/pgbus/batch.rb#track_enqueue`; `app/models/pgbus/batch_entry.rb#increment_total_jobs!`; `lib/pgbus/active_job/adapter.rb#inject_batch_metadata`, `#enqueue_immediate`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"counts the job into its batch before the message is sent"`, `:"raises AlreadyFinished before sending when the batch has finished"`, `:"tags every bulk payload with the batch id and counts them once, before the send"`; `spec/integration/batch_open_spec.rb:"raises at perform_later and sends nothing when the batch finished under a stale handle"`
- **Origin:** issue #423; PR #420

### An execution row is removed only when its `msg_id` is still nil
- **Holds because:** a row stays `msg_id`-less both when the enqueue died before the send **and** when the send landed and only the backfill failed — and in the second case the message is live and must not be un-counted. The sweep's removal is a CAS (`where(id: row.id, msg_id: nil).delete_all`) and the `total_jobs` decrement plus finish check run only when it deleted something. The bulk rescue untracks only the payloads whose id is actually nil (`msg_ids.nil? || msg_ids[index].nil?`), so a short `send_batch` response keeps the rows for messages that did land.
- **Where:** `lib/pgbus/batch/sweep.rb#uncount_orphan!`, `#classify_orphan`; `lib/pgbus/active_job/adapter.rb#enqueue_immediate` (rescue `Pgbus::EnqueueError`)
- **Proven by:** `spec/pgbus/batch_sweep_spec.rb:"un-counts a row only when the message exists nowhere"`, `:"keeps a row when the queue probe is inconclusive"`; `spec/integration/batch_flow_spec.rb:"keeps a msg_id-less row whose message is live, and resolves it as failed once dead-lettered"`, `:"still un-counts a row whose message exists nowhere"`
- **Origin:** cubic learning 6ddd6cb1; PR #420

### Resolving an execution deletes the row and increments the counter in one transaction
- **Holds because:** a crash between the two would leave the invariant broken in the direction that never heals — a deleted row with no counter means the batch finishes short. Together they are atomic, and the sweep's stale path covers the remaining window. The counter is incremented only when the delete actually removed a row, or when `legacy_untracked_batch?` says this is a pre-migration group with no rows at all.
- **Where:** `lib/pgbus/batch.rb#resolve_execution`, `#legacy_untracked_batch?`
- **Proven by:** `spec/pgbus/batch_execution_spec.rb:"deletes the execution row, increments completed_jobs, and tries to finish"`, `:"does not increment counters when the row was already gone (idempotent signal)"`, `:"increments counters for a pre-migration in-flight batch with no execution rows"`
- **Origin:** cubic learning 55a57eb3; PR #420

### `try_finish!` re-checks for execution rows in a fresh statement after winning the CAS
- **Holds because:** Postgres READ COMMITTED can let a blocked `UPDATE` win from a stale `NOT EXISTS` snapshot, so the winner can finish a batch a concurrent insert has just added to. The re-check runs inside the transaction and raises `ActiveRecord::Rollback` when a row is there. `finish_if_empty!` additionally requires `completed_jobs + failed_jobs = total_jobs`, so a pre-migration in-flight batch (zero rows, counters short) is never closed empty; `total_jobs = 0` with zero counters *is* terminal — that is a batch whose block crashed before enqueuing anything.
- **Where:** `lib/pgbus/batch.rb#try_finish!`; `app/models/pgbus/batch_entry.rb#finish_if_empty!`
- **Proven by:** `spec/pgbus/batch_execution_spec.rb:"finishes when the CAS update wins and a fresh exists? check is empty"`, `:"does not report finished when a fresh exists? check finds rows (READ COMMITTED hazard)"`; `spec/integration/batch_flow_spec.rb:"finishes a batch whose block raised before any job was enqueued"`
- **Origin:** PR #420

### Completion is re-checked after `total_jobs` is published
- **Holds because:** jobs count themselves in as they are enqueued, so completion signals that arrived while the block was still open saw an incomplete `total_jobs` and could not finish the batch themselves. `#start_processing` (first block) and `#reopen` (later blocks) both call `BatchEntry.check_finished!`, which takes a row lock and a status guard so it is idempotent against a concurrent signal.
- **Where:** `lib/pgbus/batch.rb#start_processing`, `#reopen`; `app/models/pgbus/batch_entry.rb#check_finished!`
- **Proven by:** `spec/integration/batch_flow_spec.rb:"finishes a batch whose jobs all complete before total_jobs is published"`; `spec/pgbus/batch_spec.rb:"flips the batch to processing without touching total_jobs, then re-checks finish"`, `:"re-reads the row before deciding the batch is still open"`
- **Origin:** cubic learning b1c9c119; PR #417/#420

### A job discarded at enqueue time is uncounted; a blocked one is not
- **Holds because:** a job dropped by a concurrency `:discard` or a uniqueness duplicate will never signal completion, so leaving it counted makes the batch wait forever and `on_finish` never fires. A `:block` conflict is different: `BlockedExecution` stores the tagged payload, the job runs later and signals normally. `uncount_batch_job` also handles the retry case: a retry re-enqueue that never became live calls `Batch.forget_retry_reenqueued`, leaving the original attempt's row and its normal signal standing.
- **Where:** `lib/pgbus/active_job/adapter.rb#uncount_batch_job`, `#handle_conflict`; `lib/pgbus/batch.rb#untrack_enqueue`, `#forget_retry_reenqueued`
- **Proven by:** `spec/pgbus/active_job/adapter_spec.rb:"uncounts a duplicate discarded at enqueue time from the batch"`; `spec/pgbus/batch_execution_spec.rb:"deletes the row when untracking an enqueue-time discard"`
- **Origin:** cubic learning ad0eebb3

### A `retry_on` re-enqueue keeps its one execution row and is never counted again
- **Holds because:** the batch waits for a job's *terminal* outcome, not its first attempt. `Batch.track_retry` re-inserts with `ON CONFLICT (job_id) DO NOTHING`, and `note_retry_reenqueued` / `retry_reenqueued?` (a fiber-local Set, the right scope under `execution_mode: :async`) make `Executor`'s `ensure` skip the completion signal for the attempt that re-enqueued itself. The backfill after the new send re-points the same row at the new message.
- **Where:** `lib/pgbus/batch.rb#track_retry`, `#note_retry_reenqueued`, `#retry_reenqueued?`, `#clear_retry_reenqueued`; `lib/pgbus/active_job/executor.rb#execute`
- **Proven by:** `spec/integration/batch_retry_spec.rb:"keeps the batch open across a retry and finishes on the retry's success"`, `:"fires on_failure, not on_success, when the retried job eventually dead-letters"`; `spec/pgbus/active_job/executor_spec.rb:"skips the completion signal when the job re-enqueued itself for retry"`
- **Origin:** issue #424; PR #428

### Execution rows are inserted with raw SQL, not `insert_all(unique_by:)`
- **Holds because:** Rails resolves `unique_by` through the schema cache, and a poisoned or cold cache turns a legitimate dedupe into an error (issue #401). `BatchExecution.insert_for!` writes `INSERT … ON CONFLICT (job_id) DO NOTHING` directly.
- **Where:** `app/models/pgbus/batch_execution.rb#insert_for!`
- **Proven by:** `spec/pgbus/batch_execution_spec.rb:"inserts a row keyed by job_id before send"`
- **Origin:** cubic learning 8642a203; PR #420

### `executions_migrated?` caches only a successful true
- **Holds because:** a long-lived worker that cached `false` would stay on the counter path for the rest of its life after the migration ran, and a connection blip would do the same. A missing table or an error is re-probed on the next call.
- **Where:** `lib/pgbus/batch.rb#executions_migrated?`, `#reset_executions_migrated_cache!` (the same shape applies to `#callback_jobs_migrated?`)
- **Proven by:** `spec/pgbus/batch_execution_spec.rb:"is true when the executions table exists"`, `:"is false when the executions table is missing"`, `:"is false when the table check raises (unmigrated app, no connection)"`
- **Origin:** cubic learning 27f8143b; PR #420

### A batch execution stores the **physical** target queue, and probes resolve it as-is
- **Holds because:** `move_to_dead_letter` mints a new `msg_id`, and priority routing sends to `_pN` sub-queues, so a logical name is not enough to find the message again. `Batch.backfill_execution` stores `Client#target_queue`; the sweep passes that stored name through `resolve_full_queue_name`, which leaves an already-prefixed name alone, and looks the DLQ up through `dead_letter_physical_name` (which strips a `_pN` suffix, because priority sub-queues share one DLQ) plus `message_with_job_id?` rather than reusing the source `msg_id`.
- **Where:** `lib/pgbus/batch.rb#backfill_execution`; `lib/pgbus/batch/sweep.rb#classify_stale`, `#classify_orphan`; `lib/pgbus/client.rb#dead_letter_physical_name`, `#message_with_job_id?`, `#resolve_full_queue_name`
- **Proven by:** `spec/pgbus/batch_sweep_spec.rb:"resolves a row as failed when its message sits in the DLQ"`; `spec/pgbus/batch_execution_spec.rb:"writes msg_id and queue_name after send"`
- **Origin:** cubic learnings d4b8fe4f, 3d540a35; PR #420

### The stale-execution sweep only probes rows older than the stall threshold
- **Holds because:** a fresh in-flight message is not stalled, and probing every row on every interval costs a PGMQ round-trip per row for nothing. `sweep_stale_executions` filters `created_at < Time.current - stalled_for`, the same window the orphan and pending phases use.
- **Where:** `lib/pgbus/batch/sweep.rb#sweep_stale_executions`
- **Proven by:** `spec/pgbus/batch_sweep_spec.rb:"defines a 5-minute stall threshold matching solid_queue"`; the window is exercised by `spec/integration/batch_flow_spec.rb`
- **Origin:** cubic learning bb37869a; PR #420

### `finish_stalled_processing` finishes a row-less processing batch only when its counters are already terminal
- **Holds because:** a batch that was in flight when the migration ran has zero execution rows and counters short of `total_jobs` — zero rows alone do not establish a stall. The real stalls are a finish `UPDATE` that rolled back after a callback enqueue failed, and a `total_jobs = 0` batch whose block crashed.
- **Where:** `lib/pgbus/batch/sweep.rb#finish_stalled_processing`, `#counters_terminal?`
- **Proven by:** `spec/pgbus/batch_sweep_spec.rb:"skips a processing batch whose counters are not terminal"`, `:"tries to finish when counters already match total_jobs"`; `spec/integration/batch_flow_spec.rb:"leaves a pre-migration in-flight batch (no rows, unfinished counters) alone"`
- **Origin:** cubic learning 2c1aff69; PR #420

### A `job_discarded` signal with no `job_id` increments `failed_jobs` and defers to the terminal-counter check
- **Holds because:** a migrated legacy (or otherwise execution-untracked) signal cannot name a row to delete, so it must not guess one. It increments the counter and calls `try_finish!`, which requires `completed + failed = total` with `total > 0`, so a single discard on a multi-job legacy batch cannot close it. Execution-tracked signals that *do* carry a `job_id` always resolve their own row instead.
- **Where:** `lib/pgbus/batch.rb#job_discarded`, `#signal_without_row`, `#update_counter`
- **Proven by:** `spec/pgbus/batch_execution_spec.rb:"increments failed_jobs and re-checks finish when job_id is omitted"`, `:"deletes the row and increments failed_jobs (not discarded_jobs)"`
- **Origin:** cubic learning c65a98ce; PR #420

### `on_finish` means "no outstanding execution rows", including rows the sweep cleared
- **Holds because:** the finish test is row absence plus terminal counters, and `Batch::Sweep` resolves rows the completion path could not — so a batch can finish because the dispatcher repaired it, not because a worker signalled. The docs page says so explicitly; any wording that ties `on_finish` to "every job reported in" overclaims.
- **Where:** `app/models/pgbus/batch_entry.rb#finish_if_empty!`; `lib/pgbus/batch/sweep.rb`; `docs/app/views/docs/pages/batches.rb`
- **Proven by:** `spec/integration/batch_flow_spec.rb:"finishes a batch whose last message is gone but the execution row remains"`, `:"fires the callback exactly once, only on the final completion"`
- **Origin:** cubic learning 647726d1; PR #420

### `pgbus.batch_finished` carries `batch_id`, `total_jobs`, `completed_jobs`, `failed_jobs`; `pgbus.batch_sweep` carries one count per phase
- **Holds because:** these payloads are the contract an app's subscriber reads, and the instrumentation catalog is a completeness claim. `Batch#instrument_finished` emits exactly those four keys; `Sweep.run` seeds `stale_executions`, `orphan_rows`, `started_batches`, `finished_batches` and `stalled_for`.
- **Where:** `lib/pgbus/batch.rb#instrument_finished`; `lib/pgbus/batch/sweep.rb#run`; `lib/pgbus/instrumentation.rb` (the catalog)
- **Proven by:** no dedicated payload-shape example found; the events themselves fire in `spec/integration/batch_flow_spec.rb`
- **Origin:** cubic learning 94b5b6af; PR #420

### `batch_sweep_interval` must be positive and the dispatcher always runs the sweep
- **Holds because:** assignment coerces `ActiveSupport::Duration` and `validate_job_path_gaps!` requires the value positive, so it is not a nil-sentinel setting like `stall_threshold` or `read_timeout` — it never means "disabled", and a nil-sentinel example that includes it is wrong.
- **Where:** `lib/pgbus/configuration.rb` (`batch_sweep_interval` writer, `#validate_job_path_gaps!`); `lib/pgbus/process/dispatcher.rb#run_maintenance_tasks`
- **Proven by:** `spec/pgbus/configuration_spec.rb` excludes it from the nil-sentinel duration example
- **Origin:** cubic learning 52e39e4c; PR #420

### The `add_batch_executions` migration translates counter columns both ways, and requires a worker restart
- **Holds because:** forward, `failed_jobs` may be absent (rename `discarded_jobs`) or present (merge then drop `discarded_jobs`). Backward, `failed_jobs` may be a pre-existing column the rollback schema must keep — so `down` re-creates `discarded_jobs`, copies the count into it, and zeroes `failed_jobs` rather than renaming or dropping it, so a re-`up` does not double-count. Because the migration removes a column mixed-version processes both write, the contract is drain-and-restart, not expand/contract: keeping `discarded_jobs` indefinitely would split counters across versions.
- **Where:** `lib/generators/pgbus/templates/add_batch_executions.rb.erb`; `app/models/pgbus/batch_entry.rb` (`COUNTER_COLUMNS`, `#discarded_jobs`)
- **Proven by:** `spec/generators/pgbus/add_batch_executions_generator_spec.rb:"creates pgbus_batch_executions and renames failure columns"`; `spec/pgbus/generators/migration_detector_spec.rb:"queues add_batch_executions when the table exists but pre-rename columns remain"`
- **Origin:** cubic learnings 796f8895, 2294d614, 17cc262c; PR #420

### The integration bootstrap schema stays in parity with the install and upgrade DDL
- **Holds because:** the orphan sweep depends on a partial index on `created_at WHERE msg_id IS NULL`, and cascade deletion on the `batch_id` foreign key to `pgbus_batches.batch_id`. A test schema missing either exercises a different plan than production. Both the install template and `spec/integration_helper.rb` create them, and the index/FK statements run outside the create-table guard so a pre-existing table still gets them.
- **Where:** `lib/generators/pgbus/templates/migration.rb.erb`; `spec/integration_helper.rb#bootstrap_integration_tables`
- **Proven by:** the integration suite itself — the orphan phase in `spec/integration/batch_flow_spec.rb` runs against this schema
- **Origin:** cubic learning 9ce33b14; PR #420
