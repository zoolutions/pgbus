# Configuration

`Pgbus::Configuration` (`lib/pgbus/configuration.rb`, 1794 lines) is one object
built in `Pgbus.configure { |c| … }`, plus `Configuration::CapsuleDSL`
(184 lines) for the worker string form. It is the widest file in the gem and is
deliberately flat: an `attr_accessor` per setting, `#initialize` assigning every
default in one place, and custom writers only where a value has to be validated
or coerced at assignment.

## Where the connection comes from

`#connection_options` answers in this order:

1. `config.database_url` (a String) — a dedicated PGMQ connection.
2. `config.connection_params` (a Hash) — likewise, forwarding a
   database.yml-style `:variables` block so `client_min_messages` and friends
   reach pgmq's connections while a non-libpq key never reaches `PG.connect`
   (issue #332).
3. Otherwise a **Proc** returning ActiveRecord's `raw_connection` — the shared
   path, which is what forces `pool_size: 1` and the serializing mutex in
   `Client#initialize`.

`#resolved_pool_size` is `pool_size` when set, else the sum of the enabled
roles' thread counts (workers, event consumers at 3 each by default, plus one
for the dispatcher and one for the scheduler), with a warning when that is
oversized.

`config.connects_to` (a Hash such as `{ database: { writing: :pgbus } }`) is
splatted into ActiveRecord's `connects_to` at engine boot, which is how the
separate-database deployment works — see
[../schema/summary.md](../schema/summary.md).

## Capsules

`config.workers` accepts either the array form
(`[{ queues: ["critical"], threads: 5 }]`) or the string DSL, which
`CapsuleDSL.parse` turns into that same array:

| Operator | Meaning |
|---|---|
| `,` | queue separator inside one capsule — **list order is strict priority** |
| `;` | capsule separator — each becomes its own forked process and thread pool |
| `:N` | thread count (`DEFAULT_THREADS` = 5) |
| `*` | wildcard, all queues |
| `prefix_*` | trailing wildcard, prefix match |

Strict priority is enforced two ways at runtime: with `priority_levels > 1` the
worker reads queues one at a time in list order (`Worker#fetch_prioritized`);
otherwise it relies on `Client#read_multi`'s `UNION ALL` filling its `LIMIT`
from earlier-listed queues first — an Append-node behaviour Postgres does not
formally promise, pinned by `spec/integration/multi_queue_priority_spec.rb`.
Parse errors raise `CapsuleDSL::ParseError` (an `ArgumentError`) naming the
offending input and the rule it broke.

## Queue naming

A physical name is *built* by these three methods and by `Client`'s
`#resolve_full_queue_name` / `#target_queue` / `#dead_letter_physical_name`,
which call them. Elsewhere the prefix is only ever *stripped* back off a name
PGMQ returned. The three:

- `#queue_name(name)` → `QueueNameValidator.normalize("#{queue_prefix}_#{name}")`
  (`queue_prefix` defaults to `"pgbus"`, so `default` → `pgbus_default`).
  `QueueNameValidator::MAX_QUEUE_NAME_LENGTH` is 47.
- `#dead_letter_queue_name(name)` → that plus `Pgbus::DEAD_LETTER_SUFFIX`
  (`"_dlq"`, a constant, not a setting).
- `#priority_queue_name(name, priority)` → `…_p<N>`, and
  `#priority_queue_names(name)` enumerates them when `priority_levels > 1`.

## Roles

`VALID_ROLES` is `%i[workers dispatcher scheduler consumers outbox]`.
`#role_enabled?(role)` is true unless `config.roles` narrows it; an unknown role
name raises `Pgbus::ConfigurationError` naming the valid set.

## Validation

`#validate!` is one method that checks every numeric and enum setting and raises
`Pgbus::ConfigurationError` with the setting's name and rule. It delegates to
per-area checks: `validate_visibility_heartbeat!`, `validate_shutdown_timeout!`
(shutdown must cover `drain_timeout + SHUTDOWN_TIMEOUT_MARGIN`, 5),
`validate_job_path_gaps!`, `validate_fair_share!`, `validate_streams!`,
`validate_streams_autoscale!`, `validate_metrics_backend!`,
`validate_no_queue_overlap!`. `config.eager_validation` (default true) runs it at
boot rather than at first use.

Two lease-related derivations live here rather than in `Concurrency`:
`#effective_visibility_heartbeat_interval` is
`visibility_heartbeat_interval || visibility_timeout / 3.0`, and
`validate_visibility_heartbeat!` requires an explicit interval to be positive
and **strictly less than** `visibility_timeout`.

## Other settings worth knowing

`max_jobs_per_worker` / `max_memory_mb` / `max_worker_lifetime` (recycling),
`max_retries` and the `retry_backoff*` trio, `visibility_timeout`,
`polling_interval`, `prefetch_limit`, `execution_mode` (`:threads` or `:async`),
`priority_levels` / `default_priority`, `fair_share` / `event_fair_share`,
`archive_retention`, `batch_retention` / `batch_sweep_interval` /
`batch_stall_threshold`, `outbox_*`, `allowed_global_id_models`,
`pgmq_schema_mode`, `worker_notify_scope`, `listen_notify`, the `web_*` and
`streams_*` families, `health_port` / `health_bind`, `log_format`,
`error_reporters`, `metrics_backend`. The published reference is
`docs/app/models/config_reference.rb` plus
`docs/app/views/docs/pages/configuration_reference.rb`.

See also: [../process/summary.md](../process/summary.md),
[../client/summary.md](../client/summary.md).
