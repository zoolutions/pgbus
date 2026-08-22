# frozen_string_literal: true

# The single source of truth for the Configuration reference page — every option
# as {name, type, default, description}, in groups. The page renders grouped
# PropTables from GROUPS; the drift spec (spec/config_reference_spec.rb) checks
# both directions against the real Pgbus::Configuration accessors, so a renamed
# or added option fails the build until this list is updated.
#
# INTERNAL_ONLY lists accessors that are deliberately NOT surfaced here — private
# plumbing, test hooks, or values set indirectly by other options. Keeping them
# in an explicit allowlist means a NEW undocumented accessor still trips the spec.
module ConfigReference
  GROUPS = {
    "Connection" => [
      { name: "database_url", type: "String, nil", default: "nil", desc: "PostgreSQL URL (auto-detected in Rails)." },
      { name: "connection_params", type: "Hash, nil", default: "nil", desc: "Extra connection parameters merged into the pool." },
      { name: "pool_size", type: "Integer, nil", default: "nil (auto)", desc: "Connection pool size; auto-tuned from thread counts when nil." },
      { name: "pool_timeout", type: "Numeric", default: "5", desc: "Seconds to wait for a pooled connection." },
      { name: "connects_to", type: "Hash, nil", default: "nil", desc: "Rails multi-database config for a dedicated pgbus database." },
      { name: "require_primary", type: "Boolean", default: "false", desc: "Reject a job connection that lands on a read-only replica (pg_is_in_recovery) at boot — pooler safety against a read/write splitter routing pgmq reads to a standby." },
      { name: "connection_guc_mode", type: "Symbol", default: ":options", desc: "How database.yml GUCs (variables:) reach pgbus's connections — :options (libpq startup param) or :session (post-connect SET, for a transaction-mode PgBouncer that rejects the options param). Applies to the pgmq pools and the dedicated LISTEN connections (streamer, worker notify)." }
    ],
    "Queues" => [
      { name: "queue_prefix", type: "String", default: '"pgbus"', desc: "Prefix for every PGMQ queue name." },
      { name: "default_queue", type: "String", default: '"default"', desc: "Queue for jobs without an explicit queue." },
      { name: "priority_levels", type: "Integer, nil", default: "nil", desc: "Number of priority sub-queues (2–10); nil disables." },
      { name: "default_priority", type: "Integer", default: "1", desc: "Priority for jobs without an explicit one." },
      { name: "group_mode", type: "Symbol, nil", default: "nil", desc: "Grouped-read ordering mode for a queue. Experimental — exempt from the 1.0 stability promise." }
    ],
    "Workers" => [
      { name: "workers", type: "String / Array", default: "default: 5", desc: "Worker capsule definitions (string DSL or array)." },
      { name: "event_consumers", type: "String / Array, nil", default: "nil", desc: "Event-consumer process definitions." },
      { name: "roles", type: "Array, nil", default: "nil (all)", desc: "Supervisor role filter — usually set via CLI flags." },
      { name: "execution_mode", type: "Symbol", default: ":threads", desc: "Global execution mode (:threads or :async)." },
      { name: "polling_interval", type: "Numeric", default: "0.1", desc: "Seconds between polls (LISTEN/NOTIFY is primary)." },
      { name: "prefetch_limit", type: "Integer, nil", default: "nil", desc: "Max in-flight messages per worker." },
      { name: "visibility_timeout", type: "Duration", default: "30", desc: "How long a read message stays invisible before retry." }
    ],
    "Worker recycling" => [
      { name: "max_jobs_per_worker", type: "Integer, nil", default: "nil", desc: "Recycle a worker after N jobs." },
      { name: "max_memory_mb", type: "Integer, nil", default: "nil", desc: "Recycle a worker above N MB RSS." },
      { name: "max_worker_lifetime", type: "Duration, nil", default: "nil", desc: "Recycle a worker after N seconds." }
    ],
    "Retries & reliability" => [
      { name: "max_retries", type: "Integer", default: "5", desc: "Failed reads before routing to the dead-letter queue." },
      { name: "retry_backoff", type: "Numeric", default: "5", desc: "Base delay (seconds) for exponential retry backoff." },
      { name: "retry_backoff_max", type: "Numeric", default: "300", desc: "Cap on the retry delay." },
      { name: "retry_backoff_jitter", type: "Float", default: "0.15", desc: "Jitter factor (0–1) added to retry delays." },
      { name: "circuit_breaker_enabled", type: "Boolean", default: "true", desc: "Auto-pause a queue after consecutive failures." },
      { name: "listen_notify", type: "Boolean", default: "true", desc: "Use PGMQ LISTEN/NOTIFY for instant wake-up." },
      { name: "worker_notify_scope", type: "Symbol", default: ":supervisor",
        desc: "Where the LISTEN connection lives: :supervisor shares ONE direct connection per host " \
              "(forks woken over pipes); :fork keeps one dedicated connection per worker/consumer fork." },
      { name: "streams_listen_scope", type: "Symbol", default: ":master",
        desc: "Where the streams LISTEN connection lives: :master shares ONE connection per web host " \
              "(Puma workers connect to a master hub, with automatic per-worker fallback); " \
              ":process keeps one per web process." },
      { name: "zombie_detection", type: "Boolean", default: "true", desc: "Detect and reclaim work from crashed workers." }
    ],
    "Dispatcher & maintenance" => [
      { name: "dispatch_interval", type: "Numeric", default: "1.0", desc: "Seconds between dispatcher maintenance ticks." },
      { name: "archive_retention", type: "Duration, nil", default: "7.days", desc: "How long to keep archived messages; nil disables cleanup." },
      { name: "batch_retention", type: "Duration, nil", default: "7.days", desc: "How long to keep finished batches; nil disables cleanup." },
      { name: "batch_sweep_interval", type: "Duration", default: "5.minutes", desc: "How often the dispatcher repairs stalled batches (execution-row sweep)." },
      { name: "idempotency_ttl", type: "Duration, nil", default: "7.days", desc: "How long processed-event records are kept for dedup." }
    ],
    "Outbox" => [
      { name: "outbox_enabled", type: "Boolean", default: "false", desc: "Enable the transactional outbox poller." },
      { name: "outbox_poll_interval", type: "Numeric", default: "1.0", desc: "Seconds between outbox poll cycles." },
      { name: "outbox_batch_size", type: "Integer", default: "100", desc: "Max entries claimed per outbox cycle." },
      { name: "outbox_retention", type: "Duration, nil", default: "1.day", desc: "How long to keep published outbox entries." }
    ],
    "Recurring tasks" => [
      { name: "recurring_tasks", type: "Hash, nil", default: "nil", desc: "Recurring task definitions as a hash." },
      { name: "recurring_tasks_files", type: "Array, nil", default: "nil", desc: "Paths to recurring.yml files. Replaces the deprecated singular recurring_tasks_file." },
      { name: "recurring_schedule_interval", type: "Numeric", default: "1.0", desc: "Seconds between scheduler ticks." },
      { name: "recurring_execution_retention", type: "Duration, nil", default: "7.days", desc: "How long to keep recurring execution history." },
      { name: "recurring_enabled", type: "Boolean", default: "true", desc: "Run the recurring scheduler (set false to disable it entirely). Replaces the deprecated skip_recurring." }
    ],
    "Job stats" => [
      { name: "stats_enabled", type: "Boolean", default: "true", desc: "Record job execution stats for insights." },
      { name: "stats_retention", type: "Duration, nil", default: "30.days", desc: "How long to keep job stats." },
      { name: "stats_flush_size", type: "Integer", default: "100", desc: "Buffered stats per worker before a bulk insert." },
      { name: "stats_flush_interval", type: "Numeric", default: "5", desc: "Seconds between periodic stat flushes." }
    ],
    "Dashboard" => [
      { name: "web_auth", type: "Callable, nil", default: "nil", desc: "Lambda authorizing dashboard access." },
      { name: "base_controller_class", type: "String", default: '"::ActionController::Base"', desc: "Base class for dashboard controllers." },
      { name: "return_to_app_url", type: "String, nil", default: "nil", desc: 'URL for the "back to app" button.' },
      { name: "web_refresh_interval", type: "Integer", default: "5000", desc: "Dashboard auto-refresh interval (ms)." },
      { name: "web_live_updates", type: "Boolean", default: "true", desc: "Enable Turbo Frames auto-refresh." },
      { name: "web_per_page", type: "Integer", default: "25", desc: "Dashboard pagination size." },
      { name: "web_filter_sensitive", type: "Boolean", default: "true", desc: "Redact sensitive values in dashboard payload views. Replaces the deprecated dashboard_filter_sensitive." },
      { name: "web_filter_parameters", type: "Array, nil", default: "nil (auto)", desc: "Parameter-name patterns to redact; nil auto-detects from Rails. Replaces the deprecated dashboard_filter_parameters." },
      { name: "metrics_enabled", type: "Boolean", default: "true", desc: "Expose Prometheus gauges on the dashboard." }
    ],
    "Metrics & logging" => [
      { name: "metrics_backend", type: "Symbol, Object, nil", default: "nil", desc: "Metrics adapter: nil, :prometheus, :statsd, or a Backend." },
      { name: "statsd_host", type: "String", default: '"127.0.0.1"', desc: "StatsD UDP host." },
      { name: "statsd_port", type: "Integer", default: "8125", desc: "StatsD UDP port." },
      { name: "log_format", type: "Symbol", default: ":text", desc: "Log formatter (:text or :json)." },
      { name: "error_reporters", type: "Array", default: "[]", desc: "Callables invoked on caught exceptions." },
      { name: "appsignal_enabled", type: "Boolean", default: "true", desc: "Enable the AppSignal subscriber + probe (when the gem is present)." },
      { name: "appsignal_probe_enabled", type: "Boolean", default: "true", desc: "Enable the minutely AppSignal gauge probe." }
    ],
    "Health & liveness" => [
      { name: "health_port", type: "Integer, nil", default: "nil", desc: "Port for HTTP liveness/readiness probes; nil disables." },
      { name: "health_bind", type: "String", default: '"127.0.0.1"', desc: "Bind address for the health server." },
      { name: "stall_threshold", type: "Numeric", default: "300", desc: "Seconds without progress before a worker is stalled." },
      { name: "read_timeout", type: "Numeric", default: "30", desc: "Read timeout for worker fetches." },
      { name: "drain_timeout", type: "Numeric", default: "30", desc: "Seconds to wait for in-flight jobs to finish during graceful shutdown before abandoning them." },
      { name: "shutdown_timeout", type: "Numeric, nil", default: "drain_timeout + 5",
        desc: "Seconds the supervisor waits for children after TERM before SIGKILL. nil derives drain_timeout + 5; " \
              "an orchestrator's stop grace period (Kamal stop_timeout, K8s terminationGracePeriodSeconds) must exceed it." }
    ],
    "Streams (SSE)" => [
      { name: "streams_enabled", type: "Boolean", default: "true", desc: "Enable the SSE streams transport." },
      { name: "streams_default_retention", type: "Numeric", default: "300", desc: "Default stream retention in seconds." },
      { name: "streams_retention", type: "Hash", default: "{}", desc: "Per-stream retention overrides." },
      { name: "streams_heartbeat_interval", type: "Numeric", default: "15", desc: "SSE heartbeat interval (seconds)." },
      { name: "streams_max_connections", type: "Integer", default: "2000", desc: "Max SSE connections per web-server process." },
      { name: "streams_idle_timeout", type: "Numeric", default: "3600", desc: "Close idle SSE connections after N seconds." },
      { name: "streams_pool_size", type: "Integer", default: "5", desc: "Dedicated DB pool for durable-stream publish + replay, isolated from the job pool so a saturated worker pool can't delay broadcasts. Ignored on the shared-ActiveRecord connection path." },
      { name: "streams_pool_timeout", type: "Numeric", default: "5", desc: "Seconds to wait for a connection from the dedicated streams pool." },
      { name: "streams_pool_autoscale", type: "Boolean", default: "false", desc: "Self-tuning: a periodic maintenance check (on the streamer's idle LISTEN connection — no extra connection) grows the streams pool into a fair share of live Postgres connection headroom under saturation, shrinks back to streams_pool_size when idle, and emergency-shrinks if the DB runs low on connections. Opt-in; no-op on the shared-ActiveRecord path." },
      { name: "streams_pool_max", type: "Integer, nil", default: "nil", desc: "Optional hard ceiling for streams-pool autoscaling. nil lets the dynamic per-process fair share of DB headroom be the effective cap." },
      { name: "streams_path", type: "String, nil", default: "nil", desc: "Custom SSE endpoint path (auto-detected from mount)." },
      { name: "streams_falcon_streaming_body", type: "Boolean", default: "false", desc: "Use Falcon-native streaming body instead of hijack. Experimental — exempt from the 1.0 stability promise." },
      { name: "streams_stats_enabled", type: "Boolean", default: "false", desc: "Record stream broadcast/connect/disconnect stats." },
      { name: "streams_test_mode", type: "Boolean", default: "false", desc: "Return a stub SSE response (auto-enabled by the test helpers)." },
      { name: "streams_presence_patterns", type: "Array", default: "[]", desc: "Streams (exact string or Regexp) that get connection-driven presence: auto-join on connect, auto-leave on disconnect, heartbeat touch. Experimental — exempt from the 1.0 stability promise." },
      { name: "streams_presence_member", type: "Callable, nil", default: "nil", desc: "Custom `->(context) { { id:, metadata: } }` extractor for connection-driven presence; nil uses the built-in Hash/#id extractor. Experimental — exempt from the 1.0 stability promise." },
      { name: "streams_broadcast_queue", type: "String, nil", default: "nil", desc: "Dedicated queue for turbo-rails' async broadcast jobs. nil leaves them on the default queue (a broadcasts_to render+broadcast can wait behind long-running jobs). Set a queue name and back it with a worker capsule to isolate broadcast latency." }
    ],
    "Validation" => [
      { name: "eager_validation", type: "Boolean", default: "true", desc: "Validate configuration eagerly at boot." },
      { name: "allowed_global_id_models", type: "Array, nil", default: "nil", desc: "Allowlist of Class/Module models permitted as GlobalID job arguments and EventBus payloads. nil = allow-all (default). [] = deny-all. Apps using ActiveStorage attachments should include ActiveStorage::Blob (and related models) so Rails' analyze/purge/transform jobs still deserialize." },
      { name: "pgmq_schema_mode", type: "Symbol", default: ":auto", desc: "PGMQ schema install mode (:auto, :extension, :embedded)." },
      { name: "doctor_on_boot", type: "Symbol, nil", default: "nil", desc: "Run doctor preflight checks inside the booting supervisor (one Rails boot instead of `pgbus doctor` + `pgbus start`). nil/false = off. :report logs the report and always boots; :strict refuses to boot on a fatal check (bad config or an absent PGMQ schema) — transient-shaped failures (Queues/Database) never abort. Set via the --doctor / --doctor-strict start flags too." }
    ]
  }.freeze

  # Accessors deliberately not documented on the reference page: injected
  # collaborators, indirect/derived writers, low-level stream tuning, and
  # dashboard internals. Listed explicitly so a NEW undocumented accessor still
  # fails the drift spec.
  INTERNAL_ONLY = %w[
    logger
    web_data_source
    skip_recurring
    recurring_tasks_file
    dashboard_filter_parameters
    dashboard_filter_sensitive
    insights_default_minutes
    streams_signed_name_secret
    streams_database_url
    streams_host
    streams_port
    streams_pool_database_url
    streams_pool_host
    streams_pool_port
    streams_listen_health_check_ms
    streams_write_deadline_ms
    streams_fanout_write_deadline_ms
    streams_dispatch_queue_limit
    streams_writer_threads
    streams_writer_buffer_limit
    streams_pool_autoscale_interval
    streams_application_name
    streams_orphan_sweep_interval
    streams_orphan_threshold
    streams_durable_patterns
    streams_default_broadcast_mode
    worker_notify_database_url
    worker_notify_host
    worker_notify_port
    worker_notify_wakeup
  ].freeze

  module_function

  # Every documented option name, flat — used by the page and the drift spec.
  def documented_names
    GROUPS.values.flatten.map { |o| o[:name] }
  end
end
