# frozen_string_literal: true

require "concurrent/map"
require "zeitwerk"

module Pgbus
  # Suffix appended to a queue name to derive its dead-letter companion
  # (e.g. "pgbus_default" -> "pgbus_default_dlq"). Hard-coded here because
  # changing it on a running deployment would orphan every existing DLQ
  # message; nothing in the codebase or in user reports has ever needed
  # this to be configurable.
  DEAD_LETTER_SUFFIX = "_dlq"

  # Rake task-name prefixes during which pgbus must NOT open a database
  # connection: schema management (an idle session on a dedicated pgbus
  # database blocks that same process's DROP DATABASE during db:test:purge —
  # issue #409) and asset precompile (where a database may legitimately not
  # exist). Consulted by Pgbus.database_task?.
  DATABASE_TASK_PREFIXES = %w[db: db_test: assets: webpacker: yarn:].freeze

  # Error-hierarchy policy (the 1.0 contract, issue #282):
  #
  #   * OPERATIONAL errors — a pgbus subsystem failed to do its job at runtime
  #     (bad configuration, a pool that's shutting down, a batch enqueue that
  #     came back short, a rejected GlobalID, a missing PGMQ version) — descend
  #     from Pgbus::Error. `rescue Pgbus::Error` is meant to catch every one of
  #     them; that is the whole point of the hierarchy.
  #
  #   * ARGUMENT-SHAPE errors — the caller handed a method a malformed argument
  #     (a stream name too long for the queue-name budget, an unparseable
  #     cursor, a capsule DSL line that doesn't parse) — stay ArgumentError
  #     subclasses. That's what ArgumentError means, and rescuing it is the
  #     caller's responsibility, not an operational concern. These live in
  #     their own files: Streams::StreamNameTooLong, Streams::Cursor::InvalidCursor,
  #     Configuration::CapsuleDSL::ParseError.
  #
  # When adding a new error class, decide which half it belongs to and parent
  # it accordingly — do not default to StandardError.
  class Error < StandardError; end
  class ConfigurationError < Error; end
  class SerializationError < Error; end
  class QueueNotFoundError < Error; end
  class DeadLetterError < Error; end
  class ConcurrencyLimitExceeded < Error; end
  class JobNotUnique < Error; end
  class SchemaNotReady < Error; end
  class ReadTimeoutError < Error; end

  # Raised by the ActiveJob adapter when a batch enqueue (perform_all_later)
  # comes back with a msg_id count that doesn't match the number of jobs sent —
  # a data-integrity signal that some jobs may not have been persisted.
  class EnqueueError < Error; end
  # A Current attribute could not be serialized for job persistence (issue #430).
  class CurrentAttributesError < Error; end

  # Raised by the execution pools when work can't be accepted: the pool is
  # shutting down, or it's momentarily at capacity. Consumer-reachable during
  # job submission, so it descends from Pgbus::Error rather than RuntimeError.
  class ExecutionPoolError < Error; end

  # Raised by Client read paths when the in-memory connection-health circuit
  # breaker (Client::ConnectionHealth) is open: the database has failed enough
  # consecutive connection attempts that reads now fail fast without a pool
  # checkout, sparing a dead database from the whole fleet re-polling and the
  # error tracker from per-poll noise. Workers rescue this and idle until the
  # breaker's backoff window admits a probe. See issue #197.
  class ConnectionCircuitOpenError < Error; end

  module Process
    # A LISTEN connection landed on a read-only replica (pg_is_in_recovery() =>
    # t). After a failover, stale DNS can point a fresh connection at the
    # demoted master; NOTIFY fires only on the primary, so such a connection
    # connects "successfully" but never wakes. Raised by
    # Process::PrimaryValidator so the listener rejects it and retries with
    # backoff (re-resolving DNS each attempt).
    #
    # Defined here rather than in process/primary_validator.rb because Zeitwerk
    # requires each managed file to define exactly the constant matching its
    # path — a second class in that file would break eager_load. lib/pgbus.rb
    # is the (require-loaded) gem entry point, so it can define the error and
    # explicitly namespace Process without conflicting with Zeitwerk's
    # management of the lib/pgbus/process/ directory.
    class ReplicaConnectionError < Error; end
  end

  class << self
    # The gem's Zeitwerk loader, the app/models loader used outside Rails, and
    # the configuration: built once when this file loads (see the bottom of
    # the module body) and replaced only by reset!/teardown under @state_lock.
    # Readers need no lock: a reference is published whole.
    attr_reader :loader, :models_loader, :configuration

    # Process-global flag set by Worker#graceful_shutdown so the adapter
    # can report stopping? to ActiveJob::Continuation (Rails 8.1+).
    def stopping
      @state_lock.synchronize { @stopping }
    end

    def stopping=(value)
      @state_lock.synchronize { @stopping = value ? true : false }
    end

    private

    # Zeitwerk::Loader.for_gem works out the gem root from its caller, so this
    # must stay in lib/pgbus.rb.
    def build_loader
      Zeitwerk::Loader.for_gem.tap do |loader|
        loader.inflector.inflect(
          "pgbus" => "Pgbus",
          "cli" => "CLI",
          "dlq" => "DLQ",
          "dsl" => "DSL",
          "capsule_dsl" => "CapsuleDSL",
          "mcp" => "MCP"
        )
        loader.ignore("#{__dir__}/generators")
        loader.ignore("#{__dir__}/active_job")
        loader.ignore("#{__dir__}/pgbus/testing")
        # The MCP diagnostic server is optional — its tool classes subclass
        # MCP::Tool from the (optional) `mcp` gem. Keeping it out of Zeitwerk
        # means we never reference the gem's constants at autoload time;
        # `Pgbus::MCP.load!` (called by the `pgbus mcp` CLI command) requires
        # the gem and loads the subsystem explicitly.
        loader.ignore("#{__dir__}/pgbus/mcp")
        loader.ignore("#{__dir__}/pgbus/mcp.rb")
        # The Phlex stream helper references Phlex::Rails::HelperMacros from the
        # optional `phlex-rails` gem. Keep it out of Zeitwerk so eager_load never
        # touches those constants; a Phlex app requires it explicitly via
        # `require "pgbus/streams/phlex_helpers"`.
        loader.ignore("#{__dir__}/pgbus/streams/phlex_helpers.rb")
        # Vendor integrations are loaded conditionally (when the vendor gem
        # is present) by lib/pgbus/engine.rb. Keeping them out of Zeitwerk
        # means we don't reference vendor constants at autoload time.
        loader.ignore("#{__dir__}/pgbus/integrations")
        # lib/puma/plugin/pgbus_streams.rb is a Puma plugin — it's required
        # explicitly by the user from config/puma.rb via `plugin :pgbus_streams`.
        # Without this ignore, Zeitwerk scans lib/puma/ under the pgbus loader
        # root and tries to autoload Puma::Plugin, which collides with the real
        # Puma::Plugin class defined by the puma gem itself.
        loader.ignore("#{__dir__}/puma")
      end
    end

    # Separate loader for app/models used only in non-Rails contexts (specs,
    # standalone scripts). When the Engine boots, Rails' autoloader takes over
    # app/models and this loader is torn down to avoid conflicts.
    def build_models_loader
      models_dir = File.expand_path("../app/models", __dir__)
      return nil unless File.directory?(models_dir)

      Zeitwerk::Loader.new.tap do |loader|
        loader.tag = "pgbus-models"
        loader.push_dir(models_dir)
        loader.setup
      end
    end

    public

    def teardown_models_loader!
      loader = @state_lock.synchronize do
        current = @models_loader
        @models_loader = nil
        current
      end
      loader&.unregister
    end

    # True when the current process is running a rake task (db:*, assets:*, …
    # — see DATABASE_TASK_PREFIXES) during which pgbus must not touch the
    # database: the database may not exist yet, and an idle connection on a
    # dedicated pgbus database blocks that same process's DROP DATABASE
    # during db:test:purge (issue #409). Use it to guard boot-time database
    # touches in an initializer:
    #
    #   Rails.application.config.after_initialize do
    #     Pgbus::StreamQueue.table_exists? unless Pgbus.database_task?
    #   end
    #
    # False outside a Rake run, and false (never raise into boot) when
    # detection itself fails.
    def database_task?
      return false unless defined?(::Rake) && ::Rake.respond_to?(:application)

      ::Rake.application.top_level_tasks.any? do |task|
        DATABASE_TASK_PREFIXES.any? { |prefix| task.to_s.start_with?(prefix) }
      end
    rescue StandardError => e
      logger.debug { "[Pgbus] database_task? detection failed, assuming non-schema: #{e.class}: #{e.message}" }
      false
    end

    def configure
      yield configuration
      # Fail loud at boot on an invalid value rather than leaving it dormant
      # until a worker path consumes it. Checking the flag after the yield
      # lets a block opt out for itself via `c.eager_validation = false`.
      configuration.validate! if configuration.eager_validation
      configuration
    end

    # Built on first use, under @state_lock: two threads making the first
    # call at once must not each build a Client (the loser's pool would be
    # orphaned, its connections never closed). Not built at boot: a Client
    # resolves connection options, which a rake db: task must not need.
    def client
      @state_lock.synchronize { @client ||= Client.new(configuration) }
    end

    # Entry point for the streams subsystem — `Pgbus.stream(name).broadcast(html)`
    # or `Pgbus.stream(@order).current_msg_id`. Defined on Pgbus itself rather
    # than inside lib/pgbus/streams.rb because that file is only Zeitwerk-loaded
    # when Pgbus::Streams::Stream is first referenced — the chicken-and-egg
    # problem means `Pgbus.stream(...)` would be undefined on the first call.
    # Referencing Streams::Stream inside the method body forces Zeitwerk to
    # load lib/pgbus/streams.rb lazily on first use, which is fine.
    #
    # Caches Stream instances by logical name so high-frequency callers
    # (e.g. Turbo::StreamsChannel.broadcast_stream_to inside an
    # after_update_commit callback firing 1000x/sec) don't allocate a new
    # Stream + Mutex per broadcast. The cache is process-local; reset!
    # clears it. The cache key is the resolved name string, not the raw
    # streamables, so `Pgbus.stream(@order)` and `Pgbus.stream(@order)`
    # in the same process return the same instance.
    # `durable:` defaults to `nil` so the configuration resolves the mode:
    # `streams_durable_patterns` first (exact string or regex match), then
    # `streams_default_broadcast_mode`. Pass `durable: true`/`false`
    # explicitly to bypass the resolver for this Stream instance.
    def stream(streamables, durable: nil)
      name = Streams::Stream.name_from(streamables)
      resolved = durable.nil? ? configuration.stream_durable?(name) : durable
      cache_key = "#{name}:#{resolved ? "d" : "e"}"
      stream_cache.compute_if_absent(cache_key) { Streams::Stream.new(streamables, durable: resolved) }
    end

    # Compose a short, pgbus-safe stream identifier from any mix of
    # records, strings, symbols, and arrays. Delegates to
    # `Pgbus::Streams::Key.stream_key`; raises `ArgumentError` if the
    # resulting key would overflow the pgbus queue-name budget. See
    # `lib/pgbus/streams/key.rb` for the digest policy and rationale.
    #
    #   Pgbus.stream_key(chat, :messages)
    #   # => "ai_chat_3a4f9c21b7d20e18:messages"
    #
    #   Pgbus.stream(Pgbus.stream_key(chat, :messages)).broadcast(html)
    def stream_key(*parts, **)
      Streams::Key.stream_key(*parts, **)
    end

    # Accepts an already-built stream key verbatim (no re-keying), only
    # enforcing the queue-name budget. Use when you hold a key string and
    # want to pass it to both `turbo_stream_from` and a broadcaster
    # without the colon-separator guard raising on the second call.
    #
    #   key = Pgbus.stream_key(chat, :messages)
    #   Pgbus.stream(Pgbus.stream_key!(key)).broadcast(html)
    def stream_key!(key)
      Streams::Key.stream_key!(key)
    end

    # The maximum length (in characters) a stream name may be before it
    # overflows the pgbus queue-name budget. A stream name becomes the physical
    # queue `#{queue_prefix}_<name>`, and PGMQ caps queue names, so the usable
    # budget is `QueueNameValidator::MAX_QUEUE_NAME_LENGTH - queue_prefix.length
    # - 1`. Depends on `config.queue_prefix`. Use it to size or truncate stream
    # identifiers up front instead of hand-computing the budget at every call
    # site.
    #
    #   name = candidate.length > Pgbus.stream_name_budget ? Pgbus.stream_key(record) : candidate
    def stream_name_budget
      Streams::Key.queue_name_budget
    end

    # Publish an event to the bus — the top-level shortcut for
    # `Pgbus::EventBus::Publisher.publish`, symmetric with `Pgbus.stream`.
    #
    #   Pgbus.publish("orders.created", { order_id: 42 })
    #   Pgbus.publish("orders.created", order, headers: { "x-trace" => id }, delay: 30)
    def publish(routing_key, payload, headers: nil, delay: 0)
      EventBus::Publisher.publish(routing_key, payload, headers: headers, delay: delay)
    end

    # Publish an event with a delay. Mirrors
    # `Pgbus::EventBus::Publisher.publish_later`; `delay:` is required (a
    # publish_later with no delay is just publish).
    #
    #   Pgbus.publish_later("orders.reminder", { order_id: 42 }, delay: 1.hour)
    def publish_later(routing_key, payload, delay:, headers: nil)
      EventBus::Publisher.publish_later(routing_key, payload, delay: delay, headers: headers)
    end

    # A fresh configuration (never nil, so readers need no lock) and no client.
    def reset!
      @state_lock.synchronize do
        @client&.close
        @client = nil
        @configuration = Configuration.new
        @stream_cache = Concurrent::Map.new
      end
    end

    # Discard the inherited PGMQ client after fork.
    # Do NOT call close — the parent's @pgmq_mutex is in undefined
    # state post-fork and attempting to acquire it can deadlock.
    # The next call to Pgbus.client will lazily create a fresh one.
    # Cached streams hold the client they were built with, so they go too.
    # (@state_lock itself is safe to take in the child: Ruby abandons a Mutex
    # whose owning thread did not survive the fork.)
    def reset_client!
      @state_lock.synchronize do
        @client = nil
        @stream_cache = Concurrent::Map.new
      end
    end

    def logger
      configuration.logger
    end

    def logger=(value)
      configuration.logger = value
    end

    private

    # Process-local, like the client it holds; replaced (never mutated in
    # place) by reset!/reset_client!.
    attr_reader :stream_cache
  end

  # Module state, built here at load rather than by `||=` on first use, so no
  # two threads can each build their own (issue #527). @state_lock guards every
  # later write: the client memo, reset!/reset_client!, the stopping flag.
  @state_lock = Mutex.new
  @stopping = false
  @client = nil
  @stream_cache = Concurrent::Map.new
  @loader = build_loader
  @loader.setup

  # After loader.setup: Configuration is Zeitwerk-managed.
  @configuration = Configuration.new

  # In non-Rails contexts, set up model autoloading via a separate loader.
  # This is torn down by the Engine initializer when Rails boots.
  @models_loader = defined?(Rails::Engine) ? nil : build_models_loader
end

require "active_job/queue_adapters/pgbus_adapter" if defined?(ActiveJob)
require "pgbus/engine" if defined?(Rails::Engine)

# Define the optional MCP namespace and its `load!` entrypoint. This file is
# excluded from Zeitwerk (it references the optional `mcp` gem only inside
# load!), so require it explicitly here. Requiring it does NOT load the gem.
require "pgbus/mcp"
