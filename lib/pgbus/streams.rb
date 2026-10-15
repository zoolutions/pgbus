# frozen_string_literal: true

module Pgbus
  # Pgbus::Streams is the SSE-based pub/sub subsystem that replaces
  # `turbo_stream_from`. The user-facing entrypoint is `Pgbus.stream(name)`,
  # which returns a `Pgbus::Streams::Stream` providing `#broadcast`,
  # `#current_msg_id`, and `#read_after`.
  module Streams
    # Raised when a composed stream name would overflow PGMQ's queue-name
    # budget (derived from PostgreSQL's NAMEDATALEN=64, minus PGMQ's `q_`
    # table prefix, minus the configured queue_prefix + separator).
    #
    # Inherits from ArgumentError so existing rescues of the underlying
    # QueueNameValidator error keep working; callers that want to handle
    # this specifically can rescue Pgbus::Streams::StreamNameTooLong.
    class StreamNameTooLong < ArgumentError; end

    # Raised when an ephemeral broadcast's JSON payload exceeds PostgreSQL's
    # NOTIFY payload budget (< 8000 bytes). Ephemeral frames ride the NOTIFY
    # itself, so the cap is a hard PostgreSQL limit — durable mode (payload
    # stored in PGMQ, NOTIFY as a bare wake) has no such cap.
    #
    # Stream#broadcast never raises this: an oversized ephemeral frame
    # auto-degrades to a durable publish (issue #391). The error exists for
    # direct Client#notify_stream callers, where the previous failure mode
    # was a misleading PGMQ::Errors::ConnectionError ("payload string too
    # long") that pointed diagnosis at the connection instead of the payload.
    class PayloadTooLarge < Pgbus::Error; end

    # The default SSE `event:` name for a broadcast frame. Turbo's
    # StreamObserver consumes frames the client re-dispatches as the
    # `message` DOM event; the client maps this SSE event name to
    # `message`. Broadcasts may override it with a typed name (issue #170).
    DEFAULT_SSE_EVENT = "turbo-stream"

    class << self
      # Process-wide registry of server-side audience filter predicates.
      # Register filters at boot time via:
      #   Pgbus::Streams.filters.register(:admin_only) { |user| user.admin? }
      # See lib/pgbus/streams/filters.rb for the full API.
      #
      # Process-wide publish-side coalescer for high-frequency broadcasts
      # (issue #171); the flush re-enters the normal broadcast path so a
      # coalesced frame is just a deferred ordinary broadcast.
      #
      # Both are built when this file loads, not by `||=` on first use: two
      # first callers could each build a registry, and a filter registered on
      # the one that lost was gone (issue #527). Replaced only by the resets.
      attr_reader :filters, :coalescer

      # Fresh filters registry. Used by tests; not intended for runtime.
      def reset_filters!
        @reset_lock.synchronize { @filters = Filters.new }
      end

      # Fresh coalescer. Used by tests; not intended for runtime.
      def reset_coalescer!
        @reset_lock.synchronize { @coalescer = build_coalescer }
      end

      private

      def build_coalescer
        # `target:` is part of the flush signature but unused here — it's only
        # a coalescing key; the target is already encoded in the payload's
        # turbo-stream tag. Absorbed via ** so it isn't a broadcast argument.
        Coalescer.new(
          flush: lambda do |stream_name:, payload:, opts:, **|
            Pgbus.stream(stream_name).broadcast(payload, **opts)
          end
        )
      end
    end

    @reset_lock = Mutex.new
    @filters = Filters.new
    @coalescer = build_coalescer

    # A handle on a single logical stream. The name can be any string, an
    # object responding to `to_gid_param`, or an array of streamables (which
    # are joined with colons — turbo-rails-compatible).
    #
    # Stream is *not* a singleton — callers create instances ad hoc, but the
    # underlying queue is created lazily and only once per process per name.
    class Stream
      attr_reader :name

      def initialize(streamables, client: Pgbus.client, durable: true)
        @name = self.class.name_from(streamables)
        self.class.validate_name_length!(@name, streamables)
        @client = client
        @durable = durable
        @ensured = false
        @ensure_mutex = Mutex.new
      end

      def durable?
        @durable
      end

      # Broadcasts a Turbo Stream HTML payload through the pgbus streamer.
      # PGMQ's `message` column is JSONB, so raw HTML strings can't be passed
      # directly. We wrap as `{"html": "..."}` on the way in and unwrap in
      # Pgbus::Web::Streamer::StreamEventDispatcher before delivering to the SSE client.
      # Callers pass a plain HTML string; the wrapping is an implementation
      # detail.
      #
      # Transactional semantics: if this call is made inside an open
      # ActiveRecord transaction, the PGMQ insert is deferred to an
      # after_commit callback. If the transaction rolls back, the broadcast
      # silently drops — clients never see the change that the database
      # never persisted. This is the feature no other Rails real-time stack
      # (including turbo-rails over ActionCable) can offer: the broadcast
      # and the data mutation are atomic with respect to each other.
      # Returns the assigned msg_id when sent synchronously, nil when
      # deferred (the id isn't known until the after_commit callback runs).
      #
      # Audience filtering: pass `visible_to:` with a filter label (a
      # Symbol previously registered via Pgbus::Streams.filters.register)
      # to restrict delivery to connections whose authorize-hook context
      # satisfies the predicate. The label travels with the broadcast
      # through PGMQ; the predicate itself lives in-process on the
      # subscriber side and is evaluated per-connection by the Dispatcher.
      # Per-broadcast `durable:` overrides the stream-level default for a
      # single broadcast. `nil` (the default) defers to the stream's own
      # `durable?` setting; `true`/`false` flip the mode for this call only.
      #
      # Actor-echo suppression: pass `exclude:` with the broadcaster's own
      # SSE connection id (surfaced to the page as
      # `<meta name="pgbus-connection-id">` / the element's `connection-id`
      # attribute, sent back on the action request as the
      # `X-Pgbus-Connection` header). The dispatcher skips delivery to that
      # one connection, so the actor doesn't receive the echo of its own
      # broadcast — it already applied the change via the action's HTTP
      # response. Everyone else gets the broadcast. A nil/blank `exclude`
      # is a no-op (the common path).
      # Typed SSE event name: pass `event:` to set the SSE `event:` field
      # on the delivered frame (e.g. `event: "presence"`, `event:
      # "reactive"`) while keeping the payload a Turbo Stream. Clients that
      # care can route on the typed event without sniffing the HTML;
      # default consumers still receive the standard turbo-stream/`message`
      # path. The default (`nil` or `"turbo-stream"`) is not carried on the
      # wire — it's the implicit default the dispatcher applies.
      # High-frequency coalescing: pass `coalesce:` (a window in milliseconds,
      # or `true` for the default window) together with `target:` to batch
      # rapid broadcasts to the same (stream, target) and publish only the
      # latest within the window. Superseded frames never reach the bus.
      # Last-write-wins, so this is only safe for idempotent replace/update
      # of a stable target (the high-frequency case: cursors, typing,
      # progress). Returns nil — the actual broadcast is deferred to the
      # coalescer's flush. See issue #171 and Pgbus::Streams::Coalescer.
      def broadcast(payload, visible_to: nil, durable: nil, exclude: nil, event: nil, coalesce: nil, target: nil)
        if coalesce
          return coalesce_broadcast(
            payload,
            coalesce: coalesce,
            target: target,
            visible_to: visible_to,
            durable: durable,
            exclude: exclude,
            event: event
          )
        end

        wrapped = { "html" => payload.to_s }
        wrapped["visible_to"] = visible_to.to_s if visible_to
        wrapped["exclude"] = exclude.to_s if exclude && !exclude.to_s.empty?
        wrapped["event"] = event.to_s if event && event.to_s != DEFAULT_SSE_EVENT

        use_durable = durable.nil? ? @durable : durable
        return broadcast_ephemeral(wrapped) unless use_durable

        ensure_queue!
        transaction = current_open_transaction
        instrument_payload = {
          stream: @name,
          visible_to: visible_to,
          deferred: !transaction.nil?,
          bytes: wrapped["html"].bytesize
        }
        Instrumentation.instrument("pgbus.stream.broadcast", instrument_payload) do
          if transaction
            transaction.after_commit { @client.send_stream_message(@name, wrapped) }
            nil
          else
            @client.send_stream_message(@name, wrapped)
          end
        end
      end

      # Renders a renderable (a Phlex component, a ViewComponent, or a
      # pre-rendered HTML string) into a complete `<turbo-stream>` action
      # tag and broadcasts it atomically — the render and the broadcast in
      # one call. This removes the #1 footgun in server-driven UI: building
      # the off-request render context and the turbo-stream wrapper by hand
      # at every call site.
      #
      #   Pgbus.stream("chat", room).broadcast_render(
      #     renderable: Chat::Message.new(chat_message: msg),
      #     action: :append, target: "chat-messages-#{room}",
      #     exclude: connection_id   # composes with #165
      #   )
      #
      # `action` defaults to :replace. `target` is required. `exclude:`,
      # `visible_to:`, and `durable:` are forwarded to #broadcast unchanged,
      # so actor-echo suppression and audience filtering compose. Returns
      # whatever #broadcast returns (msg_id, or nil when deferred to
      # after_commit inside a transaction).
      #
      # See Pgbus::Streams::Renderer for the renderable-resolution order.
      # Components that need URL helpers or a full view context should be
      # rendered by the app (which has the request context) and the
      # resulting string passed as `renderable:`.
      def broadcast_render(target:, action: :replace, renderable: nil, visible_to: nil, durable: nil, exclude: nil,
                           event: nil, coalesce: nil)
        html = Renderer.turbo_stream_tag(action: action, target: target, renderable: renderable)
        broadcast(html, visible_to: visible_to, durable: durable, exclude: exclude, event: event,
                        coalesce: coalesce, target: target)
      end

      def current_msg_id
        @client.stream_current_msg_id(@name)
      end

      def read_after(after_id:, limit: 500)
        @client.read_after(@name, after_id: after_id, limit: limit)
      end

      def ensure!
        ensure_queue!
        self
      end

      # Returns a Pgbus::Streams::Presence handle for this stream.
      # The Presence object exposes join/leave/touch/members/sweep!
      # for tracking who is currently subscribed. See
      # lib/pgbus/streams/presence.rb for the API.
      def presence
        @presence ||= Presence.new(self)
      end

      # Mirrors `Turbo::Streams::StreamName#stream_name_from`. Strings pass
      # through; objects with `to_gid_param` or `to_param` are coerced; arrays
      # are joined with `:`. The result is suitable both as a logical stream
      # identifier and as the input to QueueNameValidator (after sanitisation).
      def self.name_from(streamables)
        if streamables.is_a?(Array)
          streamables.map { |s| name_from(s) }.join(":")
        elsif streamables.respond_to?(:to_gid_param)
          streamables.to_gid_param
        elsif streamables.respond_to?(:to_param) && !streamables.is_a?(Symbol)
          streamables.to_param
        else
          streamables.to_s
        end
      end

      # Enforces the pgbus queue-name budget at the Stream-construction
      # boundary so a forgotten call site fails with an actionable error
      # (pointing at the offending streamables and suggesting
      # `Pgbus.stream_key`) instead of an opaque QueueNameValidator
      # failure three frames deep in Client#ensure_stream_queue.
      #
      # The budget is computed from `config.queue_prefix` at call time
      # so apps that override the prefix get the correct limit. Does not
      # mutate the name — silent truncation is a footgun for
      # multi-tenant apps where collisions would mix broadcasts across
      # records. Callers who need a short, safe identifier should use
      # `Pgbus.stream_key(...)` or include `Pgbus::Streams::Streamable`
      # on their ActiveRecord models.
      def self.validate_name_length!(name, streamables)
        budget = Key.queue_name_budget
        return if name.length <= budget

        raise StreamNameTooLong,
              "Stream name #{name.inspect} is #{name.length} chars, " \
              "exceeds pgbus budget of #{budget} " \
              "(queue_prefix=#{Pgbus.configuration.queue_prefix.inspect}, " \
              "pgbus_max_queue_name_length=#{QueueNameValidator::MAX_QUEUE_NAME_LENGTH}). " \
              "Streamables: #{streamables.inspect}. " \
              "Use Pgbus.stream_key(*streamables) to produce a safe short name, " \
              "or include Pgbus::Streams::Streamable on the model."
      end

      private

      # Ephemeral frames ride the NOTIFY payload itself, which PostgreSQL
      # caps below 8000 bytes. A frame over the cap auto-degrades to a
      # durable publish (issue #391): payload stored in PGMQ, the queue's
      # insert trigger fires the NOTIFY as a bare wake on the same channel
      # the subscriber already LISTENs on — delivery semantics preserved,
      # cap irrelevant. The JSON is generated once here and passed
      # pre-serialized to notify_stream so the size check costs no extra
      # allocation on the hot path.
      def broadcast_ephemeral(wrapped)
        json = JSON.generate(wrapped)
        return durable_fallback(wrapped, json.bytesize) if json.bytesize > Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES

        @client.notify_stream(@name, json)
        nil
      end

      # The durable degrade path for an oversized ephemeral frame. Stays
      # fire-and-forget like the ephemeral path it replaces: no after_commit
      # deferral (pg_notify runs on the PGMQ pool connection, outside the
      # request's AR transaction, so the ephemeral path never deferred
      # either). Returns the msg_id like any durable publish.
      def durable_fallback(wrapped, bytes)
        Pgbus.logger.warn do
          "[Pgbus::Streams] ephemeral broadcast on #{@name.inspect} is #{bytes} bytes " \
            "(NOTIFY cap is #{Client::NotifyStream::NOTIFY_PAYLOAD_LIMIT_BYTES}); " \
            "publishing durably instead. Consider durable mode for this stream " \
            "(streams_durable_patterns or durable: true) to skip this check."
        end
        ensure_queue!
        instrument_payload = {
          stream: @name,
          visible_to: wrapped["visible_to"],
          deferred: false,
          bytes: wrapped["html"].bytesize,
          ephemeral_fallback: true
        }
        Instrumentation.instrument("pgbus.stream.broadcast", instrument_payload) do
          @client.send_stream_message(@name, wrapped)
        end
      end

      # Submits a frame to the process-wide coalescer instead of
      # broadcasting now. Requires a target (the dedupe key — there's no
      # way to last-write-win without one). The window is `coalesce` in ms
      # when numeric, else the coalescer's default. Returns nil; the
      # coalescer flushes the latest frame as an ordinary broadcast.
      def coalesce_broadcast(payload, coalesce:, target:, visible_to:, durable:, exclude:, event:)
        if target.nil? || target.to_s.empty?
          raise ArgumentError,
                "broadcast(coalesce:) requires target: — coalescing keys on (stream, target), " \
                "so the latest frame per target can win. Use broadcast_render(coalesce:) which " \
                "already has a target, or pass target: explicitly."
        end

        window_ms = coalesce_window_ms(coalesce)
        # Resolve durability against THIS stream instance now, not nil. The
        # coalescer's flush re-enters Pgbus.stream(@name), whose durability
        # could resolve differently (config patterns) than this instance —
        # passing the resolved value keeps the coalesced frame's mode stable.
        resolved_durable = durable.nil? ? @durable : durable

        submit = lambda do
          Streams.coalescer.submit(
            stream_name: @name,
            target: target.to_s,
            payload: payload.to_s,
            window_ms: window_ms,
            opts: { visible_to: visible_to, durable: resolved_durable, exclude: exclude, event: event }
          )
        end

        # Transaction gating: a coalesced DURABLE broadcast made inside an
        # open transaction must not flush if the transaction rolls back, so
        # defer the submission to after_commit (mirrors the non-coalesced
        # path). Ephemeral coalescing is fire-and-forget, so it submits now.
        transaction = resolved_durable ? current_open_transaction : nil
        if transaction
          transaction.after_commit { submit.call }
        else
          submit.call
        end
        nil
      end

      # Resolves the coalescing window in ms. `true` → the default window;
      # a positive Numeric is used as-is. Anything else is a misuse and is
      # rejected at the API boundary with an actionable error rather than a
      # cryptic NoMethodError deep inside the coalescer's timer math.
      def coalesce_window_ms(coalesce)
        return Coalescer::DEFAULT_WINDOW_MS if coalesce == true
        return coalesce if coalesce.is_a?(Numeric) && coalesce.positive?

        raise ArgumentError,
              "coalesce: must be true or a positive Numeric window in milliseconds, got #{coalesce.inspect}"
      end

      def ensure_queue!
        return if @ensured

        @ensure_mutex.synchronize do
          return if @ensured

          @client.ensure_stream_queue(@name)
          @ensured = true
        end
      end

      # Returns the current AR transaction if one is open, nil otherwise.
      # Guarded by defined? so the streams subsystem stays usable in
      # non-Rails contexts. AR's NullTransaction yields immediately from
      # after_commit, so we have to check #open? explicitly — otherwise
      # every call path would hit the "deferred" branch and we'd lose the
      # msg_id return value.
      #
      # The probe must only inspect a connection the calling thread/fiber
      # ALREADY leases — `connection_pool.active_connection?`, never
      # `ActiveRecord::Base.connection`. On Rails 7.2+ `.connection` takes a
      # sticky executor-scoped lease: on a non-executor thread (the
      # Coalescer's flush thread, app worker threads) it is never released —
      # one pool connection pinned per thread — and inside
      # `with_connection` the sticky flag defeats the block-exit release, so
      # the CALLER's connection leaks when its thread dies. Both variants
      # exhausted an exactly-sized pool deterministically (Zazu fan-out
      # incident, 2026-08-05). Semantics are unchanged: a transaction is
      # per-lease, so a thread holding no connection can have no open
      # transaction to defer on — the old code's fresh checkout always
      # answered nil anyway, at the price of the leak.
      def current_open_transaction
        return nil unless defined?(::ActiveRecord::Base)

        connection = ::ActiveRecord::Base.connection_pool.active_connection?
        return nil unless connection

        transaction = connection.current_transaction
        transaction if transaction.open?
      rescue StandardError => e
        # Defensive: if AR is loaded but not yet connected (e.g. a
        # Rake task invoked before Rails boot), don't let the transaction
        # probe break the broadcast. Debug-logged so a misconfigured
        # app can diagnose "why aren't my transactional broadcasts
        # deferring?" without impacting production performance
        # (debug is off by default).
        Pgbus.logger.debug do
          "[Pgbus::Streams::Stream] transaction probe failed (#{e.class}: #{e.message}); " \
            "falling back to synchronous broadcast"
        end
        nil
      end
    end
  end
end
