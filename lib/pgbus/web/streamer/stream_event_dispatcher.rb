# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # The single-threaded consumer of the shared dispatch_queue. Drains
      # three kinds of messages:
      #
      #   - Listener::WakeMessage(queue_name:) — a NOTIFY fired; read_after
      #     the minimum cursor and fan out to every connection on the stream
      #     (both registered and in-flight connects).
      #
      #   - ConnectMessage(connection:) — a new SSE client connected. Runs
      #     the 5-step race-free replay sequence from design doc §6.5:
      #       1. ensure_listening on the stream (so future WakeMessages
      #          deliver to the in-flight buffer)
      #       2. register an in-flight buffer keyed by connection
      #       3. read_after(connection.since_id) + enqueue to connection
      #       4. drain the in-flight buffer into the connection (dedup is
      #          handled by Connection#enqueue's cursor check)
      #       5. move the connection from in-flight to the main Registry
      #
      #   - DisconnectMessage(connection:) — unregister and, if the stream
      #     now has zero subscribers, eventually unlisten (lazy GC,
      #     implemented in the Streamer sweep rather than here).
      #
      # All state ownership lives on this one thread: the registry is
      # thread-safe (Phase 2.1) but the in-flight buffers are local to
      # the dispatcher and accessed only from this thread, so no locks.
      #
      # Named StreamEventDispatcher (rather than just "Dispatcher") to
      # disambiguate from Pgbus::Process::Dispatcher, which is an
      # unrelated worker-side pool coordinator. See issue #98 item 8.
      class StreamEventDispatcher
        WakeMessage          = Listener::WakeMessage
        ConnectMessage       = Data.define(:connection)
        DisconnectMessage    = Data.define(:connection)
        # Posted by the OutboundPump (issue #321) after a successful off-thread
        # durable fanout write, carrying the highest msg_id the writer actually
        # committed for the connection. The dispatcher — the sole owner of
        # @scanned_cursor — drains these via apply_acks and advances the scan
        # cursor by accepted_max, so a failed/partial write never advances the
        # cursor past a frame that didn't reach the socket.
        WriteAckMessage      = Data.define(:connection, :accepted_max)
        # Posted by the Heartbeat once per tick with the current presence
        # connections, so the touch (a last_seen_at refresh) runs on the
        # dispatcher thread where AR connections are released each pass.
        PresenceTouchMessage = Data.define(:connections)

        # An unwrapped stream broadcast. Similar shape to
        # Pgbus::Client::ReadAfter::Envelope (msg_id + payload) so
        # Connection#enqueue can consume either type via duck typing,
        # but adds two delivery-control fields carried through from
        # Pgbus::Streams::Stream#broadcast:
        #   - `visible_to` — audience filter label (evaluated per-connection)
        #   - `exclude`    — a connection id to skip (actor-echo suppression:
        #                    the broadcaster's own SSE connection does not
        #                    receive the echo of its own broadcast)
        # The Dispatcher uses both to decide per-connection delivery;
        # Connection never sees either field.
        #   - `event`      — the SSE `event:` name for the delivered frame.
        #                    nil means the default (turbo-stream); a typed
        #                    name (e.g. "presence", "reactive") lets clients
        #                    route without sniffing the HTML (issue #170).
        StreamEnvelope = Data.define(:msg_id, :enqueued_at, :payload, :source, :visible_to, :exclude, :event) do
          def initialize(msg_id:, enqueued_at:, payload:, source:, visible_to: nil, exclude: nil, event: nil)
            super
          end
        end

        DEFAULT_READ_LIMIT = 500

        attr_reader :stream_counter

        def initialize(client:, registry:, listener:, dispatch_queue:,
                       logger: Pgbus.logger, read_limit: DEFAULT_READ_LIMIT,
                       filters: nil, config: nil, stream_counter: nil,
                       presence_provider: nil, pump: nil, ack_queue: nil)
          @client = client
          @registry = registry
          @listener = listener
          @queue = dispatch_queue
          @logger = logger
          @read_limit = read_limit
          # Off-thread durable fanout (issue #321). When @pump is non-nil
          # (streams_writer_threads > 0), handle_durable_wake hands each
          # registered-connection write to the pump instead of writing inline;
          # the pump reports back an accepted-max on @ack_queue, which the
          # dispatcher drains in apply_acks to advance @scanned_cursor. When
          # nil (default), fanout writes stay inline — the pre-#321 behavior.
          @pump = pump
          @ack_queue = ack_queue
          # Vends a presence handle for a logical stream name. Injected so
          # tests can record join/leave/touch without a DB. Production
          # defaults to the real per-stream Presence via Pgbus.stream.
          @presence_provider = presence_provider || ->(name) { Pgbus.stream(name).presence }
          # Filters default to the process-wide registry so production
          # code picks up whatever was registered at boot. Tests inject
          # a fresh Filters instance to avoid cross-test pollution.
          @filters = filters || Pgbus::Streams.filters
          # Config is injected so the Dispatcher can read
          # `streams_stats_enabled` without reaching into the global
          # Pgbus.configuration at every call site. Tests pass a
          # throwaway config to flip the flag independently of the
          # process-wide setting. Falls back to the global config
          # for production call sites that don't specify one.
          @config = config || Pgbus.configuration
          @presence = PresenceTracker.new(config: @config, presence_provider: @presence_provider, logger: @logger)
          # Two write deadlines (issue #315 item 3). Fanout writes run serially
          # on this single thread, so a slow client stalls the connections
          # queued behind it — the short fanout deadline bounds that stall.
          # Connect-replay writes run once per subscribe (not in the hot loop),
          # so a fresh client catching up from the archive keeps the full
          # deadline and isn't spuriously evicted.
          @fanout_write_deadline_ms  = @config.streams_fanout_write_deadline_ms
          @connect_write_deadline_ms = @config.streams_write_deadline_ms
          @stream_counter = stream_counter || StreamCounter.new
          # stream_name → Array<[connection, Array<Envelope>]>
          @in_flight = Hash.new { |h, k| h[k] = [] }
          # PGMQ full table name (pgbus_<prefix>_<name>) → logical stream
          # name. Populated on connect so handle_wake can translate
          # Listener::WakeMessage#queue_name (a full table name, because
          # that's what PG NOTIFY channels carry) into the logical name
          # used by Registry and the in-flight buffer.
          @full_to_logical = {}
          # Per-connection "scanned" cursor — the highest msg_id this
          # Dispatcher has examined for a given connection, whether or
          # not it was actually delivered. Needed because an audience
          # filter can drop an entire read_after batch; without a
          # separate scan cursor the dispatcher would re-read the
          # same hidden window forever and starve later public
          # messages. Connection#last_msg_id_sent still drives the
          # client-visible Last-Event-ID; this cursor only feeds
          # minimum_cursor so subsequent read_after calls advance.
          @scanned_cursor = {}
          # @running is a soft hint, not the authoritative stop signal.
          # The :__stop__ sentinel pushed onto @queue is what actually
          # terminates run_loop — even if a torn read of @running ever
          # happened (it cannot under MRI's GVL for a single-word
          # boolean assignment), the sentinel break would still fire.
          @running = false
          @thread = nil
          @ephemeral_seq = 0
        end

        def start
          return if @running

          @running = true
          @thread = Thread.new { run_loop }
          self
        end

        # Snapshot of this component's live thread(s). Instance#shutdown! captures
        # it BEFORE calling #stop so a join that timed out is still observable
        # after #stop has cleared the reference (issue #443).
        def threads
          [@thread].compact
        end

        def stop
          return unless @running

          @running = false
          @queue << :__stop__
          if @thread && @thread.join(5).nil?
            # join returned nil → 5s timeout. The thread is still running
            # (probably blocked inside an unresponsive client write or a
            # slow Postgres query). We log and clear the reference rather
            # than calling Thread#kill, which leaves IO state corrupt.
            # The orphaned thread will exit on its own once the blocking
            # call returns and it sees @running == false on the next loop.
            @logger.warn { "[Pgbus::Streamer::StreamEventDispatcher] thread did not terminate within 5s" }
          end
          @thread = nil
          self
        end

        private

        def run_loop
          while @running
            # Apply any writer acks before doing anything else, so the scan
            # cursor is current for the wake we're about to process — without
            # this top-of-loop drain a wake burst would compute minimum_cursor
            # from a stale floor and re-read/re-filter the whole un-acked
            # window every wake (the re-read storm, issue #321).
            apply_acks
            msg = @queue.pop
            break if msg == :__stop__

            begin
              if msg.is_a?(WakeMessage) && msg.payload.nil?
                wakes, trailing = drain_wakes_for(msg)
                wakes.each { |w| handle(w) }
                handle(trailing) if trailing
              else
                handle(msg)
              end
            ensure
              release_ar_connections
            end
          end
        rescue StandardError => e
          @logger.error { "[Pgbus::Streamer::StreamEventDispatcher] crashed: #{e.class}: #{e.message}" }
          raise
        end

        # Coalesces consecutive WakeMessages from the queue into one
        # per unique stream. Returns [coalesced_wakes, trailing_msg]
        # where trailing_msg is the first non-WakeMessage we hit (or
        # nil if the queue is empty after the wakes). The caller
        # processes the wakes first, then the trailing message — same
        # order as the original queue, but with redundant wakes folded.
        def drain_wakes_for(first)
          seen = Set.new([first.queue_name])
          coalesced = [first]
          loop do
            begin
              peek = @queue.pop(true)
            rescue ThreadError
              return [coalesced, nil] # queue drained
            end

            return [coalesced, peek] unless peek.is_a?(WakeMessage) && peek.payload.nil?

            next if seen.include?(peek.queue_name)

            seen.add(peek.queue_name)
            coalesced << peek
          end
        end

        def handle(msg)
          case msg
          when WakeMessage          then handle_wake(msg)
          when ConnectMessage       then handle_connect(msg)
          when DisconnectMessage    then handle_disconnect(msg)
          when PresenceTouchMessage then handle_presence_touch(msg)
          else
            @logger.warn { "[Pgbus::Streamer::StreamEventDispatcher] unknown message: #{msg.class}" }
          end
        rescue StandardError => e
          # Intentionally swallows per-message failures so one bad
          # broadcast can't kill the dispatcher thread and orphan every
          # connected client. The top-level run_loop rescue (below)
          # does re-raise — a crash *between* messages is a real bug
          # and the supervisor should see it.
          @logger.error { "[Pgbus::Streamer::StreamEventDispatcher] handling #{msg.class} raised #{e.class}: #{e.message}" }
        end

        def handle_wake(msg)
          started_at = monotonic_ms
          stream = @full_to_logical[msg.queue_name] || msg.queue_name
          registered = @registry.connections_for(stream)
          in_flight_pairs = @in_flight[stream]
          return if registered.empty? && in_flight_pairs.empty?

          if msg.payload
            handle_ephemeral_wake(msg, stream, registered, in_flight_pairs, started_at)
          else
            handle_durable_wake(stream, registered, in_flight_pairs, started_at)
          end
        end

        def handle_durable_wake(stream, registered, in_flight_pairs, started_at)
          # Drain writer acks before computing the read floor so a burst of
          # wakes doesn't re-read the un-acked window (issue #321).
          apply_acks
          min_seen = minimum_cursor(registered, in_flight_pairs)
          raw_envelopes = @client.read_after(stream, after_id: min_seen, limit: @read_limit)
          return if raw_envelopes.empty?

          envelopes = raw_envelopes.map { |e| unwrap_stream_envelope(e) }
          max_msg_id = envelopes.map(&:msg_id).max

          registered.each do |conn|
            visible = visible_envelopes_for(envelopes, conn)
            if @pump
              # Off-thread: hand the write to the pump and DEFER the scan-cursor
              # advance to the ack (which carries the writer's accepted max).
              # Post even when `visible` is empty, with batch max, so an
              # audience-filtered-away batch still advances the cursor past the
              # hidden window (issue #321 B2). The pump raises on ephemerals.
              @pump.post(conn, visible, max_msg_id, deadline_ms: @fanout_write_deadline_ms)
            else
              safe_enqueue(conn, visible)
              advance_scanned_cursor(conn, max_msg_id)
            end
          end
          in_flight_pairs.each do |(conn, buffer)|
            buffer.concat(visible_envelopes_for(envelopes, conn))
            advance_scanned_cursor(conn, max_msg_id)
          end

          prune_dead(registered)
          @stream_counter.increment_broadcasts(stream)

          record_stat(
            stream_name: stream,
            event_type: "broadcast",
            started_at: started_at,
            fanout: registered.size + in_flight_pairs.size
          )
        end

        def handle_ephemeral_wake(msg, stream, registered, in_flight_pairs, started_at)
          parsed = JSON.parse(msg.payload)
          html = parsed.is_a?(Hash) ? parsed["html"] : nil
          return unless html.is_a?(String)

          visible_to = parsed["visible_to"]
          visible_to = visible_to.to_sym if visible_to.is_a?(String)
          exclude = parsed["exclude"]

          @ephemeral_seq += 1
          envelope = StreamEnvelope.new(
            msg_id: -@ephemeral_seq,
            enqueued_at: Time.now.utc.iso8601(6),
            payload: html,
            source: "ephemeral",
            visible_to: visible_to,
            exclude: exclude,
            event: normalize_sse_event(parsed["event"])
          )

          registered.each do |conn|
            safe_enqueue(conn, visible_envelopes_for([envelope], conn))
          end
          in_flight_pairs.each do |(conn, buffer)|
            buffer.concat(visible_envelopes_for([envelope], conn))
          end

          prune_dead(registered)
          @stream_counter.increment_broadcasts(stream)

          record_stat(
            stream_name: stream,
            event_type: "broadcast",
            started_at: started_at,
            fanout: registered.size + in_flight_pairs.size,
            ephemeral: true
          )
        end

        def handle_connect(msg)
          started_at = monotonic_ms
          connection = msg.connection
          stream = connection.stream_name

          # Step 1: subscribe first. Any WakeMessage that arrives after
          # this line will see our in-flight buffer and fan out into it.
          # The Listener is told the prefixed PGMQ queue name (not the
          # logical stream name) because the NOTIFY channel includes the
          # prefix: pgmq.q_<prefixed>.INSERT. Registry and the in-flight
          # buffer use the logical name. The Dispatcher is the single
          # translator between the two naming worlds.
          full_name = notify_queue_name_for(stream)
          @full_to_logical[full_name] = stream
          @listener.ensure_listening(full_name)

          # Step 2: install the in-flight buffer BEFORE any read.
          buffer = []
          @in_flight[stream] << [connection, buffer]

          # Step 3: read the archive for anything published before this
          # connect landed, and write to the connection.
          raw_initial = @client.read_after(
            stream,
            after_id: connection.last_msg_id_sent,
            limit: @read_limit
          )
          initial = raw_initial.map { |e| unwrap_stream_envelope(e) }
          # Connect-replay (steps 3 & 4) uses the FULL write deadline, not the
          # short fanout one: this runs once per subscribe, outside the serial
          # hot loop, and a new client catching up a large archive backlog must
          # not be killed by the 250ms fanout budget (issue #315 item 3).
          safe_enqueue(connection, visible_envelopes_for(initial, connection),
                       deadline_ms: @connect_write_deadline_ms)

          # Step 4: drain the in-flight buffer (anything published between
          # step 2 and now). Connection#enqueue dedupes by cursor, so
          # overlap with step 3 is safe. The buffer entries were already
          # filtered when enqueued by handle_wake, so no re-filter here.
          safe_enqueue(connection, buffer, deadline_ms: @connect_write_deadline_ms)

          # Step 5: promote to the main registry. From this point the
          # regular WakeMessage path handles the connection. If the
          # connection died during steps 3/4 (e.g. client vanished
          # mid-replay, Connection#enqueue marks it dead without
          # raising), no DisconnectMessage will ever be emitted, so
          # we have to scrub @full_to_logical + the PG LISTEN right
          # here. Otherwise this stream's state is pinned for the
          # life of the worker.
          remove_in_flight(stream, connection)
          @stream_counter.increment_total_connections(stream)
          if connection.dead?
            @scanned_cursor.delete(connection)
            cleanup_stream_if_unused(stream)
          else
            @stream_counter.increment_connections(stream)
            @registry.register(connection)
            presence_join(connection, stream)
          end

          record_stat(
            stream_name: stream,
            event_type: "connect",
            started_at: started_at
          )
        rescue StandardError => e
          # Same leak path for exceptions in steps 1-4. Mark dead and
          # scrub state so a transient failure on a single connect
          # doesn't permanently bloat @full_to_logical or leave a
          # dangling LISTEN on the PG connection.
          remove_in_flight(stream, connection)
          @scanned_cursor.delete(connection)
          cleanup_stream_if_unused(stream)
          connection.mark_dead!
          @logger.error { "[Pgbus::Streamer::StreamEventDispatcher] connect failed for #{connection.id}: #{e.class}: #{e.message}" }
        end

        def handle_disconnect(msg)
          started_at = monotonic_ms
          connection = msg.connection
          stream = connection.stream_name
          removed = @registry.unregister(connection)
          @scanned_cursor.delete(connection)
          @stream_counter.decrement_connections(stream) if removed
          presence_leave(connection, stream)
          cleanup_stream_if_unused(stream)

          record_stat(
            stream_name: stream,
            event_type: "disconnect",
            started_at: started_at
          )
        end

        # Touches the presence rows of the given connections and expires
        # silent seats. Posted by the Heartbeat each tick so idle but
        # still-connected members don't get swept (see PresenceTracker).
        def handle_presence_touch(msg)
          @presence.touch(msg.connections)
        end

        # If this stream has no remaining subscribers (registered or
        # in-flight), release all per-stream state so long-running
        # processes don't leak memory proportional to unique stream
        # count (important for apps that use GlobalID-keyed streams
        # like `order_42`). Three places to clean up:
        #   1. @full_to_logical (the translation map — this file)
        #   2. @in_flight[stream] (cleared by remove_in_flight already)
        #   3. Listener's @listening_to set + the PG LISTEN itself
        def cleanup_stream_if_unused(stream)
          return unless @registry.empty?(stream) && @in_flight[stream].empty?

          full_name = @full_to_logical.key(stream)
          return unless full_name

          @full_to_logical.delete(full_name)
          @listener.remove_listening(full_name)
        end

        def minimum_cursor(registered, in_flight_pairs)
          # Prefer the scanned cursor (per-connection max msg_id this
          # Dispatcher has examined) over Connection#last_msg_id_sent
          # (per-connection max successfully written). The two only
          # differ when an audience filter drops envelopes: the scanned
          # cursor advances past the hidden window so the next
          # read_after moves forward. Falls back to last_msg_id_sent
          # for connections that haven't been scanned yet (fresh
          # in-flight entries on their first handle_wake pass).
          cursors = registered.map { |c| cursor_for(c) }
          in_flight_pairs.each { |(conn, _buf)| cursors << cursor_for(conn) }
          cursors.min || 0
        end

        def cursor_for(connection)
          [@scanned_cursor.fetch(connection, 0), connection.last_msg_id_sent].max
        end

        def advance_scanned_cursor(connection, msg_id)
          return if msg_id.nil?

          current = @scanned_cursor[connection] || 0
          @scanned_cursor[connection] = msg_id if msg_id > current
        end

        # Drain every pending writer ack and advance the scan cursor by the
        # writer's accepted max (issue #321). Runs ONLY on the dispatcher
        # thread, so @scanned_cursor keeps its single-owner invariant — the
        # pump only reports a value via @ack_queue, it never mutates cursor
        # state. Non-blocking: stops when the queue is empty. A no-op when
        # offload is off (@ack_queue nil). advance_scanned_cursor is
        # monotonic-max-guarded, so a re-applied or cross-connection-reordered
        # ack is idempotent.
        #
        # Drops a stale ack for a connection that is no longer registered: the
        # ack queue is drained independently of DisconnectMessages, so a
        # successful-write ack can arrive AFTER handle_disconnect has already
        # deleted @scanned_cursor[conn] (e.g. the heartbeat swept an idle client
        # mid-write). Advancing the cursor then would resurrect the connection
        # in @scanned_cursor and leak that entry forever. The equal? check also
        # rejects an ack for a since-replaced connection object sharing the same
        # id.
        def apply_acks
          return unless @ack_queue

          loop do
            ack = @ack_queue.pop(true)
            next unless @registry.lookup(ack.connection.id).equal?(ack.connection)

            advance_scanned_cursor(ack.connection, ack.accepted_max)
          rescue ThreadError
            break # queue drained
          end
        end

        # deadline_ms defaults to the SHORT fanout deadline: every hot-loop
        # fanout call site (handle_durable_wake, handle_ephemeral_wake) uses
        # it implicitly. handle_connect passes @connect_write_deadline_ms
        # explicitly so a new client's initial replay keeps the full window
        # (issue #315 item 3).
        def safe_enqueue(connection, envelopes_or_buffer, deadline_ms: @fanout_write_deadline_ms)
          return if connection.dead?
          return if envelopes_or_buffer.empty?

          connection.enqueue(envelopes_or_buffer, deadline_ms: deadline_ms)
        end

        def prune_dead(connections)
          connections.each do |conn|
            @queue << DisconnectMessage.new(connection: conn) if conn.dead?
          end
        end

        def remove_in_flight(stream, connection)
          pairs = @in_flight[stream]
          pairs.reject! { |(conn, _buf)| conn.equal?(connection) }
          @in_flight.delete(stream) if pairs.empty?
        end

        # Translates a logical stream name (e.g. "chat") into the prefixed
        # PGMQ queue name (e.g. "pgbus_int_chat") that appears in the
        # NOTIFY channel `pgmq.q_<prefixed>.INSERT`. Mirrors the prefix
        # Pgbus::Client#send_message already applied when the broadcast
        # was published, so the Listener's LISTEN matches the NOTIFY.
        def notify_queue_name_for(stream_name)
          @client.config.queue_name(stream_name)
        end

        # Sanitizes a typed SSE event name from an untrusted broadcast
        # payload before it reaches the SSE `event:` line. Returns nil
        # (→ the default turbo-stream event) for non-strings, blanks, or
        # any value containing CR/LF — a crafted event with a newline could
        # otherwise inject extra SSE fields (a forged id:/data:) into the
        # frame and corrupt cursor/event routing. Defense in depth with
        # Envelope.message, which also strips newlines.
        def normalize_sse_event(value)
          return nil unless value.is_a?(String)

          event = value.strip
          return nil if event.empty? || event.match?(/[\r\n]/)

          event
        end

        # Pgbus::Streams::Stream#broadcast wraps HTML payloads as
        # {"html": "..."} so PGMQ's JSONB column accepts them. Here we
        # unwrap the html field and return a new envelope whose payload
        # is the raw HTML, ready for the SSE `data:` line. If the
        # payload is not a valid JSON object with an html key (e.g. a
        # legacy broadcast that predates this subsystem), we fall back
        # to passing it through untouched — a permissive approach that
        # plays nicely with ad-hoc `Pgbus.client.send_message` calls
        # pointed at stream queues by mistake.
        def unwrap_stream_envelope(envelope)
          parsed = JSON.parse(envelope.payload.to_s)
          html = parsed.is_a?(Hash) ? parsed["html"] : nil
          return envelope unless html.is_a?(String)

          visible_to = parsed["visible_to"]
          visible_to = visible_to.to_sym if visible_to.is_a?(String)
          exclude = parsed["exclude"]

          StreamEnvelope.new(
            msg_id: envelope.msg_id,
            enqueued_at: envelope.enqueued_at,
            payload: html,
            source: envelope.source,
            visible_to: visible_to,
            exclude: exclude,
            event: normalize_sse_event(parsed["event"])
          )
        rescue JSON::ParserError
          envelope
        end

        # Filters a list of envelopes against a specific connection's
        # context. Envelopes without a visible_to label pass through
        # unchanged; envelopes with a label are evaluated via the
        # Filters registry. Envelopes that predate the StreamEnvelope
        # refactor (plain ReadAfter::Envelope with no visible_to) also
        # pass through.
        #
        # Actor-echo suppression: an envelope carrying `exclude:` (a
        # connection id) is dropped for the connection whose id matches.
        # This lets the broadcaster's own SSE connection skip the echo of
        # its own broadcast — the actor already applied the change via its
        # action's HTTP response, so re-applying the SSE echo would
        # double-apply (re-run animations, clobber optimistic edits). The
        # exclude check runs *before* the audience filter so an excluded
        # actor is skipped even when it would otherwise match visible_to.
        def visible_envelopes_for(envelopes, connection)
          envelopes.select do |envelope|
            next false if excluded?(envelope, connection)

            label = envelope.respond_to?(:visible_to) ? envelope.visible_to : nil
            @filters.visible?(label, connection.context)
          end
        end

        # True when the envelope names this connection in its `exclude`
        # field. Guarded by respond_to? so plain ReadAfter::Envelopes
        # (no exclude field) and connections without an id never match.
        def excluded?(envelope, connection)
          return false unless envelope.respond_to?(:exclude)

          exclude = envelope.exclude
          return false if exclude.nil? || exclude.to_s.empty?

          connection.respond_to?(:id) && connection.id.to_s == exclude.to_s
        end

        # Connection-driven presence (issues #169, #515): see PresenceTracker.
        def presence_join(connection, stream)
          @presence.join(connection, stream)
        end

        def presence_leave(connection, stream)
          @presence.leave(connection, stream)
        end

        def monotonic_ms
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC) * 1000.0
        end

        # Opt-in stream event stat recording. Gated by
        # `config.streams_stats_enabled` (default false) because
        # stream volume can dwarf job volume in chat-style apps,
        # and the Insights surface is only worth the INSERT cost
        # if operators actually look at it. All failures are
        # swallowed by StreamStat.record! itself so a stats-table
        # outage cannot block the dispatcher.
        # Release any AR connections the dispatcher fiber acquired during
        # this iteration (typically from StreamStat.record! via BusRecord).
        # Without this, the connection stays leased while the fiber parks
        # on @queue.pop, blocking clear_reloadable_connections! on the
        # next Rails code reload (10s wedge under rack-timeout).
        def release_ar_connections
          return unless defined?(::ActiveRecord::Base)

          Pgbus::BusRecord.connection_handler.clear_active_connections!
        rescue StandardError => e
          @logger.debug { "[Pgbus::Streamer::StreamEventDispatcher] AR connection release failed: #{e.class}: #{e.message}" }
        end

        def record_stat(stream_name:, event_type:, started_at:, fanout: nil, ephemeral: false)
          return unless ephemeral || @config.streams_stats_enabled

          Pgbus::StreamStat.record!(
            stream_name: stream_name,
            event_type: event_type,
            duration_ms: (monotonic_ms - started_at).round,
            fanout: fanout
          )
        end
      end
    end
  end
end
