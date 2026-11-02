# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Wraps a single hijacked SSE client socket with its own cursor state,
      # per-io mutex, and liveness flag. Owns no threads — the Dispatcher and
      # Heartbeat threads call #enqueue / #write_comment on Connection instances
      # directly, and the per-io mutex in IoWriter serialises concurrent writes.
      #
      # Cursor semantics: `last_msg_id_sent` is strictly monotonic. `enqueue`
      # filters envelopes with `msg_id > last_msg_id_sent` and advances the
      # cursor only for envelopes that actually wrote successfully. This is
      # the client-side leg of the replay-race fix (§6.5 of the design doc).
      class Connection
        attr_reader :id, :stream_name, :io, :mutex, :context
        # The client's per-tab presence seat (?seat=), or nil (issue #515).
        attr_reader :seat
        # The presence member id this connection auto-joined as, or nil for
        # non-presence streams / anonymous connections. Set by the
        # Dispatcher on connect; read on disconnect and heartbeat touch.
        attr_accessor :presence_member
        # True when presence_member joined as a seat row, false when as a
        # member-keyed row (table not yet migrated). Leave follows the join.
        attr_accessor :presence_seat

        # last_msg_id_sent is the ONE field whose writer can cross threads: with
        # streams_writer_threads > 0 (issue #321) the OutboundPump worker thread
        # advances it inside #enqueue, while the dispatcher thread reads it via
        # cursor_for / handle_connect. A Concurrent::AtomicReference gives a real
        # happens-before on every Ruby engine (not just MRI's GVL) at ~ns cost —
        # one worker only ever writes a given connection's field (stable
        # partition), so there's no writer-writer contention. Inline mode
        # (default) reads/writes it single-threaded, semantically identical.
        def last_msg_id_sent
          @last_msg_id_sent.get
        end

        def initialize(id:, stream_name:, io:, since_id:, writer:, write_deadline_ms:, context: nil, seat: nil)
          @id = id
          @stream_name = stream_name
          @io = io
          @last_msg_id_sent = Concurrent::AtomicReference.new(since_id.to_i)
          @writer = writer
          @write_deadline_ms = write_deadline_ms
          @mutex = Mutex.new
          @dead = false
          @closed = false
          @presence_member = nil
          @created_at = monotonic
          @last_write_at = @created_at
          # Context is whatever the StreamApp's authorize hook returned
          # (a truthy non-boolean value). Typically a user model or a
          # session hash. The Dispatcher passes it to the Filters
          # registry when evaluating visible_to predicates. Defaults to
          # nil for tests that don't need audience filtering.
          @context = context
          @seat = seat
        end

        # deadline_ms defaults to the connection's own write deadline so every
        # existing caller is unchanged. The Dispatcher overrides it with the
        # short streams_fanout_write_deadline_ms for hot-loop fanout writes,
        # bounding head-of-line blocking on a slow client (issue #315 item 3).
        #
        # frames: msg_id → the frozen SSE frame, built once per wake by the
        # Dispatcher for every connection on the stream (issue #621). An
        # envelope the Hash does not hold (or no Hash: the connect replay, an
        # in-flight buffer flush) is framed here, so the bytes are the same
        # either way.
        def enqueue(envelopes, deadline_ms: @write_deadline_ms, frames: nil)
          written = []
          envelopes.each do |envelope|
            msg_id = envelope.msg_id
            ephemeral = msg_id.negative?
            next if !ephemeral && msg_id <= @last_msg_id_sent.get

            bytes = frames&.[](msg_id) || frame_for(envelope)

            result = @writer.write(self, bytes, deadline_ms: deadline_ms)
            if result == :ok
              @last_msg_id_sent.set(msg_id) unless ephemeral
              @last_write_at = monotonic
              written << envelope
            else
              mark_dead!
              break
            end
          end
          written
        end

        def write_comment(text)
          write_raw(Pgbus::Streams::Envelope.comment(text))
        end

        # The one raw writer: bytes that are already a whole SSE frame (the
        # heartbeat's shared comment, the shutdown sentinel). A successful
        # write counts as activity for the idle reaper; a failed one marks the
        # connection dead. The Heartbeat passes the short fan-out deadline
        # (issue #621); everything else keeps the connection's own.
        def write_raw(bytes, deadline_ms: @write_deadline_ms)
          result = @writer.write(self, bytes, deadline_ms: deadline_ms)
          if result == :ok
            @last_write_at = monotonic
          else
            mark_dead!
          end
          result
        end

        def write_sentinel(bytes)
          write_raw(bytes)
        end

        # The pgbus:connected frame carrying this connection's id. Written by
        # the Dispatcher once the connection is registered and its presence
        # seat joined (issue #521), so a client that acts on the id right
        # after the event finds its row.
        def write_connected
          write_raw(Pgbus::Streams::Envelope.connected(id: @id))
        end

        def idle_for
          monotonic - @last_write_at
        end

        def dead?
          @dead
        end

        def mark_dead!
          @dead = true
        end

        # Idempotent socket close for use by Instance#shutdown! and the
        # heartbeat idle reaper. Wraps the respond_to? / closed? dance
        # so callers don't need to know about StringIO-in-tests vs real
        # Socket-in-prod or about the mark_dead! ordering.
        #
        # Takes the same mutex as IoWriter.write so it can't fire
        # mid-write — otherwise the write loop could hit a half-closed
        # socket and corrupt the `last_msg_id_sent` cursor by marking
        # the connection dead between successful writes. The rescue
        # narrows to IO-related exceptions; unrelated errors (bugs in
        # the fake IO used by tests, nil-dereferences, etc.) should
        # still propagate so the test suite catches them.
        def close
          @mutex.synchronize do
            return if @closed

            @closed = true
            mark_dead!
            return unless @io.respond_to?(:close)

            @io.close unless @io.respond_to?(:closed?) && @io.closed?
          end
        rescue IOError, SystemCallError => e
          Pgbus.logger&.debug { "[Pgbus::Streamer::Connection] close failed: #{e.class}: #{e.message}" }
        end

        private

        def frame_for(envelope)
          Pgbus::Streams::Envelope.message(
            id: envelope.msg_id,
            event: Pgbus::Streams::Envelope.sse_event_for(envelope),
            data: envelope.payload
          )
        end

        def monotonic
          # Qualify ::Process because Pgbus::Process already exists as a
          # namespace for worker/supervisor/consumer and would otherwise
          # shadow the top-level constant here.
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
