# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Connection adapter for Falcon's native streaming body path.
      # Wraps a Protocol::HTTP::Body::Writable instead of a raw IO socket.
      #
      # Satisfies the same duck-type interface as Connection so the
      # Dispatcher, Heartbeat, and Instance can use either type
      # interchangeably. Key difference: no IoWriter — writes go
      # directly to body.write which is backed by Thread::Queue and
      # is fiber-safe under Falcon's scheduler.
      class FalconConnection
        attr_reader :id, :stream_name, :io, :mutex, :context
        # The client's per-tab presence seat (?seat=), or nil (issue #515).
        attr_reader :seat
        attr_accessor :presence_member

        # last_msg_id_sent mirrors Connection's cross-thread cursor (issue #321):
        # a Concurrent::AtomicReference so the OutboundPump worker's advance and
        # the dispatcher's read have a happens-before on every Ruby engine. See
        # the note on Connection#last_msg_id_sent.
        def last_msg_id_sent
          @last_msg_id_sent.get
        end

        def initialize(id:, stream_name:, body:, since_id:, write_deadline_ms:, context: nil, seat: nil)
          @id = id
          @stream_name = stream_name
          @body = body
          @io = body
          @last_msg_id_sent = Concurrent::AtomicReference.new(since_id.to_i)
          @write_deadline_ms = write_deadline_ms
          @mutex = Mutex.new
          @dead = false
          @closed = false
          @presence_member = nil
          @created_at = monotonic
          @last_write_at = @created_at
          @context = context
          @seat = seat
        end

        # deadline_ms is accepted for duck-type parity with Connection#enqueue
        # (so the Dispatcher's safe_enqueue can call either class) but ignored:
        # write_to_body is non-blocking under Falcon's fiber reactor, so there
        # is no head-of-line blocking to bound here (issue #315 item 3).
        def enqueue(envelopes, deadline_ms: @write_deadline_ms) # rubocop:disable Lint/UnusedMethodArgument
          written = []
          envelopes.each do |envelope|
            next if envelope.msg_id <= @last_msg_id_sent.get

            bytes = Pgbus::Streams::Envelope.message(
              id: envelope.msg_id,
              event: sse_event_for(envelope),
              data: envelope.payload
            )

            result = write_to_body(bytes)
            if result == :ok
              @last_msg_id_sent.set(envelope.msg_id)
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
          bytes = Pgbus::Streams::Envelope.comment(text)
          result = write_to_body(bytes)
          if result == :ok
            @last_write_at = monotonic
          else
            mark_dead!
          end
          result
        end

        def write_sentinel(bytes)
          write_to_body(bytes)
        end

        def close
          @mutex.synchronize do
            return if @closed

            @closed = true
            mark_dead!
            @body.close_write
          end
        rescue StandardError => e
          Pgbus.logger&.debug { "[Pgbus::Streamer::FalconConnection] close failed: #{e.class}: #{e.message}" }
        end

        def idle_for
          monotonic - @last_write_at
        end

        def dead?
          @dead || @body.closed?
        end

        def mark_dead!
          @dead = true
        end

        private

        # See Connection#sse_event_for. (issue #170)
        def sse_event_for(envelope)
          event = envelope.respond_to?(:event) ? envelope.event : nil
          event && !event.to_s.empty? ? event.to_s : Pgbus::Streams::DEFAULT_SSE_EVENT
        end

        def write_to_body(bytes)
          @mutex.synchronize do
            return :closed if @dead || @body.closed?

            @body.write(bytes)
            :ok
          end
        rescue Protocol::HTTP::Body::Writable::Closed
          :closed
        end

        def monotonic
          ::Process.clock_gettime(::Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end
