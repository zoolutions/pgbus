# frozen_string_literal: true

module Pgbus
  class Client
    # Fire-and-forget PG NOTIFY for ephemeral stream broadcasts. No PGMQ
    # queue is created — the payload travels via the Postgres NOTIFY channel
    # only, matching the channel naming convention that PGMQ's trigger uses:
    #   pgmq.q_<full_queue_name>.INSERT
    #
    # Subscribers already LISTEN on this channel via the Streamer's Listener.
    # When a subscriber is connected, the StreamEventDispatcher receives the
    # NOTIFY and fans out the payload. When no subscriber is connected,
    # the NOTIFY is silently discarded by Postgres — no queue, no storage,
    # no orphan tables.
    #
    # The payload is JSON-serialized into the NOTIFY's optional payload
    # parameter. Postgres caps NOTIFY payloads at < 8000 bytes; oversized
    # payloads raise a typed Pgbus::Streams::PayloadTooLarge here, at the
    # call site, instead of surfacing as a misleading
    # PGMQ::Errors::ConnectionError ("payload string too long") from deep
    # inside the driver (issue #391). Callers needing large payloads should
    # use durable mode (which inserts into PGMQ).
    module NotifyStream
      # PostgreSQL rejects NOTIFY payloads of 8000 bytes or more
      # ("payload string too long"), so 7999 is the largest deliverable
      # payload.
      NOTIFY_PAYLOAD_LIMIT_BYTES = 7999

      def notify_stream(stream_name, payload)
        full_name = config.queue_name(stream_name)
        sanitized = QueueNameValidator.sanitize!(full_name)
        channel = "pgmq.q_#{sanitized}.INSERT"
        json = payload.is_a?(String) ? payload : JSON.generate(payload)
        validate_notify_payload_size!(stream_name, json)

        Instrumentation.instrument("pgbus.stream.notify", stream: stream_name, bytes: json.bytesize) do
          with_stale_connection_retry do
            synchronized do
              @pgmq.__send__(:with_connection) do |conn|
                conn.exec_params("SELECT pg_notify($1, $2)", [channel, json])
              end
            end
          end
        end
      end

      # Fire-and-forget NOTIFY for a process signal (issue #674) on its channel,
      # `<queue_prefix>_signal_<name>` (Pgbus::Signals validates the name and
      # the payload: a String within the cap, or nil for none).
      def notify_signal(name, payload = nil)
        channel = config.queue_name("signal_#{name}")
        Instrumentation.instrument("pgbus.signal.notify", signal: name.to_s, bytes: payload.to_s.bytesize) do
          with_stale_connection_retry do
            synchronized do
              @pgmq.__send__(:with_connection) do |conn|
                conn.exec_params("SELECT pg_notify($1, $2)", [channel, payload])
              end
            end
          end
        end
      end

      private

      def validate_notify_payload_size!(stream_name, json)
        return if json.bytesize <= NOTIFY_PAYLOAD_LIMIT_BYTES

        raise Pgbus::Streams::PayloadTooLarge,
              "Ephemeral broadcast on stream #{stream_name.inspect} is #{json.bytesize} bytes; " \
              "PostgreSQL caps NOTIFY payloads at #{NOTIFY_PAYLOAD_LIMIT_BYTES} bytes. " \
              "Use durable mode for large payloads (payload stored in PGMQ, NOTIFY as wake) — " \
              "e.g. broadcast(..., durable: true), a streams_durable_patterns match, or " \
              "streams_default_broadcast_mode = :durable."
      end
    end
  end
end
