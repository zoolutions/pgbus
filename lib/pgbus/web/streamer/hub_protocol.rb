# frozen_string_literal: true

require "json"

module Pgbus
  module Web
    module Streamer
      # Framing for the master-hub Unix socket (issue #382): 4-byte big-endian
      # payload length + UTF-8 JSON. Unlike the job-side wake pipes (1-byte,
      # lossy-by-design — Process::WakePipe), stream frames can carry an
      # ephemeral broadcast's ONLY copy of its HTML, so the transport is
      # length-prefixed and lossless; drop decisions are made per-message by
      # the MasterHub, never by the wire format.
      #
      # Message shapes (JSON objects; "t" is the discriminator):
      #   worker → master: {t:"sub", q:}   subscribe, synchronous — master acks
      #                    {t:"unsub", q:} unsubscribe, fire-and-forget
      #                    {t:"ssub", c:}  subscribe a process signal channel (issue #674), master sacks
      #   master → worker: {t:"ack", q:}                 sub acknowledged (LISTEN active)
      #                    {t:"wake", q:, p: <String|nil>} durable (p:nil) or ephemeral wake
      #                    {t:"status", healthy: <bool>}   listener health broadcast
      #                    {t:"sack", c:}                  ssub acknowledged (LISTEN active)
      #                    {t:"signal", c:, p: <String|nil>} a process signal, never dropped
      #                    {t:"gap"}                       the hub's LISTEN reconnected: signals may be lost
      #
      # Reads are blocking (each side owns a dedicated reader thread); a short
      # read means the peer died mid-frame and is reported as EOF (nil), never
      # as a truncated message.
      module HubProtocol
        class ProtocolError < StandardError; end

        HEADER_BYTES = 4
        # Generous ceiling for ephemeral HTML payloads; a frame announcing
        # more than this is a corrupt stream or a runaway producer — sever
        # rather than allocate.
        MAX_FRAME_BYTES = 4 * 1024 * 1024

        module_function

        def encode(message)
          json = JSON.generate(message)
          bytes = json.b
          raise ProtocolError, "frame too large: #{bytes.bytesize} bytes (max #{MAX_FRAME_BYTES})" if
            bytes.bytesize > MAX_FRAME_BYTES

          [bytes.bytesize].pack("N") + bytes
        end

        # Returns the decoded Hash, or nil on EOF — clean close, peer death
        # mid-frame, OR a connection reset: an abrupt close can surface as
        # ECONNRESET instead of orderly EOF depending on unread data and
        # platform (Ruby 4.0 reports it deterministically where 3.x saw EOF),
        # and both mean the same thing here: the peer is gone. Raises
        # ProtocolError on an oversized announcement or malformed JSON.
        def read_frame(io)
          header = read_exactly(io, HEADER_BYTES)
          return nil unless header

          length = header.unpack1("N")
          raise ProtocolError, "frame too large: #{length} bytes (max #{MAX_FRAME_BYTES})" if length > MAX_FRAME_BYTES

          body = read_exactly(io, length)
          return nil unless body

          body = body.force_encoding(Encoding::UTF_8)
          raise ProtocolError, "malformed frame: invalid UTF-8" unless body.valid_encoding?

          decoded = JSON.parse(body)
          raise ProtocolError, "malformed frame: expected a JSON object, got #{decoded.class}" unless decoded.is_a?(Hash)

          decoded
        rescue JSON::ParserError => e
          raise ProtocolError, "malformed frame: #{e.message}"
        rescue Errno::ECONNRESET
          nil
        end

        # Blocking read of exactly +count+ bytes; nil on EOF (including EOF
        # partway through — IO#read returns the short tail once, then nil).
        def read_exactly(io, count)
          data = io.read(count)
          return nil if data.nil? || data.bytesize < count

          data
        end
      end
    end
  end
end
