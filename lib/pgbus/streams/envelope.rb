# frozen_string_literal: true

require "json"

module Pgbus
  module Streams
    # Encodes Server-Sent Events frames per https://html.spec.whatwg.org/multipage/server-sent-events.html.
    #
    # Pgbus uses three frame types:
    #   - `message(id:, event:, data:)` — a real broadcast (carries an `id:` so the client
    #     can resume via `Last-Event-ID` on reconnect)
    #   - `comment(text)` — a heartbeat or sentinel that the SSE parser ignores
    #   - `retry_directive(ms)` — tells `EventSource` how long to wait before reconnecting
    #
    # All frames end with `\n\n` (the SSE event terminator). A multiline payload is
    # framed as consecutive `data:` lines (issue #392) — the client rejoins them with
    # `\n`, so delivery is lossless. `\r\n` and lone `\r` are also SSE line terminators,
    # so they become `data:` line breaks too (rejoined as `\n`; SSE cannot represent a
    # raw `\r`). Every payload line carries the `data: ` prefix, so a crafted payload
    # cannot inject forged id:/event: fields. Single-line fields (`event:`, comments)
    # still strip newlines — there a `\r`/`\n` would terminate the field early and
    # permit SSE field injection.
    module Envelope
      NEWLINES = /[\r\n]+/
      DATA_LINE_BREAK = /\r\n|\r|\n/

      RESPONSE_HEADERS = "HTTP/1.1 200 OK\r\n" \
                         "content-type: text/event-stream\r\n" \
                         "cache-control: no-cache, no-transform\r\n" \
                         "x-accel-buffering: no\r\n" \
                         "connection: keep-alive\r\n" \
                         "\r\n"

      def self.message(id:, event:, data:)
        raise ArgumentError, "id is required" if id.nil?
        raise ArgumentError, "event is required" if event.nil? || event.to_s.empty?

        # The event name is a single SSE field line, so newlines are stripped —
        # an unescaped \r/\n would terminate the field early and let a crafted
        # value inject extra SSE fields (a forged id:/data:) into the frame.
        # The payload is framed as one `data:` line per payload line instead:
        # every line carries the `data: ` prefix, which is both spec-correct
        # (the client rejoins with \n) and injection-safe.
        "id: #{id}\nevent: #{strip_newlines(event.to_s)}\n#{data_lines(data.to_s)}\n"
      end

      def self.comment(text)
        ": #{strip_newlines(text.to_s)}\n\n"
      end

      # Emits a `pgbus:connected` frame carrying the server-minted
      # connection id as JSON. Sent once by the streamer's Dispatcher, after
      # the connection is registered and its presence seat joined (issue
      # #521), so the page can read its own connection id and send it back as
      # `X-Pgbus-Connection` on action requests (actor-echo suppression,
      # issue #165). Deliberately omits an `id:` line: this is connection
      # metadata, not a broadcast, and giving it a cursor id would corrupt
      # the client's Last-Event-ID replay position on reconnect.
      def self.connected(id:)
        raise ArgumentError, "id is required" if id.nil? || id.to_s.empty?

        "event: pgbus:connected\ndata: #{JSON.generate({ connectionId: id.to_s })}\n\n"
      end

      def self.retry_directive(milliseconds)
        unless milliseconds.is_a?(Integer) && !milliseconds.negative?
          raise ArgumentError, "retry must be a non-negative integer (got #{milliseconds.inspect})"
        end

        "retry: #{milliseconds}\n\n"
      end

      def self.http_response_headers
        RESPONSE_HEADERS
      end

      def self.strip_newlines(str)
        str.gsub(NEWLINES, "")
      end

      # One `data: <line>\n` per payload line. The -1 limit keeps trailing
      # empty strings, so a payload ending in \n round-trips as an empty
      # final `data:` line (the client's rejoin restores the newline).
      def self.data_lines(str)
        lines = str.split(DATA_LINE_BREAK, -1)
        lines = [""] if lines.empty? # "".split → [] — an empty payload still gets its data: line
        lines.map { |line| "data: #{line}\n" }.join
      end

      private_class_method :strip_newlines, :data_lines
    end
  end
end
