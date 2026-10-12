# frozen_string_literal: true

require "rack/request"

module Pgbus
  module Web
    # Rack app mounted at /pgbus/streams. Not a Rails controller — this
    # bypasses the entire Rails middleware stack for the streaming path,
    # which avoids several classes of bug (ActionDispatch::Cookies leaking
    # into long-lived requests, ActiveRecord::QueryCache holding the AR
    # connection open, flash modifications after hijack, etc.). It also
    # removes the temptation to use ActionController::Live, which has a
    # well-documented tendency to tie up Puma threads (see puma#1009,
    # puma#938, puma#569).
    #
    # Call flow (happy path, Puma 6.1+ hijack):
    #   1. Parse the signed name from PATH_INFO
    #   2. Verify it via Pgbus::Streams::SignedName (raises → 404)
    #   3. Run the authorize hook: authorize:, else config.streams_authorize
    #      (falsy → 403)
    #   4. Parse the cursor from ?since= or Last-Event-ID
    #   5. Check streams_max_connections per worker (over → 503)
    #   6. Check rack.hijack? (missing → 501)
    #   7. Hijack, write the HTTP response line + SSE headers + opening
    #      comment directly to the socket
    #   8. Build a Connection, hand to Streamer.current.register(...). The
    #      Dispatcher writes the pgbus:connected frame once the connection
    #      is registered and its presence seat joined (issue #521)
    #   9. Return [-1, {}, []] (Puma's async protocol)
    #
    # On errors, returns a normal [status, headers, body] response so
    # Rack / the reverse proxy can log and retry. The whole thing is
    # designed so a reviewer can read call/2 top-to-bottom and see the
    # full request lifecycle.
    class StreamApp
      PATH_PREFIX = "/pgbus/streams"
      # The client's per-tab presence seat (?seat=, issue #515): an opaque id
      # minted by <pgbus-stream-source presence>. Anything else is dropped.
      SEAT_FORMAT = /\A[A-Za-z0-9_-]{1,64}\z/
      # EventSource reconnect delay handed to the streams_test_mode stub (24h).
      TEST_MODE_RETRY_MS = 86_400_000
      private_constant :PATH_PREFIX

      # Used when neither authorize: nor config.streams_authorize is set.
      ALLOW_ALL = ->(_env, _stream_name) { true }
      private_constant :ALLOW_ALL

      def initialize(streamer: nil, config: nil, logger: nil, authorize: nil)
        @streamer_override = streamer
        @config_override   = config
        @logger_override   = logger
        @authorize         = authorize
      end

      def call(env)
        request = Rack::Request.new(env)
        return not_found("only GET is supported") unless request.get?

        signed_name = extract_signed_name(env)
        return not_found("missing signed stream name") if signed_name.nil?

        stream_name = verify!(signed_name)
        return not_found("invalid signed stream name") if stream_name.nil?

        authorize_result = authorize_hook.call(env, stream_name)
        return forbidden unless authorize_result

        # If authorize returned a non-boolean value (e.g. a User model),
        # treat it as the connection context for audience filtering.
        # A bare `true` means "authorized but no context" — filters that
        # depend on a context will fail-closed on these connections.
        context = authorize_result unless authorize_result == true

        cursor = parse_cursor(env, request)
        return bad_request("invalid cursor: #{cursor}") if cursor.is_a?(String)

        seat = parse_seat(request)

        return test_mode_stub if config.streams_test_mode

        return over_capacity if streamer.registry.size >= config.streams_max_connections

        if config.streams_falcon_streaming_body
          streaming_body_response(stream_name: stream_name, since_id: cursor, context: context, seat: seat)
        elsif env["rack.hijack?"]
          hijack_and_register(env, stream_name: stream_name, since_id: cursor, context: context, seat: seat)
        else
          unsupported_server
        end
      rescue StandardError => e
        # Report, don't just log: a broken streamer config (e.g. bad
        # streams_connection_options) 500s EVERY stream request on this
        # worker — logger-only made that invisible to APM (issue #352).
        ErrorReporter.report(e, { action: "stream_connect" }, config: config)
        logger.debug { e.backtrace&.join("\n") }
        server_error
      end

      private

      def extract_signed_name(env)
        path = env["PATH_INFO"].to_s
        # PATH_INFO when mounted is relative to the mount point, so it's
        # just "/<signed_name>". When not mounted (e.g. tests calling call
        # directly) it's "/pgbus/streams/<signed_name>". Handle both.
        path = path.sub(PATH_PREFIX, "")
        path = path[1..] if path.start_with?("/") # strip leading slash
        return nil if path.nil? || path.empty?

        path
      end

      def verify!(token)
        Pgbus::Streams::SignedName.verify!(token)
      rescue Pgbus::Streams::SignedName::InvalidSignedName,
             Pgbus::Streams::SignedName::MissingSecret
        nil
      end

      def parse_cursor(env, request)
        Pgbus::Streams::Cursor.parse(
          query_since: request.params["since"],
          last_event_id: env["HTTP_LAST_EVENT_ID"]
        )
      rescue Pgbus::Streams::Cursor::InvalidCursor => e
        e.message
      end

      def parse_seat(request)
        seat = request.params["seat"]
        seat if seat.is_a?(String) && seat.ascii_only? && SEAT_FORMAT.match?(seat)
      end

      def hijack_and_register(env, stream_name:, since_id:, context: nil, seat: nil)
        # Rack hijack is a one-time transition per the Rack spec. Call
        # once, use the return value, and fall back to the side-effect
        # `rack.hijack_io` variable only if the return was nil (some
        # older Rack versions populate the side-effect var instead of
        # returning the IO).
        hijack_result = env["rack.hijack"].call
        io = hijack_result || env["rack.hijack_io"]

        connection_id = SecureRandom.hex(8)
        write_headers(io, stream_name: stream_name, since_id: since_id)

        connection = Pgbus::Web::Streamer::Connection.new(
          id: connection_id,
          stream_name: stream_name,
          io: io,
          since_id: since_id,
          writer: Pgbus::Web::Streamer::IoWriter,
          write_deadline_ms: config.streams_write_deadline_ms,
          context: context,
          seat: seat
        )
        streamer.register(connection)

        [-1, {}, []]
      end

      def streaming_body_response(stream_name:, since_id:, context: nil, seat: nil)
        require "protocol/http/body/writable" unless defined?(::Protocol::HTTP::Body::Writable)
        require_relative "streamer/falcon_connection" unless defined?(Pgbus::Web::Streamer::FalconConnection)

        body = ::Protocol::HTTP::Body::Writable.new
        connection_id = SecureRandom.hex(8)

        body.write(Pgbus::Streams::Envelope.retry_directive(2_000))
        body.write(Pgbus::Streams::Envelope.comment(
                     "pgbus stream open since_id=#{since_id} stream=#{stream_name}"
                   ))

        connection = Pgbus::Web::Streamer::FalconConnection.new(
          id: connection_id,
          stream_name: stream_name,
          body: body,
          since_id: since_id,
          write_deadline_ms: config.streams_write_deadline_ms,
          context: context,
          seat: seat
        )
        streamer.register(connection)

        [200, sse_headers, body]
      end

      def sse_headers
        {
          "content-type" => "text/event-stream",
          "cache-control" => "no-cache, no-transform",
          "x-accel-buffering" => "no"
        }
      end

      def write_headers(io, stream_name:, since_id:)
        io.write(Pgbus::Streams::Envelope.http_response_headers)
        io.write(Pgbus::Streams::Envelope.retry_directive(2_000))
        io.write(Pgbus::Streams::Envelope.comment("pgbus stream open since_id=#{since_id} stream=#{stream_name}"))
      end

      # An explicit authorize: wins; otherwise config.streams_authorize, read
      # per request so the engine's mount (built when routes load) sees a hook
      # set in an initializer that runs later (issue #522). Only nil means
      # "unset": any other non-callable (false included) must reach #call and
      # fail closed with a 500, never fall through to ALLOW_ALL.
      def authorize_hook
        return @authorize unless @authorize.nil?

        hook = config.streams_authorize
        hook.nil? ? ALLOW_ALL : hook
      end

      def streamer
        @streamer_override || Pgbus::Web::Streamer.current
      end

      def config
        @config_override || Pgbus.configuration
      end

      def logger
        @logger_override || Pgbus.logger
      end

      # Error responses — plain text, no body parsing, no Rails. The client
      # is EventSource which doesn't render error bodies, but we include
      # short text for operator debugging.

      def not_found(reason)
        [404, { "content-type" => "text/plain" }, ["pgbus: #{reason}"]]
      end

      def forbidden
        [403, { "content-type" => "text/plain" }, ["pgbus: forbidden"]]
      end

      def bad_request(reason)
        [400, { "content-type" => "text/plain" }, ["pgbus: #{reason}"]]
      end

      def over_capacity
        [503, { "content-type" => "text/plain", "retry-after" => "2" }, ["pgbus: streamer over capacity"]]
      end

      def unsupported_server
        [501,
         { "content-type" => "text/plain" },
         ["pgbus streams require Puma 6.1+ or Falcon — current Rack server does not provide rack.hijack"]]
      end

      def server_error
        [500, { "content-type" => "text/plain" }, ["pgbus: internal error"]]
      end

      # A closed SSE response makes EventSource reconnect every ~3s by default.
      # In a browser-driven test that storm keeps hitting the endpoint for the
      # whole example, so a reconnect landing after the harness has turned
      # streams_test_mode back off (teardown ordering) would start a LIVE
      # streamer inside the test process (issue #443). Telling the browser to
      # wait a day before retrying closes that window.
      def test_mode_stub
        retry_line = Pgbus::Streams::Envelope.retry_directive(TEST_MODE_RETRY_MS)
        body = "#{retry_line}: pgbus test mode — connection accepted, no polling\n\n"
        [200, sse_headers, [body]]
      end
    end
  end
end
