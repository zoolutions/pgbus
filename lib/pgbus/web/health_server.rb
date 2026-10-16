# frozen_string_literal: true

require "socket"

module Pgbus
  module Web
    # Standalone HTTP health server for the supervisor process. When
    # `config.health_port` is set, Supervisor#run starts one of these so a
    # Kubernetes kubelet can probe the supervisor directly — the process that
    # forks and watches workers — without the supervisor having to boot Rails,
    # Puma, or the dashboard.
    #
    # It is intentionally tiny: one accept-loop thread over a TCPServer that
    # reads only the HTTP request line (`GET /readyz HTTP/1.1`), synthesizes a
    # minimal Rack env, and hands it to {HealthApp#call}. All routing and the
    # OK/DEGRADED/STALLED → status mapping live in HealthApp, so the mounted-in-
    # Rails path and this standalone path share exactly one implementation.
    #
    # This is a health probe surface, not a general HTTP server: it speaks just
    # enough HTTP/1.0 to answer a kubelet. It only parses the request line
    # (method + path), ignores headers and body, and closes the connection
    # after each response (no keep-alive).
    class HealthServer
      # Cap the request line so a client can't stream unbounded data at the
      # probe port. A real probe request line is well under 100 bytes.
      MAX_REQUEST_LINE = 8_192
      private_constant :MAX_REQUEST_LINE

      # Per-connection I/O deadline. A client that connects and sends nothing
      # would otherwise park the single accept loop forever and starve every
      # later probe. Kept under HealthProbe::DEFAULT_TIMEOUT (2s) so one wedged
      # connection still can't push the next probe past its own deadline.
      READ_TIMEOUT = 1
      private_constant :READ_TIMEOUT

      # How long #stop waits for the accept loop before killing it.
      JOIN_TIMEOUT = 2
      private_constant :JOIN_TIMEOUT

      STATUS_TEXT = {
        200 => "OK", 404 => "Not Found", 405 => "Method Not Allowed", 503 => "Service Unavailable"
      }.freeze
      private_constant :STATUS_TEXT

      # @return [Integer, nil] the bound port. Equals the configured port, or
      #   the OS-assigned port when the configured port was 0 (used in tests).
      #   nil before #start.
      attr_reader :port

      def initialize(port:, bind: "127.0.0.1", app: nil, logger: nil, read_timeout: READ_TIMEOUT)
        @configured_port = port
        @bind = bind
        @app = app || HealthApp.new
        @logger = logger
        @read_timeout = read_timeout
        @server = nil
        @thread = nil
        @port = nil
      end

      # Bind the port and start the accept loop. Idempotent: a second call
      # while running is a no-op (so a supervisor restart path can't double-bind).
      def start
        return if @server

        @server = TCPServer.new(@bind, @configured_port)
        @port = @server.addr[1]
        # The accept loop, owned by this server: started once (the @server
        # check above) by the supervisor, joined (then killed if it hangs) by
        # #stop.
        @thread = Thread.new { accept_loop } # rubocop:disable ThreadSafety/NewThread
        logger.info { "[Pgbus::Web::HealthServer] listening on #{@bind}:#{@port} (/livez, /readyz)" }
      end

      # Close the listening socket (unblocking the accept loop, which then
      # exits) and join the thread. Safe to call when never started or twice.
      def stop
        server = @server
        @server = nil
        close_socket(server)
        stop_thread(@thread)
        @thread = nil
        @port = nil
      end

      private

      # A loop parked on an already-accepted client never sees the listening
      # socket close, so kill it rather than return "stopped" while it is still
      # running. Thread#kill is asynchronous — join again so #stop only returns
      # once the thread is really gone.
      def stop_thread(thread)
        return unless thread
        return if thread.join(JOIN_TIMEOUT)

        thread.kill
        thread.join(JOIN_TIMEOUT)
      end

      # accept blocks until stop closes the socket, which raises here and ends
      # the loop. Any per-connection error is logged and swallowed so one bad
      # client can never take the probe surface down.
      def accept_loop
        loop do
          server = @server
          break unless server

          client =
            begin
              server.accept
            rescue IOError, Errno::EBADF, Errno::EINVAL
              break # socket closed by #stop
            end

          handle_client(client)
        end
      end

      def handle_client(client)
        # IO#timeout= is core Ruby since 3.2 (below this gem's 3.3 floor) — no
        # require, and there is no "io/timeout" file to require.
        client.timeout = @read_timeout
        method, path = read_request_line(client)
        return unless method

        status, headers, body = @app.call(rack_env(method, path))
        write_response(client, status, headers, body)
      rescue StandardError => e
        logger.warn { "[Pgbus::Web::HealthServer] connection error: #{e.class}: #{e.message}" }
      ensure
        close_socket(client)
      end

      # Read and parse only the first line: "METHOD PATH HTTP/x.y". Returns
      # [method, path] or [nil, nil] when the line is missing/garbage so the
      # caller drops the connection without dispatching. A client that stays
      # silent past the deadline raises IO::TimeoutError (an IOError, so the
      # caller's rescue StandardError logs it and frees the loop).
      def read_request_line(client)
        line = client.gets("\r\n", MAX_REQUEST_LINE)
        return [nil, nil] if line.nil?

        method, path, = line.strip.split(" ", 3)
        return [nil, nil] if method.nil? || path.nil?

        [method, path]
      end

      # Minimal Rack env — HealthApp reads only REQUEST_METHOD and PATH_INFO.
      # Strip any query string from the path so PATH_INFO stays a bare path.
      def rack_env(method, path)
        { "REQUEST_METHOD" => method, "PATH_INFO" => path.split("?", 2).first }
      end

      def write_response(client, status, headers, body)
        reason = STATUS_TEXT.fetch(status, "OK")
        payload = Array(body).join
        lines = ["HTTP/1.0 #{status} #{reason}"]
        headers.each { |k, v| lines << "#{k}: #{v}" }
        lines << "Content-Length: #{payload.bytesize}"
        lines << "Connection: close"
        client.write("#{lines.join("\r\n")}\r\n\r\n#{payload}")
      end

      def close_socket(socket)
        socket&.close
      rescue IOError, Errno::EBADF
        nil
      end

      def logger
        @logger || Pgbus.logger
      end
    end
  end
end
