# frozen_string_literal: true

module Pgbus
  module Web
    module Streamer
      # Notices an SSE client that went away the moment its socket says so,
      # instead of on the next heartbeat write (issue #519).
      #
      # The server never reads a hijacked SSE socket after the request, so a
      # closed tab used to stay registered (and present) until a heartbeat
      # write failed, up to streams_heartbeat_interval later. Often longer:
      # the first write after the client's FIN still succeeds. The client
      # sends nothing after its request, so the socket only turns readable
      # when the peer closes or resets it. This thread waits on every live
      # socket with IO.select and probes the readable ones with a
      # non-blocking read. EOF or a reset means the client is gone: the
      # connection is marked dead, closed, and handed to the dispatcher's
      # disconnect path, the same DisconnectMessage the heartbeat posts. That
      # path is idempotent, so a heartbeat racing the watcher still leaves
      # the seat once.
      #
      # Only connections with a real socket are watched: Puma's and Falcon's
      # rack.hijack paths. Falcon's experimental streaming-body path
      # (FalconConnection) exposes no socket and keeps the heartbeat.
      #
      # `interval` bounds how long a new connection waits to join the
      # watched set; a close on a watched socket wakes the select at once,
      # and so does #stop (a self-pipe in the same select).
      class PeerCloseWatcher
        # Stray bytes from a client are read and discarded in chunks of this.
        DRAIN_BYTES = 4096

        # How long #stop waits for the thread. It is woken at once, so this
        # only bounds a reap stuck closing a socket (see Heartbeat#stop).
        STOP_JOIN_SECONDS = 5

        def initialize(registry:, dispatch_queue:, interval: 1.0, logger: Pgbus.logger)
          @registry = registry
          @queue = dispatch_queue
          @interval = interval
          @logger = logger
          @running = false
          @thread = nil
          @wake_reader = nil
          @wake_writer = nil
        end

        def start
          return if @running

          @running = true
          @wake_reader, @wake_writer = IO.pipe
          @thread = Thread.new { run_loop }
          self
        end

        # See Heartbeat#threads (issue #443).
        def threads
          [@thread].compact
        end

        def stop
          return unless @running

          @running = false
          @wake_writer.write_nonblock(".", exception: false)
          @thread&.join(STOP_JOIN_SECONDS)
          @thread = nil
          close_wake_pipe
          self
        end

        # One wait-and-probe pass, at most `interval` long. Public for tests.
        def tick
          by_socket = watched_sockets
          watched = by_socket.keys
          watched << @wake_reader if @wake_reader
          if watched.empty?
            sleep @interval
            return
          end

          ready, = IO.select(watched, nil, nil, @interval)
          Array(ready).each do |socket|
            connection = by_socket[socket]
            reap(connection) if connection && peer_closed?(connection)
          end
        rescue IOError, Errno::EBADF
          # A socket was closed under the select (shutdown, a dispatcher
          # write that failed). The next pass rebuilds the set without it.
          nil
        end

        private

        def run_loop
          while @running
            begin
              tick
            rescue StandardError => e
              @logger.error { "[Pgbus::Streamer::PeerCloseWatcher] tick raised: #{e.class}: #{e.message}" }
              sleep @interval
            end
          end
        end

        # select needs a real IO: Puma's TLS socket and Falcon's hijacked
        # stream answer #to_io with the underlying socket.
        def watched_sockets
          sockets = {}
          @registry.each_connection do |connection|
            next if connection.dead?

            socket = socket_of(connection)
            sockets[socket] = connection if socket
          end
          sockets
        end

        def socket_of(connection)
          io = connection.io
          return nil unless io.respond_to?(:to_io) && io.respond_to?(:read_nonblock)

          socket = io.to_io
          socket if socket.is_a?(::BasicSocket) && !socket.closed?
        end

        def peer_closed?(connection)
          # A nil read is EOF; :wait_readable (TLS, a spurious wake) and stray
          # bytes leave the connection alone.
          connection.io.read_nonblock(DRAIN_BYTES, exception: false).nil?
        rescue IO::WaitReadable
          # A socket wrapper that raises instead of honouring exception:
          # false (EAGAIN is a SystemCallError, so this rescue comes first).
          false
        rescue *peer_gone_errors
          true
        end

        # A TLS socket reports an abrupt EOF as an SSL error, not an IOError:
        # OpenSSL::SSL::SSLError (OpenSSL 3) or Puma::MiniSSL::SSLError.
        # Resolved per call, since either library may load after this file.
        def peer_gone_errors
          [IOError, SystemCallError,
           (OpenSSL::SSL::SSLError if defined?(OpenSSL::SSL::SSLError)),
           (Puma::MiniSSL::SSLError if defined?(Puma::MiniSSL::SSLError))].compact
        end

        def close_wake_pipe
          [@wake_reader, @wake_writer].each { |io| io&.close unless io&.closed? }
          @wake_reader = @wake_writer = nil
        end

        def reap(connection)
          return if connection.dead?

          connection.mark_dead!
          connection.close
          @queue << StreamEventDispatcher::DisconnectMessage.new(connection: connection)
        end
      end
    end
  end
end
