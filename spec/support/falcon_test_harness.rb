# frozen_string_literal: true

require "socket"

module SseTestSupport
  # Falcon's railtie auto-loads on `require "falcon"` and assumes a fully
  # configured Rails environment. The unit test suite loads every file
  # under spec/support/ via spec_helper, but doesn't boot Rails, so we
  # defer the falcon + async requires until an integration spec actually
  # uses the harness. This keeps the unit suite fast and isolated.
  # Called from the example's thread only (the harness is built per example).
  # rubocop:disable-next ThreadSafety/ClassInstanceVariable
  def self.require_falcon_runtime
    return if @falcon_runtime_loaded

    require "falcon"
    require "falcon/server"
    require "protocol/rack/adapter"
    require "async"
    require "async/http/endpoint"
    @falcon_runtime_loaded = true
  end

  # Boots a Falcon::Server on an ephemeral port inside an Async reactor
  # running on a background thread. Same interface as PumaTestHarness
  # (#port, #url, #shutdown) so tests can parameterize over the server.
  #
  # Falcon is fundamentally different from Puma under the hood: it runs
  # on a fiber scheduler (Async), every request gets its own fiber, and
  # IO is non-blocking by default. The rack.hijack path goes through
  # protocol-rack's `Protocol::Rack::Adapter::Generic#unwrap_request`
  # which sets env["rack.hijack?"] = true and env["rack.hijack"] = proc,
  # same as Puma — so StreamApp doesn't need a different code path.
  # This harness exists to PROVE that equivalence end-to-end.
  class FalconTestHarness
    attr_reader :port

    def self.boot(rack_app:, host: "127.0.0.1")
      harness = new(rack_app: rack_app, host: host)
      harness.start
      harness
    end

    def initialize(rack_app:, host:)
      SseTestSupport.require_falcon_runtime
      @rack_app = rack_app
      @host     = host
      @port     = nil
      @thread   = nil
      @stop_pipe_r = nil
      @stop_pipe_w = nil
    end

    def start
      # Discover an ephemeral port by briefly binding a TCP listener,
      # grabbing the port, and closing it. There's a small race where
      # another process could snag the port between our close and
      # Falcon's bind — acceptable for test infrastructure.
      probe = TCPServer.new(@host, 0)
      @port = probe.addr[1]
      probe.close

      # Self-pipe: writing a byte to @stop_pipe_w unblocks the Async
      # reactor and lets us shut down cleanly.
      @stop_pipe_r, @stop_pipe_w = IO.pipe

      server_ready = Thread::Queue.new
      port = @port
      host = @host
      rack_app = @rack_app
      stop_pipe_r = @stop_pipe_r

      @thread = Thread.new do
        Thread.current.abort_on_exception = false
        Sync do
          # Async::HTTP::Endpoint is what Falcon::Server.new expects —
          # it carries a protocol attribute and knows how to bind.
          endpoint = ::Async::HTTP::Endpoint.parse("http://#{host}:#{port}")
          middleware = ::Falcon::Server.middleware(rack_app, verbose: false, cache: false)
          server = ::Falcon::Server.new(middleware, endpoint)
          server_task = server.run
          server_ready << :ready

          # Block the reactor until the stop pipe becomes readable.
          # IO.select inside a fiber-scheduled reactor yields the
          # fiber automatically — Async's scheduler instruments it.
          stop_pipe_r.wait_readable
          server_task.each(&:stop)
        end
      rescue StandardError => e
        # Pass the exception object itself through the queue so the
        # main thread can re-raise with the original class and
        # backtrace instead of a generic "failed to start" string.
        server_ready << e
        warn "[FalconTestHarness] reactor error: #{e.class}: #{e.message}"
        warn e.backtrace.first(5).join("\n")
      end

      result = server_ready.pop
      raise result if result.is_a?(Exception)

      wait_until_accepting(timeout: 5)
      self
    rescue StandardError
      # Any failure on the boot path leaves @thread and the self-pipe
      # orphaned — the `after` hook won't call shutdown because the
      # lazy `let(:harness)` re-raises before ever binding the harness
      # ivar. Tear down manually so the test process doesn't leak
      # threads or file descriptors between examples.
      shutdown
      raise
    end

    def url(path = "/")
      "http://#{@host}:#{@port}#{path}"
    end

    def shutdown
      # Signal the reactor via self-pipe. Falcon's server_task.stop is
      # cooperative and can take time to unwind all fibers, so we only
      # give it a short window before killing the whole thread. The
      # production shutdown path (signal-driven, no harness) has no
      # such hurry. Safe to call multiple times or when boot failed
      # midway — each ivar is guarded independently so partial state
      # from a failed start still gets cleaned up.
      if @thread
        begin
          @stop_pipe_w&.write("\0")
        rescue IOError
          # pipe already closed — fine, we're about to join/kill anyway
        end
        joined = @thread.join(1)
        @thread.kill unless joined
        @thread = nil
      end
      @stop_pipe_w&.close
      @stop_pipe_w = nil
      @stop_pipe_r&.close
      @stop_pipe_r = nil
    end

    private

    def wait_until_accepting(timeout:)
      # Monotonic clock so the wait can't be fooled by NTP corrections
      # or system-time jumps mid-boot. Matches PumaTestHarness and
      # SseTestClient's existing use of CLOCK_MONOTONIC.
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        begin
          sock = TCPSocket.new(@host, @port)
          sock.close
          return
        rescue Errno::ECONNREFUSED, Errno::ECONNRESET
          sleep 0.02
        end
      end
      raise "FalconTestHarness failed to accept connections on port #{@port} within #{timeout}s"
    end
  end
end
