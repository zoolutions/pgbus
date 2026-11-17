# frozen_string_literal: true

module Pgbus
  module Web
    # The worker-local coordinator that owns SSE connections, one PG LISTEN
    # session, and the dispatch/heartbeat threads. Lazily created on the
    # first SSE connection to a Puma worker (or eagerly in tests). There is
    # exactly one Instance per Puma worker process; the module-level
    # accessors memoise it.
    #
    # This is NOT a Singleton in the GoF sense — tests are free to construct
    # throwaway Instances directly and dependency-inject everything. The
    # `current` / `reset!` helpers exist purely so the Rack StreamApp can
    # share an instance across requests within a worker without passing it
    # through every method call.
    module Streamer
      # Module-level mutex protecting `@current`. Without this, two
      # concurrent first-callers inside a multi-threaded Puma worker
      # can both build and start an Instance, leaking listener,
      # dispatcher, and heartbeat threads plus a PG connection. Same
      # gap would let `reset!` overlap teardown with a fresh
      # replacement instance.
      @current_mutex = Mutex.new

      class << self
        # Returns the worker-local instance, creating it on first call.
        # `factory_opts` are passed to `Instance.new` the first time.
        def current(**factory_opts)
          @current_mutex.synchronize do
            @current ||= Instance.new(**factory_opts).tap(&:start)
          end
        end

        # Explicitly set the current instance — used by tests and by the
        # Puma plugin to inject a pre-built instance.
        def current=(instance)
          @current_mutex.synchronize { @current = instance }
        end

        # A signal registered after this process's streamer started (issue
        # #674): LISTEN on its channel now. No streamer yet: nothing to do, a
        # streamer started later subscribes every registered channel.
        def listen_signal(channel)
          instance = @current_mutex.synchronize { @current }
          instance&.listen_signal(channel)
        end

        def stream_counter
          @current_mutex.synchronize { @current&.stream_counter }
        end

        # Tears down the current instance, if any. Returns nil when there was
        # nothing to reset, otherwise Instance#shutdown!'s list of component
        # names whose threads survived their bounded joins ([] = clean).
        def reset!
          instance = nil
          @current_mutex.synchronize do
            instance = @current
            @current = nil
          end
          instance&.shutdown!
        end
      end
    end
  end
end
