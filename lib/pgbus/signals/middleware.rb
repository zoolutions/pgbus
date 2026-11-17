# frozen_string_literal: true

module Pgbus
  module Signals
    # Starts this process's streamer before its first request runs, when a
    # process signal is registered (issue #674), so a web process that never
    # served an SSE connection still receives signals, and is LISTENing
    # before the request reads anything a signal would invalidate. After the
    # first request (or without a subscriber) it costs a lock and a pid
    # comparison. Installed by Pgbus::Engine when streams are enabled.
    class Middleware
      def initialize(app)
        @app = app
      end

      def call(env)
        Pgbus::Signals.start_listening!
        @app.call(env)
      end
    end
  end
end
