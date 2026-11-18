# frozen_string_literal: true

module Pgbus
  module Signals
    # Starts this process's streamer, on a background thread, at its first
    # request when a process signal is registered (issue #674), so a web
    # process that never served an SSE connection still receives signals. The
    # request does not wait for the LISTEN: what it (or anything before it)
    # read before the LISTEN is covered by the on_gap the streamer runs once
    # its first LISTEN is in place. After that it costs one atomic read.
    # Installed by Pgbus::Engine when streams are enabled.
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
