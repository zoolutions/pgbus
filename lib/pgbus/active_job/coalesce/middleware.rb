# frozen_string_literal: true

module Pgbus
  module ActiveJob
    class Coalesce
      # Coalesces a request's enqueues (issue #682): the request runs inside
      # Pgbus.coalesce, and the buffer is sent before the response tuple is
      # returned, never after the response (rack.response_finished), so a
      # crash once the client has its answer cannot lose a job. A job
      # enqueued while a streaming body is iterated goes out at once.
      #
      # Opt-in: insert it after ActionDispatch::Executor, or set
      # config.pgbus.coalesce_requests = true (Pgbus::Engine does the insert).
      # With on_failure: :raise (the default) a job that cannot be sent fails
      # the request; an endpoint that must not answer 500 after committing its
      # writes passes :report or a callable. Inserted after the Executor, the
      # error surfaces above ActionDispatch::ShowExceptions, so the client
      # gets the server's bare 500, not the app's error page.
      class Middleware
        def initialize(app, on_failure: :raise)
          Coalesce.validate_on_failure!(on_failure)
          @app = app
          @on_failure = on_failure
        end

        # A flush that raises (on_failure: :raise) closes the body first, as
        # Rack requires of a response it will never send.
        def call(env)
          response = nil
          Pgbus.coalesce(on_failure: @on_failure) { response = @app.call(env) }
        rescue EnqueueError
          body = response&.at(2)
          body.close if body.respond_to?(:close)
          raise
        end
      end
    end
  end
end
