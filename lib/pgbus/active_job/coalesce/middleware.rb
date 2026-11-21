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
      # writes passes :report or a callable.
      class Middleware
        def initialize(app, on_failure: :raise)
          Coalesce.validate_on_failure!(on_failure)
          @app = app
          @on_failure = on_failure
        end

        def call(env)
          Pgbus.coalesce(on_failure: @on_failure) { @app.call(env) }
        end
      end
    end
  end
end
