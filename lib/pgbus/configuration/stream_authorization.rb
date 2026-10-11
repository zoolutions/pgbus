# frozen_string_literal: true

module Pgbus
  class Configuration
    # The authorize hook the engine-mounted StreamApp uses (issue #522). Same
    # contract as StreamApp.new(authorize:):
    #
    #   config.streams_authorize = ->(env, stream_name) { current_user_from(env) }
    #
    # A falsy return answers 403, `true` authorizes with no context, any other
    # value becomes the connection context (what visible_to: filters and
    # connection-driven presence read). nil (the default) authorizes every
    # connection with no context. An app that mounts its own
    # StreamApp.new(authorize:) keeps its hook; this one is not consulted.
    module StreamAuthorization
      attr_accessor :streams_authorize

      private

      def validate_stream_authorization!
        return if streams_authorize.nil? || streams_authorize.respond_to?(:call)

        raise Pgbus::ConfigurationError,
              "streams_authorize must respond to #call (a ->(env, stream_name) { } lambda) or be nil"
      end
    end
  end
end
